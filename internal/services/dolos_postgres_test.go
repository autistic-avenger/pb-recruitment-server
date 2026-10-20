package services

import (
	"app/internal/s3"
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func dolosPostgresDB(t *testing.T) (*sql.DB, *pgx.ConnConfig) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin := stdlib.OpenDB(*cfg)
	schema := "dolos_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec("CREATE SCHEMA " + schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*cfg)
	t.Cleanup(func() {
		db.Close()
		if _, err := admin.Exec("DROP SCHEMA " + schema + " CASCADE"); err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	paths, err := filepath.Glob("../migrations/*.up.sql")
	if err != nil || len(paths) == 0 {
		t.Fatalf("migration files: %v", err)
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(string(data)); err != nil {
			t.Fatalf("%s: %v", filepath.Base(path), err)
		}
	}
	return db, cfg
}

type dolosCommitBarrier struct {
	committed, release chan struct{}
	once               sync.Once
}

func (b *dolosCommitBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}

func (b *dolosCommitBarrier) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if data.Err == nil && data.CommandTag.String() == "COMMIT" {
		b.once.Do(func() {
			// Pause after PostgreSQL releases the generation lock, before the worker resumes.
			close(b.committed)
			select {
			case <-b.release:
			case <-ctx.Done():
			}
		})
	}
}

func TestDolosStaleSingletonCannotOverwriteNewUploadPostgres(t *testing.T) {
	db, cfg := dolosPostgresDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO contests(id,name,registration_start_time,registration_end_time,start_time,end_time)
		VALUES('contest','Test',0,0,0,1);
		INSERT INTO problems(id,contest_id,name,score,type) VALUES('problem','contest','Test',10,'code');
		INSERT INTO submissions(id,user_id,contest_id,problem_id,type,language,status,created_at)
		VALUES('source-a','alice','contest','problem','code','python','accepted',1);
	`); err != nil {
		t.Fatal(err)
	}
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, base64.StdEncoding.EncodeToString([]byte("print('synthetic')")))
	}))
	t.Cleanup(store.Close)
	for key, value := range map[string]string{
		"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "AWS_SESSION_TOKEN": "",
		"AWS_REGION": "us-east-1", "AWS_EC2_METADATA_DISABLED": "true",
		"AWS_CONFIG_FILE": os.DevNull, "AWS_SHARED_CREDENTIALS_FILE": os.DevNull,
		"AWS_ENDPOINT_URL_S3": store.URL, "S3_SUBMISSIONS_BUCKET": "test",
	} {
		t.Setenv(key, value)
	}
	accepted, releaseUpload := make(chan struct{}), make(chan struct{})
	var posts atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		if posts.Add(1) == 1 {
			close(accepted)
		}
		select {
		case <-releaseUpload:
		case <-ctx.Done():
			return
		}
		fmt.Fprint(w, `{"id":"new-report","status":"finished"}`)
	}))
	t.Cleanup(provider.Close)
	d := &DolosService{db: db, s3: s3.NewS3Client(), client: provider.Client(), baseURL: provider.URL}
	barrier := &dolosCommitBarrier{committed: make(chan struct{}), release: make(chan struct{})}
	cfg.Tracer = barrier
	oldDB := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { oldDB.Close() })
	old := *d
	old.db = oldDB
	oldDone := make(chan error, 1)
	go func() { oldDone <- old.processContest(ctx, "contest", "Test", 1) }()
	select {
	case <-barrier.committed:
	case <-ctx.Done():
		t.Fatal("old worker did not initialize its generation")
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE contests SET end_time=2 WHERE id='contest';
		INSERT INTO submissions(id,user_id,contest_id,problem_id,type,language,status,created_at)
		VALUES('source-b','bob','contest','problem','code','python','accepted',2);
	`); err != nil {
		t.Fatal(err)
	}
	newDone := make(chan error, 1)
	go func() { newDone <- d.processContest(ctx, "contest", "Test", 2) }()
	select {
	case <-accepted:
	case <-ctx.Done():
		t.Fatal("new worker did not upload its generation")
	}
	close(barrier.release)
	if err := <-oldDone; err != nil {
		t.Fatal(err)
	}
	close(releaseUpload)
	if err := <-newDone; err != nil {
		t.Fatal(err)
	}
	var status, reportID string
	if err := db.QueryRowContext(ctx, `SELECT status,COALESCE(dolos_report_id,'') FROM contest_plagiarism_reports WHERE contest_id='contest'`).Scan(&status, &reportID); err != nil {
		t.Fatal(err)
	}
	if status != "finished" || reportID != "new-report" || posts.Load() != 1 {
		t.Fatalf("status=%s report=%q uploads=%d", status, reportID, posts.Load())
	}
	report, err := d.GetReport(ctx, "contest")
	if err != nil || report.Status != "finished" {
		t.Fatalf("report=%+v: %v", report, err)
	}
}
