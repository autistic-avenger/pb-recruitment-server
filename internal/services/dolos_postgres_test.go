package services

import (
	"app/internal/common"
	"app/internal/s3"
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
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
	old := &DolosService{
		db:         oldDB,
		s3:         d.s3,
		client:     d.client,
		baseURL:    d.baseURL,
		pairsCache: make(map[string]cachedReportPairs),
	}
	oldDone := make(chan error, 1)
	go func() { oldDone <- old.processContest(ctx, "contest", 1) }()
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
	go func() { newDone <- d.processContest(ctx, "contest", 2) }()
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

func TestDolosMissingUserLookupClearsUserIDPostgres(t *testing.T) {
	db, _ := dolosPostgresDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO contests(id, name, registration_start_time, registration_end_time, start_time, end_time)
		VALUES('contest-user', 'Test Contest', 0, 0, 0, 100);
		INSERT INTO problems(id, contest_id, name, score, type)
		VALUES('problem1', 'contest-user', 'Problem 1', 10, 'code');
		INSERT INTO users(id, name, email, role)
		VALUES('usr-real', 'Real User', 'real@example.com', 'participant');
		INSERT INTO submissions(id, user_id, contest_id, problem_id, type, language, status, created_at)
		VALUES('sub-found', 'usr-real', 'contest-user', 'problem1', 'code', 'python', 'accepted', 10);
		INSERT INTO submissions(id, user_id, contest_id, problem_id, type, language, status, created_at)
		VALUES('sub-missing', 'usr-ghost', 'contest-user', 'problem1', 'code', 'python', 'accepted', 20);
		INSERT INTO contest_plagiarism_runs(contest_id, analyzed_end_time, source_count, latest_submission_at, source_signature)
		VALUES('contest-user', 100, 2, 20, 'sig123');
		INSERT INTO contest_plagiarism_reports(contest_id, problem_id, language, status, dolos_report_id, report_url)
		VALUES('contest-user', 'problem1', 'python', 'finished', 'rep-users', 'https://dolos.example.com/rep-users');
	`); err != nil {
		t.Fatal(err)
	}

	csvData := `id,leftfileid,rightfileid,similarity,totaloverlap,longestfragment,leftfilepath,rightfilepath
1,1,2,0.85,100,50,sub-found/problem1.py,sub-missing/problem1.py
`
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/reports/rep-users/data/pairs.csv" {
			w.Header().Set("Content-Type", "text/csv")
			w.Write([]byte(csvData))
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(provider.Close)

	svc := &DolosService{
		db:         db,
		client:     provider.Client(),
		baseURL:    provider.URL,
		pairsCache: make(map[string]cachedReportPairs),
	}

	pairs, err := svc.GetPairs(ctx, "contest-user", 0)
	if err != nil {
		t.Fatalf("GetPairs failed: %v", err)
	}
	if len(pairs) != 1 {
		t.Fatalf("expected 1 pair, got %d", len(pairs))
	}
	p := pairs[0]
	if p.LeftUserID != "usr-real" || p.LeftName != "Real User" {
		t.Errorf("expected left user 'usr-real' ('Real User'), got ID=%q Name=%q", p.LeftUserID, p.LeftName)
	}
	if p.RightUserID != "" || p.RightName != "" {
		t.Errorf("expected right user to be cleared, got ID=%q Name=%q", p.RightUserID, p.RightName)
	}

	// Test pagination bounds and large integer overflow protection (Finding 5)
	overflowPage := 9223372036854775807
	pagedPairs, err := svc.GetPairs(ctx, "contest-user", overflowPage)
	if err != nil {
		t.Fatalf("GetPairs with large page integer failed: %v", err)
	}
	if len(pagedPairs) != 0 {
		t.Errorf("expected empty pairs for page %d, got %d", overflowPage, len(pagedPairs))
	}
}

func TestDolosProviderPurgedReportMarksFailedPostgres(t *testing.T) {
	db, _ := dolosPostgresDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO contests(id, name, registration_start_time, registration_end_time, start_time, end_time)
		VALUES('contest-purged', 'Purged Contest', 0, 0, 0, 100);
		INSERT INTO problems(id, contest_id, name, score, type)
		VALUES('problem1', 'contest-purged', 'Problem 1', 10, 'code');
		INSERT INTO contest_plagiarism_runs(contest_id, analyzed_end_time, source_count, latest_submission_at, source_signature)
		VALUES('contest-purged', 100, 0, 0, 'sig123');
		INSERT INTO contest_plagiarism_reports(contest_id, problem_id, language, status, dolos_report_id, report_url)
		VALUES('contest-purged', 'problem1', 'python', 'finished', 'rep-deleted', 'https://dolos.example.com/rep-deleted');
	`); err != nil {
		t.Fatal(err)
	}

	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	t.Cleanup(provider.Close)

	svc := &DolosService{
		db:         db,
		client:     provider.Client(),
		baseURL:    provider.URL,
		pairsCache: make(map[string]cachedReportPairs),
	}

	_, err := svc.GetPairs(ctx, "contest-purged", 0)
	if !errors.Is(err, common.ErrReportNotFound) {
		t.Fatalf("expected ErrReportNotFound, got %v", err)
	}

	var status, errMsg string
	if err := db.QueryRowContext(ctx, `SELECT status, COALESCE(error, '') FROM contest_plagiarism_reports WHERE contest_id='contest-purged' AND problem_id='problem1'`).Scan(&status, &errMsg); err != nil {
		t.Fatal(err)
	}
	if status != "purged" {
		t.Fatalf("expected status 'purged', got %q", status)
	}
	if !strings.Contains(errMsg, "purged") {
		t.Fatalf("expected error message to mention purged, got %q", errMsg)
	}

	rep, err := svc.GetReport(ctx, "contest-purged")
	if err != nil {
		t.Fatalf("GetReport failed: %v", err)
	}
	if rep.Status != "failed" {
		t.Fatalf("expected overall status 'failed' after report was purged, got %q", rep.Status)
	}
}
