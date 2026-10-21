package services

import (
	"app/internal/common"
	"app/internal/models/dto"
	"app/internal/s3"
	"app/internal/stores"
	"context"
	"crypto/rand"
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

func TestDolosStaleWorkersCannotClaimNewGenerationPostgres(t *testing.T) {
	for _, tc := range []struct {
		name     string
		oldPair  bool
		language string
	}{
		{name: "singleton", language: "python"},
		{name: "same language", oldPair: true, language: "python"},
		{name: "removed language", oldPair: true, language: "cpp"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, cfg := dolosPostgresDB(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if _, err := db.ExecContext(ctx, `
    INSERT INTO contests(id,name,registration_start_time,registration_end_time,start_time,end_time)
    VALUES('contest','Test',0,0,0,1);
    INSERT INTO problems(id,contest_id,name,score,type) VALUES('problem','contest','Test',10,'code');
    INSERT INTO submissions(id,user_id,contest_id,problem_id,type,language,status,created_at)
    VALUES('old-a','alice','contest','problem','code','python','accepted',1);
   `); err != nil {
				t.Fatal(err)
			}
			if tc.oldPair {
				if _, err := db.ExecContext(ctx, `INSERT INTO submissions(id,user_id,contest_id,problem_id,type,language,status,created_at)
     VALUES('old-b','bob','contest','problem','code','python','accepted',2)`); err != nil {
					t.Fatal(err)
				}
			}
			store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprint(w, base64.StdEncoding.EncodeToString([]byte("synthetic source")))
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
			var posts atomic.Int32
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					posts.Add(1)
					fmt.Fprint(w, `{"id":"new-report","status":"finished"}`)
					return
				}
				ext := languageExtension(tc.language)
				fmt.Fprintf(w, "id,leftFilePath,rightFilePath,similarity,totalOverlap,longestFragment\n1,new-a/problem%s,new-b/problem%s,0.9,100,50\n", ext, ext)
			}))
			t.Cleanup(provider.Close)
			service := &DolosService{db: db, s3: s3.NewS3Client(), client: provider.Client(), baseURL: provider.URL}
			oldBarrier := &dolosCommitBarrier{committed: make(chan struct{}), release: make(chan struct{})}
			oldCfg := cfg.Copy()
			oldCfg.Tracer = oldBarrier
			oldDB := stdlib.OpenDB(*oldCfg)
			t.Cleanup(func() { oldDB.Close() })
			old := &DolosService{db: oldDB, s3: service.s3, client: service.client, baseURL: service.baseURL}
			oldDone := make(chan error, 1)
			go func() { oldDone <- old.processContest(ctx, "contest", 1) }()
			select {
			case <-oldBarrier.committed:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if _, err := db.ExecContext(ctx, `UPDATE contests SET end_time=2 WHERE id='contest';`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO submissions(id,user_id,contest_id,problem_id,type,language,status,created_at)
    VALUES('new-a','alice','contest','problem','code',$1,'accepted',3),
          ('new-b','bob','contest','problem','code',$1,'accepted',4)`, tc.language); err != nil {
				t.Fatal(err)
			}
			currentBarrier := &dolosCommitBarrier{committed: make(chan struct{}), release: make(chan struct{})}
			currentCfg := cfg.Copy()
			currentCfg.Tracer = currentBarrier
			currentDB := stdlib.OpenDB(*currentCfg)
			t.Cleanup(func() { currentDB.Close() })
			current := &DolosService{db: currentDB, s3: service.s3, client: service.client, baseURL: service.baseURL}
			currentDone := make(chan error, 1)
			go func() { currentDone <- current.processContest(ctx, "contest", 2) }()
			select {
			case <-currentBarrier.committed:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			close(oldBarrier.release)
			if err := <-oldDone; err != nil {
				t.Fatal(err)
			}
			var count, attempts int
			var status string
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM contest_plagiarism_reports WHERE contest_id='contest'`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, `SELECT status,attempts FROM contest_plagiarism_reports WHERE contest_id='contest' AND language=$1`, tc.language).Scan(&status, &attempts); err != nil {
				t.Fatal(err)
			}
			if count != 1 || status != "pending" || attempts != 0 || posts.Load() != 0 {
				t.Fatalf("stale worker changed current generation: rows=%d status=%s attempts=%d uploads=%d", count, status, attempts, posts.Load())
			}
			close(currentBarrier.release)
			if err := <-currentDone; err != nil {
				t.Fatal(err)
			}
			report, err := service.GetReport(ctx, "contest")
			if err != nil || report.Status != "finished" || posts.Load() != 1 {
				t.Fatalf("report=%+v err=%v uploads=%d", report, err, posts.Load())
			}
			pairs, total, truncated, err := service.GetPairsWithMeta(ctx, "contest", 0)
			if err != nil || len(pairs) != 1 || total != 1 || truncated {
				t.Fatalf("pairs=%v total=%d truncated=%v err=%v", pairs, total, truncated, err)
			}
		})
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
		INSERT INTO users(id, name, email, usn, current_year, department)
		VALUES('usr-real', 'Real User', 'real@example.com', 'USN001', 2024, 'CS');
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

	pairs, total, truncated, err := svc.GetPairsWithMeta(ctx, "contest-user", 0)
	if err != nil {
		t.Fatalf("GetPairs failed: %v", err)
	}
	if len(pairs) != 1 || total != 1 || truncated {
		t.Fatalf("expected 1 pair without truncation, got pairs=%d total=%d truncated=%v", len(pairs), total, truncated)
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
	pagedPairs, pagedTotal, pagedTruncated, err := svc.GetPairsWithMeta(ctx, "contest-user", overflowPage)
	if err != nil {
		t.Fatalf("GetPairs with large page integer failed: %v", err)
	}
	if len(pagedPairs) != 0 || pagedTotal != 1 || pagedTruncated {
		t.Errorf("unexpected page %d: pairs=%d total=%d truncated=%v", overflowPage, len(pagedPairs), pagedTotal, pagedTruncated)
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

	_, _, _, err := svc.GetPairsWithMeta(ctx, "contest-purged", 0)
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

// Pause after the report lock so deletion competes with the review's foreign-key check.
type dolosReviewBarrier struct {
	locked, release chan struct{}
	once            sync.Once
}
type dolosReviewQueryKey struct{}

func (b *dolosReviewBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, dolosReviewQueryKey{}, data.SQL)
}
func (b *dolosReviewBarrier) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	query, _ := ctx.Value(dolosReviewQueryKey{}).(string)
	if data.Err == nil && strings.Contains(query, "FOR UPDATE OF p") {
		b.once.Do(func() {
			close(b.locked)
			select {
			case <-b.release:
			case <-ctx.Done():
			}
		})
	}
}

func TestDolosReviewAndContestDeletionPostgres(t *testing.T) {
	db, cfg := dolosPostgresDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `
  INSERT INTO contests(id,name,registration_start_time,registration_end_time,start_time,end_time)
  VALUES('contest','Test',0,0,0,1);
  INSERT INTO problems(id,contest_id,name,score,type) VALUES('problem','contest','Test',10,'code');
  INSERT INTO submissions(id,user_id,contest_id,problem_id,type,language,status,created_at)
  VALUES('source-a','alice','contest','problem','code','python','accepted',1),
        ('source-b','bob','contest','problem','code','python','accepted',1);
  INSERT INTO contest_plagiarism_runs(contest_id,analyzed_end_time,source_count,latest_submission_at,source_signature)
  VALUES('contest',1,2,1,'signature');
  INSERT INTO contest_plagiarism_reports(contest_id,problem_id,language,status,dolos_report_id)
  VALUES('contest','problem','python','finished','report');
 `); err != nil {
		t.Fatal(err)
	}
	barrier := &dolosReviewBarrier{locked: make(chan struct{}), release: make(chan struct{})}
	reviewCfg := cfg.Copy()
	reviewCfg.Tracer = barrier
	reviewDB := stdlib.OpenDB(*reviewCfg)
	t.Cleanup(func() { reviewDB.Close() })
	review := NewDolosService(reviewDB, nil)
	key := pairKey("source-a/problem.py", "source-b/problem.py")
	review.setCachedPairs("report", []dto.PlagiarismPair{{Key: key}}, false)
	deleteCfg := cfg.Copy()
	deleteCfg.RuntimeParams["application_name"] = "dolos_delete_" + cfg.RuntimeParams["search_path"]
	deleteDB := stdlib.OpenDB(*deleteCfg)
	t.Cleanup(func() { deleteDB.Close() })
	reviewDone, deleteDone := make(chan error, 1), make(chan error, 1)
	go func() { reviewDone <- review.ReviewPair(ctx, "contest", key, "report", "confirmed", "review") }()
	select {
	case <-barrier.locked:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	go func() { deleteDone <- stores.NewContestStore(deleteDB).DeleteContest(ctx, "contest") }()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')`, deleteCfg.RuntimeParams["application_name"]).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(barrier.release)
	if err := <-reviewDone; err != nil {
		t.Fatalf("review failed: %v", err)
	}
	if err := <-deleteDone; err != nil {
		t.Fatalf("deletion failed: %v", err)
	}
	var remaining int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM contests WHERE id='contest'`).Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("contest not deleted: count=%d err=%v", remaining, err)
	}
}

func TestDolosLateCSVNotFoundPreservesAmbiguousUploadPostgres(t *testing.T) {
	db, _ := dolosPostgresDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `
  INSERT INTO contests(id, name, registration_start_time, registration_end_time, start_time, end_time)
  VALUES('contest', 'Synthetic', 0, 0, 0, 1);
  INSERT INTO problems(id, contest_id, name, score, type) VALUES('problem', 'contest', 'Synthetic', 1, 'code');
  INSERT INTO contest_plagiarism_reports(contest_id, problem_id, language, status, dolos_report_id)
  VALUES('contest', 'problem', 'python', 'finished', 'old-report');
 `); err != nil {
		t.Fatal(err)
	}
	arrived, release := make(chan struct{}), make(chan struct{})
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(arrived)
		select {
		case <-release:
			http.NotFound(w, r)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(provider.Close)
	svc := &DolosService{db: db, client: provider.Client(), baseURL: provider.URL}
	done := make(chan error, 1)
	go func() { _, err := svc.openReportPairsCSV(ctx, "old-report"); done <- err }()
	select {
	case <-arrived:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// A replacement upload failed after provider acceptance while the old CSV request was in flight.
	const ambiguousError = "Dolos upload outcome is ambiguous: synthetic connection loss"
	if _, err := db.ExecContext(ctx, `UPDATE contest_plagiarism_reports SET status='failed', error=$1 WHERE contest_id='contest'`, ambiguousError); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; !errors.Is(err, common.ErrReportNotFound) {
		t.Fatalf("late CSV response: %v", err)
	}
	var status, message string
	if err := db.QueryRowContext(ctx, `SELECT status, error FROM contest_plagiarism_reports`).Scan(&status, &message); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || message != ambiguousError {
		t.Fatalf("late CSV response overwrote upload outcome: status=%q error=%q", status, message)
	}
	result, err := db.ExecContext(ctx, `
  UPDATE contest_plagiarism_reports SET status='pending', attempts=0, claim_token=NULL, error=NULL,
   next_retry_at=NOW(), uploaded_at=NULL, updated_at=NOW()
  WHERE contest_id='contest' AND status IN ('failed','error','purged','unavailable')
   AND COALESCE(error,'') NOT LIKE 'Dolos upload outcome is ambiguous%'
 `)
	if err != nil {
		t.Fatal(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if affected != 0 {
		t.Fatalf("routine reset must exclude ambiguous uploads, updated %d rows", affected)
	}
}

func TestDolosOversizedArchiveFailsPermanentlyPostgres(t *testing.T) {
	db, _ := dolosPostgresDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `INSERT INTO contests(id,name,registration_start_time,registration_end_time,start_time,end_time) VALUES('contest','Synthetic',0,0,0,1); INSERT INTO problems(id,contest_id,name,score,type) VALUES('problem','contest','Synthetic',1,'code')`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 21; i++ {
		if _, err := db.ExecContext(ctx, `INSERT INTO submissions(id,user_id,contest_id,problem_id,type,language,status,created_at) VALUES($1,$2,'contest','problem','code','python','accepted',1)`, fmt.Sprintf("source-%02d", i), fmt.Sprintf("user-%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
	data := make([]byte, 540*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	source := "#" + base64.StdEncoding.EncodeToString(data) + "\n"
	stored := base64.StdEncoding.EncodeToString([]byte(source))
	if len(stored) >= 1024*1024 {
		t.Fatal("fixture exceeds per-object limit")
	}
	var reads, posts atomic.Int32
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reads.Add(1); fmt.Fprint(w, stored) }))
	defer store.Close()
	for k, v := range map[string]string{"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "AWS_SESSION_TOKEN": "", "AWS_REGION": "us-east-1", "AWS_EC2_METADATA_DISABLED": "true", "AWS_CONFIG_FILE": os.DevNull, "AWS_SHARED_CREDENTIALS_FILE": os.DevNull, "AWS_ENDPOINT_URL_S3": store.URL, "S3_SUBMISSIONS_BUCKET": "test"} {
		t.Setenv(k, v)
	}
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		fmt.Fprint(w, `{"id":"unexpected","status":"finished"}`)
	}))
	defer provider.Close()
	service := &DolosService{db: db, s3: s3.NewS3Client(), baseURL: provider.URL, client: provider.Client()}
	if err := service.processContest(ctx, "contest", 1); err != nil {
		t.Fatal(err)
	}
	firstReads := reads.Load()
	var status, msg string
	var attempts int
	if err := db.QueryRowContext(ctx, `SELECT status,error,attempts FROM contest_plagiarism_reports`).Scan(&status, &msg, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || !strings.Contains(msg, "exceeds 10 MB limit") || attempts != 1 || posts.Load() != 0 {
		t.Fatalf("status=%s attempts=%d reads=%d posts=%d error=%s", status, attempts, reads.Load(), posts.Load(), msg)
	}
	if _, err := db.ExecContext(ctx, `UPDATE contest_plagiarism_reports SET next_retry_at=NOW()`); err != nil {
		t.Fatal(err)
	}
	if err := service.processContest(ctx, "contest", 1); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status,attempts FROM contest_plagiarism_reports`).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || attempts != 1 || reads.Load() != firstReads || posts.Load() != 0 {
		t.Fatalf("status=%s attempts=%d reads=%d first=%d posts=%d", status, attempts, reads.Load(), firstReads, posts.Load())
	}
}
