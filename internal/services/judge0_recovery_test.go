package services

import (
	"app/internal/judge0"
	"app/internal/models"
	"app/internal/stores"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/labstack/gommon/log"
	"go.uber.org/fx"
)

type recoveryTestStore struct {
	*stores.ExecutionStore
	items     []models.Execution
	tokenless []models.Execution
	marked    []string
	finals    []stores.FinalExecutionResult
	cutoff    int64
}

func (s *recoveryTestStore) PendingTokenlessBefore(_ context.Context, cutoff int64, _ int) ([]string, error) {
	s.cutoff = cutoff
	var ids []string
	for _, item := range s.tokenless {
		if item.CreatedAt < cutoff {
			ids = append(ids, item.ID)
		}
	}
	return ids, nil
}
func (s *recoveryTestStore) MarkFailed(_ context.Context, ids []string) error {
	s.marked = append(s.marked, ids...)
	return nil
}
func (*recoveryTestStore) TerminalPendingParents(context.Context, int) ([]stores.FinalExecutionResult, error) {
	return nil, nil
}
func (s *recoveryTestStore) ProcessFinal(_ context.Context, result stores.FinalExecutionResult) error {
	s.finals = append(s.finals, result)
	return nil
}
func (s *recoveryTestStore) PendingWithTokens(_ context.Context, limit int, created int64, id string) ([]models.Execution, error) {
	var out []models.Execution
	for _, item := range s.items {
		if item.CreatedAt > created || item.CreatedAt == created && item.ID > id {
			out = append(out, item)
		}
	}
	return out[:min(limit, len(out))], nil
}

func TestRecoveryCursorSurvivesTimeout(t *testing.T) {
	store := &recoveryTestStore{items: []models.Execution{
		{ID: "a", Judge0Token: "slow", CreatedAt: 1},
		{ID: "b", Judge0Token: "missing", CreatedAt: 1},
		{ID: "c", Judge0Token: "queued", CreatedAt: 2},
	}}
	var mu sync.Mutex
	var requests []string
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/workers" {
			w.Write([]byte(`[{"queue":"1.13.1","size":1,"available":1,"working":0}]`))
			return
		}
		token := r.URL.Query().Get("tokens")
		mu.Lock()
		requests = append(requests, token)
		mu.Unlock()
		if token == "slow" {
			cancel()
			<-r.Context().Done()
			return
		}
		if token == "missing" {
			w.Write([]byte(`{"submissions":[null]}`))
			return
		}
		w.Write([]byte(`{"submissions":[{"token":"queued","status":{"id":1}}]}`))
	}))
	defer server.Close()
	t.Setenv("JUDGE0_URL", server.URL)
	t.Setenv("JUDGE0_AUTH_TOKEN", "")
	t.Setenv("JUDGE0_BATCH_SIZE", "1")
	ss := NewSubmissionService(&stores.Storage{Executions: store}, nil, judge0.NewClient())
	ss.RecoverJudge0(ctx)
	if ss.recoveryCursorID != "a" {
		t.Fatalf("cursor advanced to %q", ss.recoveryCursorID)
	}
	ss.RecoverJudge0(context.Background())
	mu.Lock()
	got := slices.Clone(requests)
	mu.Unlock()
	if !slices.Equal(got, []string{"slow", "missing", "queued"}) {
		t.Fatalf("requests = %v", got)
	}
	if ss.recoveryCursorID != "" || ss.recoveryCursorTime != 0 {
		t.Fatal("empty page must reset the cursor")
	}
}

func TestRecoveryRequiresConfirmedIdle(t *testing.T) {
	for _, tc := range []struct {
		name, workers string
		idle          bool
	}{
		{"idle", `[{"queue":"1.13.1","size":0,"available":1,"working":0}]`, true},
		{"queued", `[{"queue":"1.13.1","size":1,"available":1,"working":0}]`, false},
		{"working", `[{"queue":"1.13.1","size":0,"available":1,"working":1}]`, false},
		{"no workers", `[{"queue":"1.13.1","size":0,"available":0,"working":0}]`, false},
		{"unknown", `[]`, false},
		{"malformed", `{`, false},
		{"missing fields", `[{"queue":"1.13.1"}]`, false},
		{"mixed queues", `[{"queue":"a","size":0,"available":1,"working":0},{"queue":"b","size":1,"available":1,"working":0}]`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			previous := log.Output()
			log.SetOutput(&output)
			defer log.SetOutput(previous)
			store := &recoveryTestStore{
				items:     []models.Execution{{ID: "a", Judge0Token: "queued", CreatedAt: 1}, {ID: "b", Judge0Token: "processing", CreatedAt: 1}, {ID: "c", Judge0Token: "accepted", CreatedAt: 1}},
				tokenless: []models.Execution{{ID: "lost", CreatedAt: 1}, {ID: "recent", CreatedAt: time.Now().Unix()}},
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/workers" {
					w.Write([]byte(tc.workers))
					return
				}
				w.Write([]byte(`{"submissions":[{"token":"queued","status":{"id":1}},{"token":"processing","status":{"id":2}},{"token":"accepted","status":{"id":3}}]}`))
			}))
			defer server.Close()
			t.Setenv("JUDGE0_URL", server.URL)
			t.Setenv("JUDGE0_BATCH_SIZE", "20")
			ss := NewSubmissionService(&stores.Storage{Executions: store}, nil, judge0.NewClient())
			ss.RecoverJudge0(context.Background())
			if tc.name == "no workers" {
				logged := output.String()
				if !strings.Contains(logged, "judge0 recovery worker observation failed") || !strings.Contains(logged, "no available workers") || !strings.Contains(logged, "1.13.1") {
					t.Fatalf("missing actionable worker warning: %s", logged)
				}
			}
			if len(store.marked) != 0 || len(store.finals) != 1 || store.finals[0].Status != "accepted" {
				t.Fatalf("first observation: marked=%v, finals=%+v", store.marked, store.finals)
			}
			// A second immediate call is not a second sweep.
			ss.RecoverJudge0(context.Background())
			if len(store.marked) != 0 {
				t.Fatal("cleanup must wait for a separate sweep")
			}
			ss.recoveryIdleSince = time.Now().Add(-16 * time.Second)
			store.finals = nil
			ss.RecoverJudge0(context.Background())
			if tc.idle {
				if !slices.Equal(store.marked, []string{"lost"}) || len(store.finals) != 3 || store.finals[0].Status != "judge_error" || store.finals[1].Status != "judge_error" || store.finals[2].Status != "accepted" {
					t.Fatalf("idle cleanup: marked=%v, finals=%+v", store.marked, store.finals)
				}
			} else if len(store.marked) != 0 || !ss.recoveryIdleSince.IsZero() || len(store.finals) != 1 || store.finals[0].Status != "accepted" {
				t.Fatalf("unsafe cleanup: marked=%v, finals=%+v", store.marked, store.finals)
			}
		})
	}
}

func TestRecoveryBatchesAcrossPages(t *testing.T) {
	store := &recoveryTestStore{}
	for i := 0; i < 121; i++ {
		id := fmt.Sprintf("%03d", i)
		store.items = append(store.items, models.Execution{ID: id, Judge0Token: id, CreatedAt: 1})
	}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/workers" {
			w.Write([]byte(`[{"queue":"1.13.1","size":121,"available":1,"working":1}]`))
			return
		}
		requests++
		tokens := strings.Split(r.URL.Query().Get("tokens"), ",")
		if len(tokens) > 20 {
			t.Error("batch limit exceeded")
		}
		results := make([]judge0.SubmissionStatusResponse, len(tokens))
		for i, token := range tokens {
			results[i].Token, results[i].Status.ID = token, 3
		}
		json.NewEncoder(w).Encode(map[string]any{"submissions": results})
	}))
	defer server.Close()
	t.Setenv("JUDGE0_URL", server.URL)
	t.Setenv("JUDGE0_BATCH_SIZE", "20")
	ss := NewSubmissionService(&stores.Storage{Executions: store}, nil, judge0.NewClient())
	ss.RecoverJudge0(context.Background())
	if len(store.finals) != 121 || requests != 8 || ss.recoveryCursorID != "" {
		t.Fatalf("finals=%d, requests=%d, cursor=%q", len(store.finals), requests, ss.recoveryCursorID)
	}
}

type testLifecycle struct{ hooks []fx.Hook }

func (l *testLifecycle) Append(h fx.Hook) { l.hooks = append(l.hooks, h) }

func TestRecoveryWorkerStops(t *testing.T) {
	lc := &testLifecycle{}
	NewJudge0Recovery(lc, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := lc.hooks[0].OnStart(ctx); err != nil {
		t.Fatal(err)
	}
	if err := lc.hooks[0].OnStop(ctx); err != nil {
		t.Fatalf("worker failed to stop: %v", err)
	}
}

func TestJudge0StatusAndRuntime(t *testing.T) {
	want := []string{"pending", "pending", "accepted", "wrong_answer", "tle", "failed_to_process", "rte", "rte", "rte", "rte", "rte", "rte", "judge_error", "judge_error"}
	for id, status := range want {
		got, final := mapJudgeStatus(id + 1)
		if got != status || final != (id >= 2) {
			t.Fatalf("status %d = %q/%v", id+1, got, final)
		}
	}
	for _, tc := range []struct {
		raw     string
		want    int64
		invalid bool
	}{
		{`null`, 0, false}, {`"0.125"`, 125, false}, {`0.1256`, 126, false}, {`"0"`, 0, false},
		{`"invalid"`, 0, true}, {`"-1"`, 0, true},
	} {
		got, err := judgeRuntimeMillis(json.RawMessage(tc.raw))
		if (err != nil) != tc.invalid || err == nil && got != tc.want {
			t.Fatalf("runtime %s = %d, %v", tc.raw, got, err)
		}
	}
}
