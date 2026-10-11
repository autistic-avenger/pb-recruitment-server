package controllers

import (
	"app/internal/common"
	"app/internal/models"
	"app/internal/models/dto"
	"app/internal/services"
	"app/internal/stores"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
)

type contestUpdateTestStore struct {
	submissionContestStore
	updated *models.Contest
}

func (s *contestUpdateTestStore) UpdateContest(_ context.Context, contest *models.Contest) error {
	s.updated = contest
	return nil
}

func TestUpdateContestStartTime(t *testing.T) {
	now := time.Now().UnixMilli()
	for _, tc := range []struct {
		name                 string
		start, end, newStart int64
		want                 int
	}{
		{"running postponed", now - 120_000, now + 60_000, now + 120_000, http.StatusConflict},
		{"running moved earlier", now - 120_000, now + 60_000, now - 180_000, http.StatusConflict},
		{"ended postponed", now - 120_000, now - 60_000, now + 120_000, http.StatusConflict},
		{"running extended", now - 120_000, now + 60_000, now - 120_000, http.StatusOK},
		{"ended extended", now - 120_000, now - 60_000, now - 120_000, http.StatusOK},
		{"upcoming rescheduled", now + 60_000, now + 120_000, now + 180_000, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			contestStore := &contestUpdateTestStore{submissionContestStore: submissionContestStore{
				contest: models.Contest{ID: "contest", StartTime: tc.start, EndTime: tc.end},
			}}
			controller := NewContestController(services.NewContestService(&stores.Storage{Contests: contestStore}, nil))
			recorder := httptest.NewRecorder()
			ctx := echo.New().NewContext(httptest.NewRequest(http.MethodPut, "/admin/contest/contest", nil), recorder)
			ctx.SetParamNames("id")
			ctx.SetParamValues("contest")
			req := &dto.UpsertContestRequest{Name: "Contest", StartTime: tc.newStart, EndTime: now + 300_000}
			ctx.Set(common.VALIDATED_REQUEST_BODY, req)
			if err := controller.HandleUpdateContest(ctx); err != nil {
				t.Fatal(err)
			}
			if recorder.Code != tc.want {
				t.Fatalf("HTTP %d %s, want %d", recorder.Code, recorder.Body.String(), tc.want)
			}
			if tc.want == http.StatusConflict {
				if contestStore.updated != nil || !strings.Contains(recorder.Body.String(), "start time cannot be changed") {
					t.Fatalf("rejected update reached store or response missing reason: %s", recorder.Body.String())
				}
			} else if contestStore.updated == nil || contestStore.updated.StartTime != req.StartTime || contestStore.updated.EndTime != req.EndTime {
				t.Fatalf("update not saved: %+v", contestStore.updated)
			}
		})
	}
}

func TestProblemMutationsAfterStart(t *testing.T) {
	now := time.Now().UnixMilli()
	for _, end := range []int64{now + 60_000, now - 60_000} {
		for _, action := range []string{"update", "delete"} {
			t.Run(action+"/"+time.UnixMilli(end).Format(time.RFC3339), func(t *testing.T) {
				storage := &stores.Storage{Contests: submissionContestStore{contest: models.Contest{StartTime: now - 120_000, EndTime: end}}}
				controller := NewContestController(services.NewContestService(storage, nil))
				recorder := httptest.NewRecorder()
				ctx := echo.New().NewContext(httptest.NewRequest(http.MethodPut, "/admin/contest/problem/problem", nil), recorder)
				ctx.SetParamNames("contestid", "problemid")
				ctx.SetParamValues("contest", "problem")
				ctx.Set(common.VALIDATED_REQUEST_BODY, &dto.CreateProblemRequest{Type: models.MCQ, Answer: []int{0}, Options: []string{"a", "b"}})
				handler := controller.HandleUpdateProblem
				if action == "delete" {
					handler = controller.HandleDeleteProblem
				}
				if err := handler(ctx); err != nil {
					t.Fatal(err)
				}
				if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), common.ErrProblemsLocked.Error()) {
					t.Fatalf("HTTP %d %s", recorder.Code, recorder.Body.String())
				}
			})
		}
	}
}

func TestProblemInvalidAnswerResponse(t *testing.T) {
	controller := NewContestController(services.NewContestService(nil, nil))
	for _, action := range []string{"create", "update"} {
		t.Run(action, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx := echo.New().NewContext(httptest.NewRequest(http.MethodPost, "/admin/contest/problem", nil), recorder)
			ctx.SetParamNames("contestid", "problemid")
			ctx.SetParamValues("contest", "problem")
			ctx.Set(common.VALIDATED_REQUEST_BODY, &dto.CreateProblemRequest{Type: models.MCQ, Answer: []int{7}, Options: []string{"a", "b"}})
			handler := controller.HandleCreateProblem
			if action == "update" {
				handler = controller.HandleUpdateProblem
			}
			if err := handler(ctx); err != nil {
				t.Fatal(err)
			}
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("HTTP %d %s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

type mockRankingStore struct {
	consolidateFunc func(ctx context.Context, contestID string, regrade bool) (*dto.ConsolidationResponse, error)
	updateFunc      func(ctx context.Context, contestID, userID string, req *dto.UpdateLeaderboardUserRequest) error
	leaderboardFunc func(ctx context.Context, contestID string, page int) (*dto.GetLeaderboardResponse, error)
}

func (m *mockRankingStore) ConsolidateContest(ctx context.Context, contestID string, regrade bool) (*dto.ConsolidationResponse, error) {
	if m.consolidateFunc != nil {
		return m.consolidateFunc(ctx, contestID, regrade)
	}
	return nil, nil
}

func (m *mockRankingStore) UpdateLeaderboardUser(ctx context.Context, contestID, userID string, req *dto.UpdateLeaderboardUserRequest) error {
	if m.updateFunc != nil {
		return m.updateFunc(ctx, contestID, userID, req)
	}
	return nil
}

func (m *mockRankingStore) GetLeaderboard(ctx context.Context, contestID string, page int) (*dto.GetLeaderboardResponse, error) {
	if m.leaderboardFunc != nil {
		return m.leaderboardFunc(ctx, contestID, page)
	}
	return nil, nil
}

func TestHandleConsolidateContest(t *testing.T) {
	t.Run("success with regrade", func(t *testing.T) {
		var calledContestID string
		var calledRegrade bool
		rankStore := &mockRankingStore{
			consolidateFunc: func(_ context.Context, contestID string, regrade bool) (*dto.ConsolidationResponse, error) {
				calledContestID = contestID
				calledRegrade = regrade
				return &dto.ConsolidationResponse{
					ContestID:          contestID,
					MCQsGraded:         3,
					ParticipantsRanked: 10,
					ProcessedAt:        123456789,
					Status:             "success",
				}, nil
			},
		}
		controller := NewContestController(services.NewContestService(&stores.Storage{Rankings: rankStore}, nil))
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/admin/contest/cid-123/consolidate?regrade=true", nil)
		ctx := echo.New().NewContext(req, recorder)
		ctx.SetParamNames("id")
		ctx.SetParamValues("cid-123")

		if err := controller.HandleConsolidateContest(ctx); err != nil {
			t.Fatal(err)
		}
		if recorder.Code != http.StatusOK {
			t.Fatalf("HTTP %d, want 200", recorder.Code)
		}
		if calledContestID != "cid-123" || !calledRegrade {
			t.Errorf("called with %s, %v; want cid-123, true", calledContestID, calledRegrade)
		}
		if !strings.Contains(recorder.Body.String(), `"status":"success"`) {
			t.Errorf("unexpected body: %s", recorder.Body.String())
		}
	})

	t.Run("missing contest id", func(t *testing.T) {
		controller := NewContestController(services.NewContestService(nil, nil))
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/admin/contest//consolidate", nil)
		ctx := echo.New().NewContext(req, recorder)
		if err := controller.HandleConsolidateContest(ctx); err != nil {
			t.Fatal(err)
		}
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("HTTP %d, want 400", recorder.Code)
		}
	})

	t.Run("contest not found", func(t *testing.T) {
		rankStore := &mockRankingStore{
			consolidateFunc: func(_ context.Context, _ string, _ bool) (*dto.ConsolidationResponse, error) {
				return nil, common.ContestNotFoundError
			},
		}
		controller := NewContestController(services.NewContestService(&stores.Storage{Rankings: rankStore}, nil))
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/admin/contest/missing/consolidate", nil)
		ctx := echo.New().NewContext(req, recorder)
		ctx.SetParamNames("id")
		ctx.SetParamValues("missing")

		if err := controller.HandleConsolidateContest(ctx); err != nil {
			t.Fatal(err)
		}
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("HTTP %d, want 404", recorder.Code)
		}
	})

	t.Run("internal server error", func(t *testing.T) {
		rankStore := &mockRankingStore{
			consolidateFunc: func(_ context.Context, _ string, _ bool) (*dto.ConsolidationResponse, error) {
				return nil, errors.New("db query failed")
			},
		}
		controller := NewContestController(services.NewContestService(&stores.Storage{Rankings: rankStore}, nil))
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/admin/contest/cid-123/consolidate", nil)
		ctx := echo.New().NewContext(req, recorder)
		ctx.SetParamNames("contestid")
		ctx.SetParamValues("cid-123")

		if err := controller.HandleConsolidateContest(ctx); err != nil {
			t.Fatal(err)
		}
		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("HTTP %d, want 500", recorder.Code)
		}
	})
}
