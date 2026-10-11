package controllers

import (
	"app/internal/common"
	"app/internal/models"
	"app/internal/models/dto"
	"app/internal/services"
	"app/internal/stores"
	"context"
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

type visibilityProblemStore struct {
	*stores.ProblemStore
}

func (visibilityProblemStore) GetProblemList(context.Context, string) ([]dto.ProblemOverview, error) {
	return []dto.ProblemOverview{{ID: "prob-1", Name: "Problem 1", Score: 10, Type: "code"}}, nil
}

func (visibilityProblemStore) GetProblem(context.Context, string, string) (*dto.GetProblemStatementResponse, error) {
	return &dto.GetProblemStatementResponse{ProblemID: "prob-1", Name: "Problem 1", Score: 10, Type: models.Code}, nil
}

func TestProblemVisibilityBeforeStartAndRegistration(t *testing.T) {
	now := time.Now().UnixMilli()
	tests := []struct {
		name       string
		start, end int64
		registered bool
		lookupErr  error
		wantCode   int
	}{
		{
			name:       "upcoming contest - hidden before start",
			start:      now + 60_000,
			end:        now + 120_000,
			registered: true,
			wantCode:   http.StatusForbidden,
		},
		{
			name:       "unregistered user - forbidden",
			start:      now - 60_000,
			end:        now + 60_000,
			registered: false,
			wantCode:   http.StatusForbidden,
		},
		{
			name:       "running contest and registered - visible",
			start:      now - 60_000,
			end:        now + 60_000,
			registered: true,
			wantCode:   http.StatusOK,
		},
		{
			name:       "ended contest and registered - visible",
			start:      now - 120_000,
			end:        now - 60_000,
			registered: true,
			wantCode:   http.StatusOK,
		},
		{
			name:      "contest not found - 404",
			lookupErr: common.ContestNotFoundError,
			wantCode:  http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			storage := &stores.Storage{
				Contests: submissionContestStore{
					contest:    models.Contest{ID: "contest", StartTime: tc.start, EndTime: tc.end},
					registered: tc.registered,
					err:        tc.lookupErr,
				},
				Problems: visibilityProblemStore{},
			}
			controller := NewContestController(services.NewContestService(storage, nil))

			// Test problem list endpoint
			{
				recorder := httptest.NewRecorder()
				ctx := echo.New().NewContext(httptest.NewRequest(http.MethodGet, "/contests/contest/problems", nil), recorder)
				ctx.SetParamNames("id")
				ctx.SetParamValues("contest")
				ctx.Set(common.AUTH_USER_ID, "user-1")

				if err := controller.GetContestProblemsList(ctx); err != nil {
					t.Fatal(err)
				}
				if recorder.Code != tc.wantCode {
					t.Errorf("GetContestProblemsList HTTP %d, want %d", recorder.Code, tc.wantCode)
				}
			}

			// Test problem statement endpoint
			{
				recorder := httptest.NewRecorder()
				ctx := echo.New().NewContext(httptest.NewRequest(http.MethodGet, "/contests/contest/problems/prob-1", nil), recorder)
				ctx.SetParamNames("id", "problem_id")
				ctx.SetParamValues("contest", "prob-1")
				ctx.Set(common.AUTH_USER_ID, "user-1")

				if err := controller.GetContestProblem(ctx); err != nil {
					t.Fatal(err)
				}
				if recorder.Code != tc.wantCode {
					t.Errorf("GetContestProblem HTTP %d, want %d", recorder.Code, tc.wantCode)
				}
			}
		})
	}
}
