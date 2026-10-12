package services

import (
	"app/internal/common"
	"app/internal/models"
	"app/internal/models/dto"
	"app/internal/stores"
	"context"
	"errors"
	"testing"
	"time"
)

func TestMCQAnswerRange(t *testing.T) {
	for _, tc := range []struct {
		name    string
		kind    models.SubmissionType
		answers []int
		valid   bool
	}{
		{"negative", models.MCQ, []int{-1}, false},
		{"out of range", models.MCQ, []int{2}, false},
		{"valid boundaries", models.MCQ, []int{0, 1}, true},
		{"code", models.Code, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &dto.CreateProblemRequest{Type: tc.kind, Answer: tc.answers, Options: []string{"a", "b"}}
			if err := validateMCQAnswerRange(req); (err == nil) != tc.valid {
				t.Errorf("range error=%v valid=%v", err, tc.valid)
			}
			if tc.valid {
				return
			}
			svc := NewContestService(nil, nil)
			// Invalid answers must fail before database access or S3 writes.
			if _, err := svc.CreateProblem(context.Background(), "contest", req); !errors.Is(err, common.ErrInvalidAnswer) {
				t.Errorf("create error=%v", err)
			}
			if _, err := svc.UpdateProblem(context.Background(), "contest", "problem", req); !errors.Is(err, common.ErrInvalidAnswer) {
				t.Errorf("update error=%v", err)
			}
		})
	}
}

type problemContestTestStore struct {
	*stores.ContestStore
	start, end int64
	err        error
}

func (s problemContestTestStore) GetContest(context.Context, string) (*dto.GetContestResponse, error) {
	return &dto.GetContestResponse{Contest: models.Contest{StartTime: s.start, EndTime: s.end}}, s.err
}

func TestProblemEditWindow(t *testing.T) {
	now := time.Now().UnixMilli()
	lookupErr := errors.New("contest lookup failed")
	for _, tc := range []struct {
		name               string
		start, end         int64
		lookupErr, wantErr error
	}{
		{"upcoming", now + 60_000, now + 120_000, nil, nil},
		{"running", now - 60_000, now + 60_000, nil, common.ErrProblemsLocked},
		{"ended", now - 120_000, now - 60_000, nil, common.ErrProblemsLocked},
		{"database failure", 0, 0, lookupErr, lookupErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage := &stores.Storage{Contests: problemContestTestStore{start: tc.start, end: tc.end, err: tc.lookupErr}}
			err := NewContestService(storage, nil).checkProblemEditable(context.Background(), "contest")
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("error=%v want %v", err, tc.wantErr)
			}
		})
	}
}

type serviceRankingTestStore struct {
	consolidateFunc func(ctx context.Context, contestID string, regrade bool) (*dto.ConsolidationResponse, error)
	updateFunc      func(ctx context.Context, contestID, userID string, req *dto.UpdateLeaderboardUserRequest) error
	leaderboardFunc func(ctx context.Context, contestID string, page int) (*dto.GetLeaderboardResponse, error)
}

func (s serviceRankingTestStore) ConsolidateContest(ctx context.Context, contestID string, regrade bool) (*dto.ConsolidationResponse, error) {
	if s.consolidateFunc != nil {
		return s.consolidateFunc(ctx, contestID, regrade)
	}
	return nil, nil
}

func (s serviceRankingTestStore) UpdateLeaderboardUser(ctx context.Context, contestID, userID string, req *dto.UpdateLeaderboardUserRequest) error {
	if s.updateFunc != nil {
		return s.updateFunc(ctx, contestID, userID, req)
	}
	return nil
}

func (s serviceRankingTestStore) GetLeaderboard(ctx context.Context, contestID string, page int) (*dto.GetLeaderboardResponse, error) {
	if s.leaderboardFunc != nil {
		return s.leaderboardFunc(ctx, contestID, page)
	}
	return nil, nil
}

func (s serviceRankingTestStore) InvalidateCache(contestID string) {}

func TestConsolidateContestService(t *testing.T) {
	t.Run("nil ranking store error", func(t *testing.T) {
		svc := NewContestService(&stores.Storage{}, nil)
		_, err := svc.ConsolidateContest(context.Background(), "c1", false)
		if err == nil {
			t.Fatal("expected error with nil ranking store")
		}
	})

	t.Run("delegates to ranking store", func(t *testing.T) {
		called := false
		rankStore := serviceRankingTestStore{
			consolidateFunc: func(_ context.Context, cid string, regrade bool) (*dto.ConsolidationResponse, error) {
				called = true
				if cid != "c123" || !regrade {
					t.Errorf("unexpected args: cid=%s, regrade=%v", cid, regrade)
				}
				return &dto.ConsolidationResponse{ContestID: cid, Status: "success"}, nil
			},
		}
		svc := NewContestService(&stores.Storage{Rankings: rankStore}, nil)
		res, err := svc.ConsolidateContest(context.Background(), "c123", true)
		if err != nil {
			t.Fatal(err)
		}
		if !called || res == nil || res.Status != "success" {
			t.Errorf("unexpected result: called=%v, res=%+v", called, res)
		}
	})
}
