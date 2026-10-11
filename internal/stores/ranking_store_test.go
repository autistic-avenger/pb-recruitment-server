package stores

import (
	"context"
	"fmt"
	"testing"
	"time"

	"app/internal/models/dto"
)

func ts(v int64) *int64 { return &v }

// ponytail: one check for the solve aggregation. The earlier version counted a
// problem as solved when score == max_score, which also matched an unsolved
// zero-point problem, and it is easy to reintroduce that by "simplifying" the
// nil check away.
func TestSummarizeSolves(t *testing.T) {
	cases := []struct {
		name      string
		scores    []dto.ProblemScore
		wantSolve int
		wantLast  int64
	}{
		{"nothing solved", []dto.ProblemScore{
			{Score: 0, MaxScore: 100},
			{Score: 0, MaxScore: 50},
		}, 0, 0},
		{"zero point problem is not a solve", []dto.ProblemScore{
			{Score: 0, MaxScore: 0},
		}, 0, 0},
		{"takes the latest first solve", []dto.ProblemScore{
			{Score: 100, MaxScore: 100, SolvedAt: ts(3000)},
			{Score: 50, MaxScore: 50, SolvedAt: ts(1000)},
		}, 2, 3000},
		{"unsolved problems do not affect the time", []dto.ProblemScore{
			{Score: 100, MaxScore: 100, SolvedAt: ts(2000)},
			{Score: 0, MaxScore: 100},
		}, 1, 2000},
		{"no scores", nil, 0, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			solved, last := summarizeSolves(tc.scores)
			if solved != tc.wantSolve {
				t.Errorf("solved = %d, want %d", solved, tc.wantSolve)
			}
			if last != tc.wantLast {
				t.Errorf("lastSolve = %d, want %d", last, tc.wantLast)
			}
		})
	}
}

func TestLeaderboardCache(t *testing.T) {
	store := NewRankingStore(nil)
	store.SetCacheTTL(100 * time.Millisecond)

	contestID := "contest-1"
	page := 0
	key := fmt.Sprintf("%s:%d", contestID, page)

	mockResp := &dto.GetLeaderboardResponse{
		Entries: []dto.LeaderboardEntry{
			{
				Rank:       1,
				UserID:     "user-1",
				Username:   "Alice",
				TotalScore: 100,
			},
		},
		Page:       0,
		TotalPages: 1,
		TotalCount: 1,
	}

	// Manually populate cache to verify caching mechanics without database
	store.cacheMu.Lock()
	store.cache[key] = leaderboardCacheEntry{
		data:      mockResp,
		expiresAt: time.Now().Add(100 * time.Millisecond),
	}
	store.cacheMu.Unlock()

	// Should hit cache via GetLeaderboard
	resp, err := store.GetLeaderboard(context.Background(), contestID, page)
	if err != nil {
		t.Fatalf("expected cache hit from GetLeaderboard, got error: %v", err)
	}
	if resp == nil || len(resp.Entries) != 1 || resp.Entries[0].Username != "Alice" {
		t.Fatalf("unexpected cached data: %+v", resp)
	}

	// Mutating cloned response should not alter internal cache
	resp.Entries[0].Username = "Bob"
	respAgain, err := store.GetLeaderboard(context.Background(), contestID, page)
	if err != nil {
		t.Fatalf("expected second cache hit: %v", err)
	}
	if respAgain.Entries[0].Username != "Alice" {
		t.Errorf("cache was mutated by clone modification, got %s, want Alice", respAgain.Entries[0].Username)
	}

	// Invalidate cache for contest
	store.InvalidateCache(contestID)
	// Cache miss should attempt DB query and fail because db is nil
	if _, err := store.GetLeaderboard(context.Background(), contestID, page); err == nil {
		t.Fatal("expected DB error after cache invalidation")
	}

	// Test expiration
	store.cacheMu.Lock()
	store.cache[key] = leaderboardCacheEntry{
		data:      mockResp,
		expiresAt: time.Now().Add(-1 * time.Second), // already expired
	}
	store.cacheMu.Unlock()

	// Expired entry should be treated as cache miss and attempt DB query
	if _, err := store.GetLeaderboard(context.Background(), contestID, page); err == nil {
		t.Fatal("expected DB error for expired entry")
	}
}
