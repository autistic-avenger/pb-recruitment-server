package stores

import (
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

	// Should hit cache
	store.cacheMu.RLock()
	entry, ok := store.cache[key]
	store.cacheMu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) {
		t.Fatal("expected cached entry to be present and not expired")
	}

	cloned := cloneLeaderboardResponse(entry.data)
	if cloned == nil || len(cloned.Entries) != 1 || cloned.Entries[0].Username != "Alice" {
		t.Fatalf("unexpected cloned data: %+v", cloned)
	}

	// Mutating cloned response should not alter internal cache
	cloned.Entries[0].Username = "Bob"
	store.cacheMu.RLock()
	cachedUsername := store.cache[key].data.Entries[0].Username
	store.cacheMu.RUnlock()
	if cachedUsername != "Alice" {
		t.Errorf("cache was mutated by clone modification, got %s, want Alice", cachedUsername)
	}

	// Invalidate cache for contest
	store.InvalidateCache(contestID)
	store.cacheMu.RLock()
	_, ok = store.cache[key]
	store.cacheMu.RUnlock()
	if ok {
		t.Error("expected cache entry to be invalidated")
	}

	// Test expiration
	store.cacheMu.Lock()
	store.cache[key] = leaderboardCacheEntry{
		data:      mockResp,
		expiresAt: time.Now().Add(-1 * time.Second), // already expired
	}
	store.cacheMu.Unlock()

	store.cacheMu.RLock()
	expiredEntry, ok := store.cache[key]
	store.cacheMu.RUnlock()
	if !ok {
		t.Fatal("entry should exist in map")
	}
	if !time.Now().After(expiredEntry.expiresAt) {
		t.Error("expected entry to be expired")
	}
}
