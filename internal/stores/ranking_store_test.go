package stores

import (
	"testing"

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
