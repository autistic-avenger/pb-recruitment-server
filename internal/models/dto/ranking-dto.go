package dto

type UpdateLeaderboardUserRequest struct {
	Hidden       *bool `json:"hidden"`
	Disqualified *bool `json:"disqualified"`
}

type LeaderboardEntry struct {
	Rank               int            `json:"rank"`
	UserID             string         `json:"user_id"`
	Username           string         `json:"username"`
	USN                string         `json:"usn"`
	TotalScore         int            `json:"total_score"`
	ProblemsSolved     int            `json:"problems_solved"`
	LastSubmissionTime int64          `json:"last_submission_time"`
	ProblemScores      []ProblemScore `json:"problem_scores,omitempty"`
}

type ProblemScore struct {
	ProblemID   string `json:"problem_id"`
	ProblemName string `json:"problem_name"`
	Score       int    `json:"score"`
	MaxScore    int    `json:"max_score"`
	Attempts    int    `json:"attempts"`
	SolvedAt    *int64 `json:"solved_at,omitempty"`
}

type GetLeaderboardResponse struct {
	Entries    []LeaderboardEntry `json:"entries"`
	Page       int                `json:"page"`
	TotalPages int                `json:"total_pages"`
	TotalCount int                `json:"total_count"`
}
