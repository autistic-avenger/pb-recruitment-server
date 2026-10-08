package models

type Execution struct {
	ID            string `json:"id"`
	SubmissionID  string `json:"submission_id"`
	TestCaseIndex int    `json:"test_case_index"`
	Judge0Token   string `json:"judge0_token,omitempty"`
	Status        string `json:"status"`
	Runtime       int64  `json:"runtime,omitempty"`
	Memory        int64  `json:"memory,omitempty"`
	CreatedAt     int64  `json:"created_at"`
}
