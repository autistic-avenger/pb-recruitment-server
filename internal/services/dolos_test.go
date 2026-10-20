package services

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"app/internal/models/dto"
)

func TestDolosCSVHeaderAndPathParsing(t *testing.T) {
	// Official Dolos v2.6.0 headers with synthetic test data
	csvContent := "id,leftFilePath,rightFilePath,similarity,totalOverlap,longestFragment\n" +
		"1,/tmp/dolos-unzip-xyz/userA/prob1_sub123.py,/tmp/dolos-unzip-xyz/userB/prob1_sub456.py,0.92,150,45\n" +
		"2,/tmp/dolos-unzip-xyz/userC/prob1_sub789.py,/tmp/dolos-unzip-xyz/userD/prob1_sub012.py,0.75,80,25\n"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/reports/rep-123/data/pairs" {
			w.Header().Set("Content-Type", "text/csv")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(csvContent))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	svc := &DolosService{
		baseURL: server.URL,
		client:  server.Client(),
	}

	pairs, err := svc.fetchReportPairs(context.Background(), "rep-123")
	if err != nil {
		t.Fatalf("fetchReportPairs failed: %v", err)
	}

	if len(pairs) != 2 {
		t.Fatalf("expected 2 pairs, got %d", len(pairs))
	}

	// First pair assertions
	p1 := pairs[0]
	if p1.LeftUserID != "userA" {
		t.Errorf("expected left user ID 'userA', got %q", p1.LeftUserID)
	}
	if p1.RightUserID != "userB" {
		t.Errorf("expected right user ID 'userB', got %q", p1.RightUserID)
	}
	if p1.LeftFile != "userA/prob1_sub123.py" {
		t.Errorf("expected canonical left file 'userA/prob1_sub123.py', got %q", p1.LeftFile)
	}
	if p1.RightFile != "userB/prob1_sub456.py" {
		t.Errorf("expected canonical right file 'userB/prob1_sub456.py', got %q", p1.RightFile)
	}
	if p1.Similarity != 0.92 {
		t.Errorf("expected similarity 0.92, got %f", p1.Similarity)
	}
	if p1.TotalOverlap != 150 {
		t.Errorf("expected total overlap 150, got %d", p1.TotalOverlap)
	}
	if p1.LongestFragment != 45 {
		t.Errorf("expected longest fragment 45, got %d", p1.LongestFragment)
	}

	// Verify key determinism regardless of path ordering
	expectedKey := pairKey("userA/prob1_sub123.py", "userB/prob1_sub456.py")
	reversedKey := pairKey("userB/prob1_sub456.py", "userA/prob1_sub123.py")
	if p1.Key != expectedKey || p1.Key != reversedKey {
		t.Errorf("expected pair key %q, got %q", expectedKey, p1.Key)
	}
}

func TestDolosOverallStatus(t *testing.T) {
	tests := []struct {
		name     string
		statuses []string
		want     string
	}{
		{name: "purged is failed", statuses: []string{"purged"}, want: "failed"},
		{name: "unknown remains running", statuses: []string{"unknown"}, want: "processing"},
		{name: "running masks failure", statuses: []string{"failed", "running"}, want: "processing"},
		{name: "all skipped", statuses: []string{"skipped", "skipped"}, want: "skipped"},
		{name: "finished and skipped", statuses: []string{"finished", "skipped"}, want: "finished"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reports := make([]dto.ProblemReportSummary, len(tc.statuses))
			for i, status := range tc.statuses {
				reports[i].Status = status
			}
			if got := dolosOverallStatus(reports); got != tc.want {
				t.Errorf("dolosOverallStatus() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDolosCanonicalPathExtraction(t *testing.T) {
	cases := []struct {
		input        string
		expectedPath string
		expectedUser string
	}{
		{
			input:        "/tmp/dolos-unzip-123/usr-42/problemA_sub12345678.py",
			expectedPath: "usr-42/problemA_sub12345678.py",
			expectedUser: "usr-42",
		},
		{
			input:        "C:\\Users\\Dolos\\Temp\\extract\\usr-99\\problemB_sub999.cpp",
			expectedPath: "usr-99/problemB_sub999.cpp",
			expectedUser: "usr-99",
		},
		{
			input:        "usr-1/sub.go",
			expectedPath: "usr-1/sub.go",
			expectedUser: "usr-1",
		},
	}

	for _, tc := range cases {
		cPath, uID := extractCanonicalPathAndUser(tc.input)
		if cPath != tc.expectedPath {
			t.Errorf("input %q: expected canonical path %q, got %q", tc.input, tc.expectedPath, cPath)
		}
		if uID != tc.expectedUser {
			t.Errorf("input %q: expected user %q, got %q", tc.input, tc.expectedUser, uID)
		}
	}
}

func TestCanonicalLanguage(t *testing.T) {
	tests := []struct {
		lang     string
		expected string
	}{
		{"python", "python"},
		{"Python3", "python"},
		{"py", "python"},
		{"c++", "cpp"},
		{"cpp", "cpp"},
		{"c", "c"},
		{"java", "java"},
		{"javascript", "javascript"},
		{"js", "javascript"},
		{"golang", "go"},
		{"rust", ""},   // unsupported submission languages return empty
		{"ruby", ""},
		{"swift", ""},
	}

	for _, tc := range tests {
		res := canonicalLanguage(tc.lang)
		if res != tc.expected {
			t.Errorf("canonicalLanguage(%q) = %q; want %q", tc.lang, res, tc.expected)
		}
	}
}
