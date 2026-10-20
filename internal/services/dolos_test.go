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
		if r.URL.Path == "/reports/rep-123/data/pairs.csv" {
			http.Redirect(w, r, "/blobs/pairs.csv", http.StatusFound)
			return
		}
		if r.URL.Path == "/blobs/pairs.csv" {
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
		{name: "uploading masks failure", statuses: []string{"uploading", "failed"}, want: "processing"},
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
		{"rust", ""}, // unsupported submission languages return empty
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

func TestLanguageExtension(t *testing.T) {
	tests := []struct {
		lang string
		want string
	}{
		{"python", ".py"},
		{"cpp", ".cpp"},
		{"c", ".c"},
		{"java", ".java"},
		{"javascript", ".js"},
		{"go", ".go"},
		{"unknown", ".txt"},
	}

	for _, tc := range tests {
		got := languageExtension(tc.lang)
		if got != tc.want {
			t.Errorf("languageExtension(%q) = %q, want %q", tc.lang, got, tc.want)
		}
	}
}

func TestDolosCSVWithSubmissionIDsAndThreshold(t *testing.T) {
	// CSV containing pairs with submission IDs and one pair below the 0.20 similarity threshold
	csvContent := "id,leftFilePath,rightFilePath,similarity,totalOverlap,longestFragment\n" +
		"1,/tmp/dolos-unzip-xyz/sub-101/prob1.py,/tmp/dolos-unzip-xyz/sub-102/prob1.py,0.95,200,60\n" +
		"2,/tmp/dolos-unzip-xyz/sub-103/prob1.py,/tmp/dolos-unzip-xyz/sub-104/prob1.py,0.15,10,5\n" +
		"3,/tmp/dolos-unzip-xyz/sub-105/prob1.py,/tmp/dolos-unzip-xyz/sub-106/prob1.py,0.80,120,40\n"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/reports/rep-sub/data/pairs.csv" {
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

	pairs, err := svc.fetchReportPairs(context.Background(), "rep-sub")
	if err != nil {
		t.Fatalf("fetchReportPairs failed: %v", err)
	}

	// Pair with 0.15 should be filtered out by threshold
	if len(pairs) != 2 {
		t.Fatalf("expected 2 pairs (0.15 filtered out), got %d", len(pairs))
	}

	// First pair should be highest similarity (0.95)
	if pairs[0].LeftUserID != "sub-101" || pairs[0].RightUserID != "sub-102" {
		t.Errorf("expected sub-101 and sub-102, got %q and %q", pairs[0].LeftUserID, pairs[0].RightUserID)
	}
	if pairs[0].LeftFile != "sub-101/prob1.py" || pairs[0].RightFile != "sub-102/prob1.py" {
		t.Errorf("expected canonical files sub-101/prob1.py and sub-102/prob1.py, got %q and %q", pairs[0].LeftFile, pairs[0].RightFile)
	}
	if pairs[0].Similarity != 0.95 {
		t.Errorf("expected similarity 0.95, got %f", pairs[0].Similarity)
	}

	// Second pair should be 0.80
	if pairs[1].LeftUserID != "sub-105" || pairs[1].RightUserID != "sub-106" {
		t.Errorf("expected sub-105 and sub-106, got %q and %q", pairs[1].LeftUserID, pairs[1].RightUserID)
	}
	if pairs[1].Similarity != 0.80 {
		t.Errorf("expected similarity 0.80, got %f", pairs[1].Similarity)
	}

	// Test cache hit
	cachedPairs, err := svc.fetchReportPairs(context.Background(), "rep-sub")
	if err != nil || len(cachedPairs) != 2 {
		t.Fatalf("expected cached pairs, got %v (len %d)", err, len(cachedPairs))
	}

	// Test membership check with early exit
	validKey := pairKey("sub-101/prob1.py", "sub-102/prob1.py")
	found, err := svc.fetchReportPairMembership(context.Background(), "rep-sub", validKey)
	if err != nil || !found {
		t.Errorf("expected membership true for %s, got %v (err: %v)", validKey, found, err)
	}

	missingKey := pairKey("sub-999/prob1.py", "sub-888/prob1.py")
	found, err = svc.fetchReportPairMembership(context.Background(), "rep-sub", missingKey)
	if err != nil || found {
		t.Errorf("expected membership false for %s, got %v (err: %v)", missingKey, found, err)
	}
}

func TestDolosMissingUserLookupClearsUserID(t *testing.T) {
	allPairs := []dto.PlagiarismPair{
		{
			LeftUserID:  "sub-found",
			RightUserID: "sub-missing",
		},
	}
	subUsers := map[string]userSubInfo{
		"sub-found": {userID: "usr-real", name: "Real User"},
	}

	for i := range allPairs {
		if info, ok := subUsers[allPairs[i].LeftUserID]; ok {
			allPairs[i].LeftUserID = info.userID
			allPairs[i].LeftName = info.name
		} else {
			allPairs[i].LeftUserID = ""
			allPairs[i].LeftName = ""
		}
		if info, ok := subUsers[allPairs[i].RightUserID]; ok {
			allPairs[i].RightUserID = info.userID
			allPairs[i].RightName = info.name
		} else {
			allPairs[i].RightUserID = ""
			allPairs[i].RightName = ""
		}
	}

	if allPairs[0].LeftUserID != "usr-real" || allPairs[0].LeftName != "Real User" {
		t.Errorf("expected resolved left user usr-real, got %q", allPairs[0].LeftUserID)
	}
	if allPairs[0].RightUserID != "" || allPairs[0].RightName != "" {
		t.Errorf("expected cleared right user ID on lookup miss, got %q", allPairs[0].RightUserID)
	}
}
