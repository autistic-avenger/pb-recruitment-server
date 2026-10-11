package judge0

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBatchBodyReadFailureIsAmbiguous(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`[{"token":"`))
	}))
	defer server.Close()
	t.Setenv("JUDGE0_URL", server.URL)
	t.Setenv("JUDGE0_AUTH_TOKEN", "")
	results, err := NewClient().CreateBatch(context.Background(), []SubmissionRequest{{LanguageID: 54}})
	if !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("body-read error = %v", err)
	}
	if len(results) != 1 || results[0].Token != "" || !errors.Is(results[0].Error, ErrUnavailable) {
		t.Fatalf("ambiguous result = %+v", results)
	}
}

func TestDispatchTimeout(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  time.Duration
	}{
		{"", 10 * time.Second},
		{"1000", time.Second},
		{"30000", 10 * time.Second},
		{"0", 10 * time.Second},
		{"-1", 10 * time.Second},
		{"invalid", 10 * time.Second},
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("JUDGE0_TIMEOUT_MS", tc.value)
			client := NewClient()
			if client.Timeout() != tc.want || client.httpClient.Timeout != tc.want {
				t.Fatalf("timeouts = %v/%v, want %v", client.Timeout(), client.httpClient.Timeout, tc.want)
			}
		})
	}
	if timeout := (*Client)(nil).Timeout(); timeout != 10*time.Second {
		t.Fatalf("fallback timeout = %v", timeout)
	}
}
