package judge0

import (
	"testing"
	"time"
)

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
