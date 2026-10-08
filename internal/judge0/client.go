package judge0

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Client struct {
	baseURL      string
	authToken    string
	callbackBase string
	httpClient   *http.Client
}

// Dispatch leaves ten seconds for DB bookkeeping within the browser's thirty-second timeout.
const maxDispatchTimeout = 10 * time.Second

func NewClient() *Client {
	timeout := maxDispatchTimeout
	if raw := os.Getenv("JUDGE0_TIMEOUT_MS"); raw != "" {
		if ms, err := time.ParseDuration(raw + "ms"); err == nil && ms > 0 {
			timeout = min(ms, maxDispatchTimeout)
		}
	}

	return &Client{
		baseURL:      os.Getenv("JUDGE0_URL"),
		authToken:    os.Getenv("JUDGE0_AUTH_TOKEN"),
		callbackBase: os.Getenv("JUDGE0_CALLBACK_BASE_URL"),
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

func (c *Client) Timeout() time.Duration {
	if c == nil || c.httpClient == nil || c.httpClient.Timeout <= 0 {
		return maxDispatchTimeout
	}
	return min(c.httpClient.Timeout, maxDispatchTimeout)
}

func (c *Client) CallbackURL(executionID string) string {
	if c.callbackBase == "" {
		return ""
	}
	return fmt.Sprintf("%s/internal/judge0/callback/%s", c.callbackBase, executionID)
}

func (c *Client) batchSize() int {
	const defaultSize = 20
	raw := os.Getenv("JUDGE0_BATCH_SIZE")
	if raw == "" {
		return defaultSize
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return defaultSize
	}
	return n
}

func (c *Client) CreateBatch(ctx context.Context, jobs []SubmissionRequest) ([]SubmissionResult, error) {
	if c.baseURL == "" {
		return nil, fmt.Errorf("%w: JUDGE0_URL is not set", ErrUnavailable)
	}
	if len(jobs) == 0 {
		return []SubmissionResult{}, nil
	}

	results := make([]SubmissionResult, len(jobs))
	size := c.batchSize()

	for start := 0; start < len(jobs); start += size {
		end := start + size
		if end > len(jobs) {
			end = len(jobs)
		}
		chunk := jobs[start:end]

		chunkResults, err := c.postBatch(ctx, chunk)
		if err != nil {
			for i := start; i < len(jobs); i++ {
				if results[i].Token == "" && results[i].Error == nil {
					results[i] = SubmissionResult{Error: err}
				}
			}
			return results, err
		}

		copy(results[start:end], chunkResults)
	}

	return results, nil
}

func (c *Client) postBatch(ctx context.Context, jobs []SubmissionRequest) ([]SubmissionResult, error) {
	payload, err := json.Marshal(map[string][]SubmissionRequest{
		"submissions": jobs,
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/submissions/batch?base64_encoded=false", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.authToken != "" {
		req.Header.Set("X-Auth-Token", c.authToken)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: status %d: %s", ErrUnavailable, resp.StatusCode, string(body))
	}

	var tokens []batchTokenResponse
	if err := json.Unmarshal(body, &tokens); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}

	results := make([]SubmissionResult, len(jobs))
	for i := range jobs {
		if i >= len(tokens) || tokens[i].Token == "" {
			results[i] = SubmissionResult{Error: ErrInvalidResponse}
			continue
		}
		results[i] = SubmissionResult{Token: tokens[i].Token}
	}
	return results, nil
}
