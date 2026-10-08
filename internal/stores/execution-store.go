package stores

import (
	"app/internal/models"
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

type ExecutionStore struct {
	db *sql.DB
}

func NewExecutionStore(db *sql.DB) *ExecutionStore {
	return &ExecutionStore{db: db}
}

func (s *ExecutionStore) InsertBatch(ctx context.Context, submissionID string, indexes []int) ([]models.Execution, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("execution store: db is not initialized")
	}
	if len(indexes) == 0 {
		return []models.Execution{}, nil
	}

	now := time.Now().Unix()
	executions := make([]models.Execution, len(indexes))
	ids := make([]string, len(indexes))
	created := make([]int64, len(indexes))

	for i, idx := range indexes {
		id := uuid.NewString()
		ids[i] = id
		created[i] = now
		executions[i] = models.Execution{
			ID:            id,
			SubmissionID:  submissionID,
			TestCaseIndex: idx,
			Status:        "pending",
			CreatedAt:     now,
		}
	}

	const q = `
		INSERT INTO submission_executions (id, submission_id, test_case_index, status, created_at)
		SELECT unnest($1::uuid[]), $2, unnest($3::int[]), 'pending', unnest($4::bigint[])
	`

	_, err := s.db.ExecContext(ctx, q, pq.Array(ids), submissionID, pq.Array(indexes), pq.Array(created))
	if err != nil {
		return nil, fmt.Errorf("insert executions: %w", err)
	}

	return executions, nil
}

func (s *ExecutionStore) SaveTokens(ctx context.Context, tokens map[string]string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("execution store: db is not initialized")
	}
	if len(tokens) == 0 {
		return nil
	}

	ids := make([]string, 0, len(tokens))
	values := make([]string, 0, len(tokens))
	for id, token := range tokens {
		ids = append(ids, id)
		values = append(values, token)
	}

	const q = `
		UPDATE submission_executions AS e
		SET judge0_token = data.token
		FROM (
			SELECT unnest($1::uuid[]) AS id, unnest($2::text[]) AS token
		) AS data
		WHERE e.id = data.id
	`

	_, err := s.db.ExecContext(ctx, q, pq.Array(ids), pq.Array(values))
	if err != nil {
		return fmt.Errorf("save judge0 tokens: %w", err)
	}
	return nil
}

func (s *ExecutionStore) MarkFailed(ctx context.Context, ids []string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("execution store: db is not initialized")
	}
	if len(ids) == 0 {
		return nil
	}

	const q = `
		WITH failed AS (
			UPDATE submission_executions
			SET status = 'judge_error'
			WHERE id = ANY($1::uuid[])
			  AND judge0_token IS NULL
			RETURNING submission_id, id
		)
		UPDATE submissions
		SET status = 'judge_error'
		WHERE id IN (SELECT submission_id FROM failed)
		  AND status = 'pending'
		  AND NOT EXISTS (
			SELECT 1
			FROM submission_executions e
			WHERE e.submission_id = submissions.id
			  AND e.id NOT IN (SELECT id FROM failed)
			  AND (e.status = 'pending' OR e.judge0_token IS NOT NULL)
		  )
	`

	_, err := s.db.ExecContext(ctx, q, pq.Array(ids))
	if err != nil {
		return fmt.Errorf("mark failed executions: %w", err)
	}
	return nil
}
