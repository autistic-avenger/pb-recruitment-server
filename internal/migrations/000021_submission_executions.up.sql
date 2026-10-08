CREATE TABLE submission_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id TEXT NOT NULL REFERENCES submissions(id) ON DELETE CASCADE,
    test_case_index INT NOT NULL,
    judge0_token TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    runtime BIGINT,
    memory BIGINT,
    created_at BIGINT NOT NULL,
    UNIQUE (submission_id, test_case_index)
);

CREATE INDEX submission_executions_judge0_token_idx
    ON submission_executions (judge0_token);
