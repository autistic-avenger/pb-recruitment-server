CREATE TABLE IF NOT EXISTS submission_rate_limits (
    user_id TEXT PRIMARY KEY,
    last_submission_at BIGINT NOT NULL
);
