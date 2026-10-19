ALTER TABLE contest_plagiarism_pair_reviews DROP CONSTRAINT contest_plagiarism_pair_reviews_pkey;
ALTER TABLE contest_plagiarism_pair_reviews ADD PRIMARY KEY (contest_id, report_id, pair_key);

CREATE TABLE contest_plagiarism_runs (
    contest_id TEXT PRIMARY KEY REFERENCES contests(id) ON DELETE CASCADE,
    analyzed_end_time BIGINT NOT NULL,
    source_count BIGINT NOT NULL,
    latest_submission_at BIGINT NOT NULL,
    source_signature TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);