CREATE TABLE contest_plagiarism_runs (
    contest_id TEXT PRIMARY KEY REFERENCES contests(id) ON DELETE CASCADE,
    analyzed_end_time BIGINT NOT NULL,
    source_count BIGINT NOT NULL,
    latest_submission_at BIGINT NOT NULL,
    source_signature TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE contest_plagiarism_reports (
    contest_id TEXT NOT NULL REFERENCES contests(id) ON DELETE CASCADE,
    problem_id TEXT NOT NULL,
    language TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    dolos_report_id TEXT,
    report_url TEXT,
    error TEXT,
    claim_token TEXT,
    attempts INT NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (contest_id, problem_id, language)
);

CREATE INDEX contest_plagiarism_reports_work_idx
    ON contest_plagiarism_reports(status, next_retry_at)
    WHERE status IN ('pending', 'queued', 'running');

CREATE TABLE contest_plagiarism_pair_reviews (
    contest_id TEXT NOT NULL REFERENCES contests(id) ON DELETE CASCADE,
    report_id TEXT NOT NULL DEFAULT '',
    pair_key TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('cleared', 'confirmed', 'unreviewed')),
    note TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (contest_id, report_id, pair_key)
);

CREATE INDEX IF NOT EXISTS submissions_contest_type_created_idx
    ON submissions(contest_id, type, created_at);
