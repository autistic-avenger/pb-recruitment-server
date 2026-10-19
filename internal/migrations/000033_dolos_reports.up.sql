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
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (contest_id, problem_id, language)
);

CREATE INDEX contest_plagiarism_reports_work_idx
    ON contest_plagiarism_reports(status, next_retry_at)
    WHERE status IN ('pending', 'queued', 'running');
