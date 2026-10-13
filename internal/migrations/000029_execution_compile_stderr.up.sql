ALTER TABLE submission_executions
    ADD COLUMN compile_output TEXT,
    ADD COLUMN stderr TEXT;
COMMENT ON COLUMN submission_executions.compile_output IS 'Truncated Judge0 compile_output;not to be returned by submission details.';
COMMENT ON COLUMN submission_executions.stderr IS 'Truncated Judge0 stderr; internal only, not to be returned by submission details.';
