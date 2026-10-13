ALTER TABLE submission_executions
    DROP COLUMN IF EXISTS compile_output,
    DROP COLUMN IF EXISTS stderr;
