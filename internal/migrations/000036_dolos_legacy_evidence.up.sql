-- Reports created before run signatures existed cannot prove which source set
-- they analyzed. Remove only those unversioned rows so the worker regenerates.
DELETE FROM contest_plagiarism_pair_reviews r
WHERE NOT EXISTS (
    SELECT 1 FROM contest_plagiarism_runs run WHERE run.contest_id = r.contest_id
);

DELETE FROM contest_plagiarism_reports p
WHERE NOT EXISTS (
    SELECT 1 FROM contest_plagiarism_runs run WHERE run.contest_id = p.contest_id
);
