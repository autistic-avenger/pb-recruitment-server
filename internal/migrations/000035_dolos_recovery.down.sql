DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM contest_plagiarism_pair_reviews
        GROUP BY contest_id, pair_key
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'cannot downgrade Dolos review keys: multiple reports have decisions for the same pair';
    END IF;
END $$;

DROP TABLE IF EXISTS contest_plagiarism_runs;
ALTER TABLE contest_plagiarism_pair_reviews DROP CONSTRAINT contest_plagiarism_pair_reviews_pkey;
ALTER TABLE contest_plagiarism_pair_reviews ADD PRIMARY KEY (contest_id, pair_key);
