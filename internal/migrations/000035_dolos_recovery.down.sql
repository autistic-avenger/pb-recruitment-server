DROP TABLE IF EXISTS contest_plagiarism_runs;
ALTER TABLE contest_plagiarism_pair_reviews DROP CONSTRAINT contest_plagiarism_pair_reviews_pkey;
ALTER TABLE contest_plagiarism_pair_reviews ADD PRIMARY KEY (contest_id, pair_key);