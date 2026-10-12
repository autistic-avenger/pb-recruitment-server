package stores

import (
	"app/internal/common"
	"app/internal/models"
	"app/internal/models/dto"
	"context"
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type leaderboardCacheEntry struct {
	data      *dto.GetLeaderboardResponse
	expiresAt time.Time
}

type RankingStore struct {
	db       *sql.DB
	cache    map[string]leaderboardCacheEntry
	cacheMu  sync.RWMutex
	cacheTTL time.Duration
}

func NewRankingStore(db *sql.DB) *RankingStore {
	if db != nil {
		_, _ = db.Exec("ALTER TABLE rankings ADD COLUMN IF NOT EXISTS rank INT DEFAULT 0")
	ttl := 3 * time.Second
	if ttlStr := os.Getenv("LEADERBOARD_CACHE_TTL_SECONDS"); ttlStr != "" {
		if sec, err := strconv.Atoi(ttlStr); err == nil && sec >= 0 {
			if sec > 60 {
				sec = 60
			}
			ttl = time.Duration(sec) * time.Second
		}
	}
	return &RankingStore{
		db:       db,
		cache:    make(map[string]leaderboardCacheEntry),
		cacheTTL: ttl,
	}
}

func (s *RankingStore) SetCacheTTL(d time.Duration) {
	if s == nil {
		return
	}
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	s.cacheTTL = d
}

func (s *RankingStore) InvalidateCache(contestID string) {
	if s == nil {
		return
	}
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	if contestID == "" {
		s.cache = make(map[string]leaderboardCacheEntry)
		return
	}
	prefix := contestID + ":"
	for k := range s.cache {
		if strings.HasPrefix(k, prefix) {
			delete(s.cache, k)
		}
	}
}

func cloneLeaderboardResponse(res *dto.GetLeaderboardResponse) *dto.GetLeaderboardResponse {
	if res == nil {
		return nil
	}
	clone := *res
	clone.Entries = make([]dto.LeaderboardEntry, len(res.Entries))
	for i, e := range res.Entries {
		entryCopy := e
		if e.ProblemScores != nil {
			entryCopy.ProblemScores = make([]dto.ProblemScore, len(e.ProblemScores))
			copy(entryCopy.ProblemScores, e.ProblemScores)
		}
		clone.Entries[i] = entryCopy
	}
	return &clone
}

func (s *RankingStore) InvalidateCache(contestID string) {
	// Invalidate leaderboard cache if enabled
}

func (s *RankingStore) UpdateLeaderboardUser(ctx context.Context, contestID string, userID string, req *dto.UpdateLeaderboardUserRequest) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("ranking store: db is not initialized")
	}

	query := "UPDATE rankings SET "
	args := []interface{}{}
	argID := 1

	if req.Hidden != nil {
		query += fmt.Sprintf("hidden = $%d, ", argID)
		args = append(args, *req.Hidden)
		argID++
	}
	if req.Disqualified != nil {
		query += fmt.Sprintf("disqualified = $%d, ", argID)
		args = append(args, *req.Disqualified)
		argID++
	}

	if len(args) == 0 {
		return fmt.Errorf("no fields to update")
	}

	query = strings.TrimSuffix(query, ", ")

	query += fmt.Sprintf(" WHERE contest_id = $%d AND user_id = $%d", argID, argID+1)
	args = append(args, contestID, userID)

	_, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Printf("ranking-store: update failed: %v", err)
		return fmt.Errorf("update ranking: %w", err)
	}

	s.InvalidateCache(contestID)

	return nil
}

const leaderboardPageSize = 20

func (s *RankingStore) GetLeaderboard(ctx context.Context, contestID string, page int) (*dto.GetLeaderboardResponse, error) {
	if s == nil {
		return nil, fmt.Errorf("ranking store: not initialized")
	}

	page = max(0, page)
	cacheKey := fmt.Sprintf("%s:%d", contestID, page)

	if s.cacheTTL > 0 {
		s.cacheMu.RLock()
		entry, ok := s.cache[cacheKey]
		s.cacheMu.RUnlock()
		if ok && time.Now().Before(entry.expiresAt) {
			return cloneLeaderboardResponse(entry.data), nil
		}
	}

	if s.db == nil {
		return nil, fmt.Errorf("ranking store: db is not initialized")
	}

	const checkContestQ = `SELECT EXISTS(SELECT 1 FROM contests WHERE id = $1)`
	var exists bool
	if err := s.db.QueryRowContext(ctx, checkContestQ, contestID).Scan(&exists); err != nil {
		log.Printf("ranking-store: check contest query failed: %v", err)
		return nil, fmt.Errorf("check contest: %w", err)
	}
	if !exists {
		return nil, common.ContestNotFoundError
	}

	offset := page * leaderboardPageSize

	const countQ = `
		SELECT COUNT(*)
		FROM rankings r
		INNER JOIN users u ON r.user_id = u.id
		WHERE r.contest_id = $1
		  AND r.hidden = false
		  AND r.disqualified = false
	`
	var totalCount int
	err := s.db.QueryRowContext(ctx, countQ, contestID).Scan(&totalCount)
	if err != nil {
		log.Printf("ranking-store: count query failed: %v", err)
		return nil, fmt.Errorf("count rankings: %w", err)
	}

	totalPages := (totalCount + leaderboardPageSize - 1) / leaderboardPageSize

	const entriesQ = `
		SELECT COALESCE(NULLIF(r.rank, 0), RANK() OVER (ORDER BY r.score DESC)) AS rank, r.user_id, u.name,u.usn,r.score
		FROM rankings r
		INNER JOIN users u 
		ON r.user_id = u.id
		WHERE r.contest_id = $1
		AND r.hidden = false
		AND r.disqualified = false
		ORDER BY rank ASC, u.name ASC, r.user_id ASC
		LIMIT $2 OFFSET $3
	`

	rows, err := s.db.QueryContext(ctx, entriesQ, contestID, leaderboardPageSize, offset)
	if err != nil {
		log.Printf("ranking-store: leaderboard query failed: %v", err)
		return nil, fmt.Errorf("query leaderboard: %w", err)
	}
	defer rows.Close()

	entries := make([]dto.LeaderboardEntry, 0, leaderboardPageSize)
	userIDs := make([]string, 0, leaderboardPageSize)

	for rows.Next() {
		var e dto.LeaderboardEntry
		if err := rows.Scan(&e.Rank, &e.UserID, &e.Username, &e.USN, &e.TotalScore); err != nil {
			log.Printf("ranking-store: row scan failed: %v", err)
			return nil, fmt.Errorf("scan leaderboard row: %w", err)
		}
		entries = append(entries, e)
		userIDs = append(userIDs, e.UserID)
	}
	if err := rows.Err(); err != nil {
		log.Printf("ranking-store: rows error: %v", err)
		return nil, fmt.Errorf("rows error: %w", err)
	}

	problemScores, err := s.getProblemScores(ctx, contestID, userIDs)
	if err != nil {
		log.Printf("ranking-store: problem scores query failed : %v", err)
		return nil, err
	}

	for i := range entries {
		scores, ok := problemScores[entries[i].UserID]
		if !ok {
			continue
		}
		entries[i].ProblemScores = scores
		entries[i].ProblemsSolved, entries[i].LastSubmissionTime = summarizeSolves(scores)
	}

	resp := &dto.GetLeaderboardResponse{
		Entries:    entries,
		Page:       page,
		TotalPages: totalPages,
		TotalCount: totalCount,
	}

	if s.cacheTTL > 0 && page < max(totalPages, 1) {
		s.cacheMu.Lock()
		s.cache[cacheKey] = leaderboardCacheEntry{
			data:      resp,
			expiresAt: time.Now().Add(s.cacheTTL),
		}
		s.cacheMu.Unlock()
	}

	return cloneLeaderboardResponse(resp), nil
}

// summarizeSolves reports how many problems the user solved and the time at
// which they reached their final score: the latest of their per-problem first
// solves. Resubmitting an already-solved problem must not push that later.
// A problem counts as solved only when it has an accepted submission, so a
// zero-point problem is not mistaken for one. lastSolve is 0 when nothing was
// solved.
func summarizeSolves(scores []dto.ProblemScore) (solved int, lastSolve int64) {
	for _, ps := range scores {
		if ps.SolvedAt == nil {
			continue
		}
		solved++
		if *ps.SolvedAt > lastSolve {
			lastSolve = *ps.SolvedAt
		}
	}
	return solved, lastSolve
}

func (s *RankingStore) getProblemScores(ctx context.Context, contestID string, userIDs []string) (map[string][]dto.ProblemScore, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}

	placeholders := make([]string, len(userIDs))
	args := make([]interface{}, 0, len(userIDs)+1)
	args = append(args, contestID)
	for i, uid := range userIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args = append(args, uid)
	}

	query := fmt.Sprintf(`
		SELECT
			sub.user_id,
			sub.problem_id,
			p.name,
			p.score AS max_score,
			COUNT(sub.id) AS attempts,
			MAX(CASE WHEN sub.status = 'accepted' THEN p.score ELSE 0 END) AS score,
			MIN(CASE WHEN sub.status = 'accepted' THEN sub.created_at ELSE NULL END) AS solved_at
		FROM submissions sub
		INNER JOIN problems p ON sub.problem_id = p.id AND sub.contest_id = p.contest_id
		WHERE sub.contest_id = $1
		  AND sub.user_id IN (%s)
		GROUP BY sub.user_id, sub.problem_id, p.name, p.score
		ORDER BY sub.user_id, p.name
	`, strings.Join(placeholders, ", "))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query problem scores: %w", err)
	}
	defer rows.Close()

	result := make(map[string][]dto.ProblemScore, len(userIDs))
	for rows.Next() {
		var userID string
		var ps dto.ProblemScore
		var solvedAt sql.NullInt64

		if err := rows.Scan(
			&userID,
			&ps.ProblemID,
			&ps.ProblemName,
			&ps.MaxScore,
			&ps.Attempts,
			&ps.Score,
			&solvedAt,
		); err != nil {
			log.Printf("ranking-store: problem score row scan failed: %v", err)
			return nil, fmt.Errorf("scan problem score row: %w", err)
		}

		if solvedAt.Valid {
			ts := solvedAt.Int64 * 1000
			ps.SolvedAt = &ts
		}

		result[userID] = append(result[userID], ps)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("problem scores rows error: %w", err)
	}

	return result, nil
}

type participantSummary struct {
	UserID             string
	TotalScore         int
	ProblemsSolved     int
	LastSubmissionTime int64
	TotalAttempts      int
	Rank               int
	Hidden             bool
	Disqualified       bool
	Shortlisted        bool
}

func isTied(a, b participantSummary) bool {
	return a.TotalScore == b.TotalScore &&
		a.ProblemsSolved == b.ProblemsSolved &&
		a.LastSubmissionTime == b.LastSubmissionTime &&
		a.TotalAttempts == b.TotalAttempts
}

func (s *RankingStore) ConsolidateContest(ctx context.Context, contestID string, regradeMCQs bool) (*dto.ConsolidationResponse, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("ranking store: db is not initialized")
	}

	const checkContestQ = `SELECT EXISTS(SELECT 1 FROM contests WHERE id = $1)`
	var exists bool
	if err := s.db.QueryRowContext(ctx, checkContestQ, contestID).Scan(&exists); err != nil {
		return nil, fmt.Errorf("check contest: %w", err)
	}
	if !exists {
		return nil, common.ContestNotFoundError
	}

	_, _ = s.db.ExecContext(ctx, `ALTER TABLE rankings ADD COLUMN IF NOT EXISTS rank INT DEFAULT 0`)

	mcqsGraded, err := s.gradeContestMCQs(ctx, contestID, regradeMCQs)
	if err != nil {
		return nil, fmt.Errorf("grade MCQs: %w", err)
	}

	rankedCount, err := s.computeAndSaveRanks(ctx, contestID)
	if err != nil {
		return nil, fmt.Errorf("compute and save ranks: %w", err)
	}

	s.InvalidateCache(contestID)

	return &dto.ConsolidationResponse{
		ContestID:          contestID,
		MCQsGraded:         mcqsGraded,
		ParticipantsRanked: rankedCount,
		ProcessedAt:        time.Now().Unix(),
		Status:             "success",
	}, nil
}

func (s *RankingStore) gradeContestMCQs(ctx context.Context, contestID string, regrade bool) (int, error) {
	query := `
		SELECT sub.id, sub.choices, p.answer
		FROM submissions sub
		INNER JOIN problems p ON p.id = sub.problem_id AND p.contest_id = sub.contest_id
		WHERE sub.contest_id = $1 AND sub.type = 'mcq' AND p.type = 'mcq'
	`
	if !regrade {
		query += ` AND sub.status = 'pending'`
	}

	rows, err := s.db.QueryContext(ctx, query, contestID)
	if err != nil {
		return 0, fmt.Errorf("query MCQ submissions: %w", err)
	}
	defer rows.Close()

	type mcqGradingResult struct {
		id     string
		status models.SubmissionStatus
	}

	var results []mcqGradingResult
	for rows.Next() {
		var id string
		var choices, answer pq.Int64Array
		if err := rows.Scan(&id, &choices, &answer); err != nil {
			return 0, fmt.Errorf("scan MCQ submission: %w", err)
		}
		status := models.WrongAnswer
		if mcqAnswersMatch(choices, answer) {
			status = models.Accepted
		}
		results = append(results, mcqGradingResult{id: id, status: status})
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("MCQ iteration error: %w", err)
	}

	if len(results) == 0 {
		return 0, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin MCQ update tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `UPDATE submissions SET status = $2 WHERE id = $1`)
	if err != nil {
		return 0, fmt.Errorf("prepare MCQ status update: %w", err)
	}
	defer stmt.Close()

	for _, res := range results {
		if _, err := stmt.ExecContext(ctx, res.id, res.status); err != nil {
			return 0, fmt.Errorf("update submission %s: %w", res.id, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit MCQ grading: %w", err)
	}

	return len(results), nil
}

func (s *RankingStore) computeAndSaveRanks(ctx context.Context, contestID string) (int, error) {
	const problemsQ = `SELECT id, score FROM problems WHERE contest_id = $1`
	pRows, err := s.db.QueryContext(ctx, problemsQ, contestID)
	if err != nil {
		return 0, fmt.Errorf("query problems: %w", err)
	}
	defer pRows.Close()

	problemScores := make(map[string]int)
	for pRows.Next() {
		var pid string
		var pscore int
		if err := pRows.Scan(&pid, &pscore); err != nil {
			return 0, fmt.Errorf("scan problem: %w", err)
		}
		problemScores[pid] = pscore
	}
	if err := pRows.Err(); err != nil {
		return 0, fmt.Errorf("problems iteration: %w", err)
	}


	participantSet := make(map[string]bool)

	regRows, err := s.db.QueryContext(ctx, `SELECT user_id FROM contest_registrations WHERE contest_id = $1`, contestID)
	if err == nil {
		for regRows.Next() {
			var uid string
			if err := regRows.Scan(&uid); err == nil {
				participantSet[uid] = true
			}
		}
		regRows.Close()
	}

	subUserRows, err := s.db.QueryContext(ctx, `SELECT DISTINCT user_id FROM submissions WHERE contest_id = $1`, contestID)
	if err != nil {
		return 0, fmt.Errorf("query submission users: %w", err)
	}
	for subUserRows.Next() {
		var uid string
		if err := subUserRows.Scan(&uid); err == nil {
			participantSet[uid] = true
		}
	}
	subUserRows.Close()

	rankUserRows, err := s.db.QueryContext(ctx, `SELECT user_id FROM rankings WHERE contest_id = $1`, contestID)
	if err == nil {
		for rankUserRows.Next() {
			var uid string
			if err := rankUserRows.Scan(&uid); err == nil {
				participantSet[uid] = true
			}
		}
		rankUserRows.Close()
	}

	if len(participantSet) == 0 {
		return 0, nil
	}

	const submissionsQ = `
		SELECT user_id, problem_id, status, created_at
		FROM submissions
		WHERE contest_id = $1
		ORDER BY created_at ASC
	`
	sRows, err := s.db.QueryContext(ctx, submissionsQ, contestID)
	if err != nil {
		return 0, fmt.Errorf("query contest submissions: %w", err)
	}
	defer sRows.Close()

	type userSolveData struct {
		earliestAccepted map[string]int64 
		totalAttempts    int
	}

	userStats := make(map[string]*userSolveData)
	for uid := range participantSet {
		userStats[uid] = &userSolveData{
			earliestAccepted: make(map[string]int64),
		}
	}

	for sRows.Next() {
		var uid, pid, status string
		var createdAt int64
		if err := sRows.Scan(&uid, &pid, &status, &createdAt); err != nil {
			return 0, fmt.Errorf("scan submission: %w", err)
		}
		data, ok := userStats[uid]
		if !ok {
			data = &userSolveData{earliestAccepted: make(map[string]int64)}
			userStats[uid] = data
			participantSet[uid] = true
		}
		data.totalAttempts++
		if status == string(models.Accepted) {
			if prev, exists := data.earliestAccepted[pid]; !exists || createdAt < prev {
				data.earliestAccepted[pid] = createdAt
			}
		}
	}
	if err := sRows.Err(); err != nil {
		return 0, fmt.Errorf("submissions iteration: %w", err)
	}

	existingFlags := make(map[string]struct {
		hidden       bool
		disqualified bool
		shortlisted  bool
	})
	flagRows, err := s.db.QueryContext(ctx, `SELECT user_id, hidden, disqualified, shortlisted FROM rankings WHERE contest_id = $1`, contestID)
	if err == nil {
		for flagRows.Next() {
			var uid string
			var h, d, s bool
			if err := flagRows.Scan(&uid, &h, &d, &s); err == nil {
				existingFlags[uid] = struct {
					hidden       bool
					disqualified bool
					shortlisted  bool
				}{h, d, s}
			}
		}
		flagRows.Close()
	}

	participants := make([]participantSummary, 0, len(participantSet))
	for uid := range participantSet {
		data := userStats[uid]
		score := 0
		solvedCount := 0
		var maxSolveTime int64

		for pid, solveTime := range data.earliestAccepted {
			if pts, ok := problemScores[pid]; ok {
				score += pts
				solvedCount++
				if solveTime > maxSolveTime {
					maxSolveTime = solveTime
				}
			}
		}

		flags := existingFlags[uid]
		participants = append(participants, participantSummary{
			UserID:             uid,
			TotalScore:         score,
			ProblemsSolved:     solvedCount,
			LastSubmissionTime: maxSolveTime,
			TotalAttempts:      data.totalAttempts,
			Hidden:             flags.hidden,
			Disqualified:       flags.disqualified,
			Shortlisted:        flags.shortlisted,
		})
	}

	sort.SliceStable(participants, func(i, j int) bool {
		a, b := participants[i], participants[j]
		if a.Disqualified != b.Disqualified {
			return !a.Disqualified && b.Disqualified
		}
		if a.TotalScore != b.TotalScore {
			return a.TotalScore > b.TotalScore
		}
		if a.ProblemsSolved != b.ProblemsSolved {
			return a.ProblemsSolved > b.ProblemsSolved
		}
		if (a.ProblemsSolved > 0) != (b.ProblemsSolved > 0) {
			return a.ProblemsSolved > 0
		}
		if a.LastSubmissionTime != b.LastSubmissionTime {
			return a.LastSubmissionTime < b.LastSubmissionTime
		}
		if a.TotalAttempts != b.TotalAttempts {
			return a.TotalAttempts < b.TotalAttempts
		}
		return a.UserID < b.UserID
	})

	for i := range participants {
		if participants[i].Disqualified {
			participants[i].Rank = 0
			continue
		}
		if i > 0 && !participants[i-1].Disqualified && isTied(participants[i], participants[i-1]) {
			participants[i].Rank = participants[i-1].Rank
		} else {
			participants[i].Rank = i + 1
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	const upsertRankQ = `
		INSERT INTO rankings (contest_id, user_id, score, rank, hidden, disqualified, shortlisted)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (contest_id, user_id)
		DO UPDATE SET 
			score = EXCLUDED.score,
			rank = EXCLUDED.rank,
			hidden = rankings.hidden,
			disqualified = rankings.disqualified,
			shortlisted = rankings.shortlisted
	`

	stmt, err := tx.PrepareContext(ctx, upsertRankQ)
	if err != nil {
		return 0, fmt.Errorf("prepare upsert: %w", err)
	}
	defer stmt.Close()

	for _, p := range participants {
		if _, err := stmt.ExecContext(ctx, contestID, p.UserID, p.TotalScore, p.Rank, p.Hidden, p.Disqualified, p.Shortlisted); err != nil {
			return 0, fmt.Errorf("upsert ranking for user %s: %w", p.UserID, err)
		}
	}

	_, _ = tx.ExecContext(ctx, `REFRESH MATERIALIZED VIEW ranking_mv`)

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit rankings: %w", err)
	}

	return len(participants), nil
}
