package stores

import (
	"app/internal/common"
	"app/internal/models/dto"
	"context"
	"database/sql"
	"fmt"
	"log"
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
		SELECT RANK() OVER (ORDER BY r.score DESC) AS rank, r.user_id, u.name,u.usn,r.score
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
