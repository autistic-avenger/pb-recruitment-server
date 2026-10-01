package stores

import (
	"app/internal/models/dto"
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"strings"
)

type RankingStore struct {
	db *sql.DB
}

func NewRankingStore(db *sql.DB) *RankingStore {
	return &RankingStore{
		db: db,
	}
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

	return nil
}

const leaderboardPageSize = 20

func (s *RankingStore) GetLeaderboard(ctx context.Context, contestID string, page int) (*dto.GetLeaderboardResponse, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("ranking store: db is not initialized")
	}

	page = max(0, page)
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

	totalPages := int(math.Ceil(float64(totalCount) / float64(leaderboardPageSize)))

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

	if len(entries) == 0 {
		return &dto.GetLeaderboardResponse{
			Entries:    entries,
			Page:       page,
			TotalPages: totalPages,
			TotalCount: totalCount,
		}, nil
	}

	problemScores, err := s.getProblemScores(ctx, contestID, userIDs)
	if err != nil {
		log.Printf("ranking-store: problem scores query failed : %v", err)
		return nil, err
	}

	for i := range entries {
		uid := entries[i].UserID
		if scores, ok := problemScores[uid]; ok {
			entries[i].ProblemScores = scores
			var solved int
			var lastSub int64
			for _, ps := range scores {
				if ps.Score == ps.MaxScore {
					solved++
				}
				if ps.SolvedAt != nil && *ps.SolvedAt > lastSub {
					lastSub = *ps.SolvedAt
				}
			}
			entries[i].ProblemsSolved = solved
			entries[i].LastSubmissionTime = lastSub*1000 //convert s-> ms
		}
	}

	return &dto.GetLeaderboardResponse{
		Entries:    entries,
		Page:       page,
		TotalPages: totalPages,
		TotalCount: totalCount,
	}, nil
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
			COALESCE(
				CASE
					WHEN p.type = 'mcq' THEN
						MAX(CASE WHEN sub.status = 'accepted' THEN p.score ELSE 0 END)
					ELSE
						MAX(CASE WHEN sub.status = 'accepted' THEN p.score ELSE 0 END)
				END,
				0
			) AS score,
			MIN(CASE WHEN sub.status = 'accepted' THEN sub.created_at ELSE NULL END) AS solved_at
		FROM submissions sub
		INNER JOIN problems p ON sub.problem_id = p.id AND sub.contest_id = p.contest_id
		WHERE sub.contest_id = $1
		  AND sub.user_id IN (%s)
		GROUP BY sub.user_id, sub.problem_id, p.name, p.score, p.type
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
			ts := solvedAt.Int64
			ps.SolvedAt = &ts
		}

		result[userID] = append(result[userID], ps)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("problem scores rows error: %w", err)
	}

	return result, nil
}
