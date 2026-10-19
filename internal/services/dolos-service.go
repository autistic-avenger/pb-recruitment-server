package services

import (
	"app/internal/common"
	"app/internal/models/dto"
	"app/internal/s3"
	"archive/zip"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/gommon/log"
	"go.uber.org/fx"
)

type DolosService struct {
	db      *sql.DB
	s3      *s3.S3
	client  *http.Client
	baseURL string
}

type DolosReport = dto.DolosContestReportResponse

type dolosReportResponse struct {
	ID      string `json:"id"`
	URL     string `json:"url"`
	HTMLURL string `json:"html_url"`
	Status  string `json:"status"`
	Error   string `json:"error"`
}

type dolosUploadError struct {
	err       error
	ambiguous bool
	retryable bool
}

func (e *dolosUploadError) Error() string { return e.err.Error() }
func (e *dolosUploadError) Unwrap() error { return e.err }

type dolosSource struct {
	id        string
	userID    string
	problemID string
	language  string
}

func NewDolosService(db *sql.DB, objectStore *s3.S3) *DolosService {
	base := strings.TrimRight(os.Getenv("DOLOS_API_URL"), "/")
	return &DolosService{
		db:      db,
		s3:      objectStore,
		baseURL: base,
		client:  &http.Client{Timeout: 65 * time.Second},
	}
}

func generateClaimToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (d *DolosService) checkContestExists(ctx context.Context, contestID string) error {
	var dummy int
	err := d.db.QueryRowContext(ctx, `SELECT 1 FROM contests WHERE id = $1`, contestID).Scan(&dummy)
	if err == sql.ErrNoRows {
		return common.ContestNotFoundError
	}
	return err
}

func (d *DolosService) GetReport(ctx context.Context, contestID string) (*DolosReport, error) {
	if err := d.checkContestExists(ctx, contestID); err != nil {
		return nil, err
	}
	fresh, err := d.contestEvidenceCurrent(ctx, contestID)
	if err != nil {
		return nil, err
	}
	if !fresh {
		return &DolosReport{Status: "processing", Reports: []dto.ProblemReportSummary{}}, nil
	}

	rows, err := d.db.QueryContext(ctx, `
		SELECT problem_id, language, status, COALESCE(report_url, ''), COALESCE(error, '')
		FROM contest_plagiarism_reports
		WHERE contest_id = $1
		ORDER BY problem_id, language
	`, contestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reports []dto.ProblemReportSummary
	for rows.Next() {
		var s dto.ProblemReportSummary
		if err := rows.Scan(&s.ProblemID, &s.Language, &s.Status, &s.ReportURL, &s.Error); err != nil {
			return nil, err
		}
		reports = append(reports, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(reports) == 0 {
		var settled bool
		if err := d.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM contest_plagiarism_runs WHERE contest_id=$1)`, contestID).Scan(&settled); err != nil {
			return nil, err
		}
		if settled {
			return &DolosReport{Status: "skipped", Reports: []dto.ProblemReportSummary{}}, nil
		}
		return &DolosReport{
			Status:  "pending",
			Reports: []dto.ProblemReportSummary{},
		}, nil
	}

	allFinished := true
	allSkipped := true
	hasRunning := false
	hasFailed := false
	var firstURL string
	var errMsgs []string

	for _, r := range reports {
		if r.Status != "finished" {
			allFinished = false
		}
		if r.Status != "skipped" {
			allSkipped = false
		}
		if r.Status == "queued" || r.Status == "running" || r.Status == "processing" || r.Status == "pending" || r.Status == "unknown" {
			hasRunning = true
		}
		if r.Status == "failed" || r.Status == "error" {
			hasFailed = true
		}
		if r.ReportURL != "" && firstURL == "" {
			firstURL = r.ReportURL
		}
		if r.Error != "" {
			errMsgs = append(errMsgs, fmt.Sprintf("%s/%s: %s", r.ProblemID, r.Language, r.Error))
		}
	}

	overallStatus := "processing"
	if allSkipped {
		overallStatus = "skipped"
	} else if allFinished || (!hasRunning && !hasFailed && !hasUnknownOrPurged(reports)) {
		overallStatus = "finished"
	} else if !hasRunning && (hasFailed || hasUnknownOrPurged(reports)) {
		overallStatus = "failed"
	}

	var combinedErr string
	if len(errMsgs) > 0 {
		combinedErr = strings.Join(errMsgs, "; ")
	}

	return &DolosReport{
		Status:    overallStatus,
		ReportURL: firstURL,
		Error:     combinedErr,
		Reports:   reports,
	}, nil
}

func (d *DolosService) contestEvidenceCurrent(ctx context.Context, contestID string) (bool, error) {
	var exists bool
	var analyzedEnd int64
	var sourceCount, latestSubmission int64
	err := d.db.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM contest_plagiarism_runs r WHERE r.contest_id=$1),
		       COALESCE((SELECT analyzed_end_time FROM contest_plagiarism_runs WHERE contest_id=$1),0),
		       (SELECT COUNT(*) FROM submissions WHERE contest_id=$1 AND type='code'),
		       COALESCE((SELECT MAX(created_at) FROM submissions WHERE contest_id=$1 AND type='code'),0)
	`, contestID).Scan(&exists, &analyzedEnd, &sourceCount, &latestSubmission)
	if err != nil {
		return false, err
	}
	if !exists {
		return true, nil
	}
	var recordedCount, recordedLatest int64
	if err := d.db.QueryRowContext(ctx, `SELECT source_count,latest_submission_at FROM contest_plagiarism_runs WHERE contest_id=$1`, contestID).Scan(&recordedCount, &recordedLatest); err != nil {
		return false, err
	}
	var currentEnd int64
	if err := d.db.QueryRowContext(ctx, `SELECT end_time FROM contests WHERE id=$1`, contestID).Scan(&currentEnd); err != nil {
		return false, err
	}
	return analyzedEnd == currentEnd && recordedCount == sourceCount && recordedLatest == latestSubmission, nil
}

func hasUnknownOrPurged(reports []dto.ProblemReportSummary) bool {
	for _, r := range reports {
		if r.Status == "unknown" || r.Status == "purged" {
			return true
		}
	}
	return false
}

func (d *DolosService) GetPairs(ctx context.Context, contestID string, pages ...int) ([]dto.PlagiarismPair, error) {
	page := 0
	if len(pages) > 0 && pages[0] > 0 {
		page = pages[0]
	}
	if err := d.checkContestExists(ctx, contestID); err != nil {
		return nil, err
	}
	fresh, err := d.contestEvidenceCurrent(ctx, contestID)
	if err != nil {
		return nil, err
	}
	if !fresh {
		return nil, common.ErrReportNotReady
	}

	rows, err := d.db.QueryContext(ctx, `
		SELECT problem_id, language, status, COALESCE(dolos_report_id, '')
		FROM contest_plagiarism_reports
		WHERE contest_id = $1
	`, contestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type repInfo struct {
		problemID string
		language  string
		status    string
		reportID  string
	}
	var reps []repInfo
	for rows.Next() {
		var r repInfo
		if err := rows.Scan(&r.problemID, &r.language, &r.status, &r.reportID); err != nil {
			return nil, err
		}
		reps = append(reps, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(reps) == 0 {
		return nil, common.ErrReportNotFound
	}

	hasFinished := false
	for _, r := range reps {
		if r.status == "finished" && r.reportID != "" {
			hasFinished = true
		}
	}

	if !hasFinished {
		for _, r := range reps {
			if r.status == "queued" || r.status == "running" || r.status == "processing" || r.status == "pending" {
				return nil, common.ErrReportNotReady
			}
		}
		return nil, common.ErrReportNotFound
	}

	var allPairs []dto.PlagiarismPair
	pairReports := make(map[string]string)
	for _, r := range reps {
		if r.status != "finished" || r.reportID == "" {
			continue
		}
		pairs, err := d.fetchReportPairs(ctx, r.reportID)
		if err != nil {
			return nil, err
		}
		allPairs = append(allPairs, pairs...)
		for _, p := range pairs {
			pairReports[p.Key] = r.reportID
		}
	}

	if len(allPairs) == 0 {
		return []dto.PlagiarismPair{}, nil
	}
	reviews, err := d.loadContestReviews(ctx, contestID)
	if err != nil {
		return nil, err
	}

	userIDs := make(map[string]bool)
	for i := range allPairs {
		if rev, ok := reviews[pairReports[allPairs[i].Key]+"|"+allPairs[i].Key]; ok {
			allPairs[i].ReviewStatus, allPairs[i].ReviewNote = rev.status, rev.note
		} else {
			allPairs[i].ReviewStatus = "unreviewed"
		}
		if allPairs[i].LeftUserID != "" {
			userIDs[allPairs[i].LeftUserID] = true
		}
		if allPairs[i].RightUserID != "" {
			userIDs[allPairs[i].RightUserID] = true
		}
	}

	userNames, err := d.loadUserNames(ctx, userIDs)
	if err != nil {
		return nil, err
	}
	for i := range allPairs {
		if name, ok := userNames[allPairs[i].LeftUserID]; ok {
			allPairs[i].LeftName = name
		}
		if name, ok := userNames[allPairs[i].RightUserID]; ok {
			allPairs[i].RightName = name
		}
	}

	sort.Slice(allPairs, func(i, j int) bool {
		if allPairs[i].Key == allPairs[j].Key {
			return allPairs[i].Similarity > allPairs[j].Similarity
		}
		return allPairs[i].Key < allPairs[j].Key
	})
	// The API remains a plain array; page is a stable zero-based offset page.
	const pageSize = 100
	if page > len(allPairs)/pageSize {
		return []dto.PlagiarismPair{}, nil
	}
	start := page * pageSize
	if start >= len(allPairs) {
		return []dto.PlagiarismPair{}, nil
	}
	end := min(start+pageSize, len(allPairs))
	allPairs = allPairs[start:end]
	return allPairs, nil
}

type reviewData struct {
	status string
	note   string
}

func (d *DolosService) loadContestReviews(ctx context.Context, contestID string) (map[string]reviewData, error) {
	rows, err := d.db.QueryContext(ctx, `
		SELECT report_id || '|' || pair_key, status, note
		FROM contest_plagiarism_pair_reviews
		WHERE contest_id = $1
	`, contestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]reviewData)
	for rows.Next() {
		var key, status, note string
		if err := rows.Scan(&key, &status, &note); err != nil {
			return nil, err
		}
		out[key] = reviewData{status: status, note: note}
	}
	return out, rows.Err()
}

func (d *DolosService) loadUserNames(ctx context.Context, userIDs map[string]bool) (map[string]string, error) {
	out := make(map[string]string)
	if len(userIDs) == 0 {
		return out, nil
	}

	ids := make([]string, 0, len(userIDs))
	for id := range userIDs {
		ids = append(ids, id)
	}

	query := `SELECT id::text, name FROM users WHERE id = ANY($1)`
	rows, err := d.db.QueryContext(ctx, query, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		out[id] = name
	}
	return out, rows.Err()
}

func (d *DolosService) fetchReportPairs(ctx context.Context, reportID string) ([]dto.PlagiarismPair, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.baseURL+"/reports/"+url.PathEscape(reportID)+"/data/pairs", nil)
	if err != nil {
		return nil, err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Dolos pairs request returned HTTP %d", resp.StatusCode)
	}

	const maxPairsBytes = 64 * 1024 * 1024
	lr := io.LimitReader(resp.Body, maxPairsBytes+1)
	bodyBytes, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if len(bodyBytes) > maxPairsBytes {
		return nil, fmt.Errorf("Dolos pairs CSV response exceeded maximum byte budget (%d bytes)", maxPairsBytes)
	}

	reader := csv.NewReader(bytes.NewReader(bodyBytes))
	reader.TrimLeadingSpace = true
	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	colIdx := map[string]int{}
	for i, h := range headers {
		colIdx[strings.ToLower(strings.TrimSpace(h))] = i
	}

	getCol := func(names ...string) (int, bool) {
		for _, n := range names {
			if idx, ok := colIdx[strings.ToLower(n)]; ok {
				return idx, true
			}
		}
		return -1, false
	}

	leftPathCol, hasLeft := getCol("leftfilepath", "left_file_path", "left file path")
	rightPathCol, hasRight := getCol("rightfilepath", "right_file_path", "right file path")
	simCol, hasSim := getCol("similarity")
	overlapCol, hasOverlap := getCol("totaloverlap", "total_overlap", "total overlap")
	longestCol, hasLongest := getCol("longestfragment", "longest_fragment", "longest fragment")

	if !hasLeft || !hasRight || !hasSim || !hasOverlap || !hasLongest {
		return nil, fmt.Errorf("Dolos CSV missing required file path or metric headers")
	}

	var pairs []dto.PlagiarismPair
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if leftPathCol >= len(row) || rightPathCol >= len(row) || simCol >= len(row) || overlapCol >= len(row) || longestCol >= len(row) {
			return nil, fmt.Errorf("Dolos CSV row has fewer columns than its header")
		}

		rawLeft := strings.TrimSpace(row[leftPathCol])
		rawRight := strings.TrimSpace(row[rightPathCol])
		if rawLeft == "" || rawRight == "" {
			continue
		}

		canonicalLeft, leftUser := extractCanonicalPathAndUser(rawLeft)
		canonicalRight, rightUser := extractCanonicalPathAndUser(rawRight)
		if canonicalLeft == "" || canonicalRight == "" {
			continue
		}

		similarity, err := strconv.ParseFloat(strings.TrimSpace(row[simCol]), 64)
		if err != nil || math.IsNaN(similarity) || math.IsInf(similarity, 0) || similarity < 0 || similarity > 1 {
			return nil, fmt.Errorf("Dolos CSV row has invalid similarity")
		}
		totalOverlap, err := strconv.Atoi(strings.TrimSpace(row[overlapCol]))
		if err != nil || totalOverlap < 0 {
			return nil, fmt.Errorf("Dolos CSV row has invalid total overlap")
		}
		longestFragment, err := strconv.Atoi(strings.TrimSpace(row[longestCol]))
		if err != nil || longestFragment < 0 {
			return nil, fmt.Errorf("Dolos CSV row has invalid longest fragment")
		}

		p := dto.PlagiarismPair{
			Key:             pairKey(canonicalLeft, canonicalRight),
			LeftFile:        canonicalLeft,
			LeftUserID:      leftUser,
			RightFile:       canonicalRight,
			RightUserID:     rightUser,
			Similarity:      similarity,
			TotalOverlap:    totalOverlap,
			LongestFragment: longestFragment,
			ReviewStatus:    "unreviewed",
		}
		pairs = append(pairs, p)
	}

	return pairs, nil
}

func extractCanonicalPathAndUser(providerPath string) (canonicalPath, userID string) {
	cleaned := path.Clean(strings.ReplaceAll(providerPath, "\\", "/"))
	parts := strings.Split(cleaned, "/")
	if len(parts) >= 2 {
		user := parts[len(parts)-2]
		fileName := parts[len(parts)-1]
		if user != "" && user != "." && user != ".." && fileName != "" {
			return user + "/" + fileName, user
		}
	}
	return cleaned, ""
}

func pairKey(left, right string) string {
	if right < left {
		left, right = right, left
	}
	return base64.RawURLEncoding.EncodeToString([]byte(left + "\x00" + right))
}

func sourceSignature(endTime int64, sources []dolosSource) string {
	parts := make([]string, 0, len(sources)+1)
	parts = append(parts, strconv.FormatInt(endTime, 10))
	for _, s := range sources {
		parts = append(parts, strings.Join([]string{s.id, s.userID, s.problemID, s.language}, "\x00"))
	}
	sort.Strings(parts[1:])
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x01")))
	return hex.EncodeToString(sum[:])
}

func (d *DolosService) fetchReportPairMembership(ctx context.Context, reportID, key string) (bool, error) {
	pairs, err := d.fetchReportPairs(ctx, reportID)
	if err != nil {
		return false, err
	}
	for _, pair := range pairs {
		if pair.Key == key {
			return true, nil
		}
	}
	return false, nil
}

func (d *DolosService) ReviewPair(ctx context.Context, contestID, key, status, note string) error {
	if err := d.checkContestExists(ctx, contestID); err != nil {
		return err
	}
	fresh, err := d.contestEvidenceCurrent(ctx, contestID)
	if err != nil {
		return err
	}
	if !fresh {
		return common.ErrReportNotReady
	}

	if key == "" {
		return common.ErrInvalidPairKey
	}
	decoded, err := base64.RawURLEncoding.DecodeString(key)
	if err != nil || !bytes.Contains(decoded, []byte("\x00")) {
		return common.ErrInvalidPairKey
	}

	// Validate against the complete canonical provider evidence; display pagination
	// and user enrichment must never decide whether a pair can be reviewed.
	rows, err := d.db.QueryContext(ctx, `SELECT dolos_report_id FROM contest_plagiarism_reports WHERE contest_id=$1 AND status='finished' AND dolos_report_id IS NOT NULL AND dolos_report_id<>'' ORDER BY problem_id, language`, contestID)
	if err != nil {
		return err
	}
	var reportIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		reportIDs = append(reportIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	foundReport := ""
	for _, id := range reportIDs {
		found, err := d.fetchReportPairMembership(ctx, id, key)
		if err != nil {
			return err
		}
		if found {
			foundReport = id
			break
		}
	}
	if foundReport == "" {
		return common.ErrPairNotFound
	}

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var currentReport string
	if err := tx.QueryRowContext(ctx, `
		SELECT p.dolos_report_id
		FROM contest_plagiarism_reports p
		JOIN contests c ON c.id=p.contest_id
		JOIN contest_plagiarism_runs r ON r.contest_id=c.id
		WHERE p.contest_id=$1 AND p.status='finished' AND p.dolos_report_id=$2
		  AND r.analyzed_end_time=c.end_time
		  AND r.source_count=(SELECT COUNT(*) FROM submissions WHERE contest_id=c.id AND type='code')
		  AND r.latest_submission_at=COALESCE((SELECT MAX(created_at) FROM submissions WHERE contest_id=c.id AND type='code'),0)
		FOR UPDATE OF p
	`, contestID, foundReport).Scan(&currentReport); err != nil {
		if err == sql.ErrNoRows {
			return common.ErrPairNotFound
		}
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO contest_plagiarism_pair_reviews(contest_id, report_id, pair_key, status, note, updated_at)
		VALUES($1, $2, $3, $4, $5, NOW())
		ON CONFLICT(contest_id, report_id, pair_key)
		DO UPDATE SET status=EXCLUDED.status, note=EXCLUDED.note, updated_at=NOW()
	`, contestID, foundReport, key, status, note)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DolosService) Run(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := d.process(ctx); err != nil {
				log.Errorf("Dolos worker: %v", err)
			}
		}
	}
}

func (d *DolosService) process(ctx context.Context) error {
	if d.db == nil || d.s3 == nil || d.baseURL == "" {
		return nil
	}

	settledDeadline := time.Now().Add(-30 * time.Second).UnixMilli()

	rows, err := d.db.QueryContext(ctx, `
		SELECT id::text, name, end_time
		FROM contests c
		WHERE end_time <= $1 AND (
		 NOT EXISTS (SELECT 1 FROM contest_plagiarism_runs r WHERE r.contest_id=c.id)
		 OR EXISTS (
		   SELECT 1 FROM contest_plagiarism_runs r WHERE r.contest_id=c.id AND
		   (r.analyzed_end_time<>c.end_time OR r.source_count<>(SELECT COUNT(*) FROM submissions s WHERE s.contest_id=c.id AND s.type='code') OR r.latest_submission_at<>COALESCE((SELECT MAX(created_at) FROM submissions s WHERE s.contest_id=c.id AND s.type='code'),0))
		 )
		 OR EXISTS (SELECT 1 FROM contest_plagiarism_reports p WHERE p.contest_id=c.id AND ((p.status='pending' AND p.next_retry_at<=NOW()) OR (p.status='processing' AND p.updated_at<NOW()-INTERVAL '3 minutes') OR (p.status IN ('queued','running','unknown') AND p.dolos_report_id IS NOT NULL AND p.next_retry_at<=NOW())))
		)
		ORDER BY end_time, id
		LIMIT 20
	`, settledDeadline)
	if err != nil {
		return err
	}

	type contestMeta struct {
		id      string
		name    string
		endTime int64
	}
	var contests []contestMeta
	for rows.Next() {
		var c contestMeta
		if err := rows.Scan(&c.id, &c.name, &c.endTime); err != nil {
			rows.Close()
			return err
		}
		contests = append(contests, c)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	for _, c := range contests {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := d.processContest(ctx, c.id, c.name, c.endTime); err != nil {
			log.Errorf("Dolos contest %s: %v", c.id, err)
		}
	}

	return nil
}

type groupKey struct {
	problemID string
	language  string
}

func (d *DolosService) processContest(ctx context.Context, contestID, contestName string, expectedEndTime int64) error {
	var currentEndTime int64
	err := d.db.QueryRowContext(ctx, `SELECT end_time FROM contests WHERE id = $1`, contestID).Scan(&currentEndTime)
	if err != nil {
		return err
	}
	if currentEndTime != expectedEndTime || currentEndTime > time.Now().Add(-30*time.Second).UnixMilli() {
		return nil
	}

	var sourceCount int64
	var latestSubmission int64
	if err := d.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MAX(created_at),0) FROM submissions WHERE contest_id=$1 AND type='code'`, contestID).Scan(&sourceCount, &latestSubmission); err != nil {
		return err
	}
	sources, err := d.listSources(ctx, contestID)
	if err != nil {
		return err
	}
	signature := sourceSignature(currentEndTime, sources)
	var previousSignature string
	runErr := d.db.QueryRowContext(ctx, `SELECT source_signature FROM contest_plagiarism_runs WHERE contest_id=$1`, contestID).Scan(&previousSignature)
	if runErr != nil && runErr != sql.ErrNoRows {
		return runErr
	}
	if runErr == nil && previousSignature != signature {
		if _, err := d.db.ExecContext(ctx, `DELETE FROM contest_plagiarism_reports WHERE contest_id=$1`, contestID); err != nil {
			return err
		}
	}

	grouped := make(map[groupKey][]dolosSource)
	for _, s := range sources {
		k := groupKey{problemID: s.problemID, language: s.language}
		grouped[k] = append(grouped[k], s)
	}

	for k, srcList := range grouped {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		distinctUsers := make(map[string]bool)
		for _, s := range srcList {
			distinctUsers[s.userID] = true
		}

		if len(distinctUsers) < 2 {
			_, err := d.db.ExecContext(ctx, `
				INSERT INTO contest_plagiarism_reports AS r(contest_id, problem_id, language, status, error, updated_at)
				VALUES($1, $2, $3, 'skipped', 'Fewer than two distinct participants', NOW())
				ON CONFLICT(contest_id, problem_id, language)
				DO UPDATE SET status = 'skipped', dolos_report_id=NULL, report_url=NULL, error = EXCLUDED.error, claim_token=NULL, updated_at = NOW()
				WHERE r.status NOT IN ('finished')
			`, contestID, k.problemID, k.language)
			if err != nil {
				return err
			}
			continue
		}

		claimToken := generateClaimToken()
		var reportID string
		var attempts int

		claimErr := d.db.QueryRowContext(ctx, `
			INSERT INTO contest_plagiarism_reports(contest_id, problem_id, language, status, claim_token, attempts, next_retry_at, updated_at)
			VALUES($1, $2, $3, 'processing', $4, 1, NOW() + INTERVAL '3 minutes', NOW())
			ON CONFLICT(contest_id, problem_id, language)
			DO UPDATE SET
				status = 'processing',
				claim_token = $4,
				attempts = contest_plagiarism_reports.attempts + 1,
				next_retry_at = NOW() + INTERVAL '3 minutes',
				updated_at = NOW()
			WHERE (
				(contest_plagiarism_reports.status = 'pending' AND contest_plagiarism_reports.next_retry_at <= NOW())
				OR (contest_plagiarism_reports.status = 'processing' AND contest_plagiarism_reports.updated_at < NOW() - INTERVAL '3 minutes')
			) AND contest_plagiarism_reports.attempts < 6
			RETURNING COALESCE(dolos_report_id, ''), attempts
		`, contestID, k.problemID, k.language, claimToken).Scan(&reportID, &attempts)

		if claimErr == sql.ErrNoRows {
			continue
		}
		if claimErr != nil {
			return fmt.Errorf("claim Dolos report %s %s/%s: %w", contestID, k.problemID, k.language, claimErr)
		}

		archive, err := d.makeArchiveWithLease(ctx, contestID, k.problemID, k.language, claimToken, srcList)
		if err != nil {
			if saveErr := d.saveGroupError(ctx, contestID, k.problemID, k.language, claimToken, err, false); saveErr != nil {
				return saveErr
			}
			continue
		}
		// Renew/check ownership after potentially slow object reads before external upload.
		res, err := d.db.ExecContext(ctx, `UPDATE contest_plagiarism_reports SET updated_at=NOW() WHERE contest_id=$1 AND problem_id=$2 AND language=$3 AND claim_token=$4`, contestID, k.problemID, k.language, claimToken)
		if err != nil {
			return err
		}
		owned, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if owned == 0 {
			continue
		}

		const maxZipLimit = 10 * 1024 * 1024
		if len(archive) >= maxZipLimit {
			permErr := fmt.Errorf("Dolos archive size (%d bytes) reaches or exceeds 10 MB limit", len(archive))
			if saveErr := d.saveGroupError(ctx, contestID, k.problemID, k.language, claimToken, permErr, true); saveErr != nil {
				return saveErr
			}
			continue
		}

		reportName := fmt.Sprintf("%s - %s - %s", contestName, k.problemID, k.language)
		remote, err := d.createReport(ctx, reportName, archive)
		if err != nil {
			var uploadErr *dolosUploadError
			if errors.As(err, &uploadErr) && uploadErr.ambiguous {
				// A failed POST may have been accepted remotely. Retain the ambiguity
				// and require operator resolution instead of creating duplicate datasets.
				if _, saveErr := d.db.ExecContext(ctx, `UPDATE contest_plagiarism_reports SET status='failed', error=$5, claim_token=NULL, updated_at=NOW() WHERE contest_id=$1 AND problem_id=$2 AND language=$3 AND claim_token=$4`, contestID, k.problemID, k.language, claimToken, "Dolos upload outcome is ambiguous: "+err.Error()); saveErr != nil {
					return saveErr
				}
			} else {
				permanent := uploadErr == nil || !uploadErr.retryable
				if saveErr := d.saveGroupError(ctx, contestID, k.problemID, k.language, claimToken, err, permanent); saveErr != nil {
					return saveErr
				}
			}
			continue
		}

		_, err = d.db.ExecContext(ctx, `
			UPDATE contest_plagiarism_reports
			SET status = $5,
			    dolos_report_id = $6,
			    report_url = $7,
			    error = NULL,
			    claim_token = NULL,
			    next_retry_at = NOW() + INTERVAL '15 seconds',
			    updated_at = NOW()
			WHERE contest_id = $1 AND problem_id = $2 AND language = $3 AND claim_token = $4
		`, contestID, k.problemID, k.language, claimToken, remote.Status, remote.ID, remote.HTMLURL)
		if err != nil {
			log.Errorf("update report created contest %s %s/%s: %v", contestID, k.problemID, k.language, err)
		}
	}
	_, err = d.db.ExecContext(ctx, `INSERT INTO contest_plagiarism_runs(contest_id,analyzed_end_time,source_count,latest_submission_at,source_signature,updated_at) VALUES($1,$2,$3,$4,$5,NOW()) ON CONFLICT(contest_id) DO UPDATE SET analyzed_end_time=EXCLUDED.analyzed_end_time,source_count=EXCLUDED.source_count,latest_submission_at=EXCLUDED.latest_submission_at,source_signature=EXCLUDED.source_signature,updated_at=NOW()`, contestID, currentEndTime, sourceCount, latestSubmission, signature)
	if err != nil {
		return err
	}

	return d.pollActiveReports(ctx, contestID)
}

func (d *DolosService) pollActiveReports(ctx context.Context, contestID string) error {
	rows, err := d.db.QueryContext(ctx, `
		SELECT problem_id, language, dolos_report_id, attempts
		FROM contest_plagiarism_reports
		WHERE contest_id = $1
		  AND status IN ('queued', 'running', 'unknown')
		  AND dolos_report_id IS NOT NULL
		  AND dolos_report_id != ''
		  AND next_retry_at <= NOW()
	`, contestID)
	if err != nil {
		return err
	}
	defer rows.Close()

	type activeRep struct {
		problemID string
		language  string
		reportID  string
		attempts  int
	}
	var toPoll []activeRep
	for rows.Next() {
		var a activeRep
		if err := rows.Scan(&a.problemID, &a.language, &a.reportID, &a.attempts); err != nil { rows.Close(); return err }
		toPoll = append(toPoll, a)
	}
	if err := rows.Err(); err != nil { rows.Close(); return err }
	rows.Close()

	for _, a := range toPoll {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		endpoint := d.baseURL + "/reports/" + url.PathEscape(a.reportID)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			if err := d.schedulePollRetry(ctx, contestID, a.problemID, a.language, a.attempts); err != nil { return err }
			continue
		}
		resp, err := d.client.Do(req)
		if err != nil {
			if err := d.schedulePollRetry(ctx, contestID, a.problemID, a.language, a.attempts); err != nil { return err }
			continue
		}

		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			if _, err := d.db.ExecContext(ctx, `
				UPDATE contest_plagiarism_reports
				SET status = 'error', error = 'Report not found on provider (404)', updated_at = NOW()
				WHERE contest_id = $1 AND problem_id = $2 AND language = $3 AND dolos_report_id=$4
			`, contestID, a.problemID, a.language, a.reportID); err != nil { return err }
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			if err := d.schedulePollRetry(ctx, contestID, a.problemID, a.language, a.attempts); err != nil { return err }
			continue
		}

		var remote dolosReportResponse
		err = json.NewDecoder(io.LimitReader(resp.Body, 1024*1024)).Decode(&remote)
		resp.Body.Close()
		if err != nil {
			if retryErr := d.schedulePollRetry(ctx, contestID, a.problemID, a.language, a.attempts); retryErr != nil { return retryErr }
			continue
		}

		remote.Status = validateStatus(remote.Status)

		if remote.Status == "unknown" {
			if err := d.schedulePollRetry(ctx, contestID, a.problemID, a.language, a.attempts); err != nil { return err }
			continue
		}
		if _, err := d.db.ExecContext(ctx, `
			UPDATE contest_plagiarism_reports
			SET status = $4,
			    error = NULLIF($5, ''),
			    next_retry_at = CASE WHEN $4 IN ('queued', 'running') THEN NOW() + INTERVAL '15 seconds' ELSE next_retry_at END,
			    updated_at = NOW()
			WHERE contest_id = $1 AND problem_id = $2 AND language = $3 AND dolos_report_id = $6
		`, contestID, a.problemID, a.language, remote.Status, remote.Error, a.reportID); err != nil { return err }
	}
	return nil
}

func (d *DolosService) schedulePollRetry(ctx context.Context, contestID, problemID, language string, currentAttempts int) error {
	if currentAttempts >= 10 {
		_, err := d.db.ExecContext(ctx, `
			UPDATE contest_plagiarism_reports
			SET status = 'failed', error = 'Polling exceeded maximum attempts', updated_at = NOW()
			WHERE contest_id = $1 AND problem_id = $2 AND language = $3
		`, contestID, problemID, language)
		return err
	}
	_, err := d.db.ExecContext(ctx, `
		UPDATE contest_plagiarism_reports
		SET attempts = attempts + 1, next_retry_at = NOW() + INTERVAL '30 seconds', updated_at = NOW()
		WHERE contest_id = $1 AND problem_id = $2 AND language = $3
	`, contestID, problemID, language)
	return err
}

func validateStatus(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	switch s {
	case "queued", "running", "finished", "failed", "error", "purged":
		return s
	default:
		return "unknown"
	}
}

func (d *DolosService) saveGroupError(ctx context.Context, contestID, problemID, language, claimToken string, cause error, permanent bool) error {
	newStatus := "pending"
	if permanent {
		newStatus = "failed"
	}

	_, err := d.db.ExecContext(ctx, `
		UPDATE contest_plagiarism_reports
		SET status = CASE WHEN attempts >= 6 THEN 'failed' ELSE $5 END,
		    error = $6,
		    claim_token = NULL,
		    next_retry_at = CASE WHEN $5 = 'pending' AND attempts < 6 THEN NOW() + INTERVAL '1 minute' ELSE next_retry_at END,
		    updated_at = NOW()
		WHERE contest_id = $1 AND problem_id = $2 AND language = $3 AND claim_token = $4
	`, contestID, problemID, language, claimToken, newStatus, cause.Error())
	if err != nil {
		log.Errorf("save error contest %s %s/%s: %v", contestID, problemID, language, err)
		return err
	}
	return nil
}

func (d *DolosService) listSources(ctx context.Context, contestID string) ([]dolosSource, error) {
	rows, err := d.db.QueryContext(ctx, `
		SELECT DISTINCT ON (user_id, problem_id)
			id::text, user_id::text, problem_id::text, COALESCE(language, '')
		FROM submissions
		WHERE contest_id = $1 AND type = 'code'
		ORDER BY user_id, problem_id, created_at DESC, id DESC
	`, contestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []dolosSource
	for rows.Next() {
		var s dolosSource
		if err := rows.Scan(&s.id, &s.userID, &s.problemID, &s.language); err != nil {
			return nil, err
		}
		canonicalLang := canonicalLanguage(s.language)
		if canonicalLang != "" {
			s.language = canonicalLang
			out = append(out, s)
		}
	}
	return out, rows.Err()
}

func canonicalLanguage(lang string) string {
	switch strings.ToLower(strings.TrimSpace(lang)) {
	case "python", "python3", "py":
		return "python"
	case "cpp", "c++":
		return "cpp"
	case "c":
		return "c"
	case "java":
		return "java"
	case "javascript", "js", "node":
		return "javascript"
	case "go", "golang":
		return "go"
	default:
		return ""
	}
}

func (d *DolosService) makeArchive(ctx context.Context, sources []dolosSource) ([]byte, error) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	const maxObjectBytes = 1024 * 1024 // 1 MB per source code max
	const maxArchiveBytes = 10 * 1024 * 1024

	for _, s := range sources {
		code, err := d.s3.GetObjectLimit(ctx, s.id, maxObjectBytes)
		if err != nil {
			zw.Close()
			return nil, fmt.Errorf("read source %s: %w", s.id, err)
		}

		decodedCode, err := decodeSource(code)
		if err != nil {
			zw.Close()
			return nil, fmt.Errorf("decode source %s: %w", s.id, err)
		}

		ext := languageExtension(s.language)
		name := path.Join(s.userID, s.problemID+"_"+s.id+ext)

		f, err := zw.Create(name)
		if err != nil {
			zw.Close()
			return nil, err
		}
		if _, err = f.Write([]byte(decodedCode)); err != nil {
			zw.Close()
			return nil, err
		}

		if buf.Len() > maxArchiveBytes {
			zw.Close()
			return nil, fmt.Errorf("archive size exceeds 10 MB limit while packaging")
		}
	}

	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (d *DolosService) makeArchiveWithLease(ctx context.Context, contestID, problemID, language, token string, sources []dolosSource) ([]byte, error) {
	prepCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-prepCtx.Done():
				return
			case <-ticker.C:
				leaseCtx, leaseCancel := context.WithTimeout(ctx, 10*time.Second)
				res, err := d.db.ExecContext(leaseCtx, `UPDATE contest_plagiarism_reports SET updated_at=NOW() WHERE contest_id=$1 AND problem_id=$2 AND language=$3 AND claim_token=$4`, contestID, problemID, language, token)
				leaseCancel()
				if err != nil {
					cancel()
					return
				}
				n, err := res.RowsAffected()
				if err != nil || n == 0 {
					cancel()
					return
				}
			}
		}
	}()
	archive, err := d.makeArchive(prepCtx, sources)
	cancel()
	<-done
	if err != nil {
		return nil, err
	}
	var owned bool
	if err := d.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM contest_plagiarism_reports WHERE contest_id=$1 AND problem_id=$2 AND language=$3 AND claim_token=$4)`, contestID, problemID, language, token).Scan(&owned); err != nil {
		return nil, err
	}
	if !owned {
		return nil, context.Canceled
	}
	return archive, nil
}

func (d *DolosService) createReport(ctx context.Context, name string, archive []byte) (*dolosReportResponse, error) {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	_ = mw.WriteField("dataset[name]", name)

	fw, err := mw.CreateFormFile("dataset[zipfile]", "contest.zip")
	if err != nil {
		return nil, err
	}
	if _, err = fw.Write(archive); err != nil {
		return nil, err
	}
	if err = mw.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.baseURL+"/reports", &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, &dolosUploadError{err: err, ambiguous: true}
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		statusErr := fmt.Errorf("Dolos returned HTTP %d: %s", resp.StatusCode, string(data))
		if resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests {
			return nil, &dolosUploadError{err: statusErr, retryable: true}
		}
		if resp.StatusCode >= 500 {
			return nil, &dolosUploadError{err: statusErr, ambiguous: true}
		}
		return nil, &dolosUploadError{err: statusErr}
	}

	var result dolosReportResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1024*1024)).Decode(&result); err != nil {
		return nil, &dolosUploadError{err: err, ambiguous: true}
	}
	if result.ID == "" {
		return nil, &dolosUploadError{err: fmt.Errorf("Dolos response omitted report ID"), ambiguous: true}
	}
	if result.HTMLURL == "" {
		result.HTMLURL = result.URL
	}
	result.Status = validateStatus(result.Status)

	return &result, nil
}

func languageExtension(language string) string {
	switch strings.ToLower(strings.TrimSpace(language)) {
	case "python", "python3", "py":
		return ".py"
	case "cpp", "c++":
		return ".cpp"
	case "c":
		return ".c"
	case "java":
		return ".java"
	case "javascript", "js", "node":
		return ".js"
	case "go", "golang":
		return ".go"
	default:
		return ".txt"
	}
}

func NewDolosWorker(lc fx.Lifecycle, d *DolosService) {
	workerCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				defer close(done)
				d.Run(workerCtx)
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			cancel()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
}
