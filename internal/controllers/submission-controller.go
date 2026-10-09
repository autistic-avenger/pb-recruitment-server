package controllers

import (
	"app/internal/common"
	"app/internal/judge0"
	"app/internal/models/dto"
	"app/internal/services"
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"net/http"
	"time"
)

type SubmissionController struct {
	submissionService *services.SubmissionService
	contestService    *services.ContestService
	judge0Client      *judge0.Client
}

func NewSubmissionController(submissionService *services.SubmissionService, contestService *services.ContestService, judge0Client *judge0.Client) *SubmissionController {
	return &SubmissionController{
		submissionService: submissionService,
		contestService:    contestService,
		judge0Client:      judge0Client,
	}
}

func (sc *SubmissionController) Judge0Callback(c echo.Context) error {
	id := c.Param("execution_id")
	if _, err := uuid.Parse(id); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}
	if sc.judge0Client == nil || !sc.judge0Client.CallbacksEnabled() || !sc.judge0Client.VerifyCallback(id, c.QueryParam("sig")) {
		return c.NoContent(http.StatusUnauthorized)
	}
	var payload judge0.CallbackResult
	if err := json.NewDecoder(c.Request().Body).Decode(&payload); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}
	if payload.Token == "" || payload.Status.ID < 1 || payload.Status.ID > 14 {
		return c.NoContent(http.StatusBadRequest)
	}
	if err := sc.submissionService.HandleJudge0Callback(c.Request().Context(), id, payload); err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}
	return c.NoContent(http.StatusOK)
}

func (sc *SubmissionController) GetSubmissionStatus(ctx echo.Context) error {
	id := ctx.Param("id")
	userID := ctx.Get(common.AUTH_USER_ID).(string)

	sub, err := sc.submissionService.GetSubmissionStatusByID(ctx.Request().Context(), id)
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return ctx.NoContent(http.StatusNotFound)
		}

		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error": "failed to get submission status",
		})
	}

	if sub.UserID != userID {
		return ctx.NoContent(http.StatusForbidden)
	}

	return ctx.JSON(http.StatusOK, map[string]string{
		"status": string(sub.Status),
	})
}

func (sc *SubmissionController) GetSubmissionDetails(ctx echo.Context) error {
	id := ctx.Param("id")
	userID := ctx.Get(common.AUTH_USER_ID).(string)

	sub, err := sc.submissionService.GetSubmissionDetailsByID(ctx.Request().Context(), id)
	if err != nil {
		if errors.Is(err, common.ErrNotFound) || errors.Is(err, common.KeyNotFoundError) {
			return ctx.JSON(http.StatusNotFound, map[string]string{"error": err.Error()})
		}
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error": "failed to get submission details",
		})
	}

	if sub.UserID != userID {
		return ctx.NoContent(http.StatusForbidden)
	}

	return ctx.JSON(http.StatusOK, sub)
}

func (sc *SubmissionController) ListUserSubmissions(ctx echo.Context) error {
	userID := ctx.Get(common.AUTH_USER_ID).(string)

	req, ok := ctx.Get(common.VALIDATED_REQUEST_BODY).(*dto.ListProblemSubmissionsRequest)
	if !ok {
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error": "Internal error: Request DTO not found in context",
		})
	}

	submissions, err := sc.submissionService.ListUserSubmissionsByProblemID(ctx.Request().Context(), userID, req.ProblemID, req.Page)
	if err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error": "failed to list user submissions",
		})
	}

	return ctx.JSON(http.StatusOK, dto.ListProblemSubmissionsResponse{
		Submissions: submissions,
	})
}

func (sc *SubmissionController) SubmitSolution(ctx echo.Context) error {
	reqCtx, cancelPreparation := context.WithTimeout(ctx.Request().Context(), 5*time.Second)
	defer cancelPreparation()
	userID := ctx.Get(common.AUTH_USER_ID).(string)

	req, ok := ctx.Get(common.VALIDATED_REQUEST_BODY).(*dto.SubmitSubmissionRequest)
	if !ok {
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error": "Internal error: SubmitSubmissionRequest DTO not found in context",
		})
	}

	contest_response, err := sc.contestService.GetContest(reqCtx, req.ContestID, userID)
	if err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error": "failed to check contest registration",
		})
	}
	if !*contest_response.IsRegistered {
		return ctx.NoContent(http.StatusForbidden)
	}

	submissionType := req.Type

	submissionID, err := sc.submissionService.CreateSubmission(reqCtx, userID, submissionType, req)
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return ctx.NoContent(http.StatusNotFound)
		}
		if errors.Is(err, common.KeyAlreadyExistsError) {
			return ctx.JSON(http.StatusConflict, map[string]string{"error": err.Error()})
		}
		if errors.Is(err, common.ErrUnsupportedLanguage) || errors.Is(err, common.ErrNoTestcases) || errors.Is(err, common.ErrInvalidCode) {
			return ctx.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}
		return ctx.NoContent(http.StatusInternalServerError)
	}

	return ctx.JSON(http.StatusCreated, dto.SubmitSubmissionResponse{
		SubmissionID: submissionID,
	})
}
