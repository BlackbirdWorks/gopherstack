package bedrock

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *AgentsHandler) dispatchIngestionJobRoutes(
	c *echo.Context,
	kbID, dsID, dsSuffix, method string,
) error {
	jobID := strings.TrimPrefix(dsSuffix, "/ingestionjobs/")
	// strip any further sub-path
	if idx := strings.Index(jobID, "/"); idx >= 0 {
		subPath := jobID[idx:]
		jobID = jobID[:idx]

		if subPath == "/stop" && method == http.MethodPost {
			return h.handleStopIngestionJob(c, kbID, dsID, jobID)
		}
	}

	if method == http.MethodGet {
		return h.handleGetIngestionJob(c, kbID, dsID, jobID)
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown ingestion job operation"),
	)
}

func (h *AgentsHandler) handleStartIngestionJob(
	c *echo.Context,
	kbID, dsID string,
	body []byte,
) error {
	var req struct {
		Description string `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	job, err := h.Backend.StartIngestionJob(kbID, dsID, req.Description)
	if err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return c.JSON(http.StatusConflict, agentErrResp("ConflictException", err.Error()))
		}

		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusAccepted, map[string]any{respIngestionJob: job})
}

func (h *AgentsHandler) handleGetIngestionJob(c *echo.Context, kbID, dsID, jobID string) error {
	job, err := h.Backend.GetIngestionJob(kbID, dsID, jobID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respIngestionJob: job})
}

func (h *AgentsHandler) handleListIngestionJobs(c *echo.Context, kbID, dsID string) error {
	list, outToken := h.Backend.ListIngestionJobs(kbID, dsID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"ingestionJobSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleStopIngestionJob(
	c *echo.Context, kbID, dsID, jobID string,
) error {
	job, err := h.Backend.StopIngestionJob(kbID, dsID, jobID)
	if err != nil {
		if errors.Is(err, ErrValidation) {
			return c.JSON(http.StatusBadRequest, agentErrResp("ValidationException", err.Error()))
		}

		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respIngestionJob: job})
}
