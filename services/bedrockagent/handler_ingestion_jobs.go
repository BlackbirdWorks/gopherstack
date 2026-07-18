package bedrockagent

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// ---------------------------------------------------------------------------
// Ingestion job handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleStartIngestionJob(
	ctx context.Context, c *echo.Context, kbID, dsID string, body []byte,
) error {
	var req struct {
		Description string `json:"description"`
	}

	_ = json.Unmarshal(body, &req)

	job, err := h.Backend.StartIngestionJob(ctx, kbID, dsID, req.Description)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusAccepted, map[string]any{keyIngestionJob: job})
}

func (h *Handler) handleGetIngestionJob(
	ctx context.Context, c *echo.Context, kbID, dsID, jobID string,
) error {
	job, err := h.Backend.GetIngestionJob(ctx, kbID, dsID, jobID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyIngestionJob: job})
}

func (h *Handler) handleStopIngestionJob(
	ctx context.Context, c *echo.Context, kbID, dsID, jobID string,
) error {
	job, err := h.Backend.StopIngestionJob(ctx, kbID, dsID, jobID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyIngestionJob: job})
}

func (h *Handler) handleListIngestionJobs(
	ctx context.Context, c *echo.Context, kbID, dsID string,
) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	jobs, outToken, err := h.Backend.ListIngestionJobs(ctx, kbID, dsID, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ingestionJobSummaries": jobs,
		keyNextToken:            outToken,
	})
}

func classifyJobPath(method string, segs []string) string {
	idx := indexOf(segs, "ingestionjobs")
	hasJobID := len(segs) > idx+1 && segs[idx+1] != ""

	if !hasJobID {
		switch method {
		case http.MethodPut:
			return opStartIngestionJob
		case http.MethodPost, http.MethodGet:
			return opListIngestionJobs
		}
	}

	if len(segs) > idx+splitTwo && segs[idx+splitTwo] == "stop" {
		return opStopIngestionJob
	}

	return opGetIngestionJob
}
