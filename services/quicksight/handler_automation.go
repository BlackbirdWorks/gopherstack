package quicksight

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response/request keys and query params used only by automation job
// operations.
const (
	keyJobID           = "JobId"
	keyInputPayload    = "InputPayload"
	keyOutputPayload   = "OutputPayload"
	keyCreatedAt       = "CreatedAt"
	keyStartedAt       = "StartedAt"
	keyEndedAt         = "EndedAt"
	queryIncludeInput  = "includeInputPayload"
	queryIncludeOutput = "includeOutputPayload"
)

func isAutomationJobOp(op string) bool {
	switch op {
	case opStartAutomationJob, opDescribeAutomationJob:
		return true
	}

	return false
}

func (h *Handler) dispatchAutomationJob(c *echo.Context, op string) error {
	switch op {
	case opStartAutomationJob:
		return h.handleStartAutomationJob(c)
	case opDescribeAutomationJob:
		return h.handleDescribeAutomationJob(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

func (h *Handler) handleStartAutomationJob(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	automationGroupID := seg(segs, segResID)
	automationID := seg(segs, segSubResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	j, err := h.Backend.StartAutomationJob(accountID, automationGroupID, automationID, strField(body, keyInputPayload))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:       j.Arn,
		keyJobID:     j.JobID,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDescribeAutomationJob(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	automationGroupID := seg(segs, segResID)
	automationID := seg(segs, segSubResID)
	jobID := seg(segs, segSubSubResID)

	j, err := h.Backend.DescribeAutomationJob(accountID, automationGroupID, automationID, jobID)
	if err != nil {
		return httpErr(c, err)
	}

	resp := map[string]any{
		keyArn:       j.Arn,
		keyJobStatus: j.Status,
		keyCreatedAt: j.CreatedAt.Unix(),
		keyStartedAt: j.StartedAt.Unix(),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	}
	if !j.EndedAt.IsZero() {
		resp[keyEndedAt] = j.EndedAt.Unix()
	}
	if queryParam(c, queryIncludeInput) == queryValueTrue {
		resp[keyInputPayload] = j.InputPayload
	}
	if queryParam(c, queryIncludeOutput) == queryValueTrue {
		resp[keyOutputPayload] = j.OutputPayload
	}

	return writeJSON(c, http.StatusOK, resp)
}
