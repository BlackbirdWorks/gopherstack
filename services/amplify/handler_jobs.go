package amplify

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// JSON response key used by the job handlers (and by handler_deployments.go,
// whose StartDeployment response wraps a Job the same way).
const keyJobSummary = "jobSummary"

// handleBranchJobs handles POST/GET /apps/{appId}/branches/{branchName}/jobs.
func (h *Handler) handleBranchJobs(ctx context.Context, c *echo.Context, appID, branchName string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.startJob(ctx, c, appID, branchName)
	case http.MethodGet:
		return h.listJobs(ctx, c, appID, branchName)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleBranchJobID handles GET/DELETE /apps/{appId}/branches/{branchName}/jobs/{jobId}.
func (h *Handler) handleBranchJobID(
	ctx context.Context,
	c *echo.Context,
	appID, branchName, jobID string,
) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getJob(ctx, c, appID, branchName, jobID)
	case http.MethodDelete:
		return h.deleteJob(ctx, c, appID, branchName, jobID)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// startJob handles POST /apps/{appId}/branches/{branchName}/jobs.
func (h *Handler) startJob(ctx context.Context, c *echo.Context, appID, branchName string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		CommitID  string `json:"commitId"`
		CommitMsg string `json:"commitMessage"`
		JobType   string `json:"jobType"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	job, startErr := h.Backend.StartJob(appID, branchName, input.JobType, input.CommitID, input.CommitMsg)
	if startErr != nil {
		return h.handleBackendError(ctx, c, "StartJob", startErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyJobSummary: toJobSummaryView(job)})
}

// listJobs handles GET /apps/{appId}/branches/{branchName}/jobs.
func (h *Handler) listJobs(ctx context.Context, c *echo.Context, appID, branchName string) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")

	maxResults := 0
	if s := q.Get("maxResults"); s != "" {
		if n, convErr := strconv.Atoi(s); convErr == nil && n > 0 {
			maxResults = n
		}
	}

	jobs, outToken, err := h.Backend.ListJobs(appID, branchName, nextToken, maxResults)
	if err != nil {
		return h.handleBackendError(ctx, c, "ListJobs", err)
	}

	resp := map[string]any{"jobSummaries": toJobSummaryViews(jobs)}
	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

// getJob handles GET /apps/{appId}/branches/{branchName}/jobs/{jobId}.
func (h *Handler) getJob(ctx context.Context, c *echo.Context, appID, branchName, jobID string) error {
	job, err := h.Backend.GetJob(appID, branchName, jobID)
	if err != nil {
		return h.handleBackendError(ctx, c, "GetJob", err)
	}

	jobResp := map[string]any{
		"summary": toJobSummaryView(job),
		"steps":   []any{},
	}

	return c.JSON(http.StatusOK, map[string]any{"job": jobResp})
}

// deleteJob handles DELETE /apps/{appId}/branches/{branchName}/jobs/{jobId}.
func (h *Handler) deleteJob(ctx context.Context, c *echo.Context, appID, branchName, jobID string) error {
	job, err := h.Backend.DeleteJob(appID, branchName, jobID)
	if err != nil {
		return h.handleBackendError(ctx, c, "DeleteJob", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyJobSummary: toJobSummaryView(job)})
}

// stopJob handles DELETE /apps/{appId}/branches/{branchName}/jobs/{jobId}/stop.
func (h *Handler) stopJob(ctx context.Context, c *echo.Context, appID, branchName, jobID string) error {
	job, err := h.Backend.StopJob(appID, branchName, jobID)
	if err != nil {
		return h.handleBackendError(ctx, c, "StopJob", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyJobSummary: toJobSummaryView(job)})
}

type jobSummaryView struct {
	JobID     string  `json:"jobId"`
	JobARN    string  `json:"jobArn"`
	CommitID  string  `json:"commitId,omitempty"`
	CommitMsg string  `json:"commitMessage,omitempty"`
	Status    string  `json:"status"`
	Type      string  `json:"jobType"`
	StartTime float64 `json:"startTime"`
	EndTime   float64 `json:"endTime,omitempty"`
}

func toJobSummaryView(j *Job) jobSummaryView {
	v := jobSummaryView{
		StartTime: float64(j.StartTime.Unix()),
		JobID:     j.JobID,
		JobARN:    j.JobARN,
		CommitID:  j.CommitID,
		CommitMsg: j.CommitMsg,
		Status:    string(j.Status),
		Type:      string(j.Type),
	}

	if !j.EndTime.IsZero() {
		v.EndTime = float64(j.EndTime.Unix())
	}

	return v
}

func toJobSummaryViews(jobs []*Job) []jobSummaryView {
	views := make([]jobSummaryView, len(jobs))
	for i, j := range jobs {
		views[i] = toJobSummaryView(j)
	}

	return views
}
