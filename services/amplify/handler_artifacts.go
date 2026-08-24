package amplify

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// generateAccessLogs handles POST /apps/{appId}/accesslogs.
func (h *Handler) generateAccessLogs(ctx context.Context, c *echo.Context, appID string) error {
	if c.Request().Method != http.MethodPost {
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		DomainName string `json:"domainName"`
		StartTime  string `json:"startTime"`
		EndTime    string `json:"endTime"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	logURL, logErr := h.Backend.GenerateAccessLogs(appID, input.DomainName, input.StartTime, input.EndTime)
	if logErr != nil {
		return h.handleBackendError(ctx, c, "GenerateAccessLogs", logErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"logUrl": logURL})
}

// getArtifactURL handles GET /artifacts/{artifactId}.
func (h *Handler) getArtifactURL(ctx context.Context, c *echo.Context, artifactID string) error {
	if c.Request().Method != http.MethodGet {
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}

	id, artifactURL, err := h.Backend.GetArtifactURL(artifactID)
	if err != nil {
		return h.handleBackendError(ctx, c, "GetArtifactUrl", err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"artifactId":  id,
		"artifactUrl": artifactURL,
	})
}

// listArtifacts handles GET /apps/{appId}/branches/{branchName}/jobs/{jobId}/artifacts.
func (h *Handler) listArtifacts(ctx context.Context, c *echo.Context, appID, branchName, jobID string) error {
	if c.Request().Method != http.MethodGet {
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}

	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")

	maxResults := 0
	if s := q.Get("maxResults"); s != "" {
		if n, convErr := strconv.Atoi(s); convErr == nil && n > 0 {
			maxResults = n
		}
	}

	artifacts, outToken, err := h.Backend.ListArtifacts(appID, branchName, jobID, nextToken, maxResults)
	if err != nil {
		return h.handleBackendError(ctx, c, opListArtifacts, err)
	}

	resp := map[string]any{"artifacts": toArtifactViews(artifacts)}
	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

type artifactView struct {
	ArtifactID       string `json:"artifactId"`
	ArtifactFileName string `json:"artifactFileName"`
}

func toArtifactViews(arts []*Artifact) []artifactView {
	views := make([]artifactView, len(arts))
	for i, a := range arts {
		views[i] = artifactView{
			ArtifactID:       a.ArtifactID,
			ArtifactFileName: a.ArtifactFileName,
		}
	}

	return views
}
