package amplify

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// createDeployment handles POST /apps/{appId}/branches/{branchName}/deployments.
func (h *Handler) createDeployment(ctx context.Context, c *echo.Context, appID, branchName string) error {
	if c.Request().Method != http.MethodPost {
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}

	jobID, zipUploadURL, err := h.Backend.CreateDeployment(appID, branchName)
	if err != nil {
		return h.handleBackendError(ctx, c, "CreateDeployment", err)
	}

	return c.JSON(http.StatusCreated, map[string]any{
		"jobId":          jobID,
		"zipUploadUrl":   zipUploadURL,
		"fileUploadUrls": map[string]string{},
	})
}

// startDeployment handles POST /apps/{appId}/branches/{branchName}/deployments/start.
func (h *Handler) startDeployment(ctx context.Context, c *echo.Context, appID, branchName string) error {
	if c.Request().Method != http.MethodPost {
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		JobID     string `json:"jobId"`
		SourceURL string `json:"sourceUrl"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	job, startErr := h.Backend.StartDeployment(appID, branchName, input.JobID, input.SourceURL)
	if startErr != nil {
		return h.handleBackendError(ctx, c, "StartDeployment", startErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyJobSummary: toJobSummaryView(job)})
}
