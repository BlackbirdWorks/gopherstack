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

	body, readErr := httputils.ReadBody(c.Request())
	if readErr != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, readErr.Error())
	}

	var input struct {
		FileMap map[string]string `json:"fileMap"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
			return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
		}
	}

	urls, err := h.Backend.CreateDeploymentWithFiles(appID, branchName, input.FileMap)
	if err != nil {
		return h.handleBackendError(ctx, c, "CreateDeployment", err)
	}

	return c.JSON(http.StatusCreated, map[string]any{
		"jobId":          urls.JobID,
		"zipUploadUrl":   urls.ZipUploadURL,
		"fileUploadUrls": urls.FileUploadURLs,
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
		JobID         string `json:"jobId"`
		SourceURL     string `json:"sourceUrl"`
		SourceURLType string `json:"sourceUrlType"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	job, startErr := h.Backend.StartDeployment(appID, branchName, input.JobID, input.SourceURL, input.SourceURLType)
	if startErr != nil {
		return h.handleBackendError(ctx, c, "StartDeployment", startErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyJobSummary: toJobSummaryView(job)})
}
