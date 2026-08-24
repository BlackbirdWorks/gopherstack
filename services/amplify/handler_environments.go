package amplify

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// JSON response key used by the backend environment handlers.
const keyBackendEnvironment = "backendEnvironment"

// handleBackendEnvironments handles POST/GET /apps/{appId}/backendenvironments.
func (h *Handler) handleBackendEnvironments(ctx context.Context, c *echo.Context, appID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createBackendEnvironment(ctx, c, appID)
	case http.MethodGet:
		return h.listBackendEnvironments(ctx, c, appID)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleBackendEnvironmentName handles GET/DELETE /apps/{appId}/backendenvironments/{environmentName}.
func (h *Handler) handleBackendEnvironmentName(
	ctx context.Context,
	c *echo.Context,
	appID, environmentName string,
) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getBackendEnvironment(ctx, c, appID, environmentName)
	case http.MethodDelete:
		return h.deleteBackendEnvironment(ctx, c, appID, environmentName)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// createBackendEnvironment handles POST /apps/{appId}/backendenvironments.
func (h *Handler) createBackendEnvironment(ctx context.Context, c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		EnvironmentName     string `json:"environmentName"`
		StackName           string `json:"stackName"`
		DeploymentArtifacts string `json:"deploymentArtifacts"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	be, createErr := h.Backend.CreateBackendEnvironment(
		appID, input.EnvironmentName, input.StackName, input.DeploymentArtifacts,
	)
	if createErr != nil {
		return h.handleBackendError(ctx, c, "CreateBackendEnvironment", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyBackendEnvironment: toBackendEnvironmentView(be)})
}

// listBackendEnvironments handles GET /apps/{appId}/backendenvironments.
func (h *Handler) listBackendEnvironments(ctx context.Context, c *echo.Context, appID string) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")

	maxResults := 0
	if s := q.Get("maxResults"); s != "" {
		if n, convErr := strconv.Atoi(s); convErr == nil && n > 0 {
			maxResults = n
		}
	}

	envs, outToken, err := h.Backend.ListBackendEnvironments(appID, nextToken, maxResults)
	if err != nil {
		return h.handleBackendError(ctx, c, opListBackendEnvironments, err)
	}

	resp := map[string]any{"backendEnvironments": toBackendEnvironmentViews(envs)}
	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

// getBackendEnvironment handles GET /apps/{appId}/backendenvironments/{environmentName}.
func (h *Handler) getBackendEnvironment(
	ctx context.Context,
	c *echo.Context,
	appID, environmentName string,
) error {
	be, err := h.Backend.GetBackendEnvironment(appID, environmentName)
	if err != nil {
		return h.handleBackendError(ctx, c, "GetBackendEnvironment", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyBackendEnvironment: toBackendEnvironmentView(be)})
}

// deleteBackendEnvironment handles DELETE /apps/{appId}/backendenvironments/{environmentName}.
func (h *Handler) deleteBackendEnvironment(
	ctx context.Context,
	c *echo.Context,
	appID, environmentName string,
) error {
	be, err := h.Backend.DeleteBackendEnvironment(appID, environmentName)
	if err != nil {
		return h.handleBackendError(ctx, c, "DeleteBackendEnvironment", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyBackendEnvironment: toBackendEnvironmentView(be)})
}

type backendEnvironmentView struct {
	EnvironmentName       string  `json:"environmentName"`
	BackendEnvironmentARN string  `json:"backendEnvironmentArn"`
	StackName             string  `json:"stackName,omitempty"`
	DeploymentArtifacts   string  `json:"deploymentArtifacts,omitempty"`
	CreateTime            float64 `json:"createTime"`
	UpdateTime            float64 `json:"updateTime"`
}

func toBackendEnvironmentView(be *BackendEnvironment) backendEnvironmentView {
	return backendEnvironmentView{
		CreateTime:            float64(be.CreateTime.Unix()),
		UpdateTime:            float64(be.UpdateTime.Unix()),
		EnvironmentName:       be.EnvironmentName,
		BackendEnvironmentARN: be.BackendEnvironmentARN,
		StackName:             be.StackName,
		DeploymentArtifacts:   be.DeploymentArtifacts,
	}
}

func toBackendEnvironmentViews(envs []*BackendEnvironment) []backendEnvironmentView {
	views := make([]backendEnvironmentView, len(envs))
	for i, be := range envs {
		views[i] = toBackendEnvironmentView(be)
	}

	return views
}
