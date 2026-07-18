package amplify

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// JSON response keys used by the app handlers.
const keyApp = "app"

// handleApps handles POST/GET /apps.
func (h *Handler) handleApps(ctx context.Context, c *echo.Context) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createApp(ctx, c)
	case http.MethodGet:
		return h.listApps(ctx, c)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleAppID handles GET/POST/DELETE /apps/{appId}.
func (h *Handler) handleAppID(ctx context.Context, c *echo.Context, appID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getApp(ctx, c, appID)
	case http.MethodDelete:
		return h.deleteApp(ctx, c, appID)
	case http.MethodPost:
		return h.updateApp(ctx, c, appID)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// createApp handles POST /apps.
func (h *Handler) createApp(ctx context.Context, c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		Tags        map[string]string `json:"tags"`
		Name        string            `json:"name"`
		Description string            `json:"description"`
		Repository  string            `json:"repository"`
		Platform    string            `json:"platform"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	if input.Name == "" {
		return amplifyErrorJSON(c, http.StatusBadRequest, "name is required")
	}

	app, createErr := h.Backend.CreateApp(input.Name, input.Description, input.Repository, input.Platform, input.Tags)
	if createErr != nil {
		return h.handleBackendError(ctx, c, "CreateApp", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyApp: toAppView(app)})
}

// getApp handles GET /apps/{appId}.
func (h *Handler) getApp(ctx context.Context, c *echo.Context, appID string) error {
	app, err := h.Backend.GetApp(appID)
	if err != nil {
		return h.handleBackendError(ctx, c, "GetApp", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyApp: toAppView(app)})
}

// listApps handles GET /apps.
func (h *Handler) listApps(ctx context.Context, c *echo.Context) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")

	maxResults := 0
	if s := q.Get("maxResults"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			maxResults = n
		}
	}

	apps, outToken, err := h.Backend.ListApps(nextToken, maxResults)
	if err != nil {
		return h.handleBackendError(ctx, c, "ListApps", err)
	}

	resp := map[string]any{arnResourceApps: toAppViews(apps)}
	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

// deleteApp handles DELETE /apps/{appId}.
func (h *Handler) deleteApp(ctx context.Context, c *echo.Context, appID string) error {
	if err := h.Backend.DeleteApp(appID); err != nil {
		return h.handleBackendError(ctx, c, "DeleteApp", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// updateApp handles POST /apps/{appId}.
func (h *Handler) updateApp(ctx context.Context, c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Repository  string `json:"repository"`
		Platform    string `json:"platform"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	app, updateErr := h.Backend.UpdateApp(appID, input.Name, input.Description, input.Repository, input.Platform)
	if updateErr != nil {
		return h.handleBackendError(ctx, c, "UpdateApp", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyApp: toAppView(app)})
}

// parseAppsOperation maps the method for /apps to its operation name.
func parseAppsOperation(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateApp"
	case http.MethodGet:
		return "ListApps"
	default:
		return opUnknown
	}
}

// parseAppIDOperation maps the method for /apps/{appId} to its operation name.
func parseAppIDOperation(method string) string {
	switch method {
	case http.MethodGet:
		return "GetApp"
	case http.MethodDelete:
		return "DeleteApp"
	case http.MethodPost:
		return "UpdateApp"
	default:
		return opUnknown
	}
}

// appView is the JSON representation of an App with timestamps as Unix epoch
// float64 values, as required by the AWS SDK v2 deserialiser.
type appView struct {
	Tags          map[string]string `json:"tags,omitempty"`
	AppID         string            `json:"appId"`
	ARN           string            `json:"appArn"`
	Name          string            `json:"name"`
	Description   string            `json:"description,omitempty"`
	Repository    string            `json:"repository,omitempty"`
	DefaultDomain string            `json:"defaultDomain,omitempty"`
	Platform      Platform          `json:"platform"`
	CreateTime    float64           `json:"createTime"`
	UpdateTime    float64           `json:"updateTime"`
}

func toAppView(a *App) appView {
	var tagMap map[string]string
	if a.Tags != nil {
		tagMap = a.Tags.Clone()
	}

	return appView{
		Tags:          tagMap,
		CreateTime:    float64(a.CreateTime.Unix()),
		UpdateTime:    float64(a.UpdateTime.Unix()),
		AppID:         a.AppID,
		ARN:           a.ARN,
		Name:          a.Name,
		Description:   a.Description,
		Repository:    a.Repository,
		DefaultDomain: a.DefaultDomain,
		Platform:      a.Platform,
	}
}

func toAppViews(apps []*App) []appView {
	views := make([]appView, len(apps))
	for i, a := range apps {
		views[i] = toAppView(a)
	}

	return views
}
