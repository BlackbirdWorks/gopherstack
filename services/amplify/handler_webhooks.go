package amplify

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// JSON response key used by the webhook handlers.
const keyWebhook = "webhook"

// handleAppWebhooks handles POST/GET /apps/{appId}/webhooks.
func (h *Handler) handleAppWebhooks(ctx context.Context, c *echo.Context, appID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createWebhook(ctx, c, appID)
	case http.MethodGet:
		return h.listWebhooks(ctx, c, appID)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleWebhookID handles GET/POST/DELETE /webhooks/{webhookId}.
func (h *Handler) handleWebhookID(ctx context.Context, c *echo.Context, webhookID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getWebhook(ctx, c, webhookID)
	case http.MethodPost:
		return h.updateWebhook(ctx, c, webhookID)
	case http.MethodDelete:
		return h.deleteWebhook(ctx, c, webhookID)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// createWebhook handles POST /apps/{appId}/webhooks.
func (h *Handler) createWebhook(ctx context.Context, c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		BranchName  string `json:"branchName"`
		Description string `json:"description"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	wh, createErr := h.Backend.CreateWebhook(appID, input.BranchName, input.Description)
	if createErr != nil {
		return h.handleBackendError(ctx, c, "CreateWebhook", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyWebhook: toWebhookView(wh)})
}

// listWebhooks handles GET /apps/{appId}/webhooks.
func (h *Handler) listWebhooks(ctx context.Context, c *echo.Context, appID string) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")

	maxResults := 0
	if s := q.Get("maxResults"); s != "" {
		if n, convErr := strconv.Atoi(s); convErr == nil && n > 0 {
			maxResults = n
		}
	}

	webhooks, outToken, err := h.Backend.ListWebhooks(appID, nextToken, maxResults)
	if err != nil {
		return h.handleBackendError(ctx, c, "ListWebhooks", err)
	}

	resp := map[string]any{"webhooks": toWebhookViews(webhooks)}
	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

// getWebhook handles GET /webhooks/{webhookId}.
func (h *Handler) getWebhook(ctx context.Context, c *echo.Context, webhookID string) error {
	wh, err := h.Backend.GetWebhook(webhookID)
	if err != nil {
		return h.handleBackendError(ctx, c, "GetWebhook", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyWebhook: toWebhookView(wh)})
}

// updateWebhook handles POST /webhooks/{webhookId}.
func (h *Handler) updateWebhook(ctx context.Context, c *echo.Context, webhookID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		BranchName  string `json:"branchName"`
		Description string `json:"description"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	wh, updateErr := h.Backend.UpdateWebhook(webhookID, input.BranchName, input.Description)
	if updateErr != nil {
		return h.handleBackendError(ctx, c, "UpdateWebhook", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyWebhook: toWebhookView(wh)})
}

// deleteWebhook handles DELETE /webhooks/{webhookId}.
func (h *Handler) deleteWebhook(ctx context.Context, c *echo.Context, webhookID string) error {
	wh, err := h.Backend.DeleteWebhook(webhookID)
	if err != nil {
		return h.handleBackendError(ctx, c, "DeleteWebhook", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyWebhook: toWebhookView(wh)})
}

// parseWebhookIDOp maps the method for /webhooks/{webhookId} to its operation name.
func parseWebhookIDOp(method string) string {
	switch method {
	case http.MethodGet:
		return "GetWebhook"
	case http.MethodPost:
		return "UpdateWebhook"
	case http.MethodDelete:
		return "DeleteWebhook"
	default:
		return opUnknown
	}
}

type webhookView struct {
	WebhookID   string  `json:"webhookId"`
	WebhookARN  string  `json:"webhookArn"`
	AppID       string  `json:"appId"`
	BranchName  string  `json:"branchName"`
	Description string  `json:"description"`
	WebhookURL  string  `json:"webhookUrl"`
	CreateTime  float64 `json:"createTime"`
	UpdateTime  float64 `json:"updateTime"`
}

func toWebhookView(w *Webhook) webhookView {
	return webhookView{
		CreateTime:  float64(w.CreateTime.Unix()),
		UpdateTime:  float64(w.UpdateTime.Unix()),
		WebhookID:   w.WebhookID,
		WebhookARN:  w.WebhookARN,
		AppID:       w.AppID,
		BranchName:  w.BranchName,
		Description: w.Description,
		WebhookURL:  w.WebhookURL,
	}
}

func toWebhookViews(ws []*Webhook) []webhookView {
	views := make([]webhookView, len(ws))
	for i, w := range ws {
		views[i] = toWebhookView(w)
	}

	return views
}
