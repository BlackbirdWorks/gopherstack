package rolesanywhere

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// ---- Trust Anchor handlers ----

func (h *Handler) handleCreateTrustAnchor(ctx context.Context, body []byte) (any, int, error) {
	var req struct {
		Enabled *bool             `json:"enabled"`
		Name    string            `json:"name"`
		Source  TrustAnchorSource `json:"source"`
		Tags    []TagEntry        `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	ta, err := h.Backend.CreateTrustAnchor(ctx, req.Name, req.Source, req.Tags, req.Enabled)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyTrustAnchor: h.trustAnchorJSON(ctx, ta)}, http.StatusCreated, nil
}

func (h *Handler) handleGetTrustAnchor(ctx context.Context, path string) (any, int, error) {
	id := extractID(path, pathTrustanchor)

	ta, err := h.Backend.GetTrustAnchor(ctx, id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyTrustAnchor: h.trustAnchorJSON(ctx, ta)}, http.StatusOK, nil
}

func (h *Handler) handleListTrustAnchors(ctx context.Context, query string) (any, int, error) {
	pageToken, maxResults, ppErr := parsePageParams(query)
	if ppErr != nil {
		return nil, 0, ppErr
	}

	all, next, err := h.Backend.ListTrustAnchors(ctx, pageToken, maxResults)
	if err != nil {
		return nil, 0, err
	}

	list := make([]any, 0, len(all))

	for _, ta := range all {
		list = append(list, h.trustAnchorJSON(ctx, ta))
	}

	resp := map[string]any{keyTrustAnchors: list}

	if next != "" {
		resp["nextToken"] = next
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleDeleteTrustAnchor(ctx context.Context, path string) (any, int, error) {
	id := extractID(path, pathTrustanchor)

	ta, err := h.Backend.DeleteTrustAnchor(ctx, id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyTrustAnchor: h.trustAnchorJSON(ctx, ta)}, http.StatusOK, nil
}

func (h *Handler) handleUpdateTrustAnchor(ctx context.Context, path string, body []byte) (any, int, error) {
	id := extractID(path, pathTrustanchor)

	var req struct {
		Source *TrustAnchorSource `json:"source"`
		Name   string             `json:"name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	ta, err := h.Backend.UpdateTrustAnchor(ctx, id, req.Name, req.Source)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyTrustAnchor: h.trustAnchorJSON(ctx, ta)}, http.StatusOK, nil
}

func (h *Handler) handleEnableTrustAnchor(ctx context.Context, path string) (any, int, error) {
	id := extractID(path, pathTrustanchor)

	ta, err := h.Backend.EnableTrustAnchor(ctx, id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyTrustAnchor: h.trustAnchorJSON(ctx, ta)}, http.StatusOK, nil
}

func (h *Handler) handleDisableTrustAnchor(ctx context.Context, path string) (any, int, error) {
	id := extractID(path, pathTrustanchor)

	ta, err := h.Backend.DisableTrustAnchor(ctx, id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyTrustAnchor: h.trustAnchorJSON(ctx, ta)}, http.StatusOK, nil
}

// ---- JSON serialization ----

// trustAnchorJSON renders ta together with its current notification settings.
// AWS's TrustAnchorDetail carries notificationSettings on every read (Get,
// List, Create, Update, Enable, Disable, Delete), not only on the dedicated
// Put/ResetNotificationSettings responses, so every trust anchor handler
// routes through this instead of the bare trustAnchorToJSON.
func (h *Handler) trustAnchorJSON(ctx context.Context, ta *TrustAnchor) map[string]any {
	settings := h.Backend.GetNotificationSettings(ctx, ta.TrustAnchorID)

	return trustAnchorWithSettingsToJSON(ta, settings)
}

func trustAnchorToJSON(ta *TrustAnchor) map[string]any {
	m := map[string]any{
		"trustAnchorId":  ta.TrustAnchorID,
		"trustAnchorArn": ta.TrustAnchorArn,
		"name":           ta.Name, //nolint:goconst // existing issue.
		"source":         ta.Source,
		"enabled":        ta.Enabled,                        //nolint:goconst // existing issue.
		"createdAt":      ta.CreatedAt.Format(time.RFC3339), //nolint:goconst // existing issue.
		"updatedAt":      ta.UpdatedAt.Format(time.RFC3339), //nolint:goconst // existing issue.
	}

	if len(ta.Tags) > 0 {
		m["tags"] = ta.Tags
	}

	return m
}

func trustAnchorWithSettingsToJSON(ta *TrustAnchor, settings []NotificationSetting) map[string]any {
	m := trustAnchorToJSON(ta)

	if len(settings) > 0 {
		m["notificationSettings"] = settings
	}

	return m
}
