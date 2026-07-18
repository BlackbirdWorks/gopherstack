package rolesanywhere

import (
	"context"
	"encoding/json"
	"net/http"
)

// ---- Notification settings handlers ----

func (h *Handler) handlePutNotificationSettings(ctx context.Context, body []byte) (any, int, error) {
	var req struct {
		TrustAnchorID        string                `json:"trustAnchorId"`
		NotificationSettings []NotificationSetting `json:"notificationSettings"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	ta, err := h.Backend.PutNotificationSettings(ctx, req.TrustAnchorID, req.NotificationSettings)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyTrustAnchor: h.trustAnchorJSON(ctx, ta)}, http.StatusOK, nil
}

func (h *Handler) handleResetNotificationSettings(ctx context.Context, body []byte) (any, int, error) {
	var req struct {
		TrustAnchorID           string                   `json:"trustAnchorId"`
		NotificationSettingKeys []NotificationSettingKey `json:"notificationSettingKeys"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	ta, err := h.Backend.ResetNotificationSettings(ctx, req.TrustAnchorID, req.NotificationSettingKeys)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyTrustAnchor: h.trustAnchorJSON(ctx, ta)}, http.StatusOK, nil
}
