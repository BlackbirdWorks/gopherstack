package opsworks

import (
	"context"
	"encoding/json"
	"fmt"
)

// handleCreateApp handles CreateApp requests.
func (h *Handler) handleCreateApp(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
		Name    string `json:"Name"`
		Type    string `json:"Type"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	app, err := h.Backend.CreateApp(req.StackID, req.Name, req.Type)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyAppID: app.AppID}, nil
}

// handleDescribeApps handles DescribeApps requests.
func (h *Handler) handleDescribeApps(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string   `json:"StackId"`
		AppIDs  []string `json:"AppIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	apps, err := h.Backend.DescribeApps(req.StackID, req.AppIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Apps": appsToJSON(apps)}, nil
}

// handleUpdateApp handles UpdateApp requests.
func (h *Handler) handleUpdateApp(_ context.Context, body []byte) (any, error) {
	var req struct {
		AppID string `json:"AppId"`
		Name  string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateApp(req.AppID, req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDeleteApp handles DeleteApp requests.
func (h *Handler) handleDeleteApp(_ context.Context, body []byte) (any, error) {
	var req struct {
		AppID string `json:"AppId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteApp(req.AppID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func appsToJSON(apps []*App) []map[string]any {
	result := make([]map[string]any, 0, len(apps))
	for _, a := range apps {
		result = append(result, map[string]any{
			keyAppID:     a.AppID,
			keyStackID:   a.StackID,
			keyArn:       a.Arn,
			keyName:      a.Name,
			keyType:      a.Type,
			keyCreatedAt: a.CreatedAt.Format("2006-01-02T15:04:05+00:00"),
		})
	}

	return result
}
