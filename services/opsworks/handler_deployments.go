package opsworks

import (
	"context"
	"encoding/json"
	"fmt"
)

// handleCreateDeployment handles CreateDeployment requests.
func (h *Handler) handleCreateDeployment(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
		AppID   string `json:"AppId"`
		Command struct {
			Name string `json:"Name"`
		} `json:"Command"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	deployment, err := h.Backend.CreateDeployment(req.StackID, req.AppID, req.Command.Name)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyDeploymentID: deployment.DeploymentID}, nil
}

// handleDescribeDeployments handles DescribeDeployments requests.
func (h *Handler) handleDescribeDeployments(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID       string   `json:"StackId"`
		AppID         string   `json:"AppId"`
		DeploymentIDs []string `json:"DeploymentIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	deployments, err := h.Backend.DescribeDeployments(req.StackID, req.AppID, req.DeploymentIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Deployments": deploymentsToJSON(deployments)}, nil
}

func deploymentsToJSON(deployments []*Deployment) []map[string]any {
	result := make([]map[string]any, 0, len(deployments))
	for _, d := range deployments {
		completedAt := ""
		if !d.CompletedAt.IsZero() && d.CompletedAt != d.CreatedAt {
			completedAt = d.CompletedAt.Format("2006-01-02T15:04:05+00:00")
		}

		result = append(result, map[string]any{
			keyDeploymentID: d.DeploymentID,
			keyStackID:      d.StackID,
			keyAppID:        d.AppID,
			"Command":       map[string]any{keyName: d.Command},
			keyStatus:       d.Status,
			"Duration":      d.Duration,
			keyCreatedAt:    d.CreatedAt.Format("2006-01-02T15:04:05+00:00"),
			"CompletedAt":   completedAt,
		})
	}

	return result
}
