package opsworks

import (
	"context"
	"encoding/json"
	"fmt"
)

// handleDescribeCommands handles DescribeCommands requests.
// Note: AWS uses "CommandIds" (lowercase 'd') not "CommandIDs".
func (h *Handler) handleDescribeCommands(_ context.Context, body []byte) (any, error) {
	var req struct {
		DeploymentID string   `json:"DeploymentId"`
		InstanceID   string   `json:"InstanceId"`
		CommandIDs   []string `json:"CommandIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	commands, err := h.Backend.DescribeCommands(req.DeploymentID, req.InstanceID, req.CommandIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Commands": commandsToJSON(commands)}, nil
}

func commandsToJSON(commands []*Command) []map[string]any {
	result := make([]map[string]any, 0, len(commands))
	for _, c := range commands {
		result = append(result, map[string]any{
			"CommandId":      c.CommandID,
			keyDeploymentID:  c.DeploymentID,
			keyInstanceID:    c.InstanceID,
			keyType:          c.Type,
			keyStatus:        c.Status,
			"ExitCode":       c.ExitCode,
			"LogUrl":         c.LogURL,
			keyCreatedAt:     formatOpsWorksTime(c.CreatedAt),
			"AcknowledgedAt": formatOpsWorksTime(c.AcknowledgedAt),
			"CompletedAt":    formatOpsWorksTime(c.CompletedAt),
		})
	}

	return result
}
