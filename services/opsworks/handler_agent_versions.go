package opsworks

import (
	"context"
	"encoding/json"
	"fmt"
)

// handleDescribeAgentVersions handles DescribeAgentVersions requests.
func (h *Handler) handleDescribeAgentVersions(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	versions, err := h.Backend.DescribeAgentVersions(req.StackID)
	if err != nil {
		return nil, err
	}

	return map[string]any{"AgentVersions": agentVersionsToJSON(versions)}, nil
}

// handleDescribeOperatingSystems handles DescribeOperatingSystems requests.
func (h *Handler) handleDescribeOperatingSystems(_ context.Context, _ []byte) (any, error) {
	oses, err := h.Backend.DescribeOperatingSystems()
	if err != nil {
		return nil, err
	}

	return map[string]any{"OperatingSystems": operatingSystemsToJSON(oses)}, nil
}

// JSON conversion helpers.

func agentVersionsToJSON(versions []*AgentVersion) []map[string]any {
	result := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		m := map[string]any{fieldVersion: v.Version}
		if v.ConfigurationManager != nil {
			m["ConfigurationManager"] = map[string]any{
				keyName:      v.ConfigurationManager.Name,
				fieldVersion: v.ConfigurationManager.Version,
			}
		}
		result = append(result, m)
	}

	return result
}

func operatingSystemsToJSON(oses []*OperatingSystem) []map[string]any {
	result := make([]map[string]any, 0, len(oses))
	for _, os := range oses {
		managers := make([]map[string]any, 0, len(os.ConfigurationManagers))
		for _, cm := range os.ConfigurationManagers {
			managers = append(managers, map[string]any{
				keyName:      cm.Name,
				fieldVersion: cm.Version,
			})
		}
		result = append(result, map[string]any{
			"Id":                    os.ID,
			keyName:                 os.Name,
			keyType:                 os.Type,
			"ConfigurationManagers": managers,
			"ReportedVersion":       os.ReportedVersion,
			"Supported":             os.Supported,
		})
	}

	return result
}
