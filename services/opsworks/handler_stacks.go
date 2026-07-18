package opsworks

import (
	"context"
	"encoding/json"
	"fmt"
)

// handleCreateStack handles CreateStack requests.
func (h *Handler) handleCreateStack(_ context.Context, body []byte) (any, error) {
	var req struct {
		Name                      string `json:"Name"`
		Region                    string `json:"Region"`
		DefaultInstanceProfileArn string `json:"DefaultInstanceProfileArn"`
		ServiceRoleArn            string `json:"ServiceRoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	stack, err := h.Backend.CreateStack(
		req.Name, req.Region,
		req.DefaultInstanceProfileArn,
		req.ServiceRoleArn,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyStackID: stack.StackID}, nil
}

// handleCloneStack handles CloneStack requests.
func (h *Handler) handleCloneStack(_ context.Context, body []byte) (any, error) {
	var req struct {
		SourceStackID string `json:"SourceStackId"`
		Name          string `json:"Name"`
		Region        string `json:"Region"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	stack, err := h.Backend.CloneStack(req.SourceStackID, req.Name, req.Region)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyStackID: stack.StackID}, nil
}

// handleDescribeStacks handles DescribeStacks requests.
func (h *Handler) handleDescribeStacks(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackIDs []string `json:"StackIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	stacks, err := h.Backend.DescribeStacks(req.StackIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Stacks": stacksToJSON(stacks)}, nil
}

// handleUpdateStack handles UpdateStack requests.
func (h *Handler) handleUpdateStack(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
		Name    string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateStack(req.StackID, req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDeleteStack handles DeleteStack requests.
func (h *Handler) handleDeleteStack(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteStack(req.StackID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleStartStack handles StartStack requests.
func (h *Handler) handleStartStack(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.StartStack(req.StackID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleStopStack handles StopStack requests.
func (h *Handler) handleStopStack(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.StopStack(req.StackID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleGetHostnameSuggestion handles GetHostnameSuggestion requests.
func (h *Handler) handleGetHostnameSuggestion(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
		LayerID string `json:"LayerId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	hostname, err := h.Backend.GetHostnameSuggestion(req.StackID, req.LayerID)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Hostname": hostname}, nil
}

// handleDescribeStackSummary handles DescribeStackSummary requests.
func (h *Handler) handleDescribeStackSummary(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	summary, err := h.Backend.DescribeStackSummary(req.StackID)
	if err != nil {
		return nil, err
	}

	return map[string]any{"StackSummary": stackSummaryToJSON(summary)}, nil
}

// handleDescribeStackProvisioningParameters handles DescribeStackProvisioningParameters requests.
func (h *Handler) handleDescribeStackProvisioningParameters(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	params, stackArn, err := h.Backend.DescribeStackProvisioningParameters(req.StackID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"Parameters":        params,
		"AgentInstallerUrl": params["AgentInstallerUrl"],
		"StackArn":          stackArn,
	}, nil
}

func stacksToJSON(stacks []*Stack) []map[string]any {
	result := make([]map[string]any, 0, len(stacks))
	for _, s := range stacks {
		result = append(result, map[string]any{
			keyStackID:                  s.StackID,
			keyArn:                      s.Arn,
			keyName:                     s.Name,
			fieldRegion:                 s.Region,
			"DefaultInstanceProfileArn": s.DefaultInstanceProfileArn,
			"ServiceRoleArn":            s.ServiceRoleArn,
			keyStatus:                   s.Status,
			keyCreatedAt:                s.CreatedAt.Format("2006-01-02T15:04:05+00:00"),
		})
	}

	return result
}

func stackSummaryToJSON(s *StackSummary) map[string]any {
	ic := map[string]any{}
	if s.InstancesCount != nil {
		ic = map[string]any{
			"Online":   s.InstancesCount.Online,
			"Stopped":  s.InstancesCount.Stopped,
			"Starting": s.InstancesCount.Starting,
			"Stopping": s.InstancesCount.Stopping,
			"Total":    s.InstancesCount.Total,
		}
	}

	return map[string]any{
		keyStackID:       s.StackID,
		keyArn:           s.Arn,
		keyName:          s.Name,
		"InstancesCount": ic,
		"LayersCount":    s.LayersCount,
		"AppsCount":      s.AppsCount,
	}
}
