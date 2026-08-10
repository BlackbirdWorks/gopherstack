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
		LayerID string `json:"LayerId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	hostname, err := h.Backend.GetHostnameSuggestion(req.LayerID)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Hostname": hostname, keyLayerID: req.LayerID}, nil
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

	params, err := h.Backend.DescribeStackProvisioningParameters(req.StackID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"Parameters":        params,
		"AgentInstallerUrl": params["AgentInstallerUrl"],
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
			keyCreatedAt:                formatOpsWorksTime(s.CreatedAt),
		})
	}

	return result
}

// instancesCountToJSON mirrors the real types.InstancesCount field set
// exactly (19 states, all always present) -- see InstancesCount's doc
// comment in interfaces.go for why "Total"/"Starting" are not among them.
func instancesCountToJSON(ic *InstancesCount) map[string]any {
	if ic == nil {
		ic = &InstancesCount{}
	}

	return map[string]any{
		"Assigning":      ic.Assigning,
		"Booting":        ic.Booting,
		"ConnectionLost": ic.ConnectionLost,
		"Deregistering":  ic.Deregistering,
		"Online":         ic.Online,
		"Pending":        ic.Pending,
		"Rebooting":      ic.Rebooting,
		"Registered":     ic.Registered,
		"Registering":    ic.Registering,
		"Requested":      ic.Requested,
		"RunningSetup":   ic.RunningSetup,
		"SetupFailed":    ic.SetupFailed,
		"ShuttingDown":   ic.ShuttingDown,
		"StartFailed":    ic.StartFailed,
		"StopFailed":     ic.StopFailed,
		"Stopped":        ic.Stopped,
		"Stopping":       ic.Stopping,
		"Terminated":     ic.Terminated,
		"Terminating":    ic.Terminating,
		"Unassigning":    ic.Unassigning,
	}
}

func stackSummaryToJSON(s *StackSummary) map[string]any {
	return map[string]any{
		keyStackID:       s.StackID,
		keyArn:           s.Arn,
		keyName:          s.Name,
		"InstancesCount": instancesCountToJSON(s.InstancesCount),
		"LayersCount":    s.LayersCount,
		"AppsCount":      s.AppsCount,
	}
}
