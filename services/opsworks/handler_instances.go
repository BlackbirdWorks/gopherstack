package opsworks

import (
	"context"
	"encoding/json"
	"fmt"
)

// handleCreateInstance handles CreateInstance requests.
func (h *Handler) handleCreateInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID      string   `json:"StackId"`
		InstanceType string   `json:"InstanceType"`
		LayerIDs     []string `json:"LayerIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	layerID := ""
	if len(req.LayerIDs) > 0 {
		layerID = req.LayerIDs[0]
	}

	instance, err := h.Backend.CreateInstance(req.StackID, layerID, req.InstanceType)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyInstanceID: instance.InstanceID}, nil
}

// handleRegisterInstance handles RegisterInstance requests.
func (h *Handler) handleRegisterInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID  string `json:"StackId"`
		Hostname string `json:"Hostname"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	instanceID, err := h.Backend.RegisterInstance(req.StackID, req.Hostname)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyInstanceID: instanceID}, nil
}

// handleDeregisterInstance handles DeregisterInstance requests.
func (h *Handler) handleDeregisterInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeregisterInstance(req.InstanceID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleAssignInstance handles AssignInstance requests.
func (h *Handler) handleAssignInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string   `json:"InstanceId"`
		LayerIDs   []string `json:"LayerIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.AssignInstance(req.InstanceID, req.LayerIDs); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleUnassignInstance handles UnassignInstance requests.
func (h *Handler) handleUnassignInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UnassignInstance(req.InstanceID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeInstances handles DescribeInstances requests.
func (h *Handler) handleDescribeInstances(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID     string   `json:"StackId"`
		LayerID     string   `json:"LayerId"`
		InstanceIDs []string `json:"InstanceIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	instances, err := h.Backend.DescribeInstances(req.StackID, req.LayerID, req.InstanceIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Instances": instancesToJSON(instances)}, nil
}

// handleUpdateInstance handles UpdateInstance requests.
func (h *Handler) handleUpdateInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string `json:"InstanceId"`
		Hostname   string `json:"Hostname"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateInstance(req.InstanceID, req.Hostname); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDeleteInstance handles DeleteInstance requests.
func (h *Handler) handleDeleteInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteInstance(req.InstanceID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleStartInstance handles StartInstance requests.
func (h *Handler) handleStartInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.StartInstance(req.InstanceID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleStopInstance handles StopInstance requests.
func (h *Handler) handleStopInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.StopInstance(req.InstanceID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleRebootInstance handles RebootInstance requests.
func (h *Handler) handleRebootInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.RebootInstance(req.InstanceID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func instancesToJSON(instances []*Instance) []map[string]any {
	result := make([]map[string]any, 0, len(instances))
	for _, i := range instances {
		result = append(result, map[string]any{
			keyInstanceID:  i.InstanceID,
			keyStackID:     i.StackID,
			keyLayerID:     i.LayerID,
			keyArn:         i.Arn,
			"Hostname":     i.Hostname,
			"InstanceType": i.InstanceType,
			keyStatus:      i.Status,
			keyCreatedAt:   i.CreatedAt.Format("2006-01-02T15:04:05+00:00"),
		})
	}

	return result
}
