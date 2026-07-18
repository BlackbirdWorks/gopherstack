package opsworks

import (
	"context"
	"encoding/json"
	"fmt"
)

// handleRegisterVolume handles RegisterVolume requests.
func (h *Handler) handleRegisterVolume(_ context.Context, body []byte) (any, error) {
	var req struct {
		Ec2VolumeID string `json:"Ec2VolumeId"`
		StackID     string `json:"StackId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	volumeID, err := h.Backend.RegisterVolume(req.Ec2VolumeID, req.StackID)
	if err != nil {
		return nil, err
	}

	return map[string]any{"VolumeId": volumeID}, nil
}

// handleDeregisterVolume handles DeregisterVolume requests.
func (h *Handler) handleDeregisterVolume(_ context.Context, body []byte) (any, error) {
	var req struct {
		VolumeID string `json:"VolumeId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeregisterVolume(req.VolumeID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleAssignVolume handles AssignVolume requests.
func (h *Handler) handleAssignVolume(_ context.Context, body []byte) (any, error) {
	var req struct {
		VolumeID   string `json:"VolumeId"`
		InstanceID string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.AssignVolume(req.VolumeID, req.InstanceID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleUnassignVolume handles UnassignVolume requests.
func (h *Handler) handleUnassignVolume(_ context.Context, body []byte) (any, error) {
	var req struct {
		VolumeID string `json:"VolumeId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UnassignVolume(req.VolumeID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeVolumes handles DescribeVolumes requests.
func (h *Handler) handleDescribeVolumes(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID  string   `json:"InstanceId"`
		RaidArrayID string   `json:"RaidArrayId"`
		VolumeIDs   []string `json:"VolumeIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	volumes, err := h.Backend.DescribeVolumes(req.InstanceID, req.RaidArrayID, req.VolumeIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Volumes": volumesToJSON(volumes)}, nil
}

// handleUpdateVolume handles UpdateVolume requests.
func (h *Handler) handleUpdateVolume(_ context.Context, body []byte) (any, error) {
	var req struct {
		VolumeID   string `json:"VolumeId"`
		Name       string `json:"Name"`
		MountPoint string `json:"MountPoint"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateVolume(req.VolumeID, req.Name, req.MountPoint); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func volumesToJSON(vols []*Volume) []map[string]any {
	result := make([]map[string]any, 0, len(vols))
	for _, v := range vols {
		result = append(result, map[string]any{
			"VolumeId":    v.VolumeID,
			"Ec2VolumeId": v.Ec2VolumeID,
			keyStackID:    v.StackID,
			keyInstanceID: v.InstanceID,
			keyName:       v.Name,
			"MountPoint":  v.MountPoint,
			fieldRegion:   v.Region,
			keyStatus:     v.Status,
			"Size":        v.Size,
		})
	}

	return result
}
