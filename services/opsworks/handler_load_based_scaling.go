package opsworks

import (
	"context"
	"encoding/json"
	"fmt"
)

// handleSetLoadBasedAutoScaling handles SetLoadBasedAutoScaling requests.
func (h *Handler) handleSetLoadBasedAutoScaling(_ context.Context, body []byte) (any, error) {
	var req struct {
		UpScaling   *ScalingParameters `json:"UpScaling"`
		DownScaling *ScalingParameters `json:"DownScaling"`
		LayerID     string             `json:"LayerId"`
		Enable      bool               `json:"Enable"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.SetLoadBasedAutoScaling(req.LayerID, req.Enable, req.UpScaling, req.DownScaling); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeLoadBasedAutoScaling handles DescribeLoadBasedAutoScaling requests.
func (h *Handler) handleDescribeLoadBasedAutoScaling(_ context.Context, body []byte) (any, error) {
	var req struct {
		LayerIDs []string `json:"LayerIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	configs, err := h.Backend.DescribeLoadBasedAutoScaling(req.LayerIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"LoadBasedAutoScalingConfigurations": loadBasedAutoScalingToJSON(configs)}, nil
}

func loadBasedAutoScalingToJSON(configs []*LoadBasedAutoScaling) []map[string]any {
	result := make([]map[string]any, 0, len(configs))
	for _, c := range configs {
		m := map[string]any{
			keyLayerID: c.LayerID,
			"Enable":   c.Enable,
		}
		if c.UpScaling != nil {
			m["UpScaling"] = scalingParamsToJSON(c.UpScaling)
		}
		if c.DownScaling != nil {
			m["DownScaling"] = scalingParamsToJSON(c.DownScaling)
		}
		result = append(result, m)
	}

	return result
}

func scalingParamsToJSON(p *ScalingParameters) map[string]any {
	return map[string]any{
		"CpuThreshold":       p.CPUThreshold,
		"IgnoreMetricsTime":  p.IgnoreMetricsTime,
		"InstanceCount":      p.InstanceCount,
		"LoadThreshold":      p.LoadThreshold,
		"MemoryThreshold":    p.MemoryThreshold,
		"ThresholdsWaitTime": p.ThresholdsWaitTime,
	}
}
