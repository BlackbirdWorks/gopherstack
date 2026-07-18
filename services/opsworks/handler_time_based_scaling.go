package opsworks

import (
	"context"
	"encoding/json"
	"fmt"
)

// handleSetTimeBasedAutoScaling handles SetTimeBasedAutoScaling requests.
func (h *Handler) handleSetTimeBasedAutoScaling(_ context.Context, body []byte) (any, error) {
	var req struct {
		AutoScalingSchedule *AutoScalingSchedule `json:"AutoScalingSchedule"`
		InstanceID          string               `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.SetTimeBasedAutoScaling(req.InstanceID, req.AutoScalingSchedule); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeTimeBasedAutoScaling handles DescribeTimeBasedAutoScaling requests.
func (h *Handler) handleDescribeTimeBasedAutoScaling(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceIDs []string `json:"InstanceIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	configs, err := h.Backend.DescribeTimeBasedAutoScaling(req.InstanceIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"TimeBasedAutoScalingConfigurations": timeBasedAutoScalingToJSON(configs)}, nil
}

func timeBasedAutoScalingToJSON(configs []*TimeBasedAutoScaling) []map[string]any {
	result := make([]map[string]any, 0, len(configs))
	for _, c := range configs {
		schedule := map[string]any{}
		if c.AutoScalingSchedule != nil {
			s := c.AutoScalingSchedule
			schedule = map[string]any{
				"Monday":    s.Monday,
				"Tuesday":   s.Tuesday,
				"Wednesday": s.Wednesday,
				"Thursday":  s.Thursday,
				"Friday":    s.Friday,
				"Saturday":  s.Saturday,
				"Sunday":    s.Sunday,
			}
		}
		result = append(result, map[string]any{
			keyInstanceID:         c.InstanceID,
			"AutoScalingSchedule": schedule,
		})
	}

	return result
}
