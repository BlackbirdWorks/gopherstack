package opsworks

import (
	"context"
	"encoding/json"
	"fmt"
)

// handleDescribeServiceErrors handles DescribeServiceErrors requests.
func (h *Handler) handleDescribeServiceErrors(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID         string   `json:"StackId"`
		InstanceID      string   `json:"InstanceId"`
		ServiceErrorIDs []string `json:"ServiceErrorIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	errors, err := h.Backend.DescribeServiceErrors(req.StackID, req.InstanceID, req.ServiceErrorIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ServiceErrors": errors}, nil
}

// handleDescribeRaidArrays handles DescribeRaidArrays requests.
func (h *Handler) handleDescribeRaidArrays(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID   string   `json:"InstanceId"`
		StackID      string   `json:"StackId"`
		RaidArrayIDs []string `json:"RaidArrayIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	arrays, err := h.Backend.DescribeRaidArrays(req.InstanceID, req.StackID, req.RaidArrayIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"RaidArrays": arrays}, nil
}
