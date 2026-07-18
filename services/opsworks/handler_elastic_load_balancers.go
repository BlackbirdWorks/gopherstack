package opsworks

import (
	"context"
	"encoding/json"
	"fmt"
)

// handleAttachElasticLoadBalancer handles AttachElasticLoadBalancer requests.
func (h *Handler) handleAttachElasticLoadBalancer(_ context.Context, body []byte) (any, error) {
	var req struct {
		ElasticLoadBalancerName string `json:"ElasticLoadBalancerName"`
		LayerID                 string `json:"LayerId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.AttachElasticLoadBalancer(req.ElasticLoadBalancerName, req.LayerID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDetachElasticLoadBalancer handles DetachElasticLoadBalancer requests.
func (h *Handler) handleDetachElasticLoadBalancer(_ context.Context, body []byte) (any, error) {
	var req struct {
		ElasticLoadBalancerName string `json:"ElasticLoadBalancerName"`
		LayerID                 string `json:"LayerId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DetachElasticLoadBalancer(req.ElasticLoadBalancerName, req.LayerID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeElasticLoadBalancers handles DescribeElasticLoadBalancers requests.
func (h *Handler) handleDescribeElasticLoadBalancers(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID  string   `json:"StackId"`
		LayerIDs []string `json:"LayerIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	layerID := ""
	if len(req.LayerIDs) > 0 {
		layerID = req.LayerIDs[0]
	}

	elbs, err := h.Backend.DescribeElasticLoadBalancers(req.StackID, layerID)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ElasticLoadBalancers": elasticLBsToJSON(elbs)}, nil
}

func elasticLBsToJSON(elbs []*ElasticLoadBalancer) []map[string]any {
	result := make([]map[string]any, 0, len(elbs))
	for _, e := range elbs {
		result = append(result, map[string]any{
			"ElasticLoadBalancerName": e.ElasticLoadBalancerName,
			fieldRegion:               e.Region,
			"DnsName":                 e.DNSName,
			keyStackID:                e.StackID,
			keyLayerID:                e.LayerID,
		})
	}

	return result
}
