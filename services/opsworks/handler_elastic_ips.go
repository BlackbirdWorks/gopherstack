package opsworks

import (
	"context"
	"encoding/json"
	"fmt"
)

// handleRegisterElasticIP handles RegisterElasticIp requests. The real
// RegisterElasticIpInput has no "Region" member -- only ElasticIp and
// StackId, both required (confirmed against
// aws-sdk-go-v2/service/opsworks@v1.31.0's api_op_RegisterElasticIp.go).
func (h *Handler) handleRegisterElasticIP(_ context.Context, body []byte) (any, error) {
	var req struct {
		ElasticIP string `json:"ElasticIp"`
		StackID   string `json:"StackId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	eip, err := h.Backend.RegisterElasticIP(req.ElasticIP, req.StackID)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ElasticIp": eip.IP}, nil
}

// handleDeregisterElasticIP handles DeregisterElasticIp requests.
func (h *Handler) handleDeregisterElasticIP(_ context.Context, body []byte) (any, error) {
	var req struct {
		ElasticIP string `json:"ElasticIp"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeregisterElasticIP(req.ElasticIP); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleAssociateElasticIP handles AssociateElasticIp requests.
func (h *Handler) handleAssociateElasticIP(_ context.Context, body []byte) (any, error) {
	var req struct {
		ElasticIP  string `json:"ElasticIp"`
		InstanceID string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.AssociateElasticIP(req.ElasticIP, req.InstanceID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDisassociateElasticIP handles DisassociateElasticIp requests.
func (h *Handler) handleDisassociateElasticIP(_ context.Context, body []byte) (any, error) {
	var req struct {
		ElasticIP string `json:"ElasticIp"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DisassociateElasticIP(req.ElasticIP); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeElasticIps handles DescribeElasticIps requests.
func (h *Handler) handleDescribeElasticIps(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string   `json:"InstanceId"`
		StackID    string   `json:"StackId"`
		Ips        []string `json:"Ips"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	eips, err := h.Backend.DescribeElasticIps(req.StackID, req.InstanceID, req.Ips)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ElasticIps": elasticIpsToJSON(eips)}, nil
}

// handleUpdateElasticIP handles UpdateElasticIp requests.
func (h *Handler) handleUpdateElasticIP(_ context.Context, body []byte) (any, error) {
	var req struct {
		ElasticIP string `json:"ElasticIp"`
		Name      string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateElasticIP(req.ElasticIP, req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func elasticIpsToJSON(eips []*ElasticIP) []map[string]any {
	result := make([]map[string]any, 0, len(eips))
	for _, e := range eips {
		result = append(result, map[string]any{
			"Ip":          e.IP,
			"Domain":      e.Domain,
			keyName:       e.Name,
			fieldRegion:   e.Region,
			keyInstanceID: e.InstanceID,
		})
	}

	return result
}
