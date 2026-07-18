package opsworks

import (
	"context"
	"encoding/json"
	"fmt"
)

// handleRegisterRdsDBInstance handles RegisterRdsDbInstance requests.
func (h *Handler) handleRegisterRdsDBInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID          string `json:"StackId"`
		RdsDBInstanceArn string `json:"RdsDbInstanceArn"`
		DBUser           string `json:"DbUser"`
		DBPassword       string `json:"DbPassword"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.RegisterRdsDBInstance(
		req.StackID, req.RdsDBInstanceArn, req.DBUser, req.DBPassword,
	); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDeregisterRdsDBInstance handles DeregisterRdsDbInstance requests.
func (h *Handler) handleDeregisterRdsDBInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		RdsDBInstanceArn string `json:"RdsDbInstanceArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeregisterRdsDBInstance(req.RdsDBInstanceArn); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeRdsDBInstances handles DescribeRdsDbInstances requests.
func (h *Handler) handleDescribeRdsDBInstances(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID           string   `json:"StackId"`
		RdsDBInstanceArns []string `json:"RdsDbInstanceArns"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	instances, err := h.Backend.DescribeRdsDBInstances(req.StackID, req.RdsDBInstanceArns)
	if err != nil {
		return nil, err
	}

	return map[string]any{"RdsDbInstances": rdsDBInstancesToJSON(instances)}, nil
}

// handleUpdateRdsDBInstance handles UpdateRdsDbInstance requests.
func (h *Handler) handleUpdateRdsDBInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		RdsDBInstanceArn string `json:"RdsDbInstanceArn"`
		DBUser           string `json:"DbUser"`
		DBPassword       string `json:"DbPassword"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateRdsDBInstance(req.RdsDBInstanceArn, req.DBUser, req.DBPassword); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func rdsDBInstancesToJSON(rdbs []*RdsDBInstance) []map[string]any {
	result := make([]map[string]any, 0, len(rdbs))
	for _, r := range rdbs {
		result = append(result, map[string]any{
			"RdsDbInstanceArn":     r.RdsDBInstanceArn,
			"DbInstanceIdentifier": r.DBInstanceIdentifier,
			"DbUser":               r.DBUser,
			keyStackID:             r.StackID,
			fieldRegion:            r.Region,
			"Address":              r.Address,
		})
	}

	return result
}
