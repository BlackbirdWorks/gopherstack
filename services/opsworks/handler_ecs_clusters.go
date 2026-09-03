package opsworks

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// defaultEcsClustersPageSize is used when DescribeEcsClustersInput.MaxResults
// is unset -- the real API's doc comment (api_op_DescribeEcsClusters.go)
// doesn't specify a default for this deprecated operation, so this mirrors
// the "100" default other AWS list/describe operations commonly use.
const defaultEcsClustersPageSize = 100

// handleRegisterEcsCluster handles RegisterEcsCluster requests.
func (h *Handler) handleRegisterEcsCluster(_ context.Context, body []byte) (any, error) {
	var req struct {
		EcsClusterArn string `json:"EcsClusterArn"`
		StackID       string `json:"StackId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	ecsClusterArn, err := h.Backend.RegisterEcsCluster(req.EcsClusterArn, req.StackID)
	if err != nil {
		return nil, err
	}

	return map[string]any{"EcsClusterArn": ecsClusterArn}, nil
}

// handleDeregisterEcsCluster handles DeregisterEcsCluster requests.
func (h *Handler) handleDeregisterEcsCluster(_ context.Context, body []byte) (any, error) {
	var req struct {
		EcsClusterArn string `json:"EcsClusterArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeregisterEcsCluster(req.EcsClusterArn); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeEcsClusters handles DescribeEcsClusters requests.
func (h *Handler) handleDescribeEcsClusters(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID        string   `json:"StackId"`
		NextToken      string   `json:"NextToken"`
		EcsClusterArns []string `json:"EcsClusterArns"`
		MaxResults     int      `json:"MaxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	clusters, err := h.Backend.DescribeEcsClusters(req.StackID, req.EcsClusterArns)
	if err != nil {
		return nil, err
	}

	pg := page.New(clusters, req.NextToken, req.MaxResults, defaultEcsClustersPageSize)
	resp := map[string]any{"EcsClusters": ecsClustersToJSON(pg.Data)}
	if pg.Next != "" {
		resp["NextToken"] = pg.Next
	}

	return resp, nil
}

// ecsClustersToJSON omits Status: the real types.EcsCluster has no such
// member (confirmed against aws-sdk-go-v2/service/opsworks@v1.31.0's
// types/types.go). e.Status is kept on the internal domain struct only.
func ecsClustersToJSON(clusters []*EcsCluster) []map[string]any {
	result := make([]map[string]any, 0, len(clusters))
	for _, e := range clusters {
		result = append(result, map[string]any{
			"EcsClusterArn":  e.EcsClusterArn,
			"EcsClusterName": e.EcsClusterName,
			keyStackID:       e.StackID,
			"RegisteredAt":   formatOpsWorksTime(e.RegisteredAt),
		})
	}

	return result
}
