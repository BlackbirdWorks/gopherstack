package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// Expanded CreateEndpoint handler (uses FSM)
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateEndpointFSM(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		EndpointName       string      `json:"EndpointName"`
		EndpointConfigName string      `json:"EndpointConfigName"`
		Tags               []tagObject `json:"Tags"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.EndpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", errInvalidRequest)
	}
	if req.EndpointConfigName == "" {
		return nil, fmt.Errorf("%w: EndpointConfigName is required", errInvalidRequest)
	}

	tags := fromTagObjects(req.Tags)
	ep, err := h.Backend.CreateEndpointFSM(ctx, req.EndpointName, req.EndpointConfigName, tags)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: created endpoint (FSM)",
		"name",
		ep.EndpointName,
		"arn",
		ep.EndpointArn,
	)

	return json.Marshal(map[string]string{keyEndpointArn: ep.EndpointArn})
}

// handleUpdateEndpointFSM replaces the basic UpdateEndpoint handler with one
// that properly drives Updating → InService via the lifecycle simulator.
func (h *Handler) handleUpdateEndpointFSM(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		EndpointName       string `json:"EndpointName"`
		EndpointConfigName string `json:"EndpointConfigName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.EndpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", errInvalidRequest)
	}

	ep, err := h.Backend.UpdateEndpointFSM(ctx, req.EndpointName, req.EndpointConfigName)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: updated endpoint (FSM)", "name", ep.EndpointName)

	return json.Marshal(map[string]string{keyEndpointArn: ep.EndpointArn})
}

// ---------------------------------------------------------------------------
// UpdateEndpointWeightsAndCapacities (proper implementation, gap #10)
// ---------------------------------------------------------------------------

func (h *Handler) handleUpdateEndpointWeightsAndCapacitiesFull(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		EndpointName                string                     `json:"EndpointName"`
		DesiredWeightsAndCapacities []DesiredWeightAndCapacity `json:"DesiredWeightsAndCapacities"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.EndpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", errInvalidRequest)
	}

	ep, err := h.Backend.UpdateEndpointWeightsAndCapacitiesFull(
		ctx,
		req.EndpointName,
		req.DesiredWeightsAndCapacities,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: updated endpoint weights and capacities",
		"name",
		req.EndpointName,
	)

	return json.Marshal(map[string]string{keyEndpointArn: ep.EndpointArn})
}

// ---------------------------------------------------------------------------
// Endpoint handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleDescribeEndpoint(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		EndpointName string `json:"EndpointName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EndpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", errInvalidRequest)
	}

	ep, err := h.Backend.DescribeEndpoint(ctx, req.EndpointName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"EndpointName":       ep.EndpointName,
		keyEndpointArn:       ep.EndpointArn,
		"EndpointConfigName": ep.EndpointConfigName,
		"EndpointStatus":     ep.EndpointStatus,
		keyCreationTime:      epochSeconds(ep.CreationTime),
		keyLastModifiedTime:  epochSeconds(ep.LastModifiedTime),
	}
	if ep.FailureReason != "" {
		resp["FailureReason"] = ep.FailureReason
	}
	if len(ep.ProductionVariants) > 0 {
		resp["ProductionVariants"] = ep.ProductionVariants
	}

	return json.Marshal(resp)
}

type endpointSummary struct {
	EndpointName     string  `json:"EndpointName"`
	EndpointArn      string  `json:"EndpointArn"`
	EndpointStatus   string  `json:"EndpointStatus"`
	CreationTime     float64 `json:"CreationTime"`
	LastModifiedTime float64 `json:"LastModifiedTime"`
}

func (h *Handler) handleListEndpoints(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	eps, nextToken := h.Backend.ListEndpoints(ctx, req.NextToken)
	summaries := make([]endpointSummary, 0, len(eps))

	for _, ep := range eps {
		summaries = append(summaries, endpointSummary{
			EndpointName:     ep.EndpointName,
			EndpointArn:      ep.EndpointArn,
			EndpointStatus:   ep.EndpointStatus,
			CreationTime:     epochSeconds(ep.CreationTime),
			LastModifiedTime: epochSeconds(ep.LastModifiedTime),
		})
	}

	resp := map[string]any{"Endpoints": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteEndpoint(ctx context.Context, body []byte) error {
	var req struct {
		EndpointName string `json:"EndpointName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EndpointName == "" {
		return fmt.Errorf("%w: EndpointName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteEndpoint(ctx, req.EndpointName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: deleted endpoint", "name", req.EndpointName)

	return nil
}
