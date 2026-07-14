package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// OptimizationJob handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateOptimizationJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags                map[string]string `json:"Tags"`
		OptimizationJobName string            `json:"OptimizationJobName"`
		RoleArn             string            `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return nil, fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateOptimizationJob(ctx, req.OptimizationJobName, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyOptimizationJobArn: result.OptimizationJobArn})
}

func (h *Handler) handleDescribeOptimizationJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		OptimizationJobName string `json:"OptimizationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return nil, fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeOptimizationJob(ctx, req.OptimizationJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteOptimizationJob(ctx context.Context, body []byte) error {
	var req struct {
		OptimizationJobName string `json:"OptimizationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	return h.Backend.DeleteOptimizationJob(ctx, req.OptimizationJobName)
}

func (h *Handler) handleStopOptimizationJob(ctx context.Context, body []byte) error {
	var req struct {
		OptimizationJobName string `json:"OptimizationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	return h.Backend.StopOptimizationJob(ctx, req.OptimizationJobName)
}

func (h *Handler) handleListOptimizationJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListOptimizationJobs(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		items = append(items, map[string]any{
			"OptimizationJobName":   j.OptimizationJobName,
			"OptimizationJobArn":    j.OptimizationJobArn,
			"OptimizationJobStatus": j.OptimizationJobStatus,
			keyCreationTime:         epochSeconds(j.CreationTime),
			keyLastModifiedTime:     epochSeconds(j.LastModifiedTime),
		})
	}

	return listResp("OptimizationJobSummaries", items, nextToken)
}
