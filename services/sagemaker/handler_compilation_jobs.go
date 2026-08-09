package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// CompilationJob handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateCompilationJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		InputConfig        *CompilationInputConfig  `json:"InputConfig"`
		OutputConfig       *CompilationOutputConfig `json:"OutputConfig"`
		StoppingCondition  *StoppingCondition       `json:"StoppingCondition"`
		CompilationJobName string                   `json:"CompilationJobName"`
		RoleArn            string                   `json:"RoleArn"`
		Tags               []tagObject              `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CompilationJobName == "" {
		return nil, fmt.Errorf("%w: CompilationJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateCompilationJob(ctx, req.CompilationJobName, req.RoleArn, fromTagObjects(req.Tags))
	if err != nil {
		return nil, err
	}

	if req.InputConfig != nil || req.OutputConfig != nil || req.StoppingCondition != nil {
		if extErr := h.Backend.SetCompilationJobExtras(
			ctx, req.CompilationJobName, req.InputConfig, req.OutputConfig, req.StoppingCondition,
		); extErr != nil {
			return nil, extErr
		}
	}

	return json.Marshal(map[string]any{"CompilationJobArn": result.CompilationJobArn})
}

func (h *Handler) handleDescribeCompilationJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		CompilationJobName string `json:"CompilationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CompilationJobName == "" {
		return nil, fmt.Errorf("%w: CompilationJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeCompilationJob(ctx, req.CompilationJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteCompilationJob(ctx context.Context, body []byte) error {
	var req struct {
		CompilationJobName string `json:"CompilationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CompilationJobName == "" {
		return fmt.Errorf("%w: CompilationJobName is required", errInvalidRequest)
	}

	return h.Backend.DeleteCompilationJob(ctx, req.CompilationJobName)
}

func (h *Handler) handleStopCompilationJob(ctx context.Context, body []byte) error {
	var req struct {
		CompilationJobName string `json:"CompilationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CompilationJobName == "" {
		return fmt.Errorf("%w: CompilationJobName is required", errInvalidRequest)
	}

	return h.Backend.StopCompilationJob(ctx, req.CompilationJobName)
}

func (h *Handler) handleListCompilationJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListCompilationJobs(ctx, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, j := range items {
		summaries = append(summaries, map[string]any{
			"CompilationJobName":   j.CompilationJobName,
			"CompilationJobArn":    j.CompilationJobArn,
			"CompilationJobStatus": j.CompilationJobStatus,
			keyCreationTime:        epochSeconds(j.CreationTime),
			keyLastModifiedTime:    epochSeconds(j.LastModifiedTime),
		})
	}

	return json.Marshal(map[string]any{
		"CompilationJobSummaries": summaries,
		keyNextToken:              next,
	})
}
