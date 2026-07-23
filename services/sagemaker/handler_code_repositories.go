package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// CodeRepository handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateCodeRepository(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		GitConfig          map[string]string `json:"GitConfig"`
		Tags               map[string]string `json:"Tags"`
		CodeRepositoryName string            `json:"CodeRepositoryName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CodeRepositoryName == "" {
		return nil, fmt.Errorf("%w: CodeRepositoryName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateCodeRepository(ctx, req.CodeRepositoryName, req.GitConfig, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyCodeRepositoryArn: result.CodeRepositoryArn})
}

func (h *Handler) handleDescribeCodeRepository(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		CodeRepositoryName string `json:"CodeRepositoryName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CodeRepositoryName == "" {
		return nil, fmt.Errorf("%w: CodeRepositoryName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeCodeRepository(ctx, req.CodeRepositoryName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleUpdateCodeRepository(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		GitConfig          map[string]string `json:"GitConfig"`
		CodeRepositoryName string            `json:"CodeRepositoryName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CodeRepositoryName == "" {
		return nil, fmt.Errorf("%w: CodeRepositoryName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateCodeRepository(ctx, req.CodeRepositoryName, req.GitConfig)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyCodeRepositoryArn: result.CodeRepositoryArn})
}

func (h *Handler) handleDeleteCodeRepository(ctx context.Context, body []byte) error {
	var req struct {
		CodeRepositoryName string `json:"CodeRepositoryName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CodeRepositoryName == "" {
		return fmt.Errorf("%w: CodeRepositoryName is required", errInvalidRequest)
	}

	return h.Backend.DeleteCodeRepository(ctx, req.CodeRepositoryName)
}

func (h *Handler) handleListCodeRepositories(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListCodeRepositories(ctx, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, r := range items {
		summaries = append(summaries, map[string]any{
			"CodeRepositoryName": r.CodeRepositoryName,
			keyCodeRepositoryArn: r.CodeRepositoryArn,
			keyCreationTime:      epochSeconds(r.CreationTime),
			keyLastModifiedTime:  epochSeconds(r.LastModifiedTime),
		})
	}

	return json.Marshal(map[string]any{
		"CodeRepositorySummaryList": summaries,
		keyNextToken:                next,
	})
}
