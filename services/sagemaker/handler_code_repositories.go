package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// CodeRepository handlers
// ---------------------------------------------------------------------------

// createCodeRepositoryInput mirrors CreateCodeRepositoryInput
// (api_op_CreateCodeRepository.go:34-53).
type createCodeRepositoryInput struct {
	GitConfig          map[string]string `json:"GitConfig"`
	CodeRepositoryName string            `json:"CodeRepositoryName"`
	Tags               []tagObject       `json:"Tags"`
}

func (h *Handler) handleCreateCodeRepository(ctx context.Context, body []byte) ([]byte, error) {
	var req createCodeRepositoryInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CodeRepositoryName == "" {
		return nil, fmt.Errorf("%w: CodeRepositoryName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateCodeRepository(ctx, req.CodeRepositoryName, req.GitConfig, fromTagObjects(req.Tags))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyCodeRepositoryArn: result.CodeRepositoryArn})
}

// describeCodeRepositoryInput mirrors DescribeCodeRepositoryInput
// (api_op_DescribeCodeRepository.go:27-32).
type describeCodeRepositoryInput struct {
	CodeRepositoryName string `json:"CodeRepositoryName"`
}

func (h *Handler) handleDescribeCodeRepository(ctx context.Context, body []byte) ([]byte, error) {
	var req describeCodeRepositoryInput

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

// updateCodeRepositoryInput mirrors UpdateCodeRepositoryInput
// (api_op_UpdateCodeRepository.go:27-38). GitConfig's only real field is
// SecretArn (types.GitConfigForUpdate, types.go:9239-9248) -- unlike
// Create's GitConfig, Update cannot touch RepositoryUrl/Branch at all.
type updateCodeRepositoryInput struct {
	CodeRepositoryName string `json:"CodeRepositoryName"`
	GitConfig          struct {
		SecretArn string `json:"SecretArn"`
	} `json:"GitConfig"`
}

func (h *Handler) handleUpdateCodeRepository(ctx context.Context, body []byte) ([]byte, error) {
	var req updateCodeRepositoryInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CodeRepositoryName == "" {
		return nil, fmt.Errorf("%w: CodeRepositoryName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateCodeRepository(ctx, req.CodeRepositoryName, req.GitConfig.SecretArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyCodeRepositoryArn: result.CodeRepositoryArn})
}

// deleteCodeRepositoryInput mirrors DeleteCodeRepositoryInput
// (api_op_DeleteCodeRepository.go:27-32).
type deleteCodeRepositoryInput struct {
	CodeRepositoryName string `json:"CodeRepositoryName"`
}

func (h *Handler) handleDeleteCodeRepository(ctx context.Context, body []byte) error {
	var req deleteCodeRepositoryInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CodeRepositoryName == "" {
		return fmt.Errorf("%w: CodeRepositoryName is required", errInvalidRequest)
	}

	return h.Backend.DeleteCodeRepository(ctx, req.CodeRepositoryName)
}

// listCodeRepositoriesInput mirrors ListCodeRepositoriesInput
// (api_op_ListCodeRepositories.go:29-64).
type listCodeRepositoriesInput struct {
	CreationTimeAfter      *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore     *float64 `json:"CreationTimeBefore"`
	LastModifiedTimeAfter  *float64 `json:"LastModifiedTimeAfter"`
	LastModifiedTimeBefore *float64 `json:"LastModifiedTimeBefore"`
	NameContains           string   `json:"NameContains"`
	NextToken              string   `json:"NextToken"`
	SortBy                 string   `json:"SortBy"`
	SortOrder              string   `json:"SortOrder"`
	MaxResults             int32    `json:"MaxResults"`
}

func (h *Handler) handleListCodeRepositories(ctx context.Context, body []byte) ([]byte, error) {
	var req listCodeRepositoriesInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListCodeRepositories(ctx, req.NextToken, ListCodeRepositoriesFilter{
		CreationTimeAfter:      epochPtr(req.CreationTimeAfter),
		CreationTimeBefore:     epochPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:  epochPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore: epochPtr(req.LastModifiedTimeBefore),
		NameContains:           req.NameContains,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		MaxResults:             req.MaxResults,
	})

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
