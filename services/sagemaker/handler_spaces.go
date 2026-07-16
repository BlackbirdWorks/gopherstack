package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// Space handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateSpace(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags      map[string]string `json:"Tags"`
		DomainID  string            `json:"DomainId"`
		SpaceName string            `json:"SpaceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainID is required", errInvalidRequest)
	}

	if req.SpaceName == "" {
		return nil, fmt.Errorf("%w: SpaceName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateSpace(ctx, req.DomainID, req.SpaceName, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"SpaceArn": result.SpaceArn})
}

func (h *Handler) handleDescribeSpace(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainID  string `json:"DomainId"`
		SpaceName string `json:"SpaceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainID is required", errInvalidRequest)
	}

	if req.SpaceName == "" {
		return nil, fmt.Errorf("%w: SpaceName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeSpace(ctx, req.DomainID, req.SpaceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteSpace(ctx context.Context, body []byte) error {
	var req struct {
		DomainID  string `json:"DomainId"`
		SpaceName string `json:"SpaceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return fmt.Errorf("%w: DomainID is required", errInvalidRequest)
	}

	if req.SpaceName == "" {
		return fmt.Errorf("%w: SpaceName is required", errInvalidRequest)
	}

	return h.Backend.DeleteSpace(ctx, req.DomainID, req.SpaceName)
}

func (h *Handler) handleListSpaces(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainIDEquals string `json:"DomainIdEquals"`
		NextToken      string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListSpaces(ctx, req.DomainIDEquals, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, s := range items {
		summaries = append(summaries, map[string]any{
			"SpaceName":         s.SpaceName,
			"SpaceArn":          s.SpaceArn,
			keyDomainID:         s.DomainID,
			"SpaceStatus":       s.SpaceStatus,
			keyCreationTime:     s.CreationTime,
			keyLastModifiedTime: s.LastModifiedTime,
		})
	}

	return json.Marshal(map[string]any{
		"Spaces":     summaries,
		keyNextToken: next,
	})
}

func (h *Handler) handleUpdateSpace(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainID  string `json:"DomainId"`
		SpaceName string `json:"SpaceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if req.SpaceName == "" {
		return nil, fmt.Errorf("%w: SpaceName is required", errInvalidRequest)
	}

	s, err := h.Backend.UpdateSpace(ctx, req.DomainID, req.SpaceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keySpaceArn: s.SpaceArn})
}
