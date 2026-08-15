package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// Space handlers
// ---------------------------------------------------------------------------

// createSpaceInput is the CreateSpace request shape (named, not inline, so
// wire-field-audit tooling that only inspects named types can see it — see
// gopherstack-oc9v). OwnershipSettings/SpaceSettings/SpaceSharingSettings are
// carried as opaque json.RawMessage passthrough per this file's established
// convention.
type createSpaceInput struct {
	DomainID             string          `json:"DomainId"`
	SpaceName            string          `json:"SpaceName"`
	SpaceDisplayName     string          `json:"SpaceDisplayName"`
	OwnershipSettings    json.RawMessage `json:"OwnershipSettings"`
	SpaceSettings        json.RawMessage `json:"SpaceSettings"`
	SpaceSharingSettings json.RawMessage `json:"SpaceSharingSettings"`
	Tags                 []tagObject     `json:"Tags"`
}

func (h *Handler) handleCreateSpace(ctx context.Context, body []byte) ([]byte, error) {
	var req createSpaceInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainID is required", errInvalidRequest)
	}

	if req.SpaceName == "" {
		return nil, fmt.Errorf("%w: SpaceName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateSpace(ctx, req.DomainID, req.SpaceName, fromTagObjects(req.Tags), CreateSpaceOptions{
		SpaceDisplayName:     req.SpaceDisplayName,
		OwnershipSettings:    req.OwnershipSettings,
		SpaceSettings:        req.SpaceSettings,
		SpaceSharingSettings: req.SpaceSharingSettings,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"SpaceArn": result.SpaceArn})
}

type describeSpaceInput struct {
	DomainID  string `json:"DomainId"`
	SpaceName string `json:"SpaceName"`
}

func (h *Handler) handleDescribeSpace(ctx context.Context, body []byte) ([]byte, error) {
	var req describeSpaceInput

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

type deleteSpaceInput struct {
	DomainID  string `json:"DomainId"`
	SpaceName string `json:"SpaceName"`
}

func (h *Handler) handleDeleteSpace(ctx context.Context, body []byte) error {
	var req deleteSpaceInput

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

type listSpacesInput struct {
	DomainIDEquals    string `json:"DomainIdEquals"`
	SpaceNameContains string `json:"SpaceNameContains"`
	SortBy            string `json:"SortBy"`
	SortOrder         string `json:"SortOrder"`
	NextToken         string `json:"NextToken"`
	MaxResults        int32  `json:"MaxResults"`
}

func (h *Handler) handleListSpaces(ctx context.Context, body []byte) ([]byte, error) {
	var req listSpacesInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListSpaces(ctx, ListSpacesParams(req))

	summaries := make([]map[string]any, 0, len(items))
	for _, s := range items {
		summary := map[string]any{
			"SpaceName":         s.SpaceName,
			"SpaceArn":          s.SpaceArn,
			keyDomainID:         s.DomainID,
			"SpaceStatus":       s.SpaceStatus,
			keyCreationTime:     epochSeconds(s.CreationTime),
			keyLastModifiedTime: epochSeconds(s.LastModifiedTime),
		}

		if s.SpaceDisplayName != "" {
			summary["SpaceDisplayName"] = s.SpaceDisplayName
		}

		summaries = append(summaries, summary)
	}

	return json.Marshal(map[string]any{
		"Spaces":     summaries,
		keyNextToken: next,
	})
}

type updateSpaceInput struct {
	DomainID  string `json:"DomainId"`
	SpaceName string `json:"SpaceName"`
}

func (h *Handler) handleUpdateSpace(ctx context.Context, body []byte) ([]byte, error) {
	var req updateSpaceInput

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
