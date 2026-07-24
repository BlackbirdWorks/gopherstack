package xray

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type listTagsForResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
	NextToken   string `json:"NextToken"`
}

func (h *Handler) handleListTagsForResource(_ context.Context, body []byte) ([]byte, error) {
	var in listTagsForResourceInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(in.ResourceARN)
	if err != nil {
		return nil, err
	}

	pg := page.New(tags, in.NextToken, 0, defaultTagsPageSize)

	return json.Marshal(map[string]any{
		"Tags":       pg.Data,
		keyNextToken: pg.Next,
	})
}

type tagResourceInput struct {
	Tags        map[string]string `json:"Tags"`
	ResourceARN string            `json:"ResourceARN"`
}

func (h *Handler) handleTagResource(_ context.Context, body []byte) ([]byte, error) {
	var in tagResourceInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(in.ResourceARN, in.Tags); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(_ context.Context, body []byte) ([]byte, error) {
	var in untagResourceInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

const (
	defaultTagsPageSize = 50
)
