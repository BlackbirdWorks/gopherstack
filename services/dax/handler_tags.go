package dax

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

type tagResourceRequest struct {
	ResourceName string    `json:"ResourceName"`
	Tags         []tagItem `json:"Tags"`
}

type untagResourceRequest struct {
	ResourceName string   `json:"ResourceName"`
	TagKeys      []string `json:"TagKeys"`
}

type listTagsRequest struct {
	ResourceName string `json:"ResourceName"`
	NextToken    string `json:"NextToken"`
}

func (h *Handler) handleTagResource(body []byte) (any, error) {
	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	// Tags is @required (validators.go:603, validateOpTagResourceInput); nil means
	// the field was omitted from the request entirely.
	if req.Tags == nil {
		return nil, fmt.Errorf("%w: Tags is required", ErrInvalidParameterValue)
	}

	if err := validateTagItems(req.Tags); err != nil {
		return nil, err
	}

	tags := make(map[string]string, len(req.Tags))
	for _, t := range req.Tags {
		tags[t.Key] = t.Value
	}

	allTags, err := h.Backend.TagResource(req.ResourceName, tags)
	if err != nil {
		return nil, err
	}

	items := make([]tagItem, 0, len(allTags))
	for k, v := range allTags {
		items = append(items, tagItem{Key: k, Value: v})
	}

	return map[string]any{
		tagsResponseKey: items,
	}, nil
}

func (h *Handler) handleUntagResource(body []byte) (any, error) {
	var req untagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	// TagKeys is @required (validators.go:621, validateOpUntagResourceInput); nil means
	// the field was omitted from the request entirely.
	if req.TagKeys == nil {
		return nil, fmt.Errorf("%w: TagKeys is required", ErrInvalidParameterValue)
	}

	remaining, err := h.Backend.UntagResource(req.ResourceName, req.TagKeys)
	if err != nil {
		return nil, err
	}

	items := make([]tagItem, 0, len(remaining))
	for k, v := range remaining {
		items = append(items, tagItem{Key: k, Value: v})
	}

	return map[string]any{
		"Tags": items,
	}, nil
}

func (h *Handler) handleListTags(body []byte) (any, error) {
	var req listTagsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	tags, nextToken, err := h.Backend.ListTags(req.ResourceName, req.NextToken)
	if err != nil {
		return nil, err
	}

	items := make([]tagItem, 0, len(tags))
	for k, v := range tags {
		items = append(items, tagItem{Key: k, Value: v})
	}

	result := map[string]any{
		tagsResponseKey: items,
	}

	if nextToken != "" {
		result["NextToken"] = nextToken
	}

	return result, nil
}

// validateTagItems enforces AWS tag key/value constraints.
// Keys must be 1–128 chars and must not start with "aws:". Values must be 0–256 chars.
func validateTagItems(tags []tagItem) error {
	for _, t := range tags {
		klen := utf8.RuneCountInString(t.Key)
		if klen == 0 || klen > maxTagKeyLength {
			return fmt.Errorf(
				"%w: tag key length must be 1–%d characters, got %d",
				ErrInvalidParameterValue, maxTagKeyLength, klen,
			)
		}

		if strings.HasPrefix(t.Key, "aws:") {
			return fmt.Errorf(
				"%w: tag key must not start with reserved prefix \"aws:\"",
				ErrInvalidParameterValue,
			)
		}

		if vlen := utf8.RuneCountInString(t.Value); vlen > maxTagValueLength {
			return fmt.Errorf(
				"%w: tag value length must be 0–%d characters, got %d",
				ErrInvalidParameterValue, maxTagValueLength, vlen,
			)
		}
	}

	return nil
}
