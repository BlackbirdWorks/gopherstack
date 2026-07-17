package firehose

import (
	"context"
	"fmt"
	"sort"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	maxTagCount     = 50
	maxTagKeyLen    = 128
	maxTagValueLen  = 256
	maxTagListLimit = 50
)

type listTagsForDeliveryStreamInput struct {
	DeliveryStreamName   string `json:"DeliveryStreamName"`
	ExclusiveStartTagKey string `json:"ExclusiveStartTagKey"`
	Limit                int    `json:"Limit"`
}

type listTagsForDeliveryStreamOutput struct {
	Tags        []svcTags.KV `json:"Tags"`
	HasMoreTags bool         `json:"HasMoreTags"`
}

func (h *Handler) handleListTagsForDeliveryStream(
	ctx context.Context,
	in *listTagsForDeliveryStreamInput,
) (*listTagsForDeliveryStreamOutput, error) {
	tagMap, err := h.Backend.ListTagsForDeliveryStream(ctx, in.DeliveryStreamName)
	if err != nil {
		return nil, err
	}

	tagList := make([]svcTags.KV, 0, len(tagMap))
	for k, v := range tagMap {
		tagList = append(tagList, svcTags.KV{Key: k, Value: v})
	}

	sort.Slice(tagList, func(i, j int) bool { return tagList[i].Key < tagList[j].Key })

	// Apply ExclusiveStartTagKey cursor.
	if in.ExclusiveStartTagKey != "" {
		startIdx := -1
		for i, t := range tagList {
			if t.Key == in.ExclusiveStartTagKey {
				startIdx = i

				break
			}
		}
		if startIdx >= 0 {
			tagList = tagList[startIdx+1:]
		}
	}

	hasMore := false
	limit := in.Limit
	if limit <= 0 || limit > maxTagListLimit {
		limit = maxTagListLimit
	}

	if len(tagList) > limit {
		tagList = tagList[:limit]
		hasMore = true
	}

	return &listTagsForDeliveryStreamOutput{
		Tags:        tagList,
		HasMoreTags: hasMore,
	}, nil
}

type tagDeliveryStreamInput struct {
	DeliveryStreamName string       `json:"DeliveryStreamName"`
	Tags               []svcTags.KV `json:"Tags"`
}

type tagDeliveryStreamOutput struct{}

func (h *Handler) handleTagDeliveryStream(
	ctx context.Context,
	in *tagDeliveryStreamInput,
) (*tagDeliveryStreamOutput, error) {
	if err := validateTags(in.Tags); err != nil {
		return nil, err
	}

	tagMap := make(map[string]string, len(in.Tags))
	for _, t := range in.Tags {
		tagMap[t.Key] = t.Value
	}

	if err := h.Backend.TagDeliveryStream(ctx, in.DeliveryStreamName, tagMap); err != nil {
		return nil, err
	}

	return &tagDeliveryStreamOutput{}, nil
}

type untagDeliveryStreamInput struct {
	DeliveryStreamName string   `json:"DeliveryStreamName"`
	TagKeys            []string `json:"TagKeys"`
}

type untagDeliveryStreamOutput struct{}

func (h *Handler) handleUntagDeliveryStream(
	ctx context.Context,
	in *untagDeliveryStreamInput,
) (*untagDeliveryStreamOutput, error) {
	if err := h.Backend.UntagDeliveryStream(ctx, in.DeliveryStreamName, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagDeliveryStreamOutput{}, nil
}

// validateTags enforces AWS tag limits: max 50 tags, key ≤128 chars, value ≤256 chars.
func validateTags(tags []svcTags.KV) error {
	if len(tags) > maxTagCount {
		return fmt.Errorf("%w: tag count %d exceeds maximum of %d", ErrValidation, len(tags), maxTagCount)
	}

	for _, t := range tags {
		if len(t.Key) == 0 || len(t.Key) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key length must be between 1 and %d characters", ErrValidation, maxTagKeyLen)
		}
		if len(t.Value) > maxTagValueLen {
			return fmt.Errorf("%w: tag value length must not exceed %d characters", ErrValidation, maxTagValueLen)
		}
	}

	return nil
}
