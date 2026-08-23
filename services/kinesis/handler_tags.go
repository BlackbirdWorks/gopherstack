package kinesis

import (
	"cmp"
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	// maxTagKeyLen is the maximum byte length of a Kinesis tag key.
	maxTagKeyLen = 128
	// maxTagValueLen is the maximum byte length of a Kinesis tag value.
	maxTagValueLen = 256
)

type listTagsForStreamOutput struct {
	Tags        []svcTags.KV `json:"Tags"`
	HasMoreTags bool         `json:"HasMoreTags"`
}

type handleAddTagsToStreamInput struct {
	Tags       *svcTags.Tags `json:"Tags"`
	StreamName string        `json:"StreamName"`
	StreamARN  string        `json:"StreamARN"`
}

type handleRemoveTagsFromStreamInput struct {
	StreamName string   `json:"StreamName"`
	StreamARN  string   `json:"StreamARN"`
	TagKeys    []string `json:"TagKeys"`
}

type listTagsForStreamReq struct {
	ExclusiveStartTagKey string `json:"ExclusiveStartTagKey"`
	StreamName           string `json:"StreamName"`
	StreamARN            string `json:"StreamARN"`
	Limit                int    `json:"Limit"`
}

type jsonTagResourceReq struct {
	Tags        map[string]string `json:"Tags"`
	ResourceARN string            `json:"ResourceARN"`
}

type jsonUntagResourceReq struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

type jsonListTagsForResourceResp struct {
	Tags []svcTags.KV `json:"Tags"`
}

// validateTagKVs checks that all tag keys are 1-128 bytes and values are 0-256 bytes.
func validateTagKVs(tags map[string]string) error {
	for k, v := range tags {
		if len(k) == 0 || len(k) > maxTagKeyLen {
			return ErrInvalidArgument
		}
		if len(v) > maxTagValueLen {
			return ErrInvalidArgument
		}
	}

	return nil
}

func (h *Handler) handleAddTagsToStream(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req handleAddTagsToStreamInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	streamName, ctx := resolveStreamNameAndRegion(ctx, req.StreamName, req.StreamARN, h.defaultRegion())

	out, err := h.Backend.DescribeStream(ctx, &DescribeStreamInput{StreamName: streamName})
	if err != nil {
		return nil, err
	}

	var kv map[string]string
	if req.Tags != nil {
		kv = req.Tags.Clone()
	}

	if err = validateTagKVs(kv); err != nil {
		return nil, err
	}

	// Tags live on the backend's stream.Tags (persisted via Snapshot/Restore),
	// not a handler-local map, so AddTagsToStream and TagResource share one
	// source of truth and tags survive a persistence round-trip.
	existingOut, err := h.Backend.ListTagsForResource(ctx, &ListTagsForResourceInput{ResourceARN: out.StreamARN})
	if err != nil {
		return nil, err
	}
	merged := make(map[string]string, len(existingOut.Tags)+len(kv))
	maps.Copy(merged, existingOut.Tags)
	maps.Copy(merged, kv)
	if len(merged) > maxTagsPerStream {
		return nil, ErrTagLimitExceeded
	}

	if err = h.Backend.TagResource(ctx, &TagResourceInput{ResourceARN: out.StreamARN, Tags: kv}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleRemoveTagsFromStream(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req handleRemoveTagsFromStreamInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	streamName, ctx := resolveStreamNameAndRegion(ctx, req.StreamName, req.StreamARN, h.defaultRegion())

	out, err := h.Backend.DescribeStream(ctx, &DescribeStreamInput{StreamName: streamName})
	if err != nil {
		return nil, err
	}

	if err = h.Backend.UntagResource(ctx, &UntagResourceInput{
		ResourceARN: out.StreamARN,
		TagKeys:     req.TagKeys,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleListTagsForStream(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req listTagsForStreamReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	streamName, ctx := resolveStreamNameAndRegion(ctx, req.StreamName, req.StreamARN, h.defaultRegion())

	out, err := h.Backend.DescribeStream(ctx, &DescribeStreamInput{StreamName: streamName})
	if err != nil {
		return nil, err
	}

	tagsOut, err := h.Backend.ListTagsForResource(ctx, &ListTagsForResourceInput{ResourceARN: out.StreamARN})
	if err != nil {
		return nil, err
	}
	tagsMap := tagsOut.Tags

	keys := collections.SortedKeys(tagsMap)

	startIdx := 0
	if req.ExclusiveStartTagKey != "" {
		for startIdx < len(keys) && keys[startIdx] <= req.ExclusiveStartTagKey {
			startIdx++
		}
	}

	const (
		defaultTagPageSize = 10
		maxTagPageSize     = 50
	)
	limit := defaultTagPageSize
	if req.Limit >= 1 && req.Limit <= maxTagPageSize {
		limit = req.Limit
	}

	tagList := make([]svcTags.KV, 0, limit)
	for i := startIdx; i < len(keys) && len(tagList) < limit; i++ {
		tagList = append(tagList, svcTags.KV{Key: keys[i], Value: tagsMap[keys[i]]})
	}

	hasMore := startIdx+len(tagList) < len(keys)

	return &listTagsForStreamOutput{
		Tags:        tagList,
		HasMoreTags: hasMore,
	}, nil
}

func (h *Handler) handleTagResource(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonTagResourceReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := validateTagKVs(req.Tags); err != nil {
		return nil, err
	}

	// Enforce the AWS 50-tag-per-stream cap here too: TagResource and
	// AddTagsToStream share the same underlying tag set (stream.Tags), so the
	// limit must be consistent across both entry points.
	existingOut, err := h.Backend.ListTagsForResource(ctx, &ListTagsForResourceInput{ResourceARN: req.ResourceARN})
	if err != nil {
		return nil, err
	}
	merged := make(map[string]string, len(existingOut.Tags)+len(req.Tags))
	maps.Copy(merged, existingOut.Tags)
	maps.Copy(merged, req.Tags)
	if len(merged) > maxTagsPerStream {
		return nil, ErrTagLimitExceeded
	}

	if err = h.Backend.TagResource(ctx, &TagResourceInput{
		ResourceARN: req.ResourceARN,
		Tags:        req.Tags,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleUntagResource(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonUntagResourceReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.UntagResource(ctx, &UntagResourceInput{
		ResourceARN: req.ResourceARN,
		TagKeys:     req.TagKeys,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleListTagsForResource(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonResourceARNReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.ListTagsForResource(ctx, &ListTagsForResourceInput{ResourceARN: req.ResourceARN})
	if err != nil {
		return nil, err
	}

	tagList := make([]svcTags.KV, 0, len(out.Tags))
	for k, v := range out.Tags {
		tagList = append(tagList, svcTags.KV{Key: k, Value: v})
	}
	slices.SortFunc(tagList, func(a, b svcTags.KV) int {
		return cmp.Compare(a.Key, b.Key)
	})

	return jsonListTagsForResourceResp{Tags: tagList}, nil
}
