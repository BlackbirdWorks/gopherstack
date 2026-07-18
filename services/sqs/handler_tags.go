package sqs

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type jsonTagQueueReq struct {
	Tags     *tags.Tags `json:"Tags"`
	QueueURL string     `json:"QueueUrl"`
}

type jsonUntagQueueReq struct {
	QueueURL string   `json:"QueueUrl"`
	TagKeys  []string `json:"TagKeys"`
}

// --- JSON response types ---

type jsonListQueueTagsResp struct {
	Tags *tags.Tags `json:"Tags"`
}

func (h *Handler) handleTagQueue(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonTagQueueReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.TagQueue(&TagQueueInput{
		QueueURL: req.QueueURL,
		Region:   httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		Tags:     req.Tags,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleUntagQueue(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonUntagQueueReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.UntagQueue(&UntagQueueInput{
		QueueURL: req.QueueURL,
		Region:   httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		TagKeys:  req.TagKeys,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleListQueueTags(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonQueueURLReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	out, err := h.Backend.ListQueueTags(&ListQueueTagsInput{
		QueueURL: req.QueueURL,
		Region:   httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
	})
	if err != nil {
		return nil, err
	}

	return jsonListQueueTagsResp{Tags: out.Tags}, nil
}
