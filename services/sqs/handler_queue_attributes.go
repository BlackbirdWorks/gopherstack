package sqs

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

type jsonGetQueueAttributesReq struct {
	QueueURL       string   `json:"QueueUrl"`
	AttributeNames []string `json:"AttributeNames"`
}

type jsonSetQueueAttributesReq struct {
	Attributes map[string]string `json:"Attributes"`
	QueueURL   string            `json:"QueueUrl"`
}

type jsonGetQueueAttributesResp struct {
	Attributes map[string]string `json:"Attributes"`
}

func (h *Handler) handleGetQueueAttributes(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonGetQueueAttributesReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	out, err := h.Backend.GetQueueAttributes(&GetQueueAttributesInput{
		QueueURL:       req.QueueURL,
		Region:         httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		AttributeNames: req.AttributeNames,
	})
	if err != nil {
		return nil, err
	}

	attrs := out.Attributes
	if attrs == nil {
		attrs = map[string]string{}
	}

	return jsonGetQueueAttributesResp{Attributes: attrs}, nil
}

func (h *Handler) handleSetQueueAttributes(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonSetQueueAttributesReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.SetQueueAttributes(&SetQueueAttributesInput{
		QueueURL:   req.QueueURL,
		Region:     httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		Attributes: req.Attributes,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}
