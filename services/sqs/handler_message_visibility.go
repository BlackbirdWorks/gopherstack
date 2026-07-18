package sqs

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

type jsonChangeVisibilityReq struct {
	QueueURL          string `json:"QueueUrl"`
	ReceiptHandle     string `json:"ReceiptHandle"`
	VisibilityTimeout int    `json:"VisibilityTimeout"`
}

type jsonChangeBatchEntry struct {
	ID                string `json:"Id"`
	ReceiptHandle     string `json:"ReceiptHandle"`
	VisibilityTimeout int    `json:"VisibilityTimeout"`
}

type jsonChangeVisibilityBatchReq struct {
	QueueURL string                 `json:"QueueUrl"`
	Entries  []jsonChangeBatchEntry `json:"Entries"`
}

func (h *Handler) handleChangeMessageVisibility(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonChangeVisibilityReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.ChangeMessageVisibility(&ChangeMessageVisibilityInput{
		QueueURL:          req.QueueURL,
		Region:            httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		ReceiptHandle:     req.ReceiptHandle,
		VisibilityTimeout: req.VisibilityTimeout,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

//nolint:dupl // JSON batch request/response flow intentionally mirrors sibling batch handlers.
func (h *Handler) handleChangeMessageVisibilityBatch(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonChangeVisibilityBatchReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	entries := make([]ChangeMessageVisibilityBatchRequestEntry, 0, len(req.Entries))
	for _, e := range req.Entries {
		entries = append(entries, ChangeMessageVisibilityBatchRequestEntry(e))
	}

	out, err := h.Backend.ChangeMessageVisibilityBatch(&ChangeMessageVisibilityBatchInput{
		QueueURL: req.QueueURL,
		Region:   httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		Entries:  entries,
	})
	if err != nil {
		return nil, err
	}

	result := jsonBatchResult{
		Successful: make([]jsonBatchSuccess, 0, len(out.Successful)),
		Failed:     make([]jsonBatchFailure, 0, len(out.Failed)),
	}

	for _, s := range out.Successful {
		result.Successful = append(result.Successful, jsonBatchSuccess{ID: s.ID})
	}

	for _, f := range out.Failed {
		result.Failed = append(result.Failed, jsonBatchFailure(f))
	}

	return result, nil
}
