package sqs

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

type jsonAddPermissionReq struct {
	QueueURL      string   `json:"QueueUrl"`
	Label         string   `json:"Label"`
	Actions       []string `json:"Actions"`
	AWSAccountIDs []string `json:"AWSAccountIds"`
}

type jsonRemovePermissionReq struct {
	QueueURL string `json:"QueueUrl"`
	Label    string `json:"Label"`
}

func (h *Handler) handleAddPermission(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonAddPermissionReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.AddPermission(&AddPermissionInput{
		QueueURL:      req.QueueURL,
		Region:        httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		Label:         req.Label,
		AWSAccountIDs: req.AWSAccountIDs,
		Actions:       req.Actions,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleRemovePermission(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonRemovePermissionReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.RemovePermission(&RemovePermissionInput{
		QueueURL: req.QueueURL,
		Region:   httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
		Label:    req.Label,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}
