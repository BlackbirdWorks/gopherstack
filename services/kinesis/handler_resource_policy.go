package kinesis

import (
	"context"
	"encoding/json"
	"net/http"
)

type jsonResourceARNReq struct {
	ResourceARN string `json:"ResourceARN"`
}

type jsonPutResourcePolicyReq struct {
	ResourceARN string `json:"ResourceARN"`
	Policy      string `json:"Policy"`
}

type jsonGetResourcePolicyResp struct {
	Policy string `json:"Policy"`
}

func (h *Handler) handlePutResourcePolicy(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonPutResourcePolicyReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.PutResourcePolicy(ctx, &PutResourcePolicyInput{
		ResourceARN: req.ResourceARN,
		Policy:      req.Policy,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleGetResourcePolicy(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonResourceARNReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.GetResourcePolicy(ctx, &GetResourcePolicyInput{
		ResourceARN: req.ResourceARN,
	})
	if err != nil {
		return nil, err
	}

	return jsonGetResourcePolicyResp{Policy: out.Policy}, nil
}

func (h *Handler) handleDeleteResourcePolicy(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonResourceARNReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.DeleteResourcePolicy(ctx, &DeleteResourcePolicyInput{
		ResourceARN: req.ResourceARN,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}
