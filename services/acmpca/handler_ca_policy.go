package acmpca

import (
	"context"
	"encoding/json"
)

type getPolicyInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type getPolicyOutput struct {
	Policy string `json:"Policy,omitempty"`
}

type putPolicyInput struct {
	Policy      string `json:"Policy"`
	ResourceArn string `json:"ResourceArn"`
}

type putPolicyOutput struct{}

type deletePolicyInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type deletePolicyOutput struct{}

func (h *Handler) jsonGetPolicy(ctx context.Context, body []byte) (any, error) {
	var input getPolicyInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	policy, err := h.Backend.GetPolicy(ctx, input.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &getPolicyOutput{Policy: policy}, nil
}

func (h *Handler) jsonPutPolicy(ctx context.Context, body []byte) (any, error) {
	var input putPolicyInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.PutPolicy(ctx, input.ResourceArn, input.Policy); err != nil {
		return nil, err
	}

	return &putPolicyOutput{}, nil
}

func (h *Handler) jsonDeletePolicy(ctx context.Context, body []byte) (any, error) {
	var input deletePolicyInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.DeletePolicy(ctx, input.ResourceArn); err != nil {
		return nil, err
	}

	return &deletePolicyOutput{}, nil
}
