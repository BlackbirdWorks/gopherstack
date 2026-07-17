package codebuild

import (
	"context"
	"fmt"
)

type deleteResourcePolicyInput struct {
	ResourceArn string `json:"resourceArn"`
}

type deleteResourcePolicyOutput struct{}

func (h *Handler) handleDeleteResourcePolicy(
	_ context.Context,
	in *deleteResourcePolicyInput,
) (*deleteResourcePolicyOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	_ = h.Backend.DeleteResourcePolicy(in.ResourceArn)

	return &deleteResourcePolicyOutput{}, nil
}

type getResourcePolicyInput struct {
	ResourceArn string `json:"resourceArn"`
}

type getResourcePolicyOutput struct {
	Policy string `json:"policy"`
}

func (h *Handler) handleGetResourcePolicy(
	_ context.Context,
	in *getResourcePolicyInput,
) (*getResourcePolicyOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	policy, err := h.Backend.GetResourcePolicy(in.ResourceArn)
	if err != nil {
		return nil, err // ErrNotFound when no policy set
	}

	return &getResourcePolicyOutput{Policy: policy}, nil
}

type putResourcePolicyInput struct {
	Policy      string `json:"policy"`
	ResourceArn string `json:"resourceArn"`
}

type putResourcePolicyOutput struct {
	ResourceArn string `json:"resourceArn"`
}

func (h *Handler) handlePutResourcePolicy(
	_ context.Context,
	in *putResourcePolicyInput,
) (*putResourcePolicyOutput, error) {
	if in.ResourceArn == "" || in.Policy == "" {
		return nil, fmt.Errorf("%w: resourceArn and policy are required", errInvalidRequest)
	}

	if err := h.Backend.PutResourcePolicy(in.ResourceArn, in.Policy); err != nil {
		return nil, err
	}

	return &putResourcePolicyOutput{ResourceArn: in.ResourceArn}, nil
}
