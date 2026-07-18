package batch

import (
	"context"
	"fmt"
)

// --- SchedulingPolicy handlers ---

type createSchedulingPolicyInput struct {
	Tags map[string]string `json:"tags"`
	Name string            `json:"name"`
}

type createSchedulingPolicyOutput struct {
	Arn  string `json:"arn"`
	Name string `json:"name"`
}

func (h *Handler) handleCreateSchedulingPolicy(
	ctx context.Context,
	in *createSchedulingPolicyInput,
) (*createSchedulingPolicyOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	sp, err := h.Backend.CreateSchedulingPolicy(ctx, in.Name, in.Tags, nil)
	if err != nil {
		return nil, err
	}

	return &createSchedulingPolicyOutput{
		Arn:  sp.Arn,
		Name: sp.Name,
	}, nil
}

type deleteSchedulingPolicyInput struct {
	Arn string `json:"arn"`
}

func (h *Handler) handleDeleteSchedulingPolicy(
	ctx context.Context,
	in *deleteSchedulingPolicyInput,
) (*emptyOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: arn is required", ErrValidation)
	}

	if err := h.Backend.DeleteSchedulingPolicy(ctx, in.Arn); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- DescribeSchedulingPolicies handler ---

type describeSchedulingPoliciesInput struct {
	Arns []string `json:"arns"`
}

type describeSchedulingPoliciesOutput struct {
	SchedulingPolicies []*SchedulingPolicy `json:"schedulingPolicies"`
}

func (h *Handler) handleDescribeSchedulingPolicies(
	ctx context.Context,
	in *describeSchedulingPoliciesInput,
) (*describeSchedulingPoliciesOutput, error) {
	list := h.Backend.DescribeSchedulingPolicies(ctx, in.Arns)

	return &describeSchedulingPoliciesOutput{SchedulingPolicies: list}, nil
}

// --- ListSchedulingPolicies handler ---

type listSchedulingPoliciesOutput struct {
	SchedulingPolicies []*SchedulingPolicy `json:"schedulingPolicies"`
}

func (h *Handler) handleListSchedulingPolicies(
	ctx context.Context,
	_ *struct{},
) (*listSchedulingPoliciesOutput, error) {
	list := h.Backend.ListSchedulingPolicies(ctx)

	return &listSchedulingPoliciesOutput{SchedulingPolicies: list}, nil
}

// --- UpdateSchedulingPolicy handler ---

type updateSchedulingPolicyInput struct {
	Arn string `json:"arn"`
}

func (h *Handler) handleUpdateSchedulingPolicy(
	ctx context.Context,
	in *updateSchedulingPolicyInput,
) (*emptyOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: arn is required", ErrValidation)
	}

	if err := h.Backend.UpdateSchedulingPolicy(ctx, in.Arn, nil); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
