package batch

import (
	"context"
	"fmt"
)

// --- SchedulingPolicy handlers ---

type createSchedulingPolicyInput struct {
	Tags             map[string]string `json:"tags"`
	FairsharePolicy  *FairsharePolicy  `json:"fairsharePolicy,omitempty"`
	QuotaSharePolicy *QuotaSharePolicy `json:"quotaSharePolicy,omitempty"`
	Name             string            `json:"name"`
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

	sp, err := h.Backend.CreateSchedulingPolicy(ctx, in.Name, in.Tags, in.FairsharePolicy, in.QuotaSharePolicy)
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

// schedulingPolicyListing mirrors aws-sdk-go-v2/service/batch/types.
// SchedulingPolicyListingDetail: ListSchedulingPolicies returns only the ARN
// per entry (callers use DescribeSchedulingPolicies for full details), unlike
// DescribeSchedulingPolicies which returns the full SchedulingPolicyDetail
// (name/fairsharePolicy/tags too).
type schedulingPolicyListing struct {
	Arn string `json:"arn"`
}

type listSchedulingPoliciesInput struct {
	MaxResults *int32  `json:"maxResults,omitempty"`
	NextToken  *string `json:"nextToken,omitempty"`
}

type listSchedulingPoliciesOutput struct {
	NextToken          *string                   `json:"nextToken,omitempty"`
	SchedulingPolicies []schedulingPolicyListing `json:"schedulingPolicies"`
}

func (h *Handler) handleListSchedulingPolicies(
	ctx context.Context,
	in *listSchedulingPoliciesInput,
) (*listSchedulingPoliciesOutput, error) {
	all := h.Backend.ListSchedulingPolicies(ctx)

	arns := make([]string, len(all))
	for i, sp := range all {
		arns[i] = sp.Arn
	}

	var maxResults int32
	if in.MaxResults != nil {
		maxResults = *in.MaxResults
	}

	var nextToken string
	if in.NextToken != nil {
		nextToken = *in.NextToken
	}

	pageArns, outToken := paginateMapKeys(arns, nextToken, maxResults)

	listings := make([]schedulingPolicyListing, len(pageArns))
	for i, a := range pageArns {
		listings[i] = schedulingPolicyListing{Arn: a}
	}

	out := &listSchedulingPoliciesOutput{SchedulingPolicies: listings}
	if outToken != "" {
		out.NextToken = &outToken
	}

	return out, nil
}

// --- UpdateSchedulingPolicy handler ---

type updateSchedulingPolicyInput struct {
	FairsharePolicy  *FairsharePolicy  `json:"fairsharePolicy,omitempty"`
	QuotaSharePolicy *QuotaSharePolicy `json:"quotaSharePolicy,omitempty"`
	Arn              string            `json:"arn"`
}

func (h *Handler) handleUpdateSchedulingPolicy(
	ctx context.Context,
	in *updateSchedulingPolicyInput,
) (*emptyOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: arn is required", ErrValidation)
	}

	if err := h.Backend.UpdateSchedulingPolicy(ctx, in.Arn, in.FairsharePolicy, in.QuotaSharePolicy); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
