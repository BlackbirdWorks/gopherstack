package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"fmt"
)

type putResourcePolicyInput struct {
	ExpectedRevisionID *string `json:"expectedRevisionId,omitempty"`
	PolicyName         string  `json:"policyName"`
	PolicyDocument     string  `json:"policyDocument"`
	ResourceArn        string  `json:"resourceArn,omitempty"`
}

type putResourcePolicyOutput struct {
	ResourcePolicy *ResourcePolicy `json:"resourcePolicy,omitempty"`
	RevisionID     string          `json:"revisionId,omitempty"`
}

func (h *Handler) handlePutResourcePolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putResourcePolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		p, err := b.PutResourcePolicy(in.PolicyName, in.PolicyDocument, in.ResourceArn, in.ExpectedRevisionID)
		if err != nil {
			return nil, err
		}

		return &putResourcePolicyOutput{ResourcePolicy: p, RevisionID: p.RevisionID}, nil
	}

	return &putResourcePolicyOutput{
		ResourcePolicy: &ResourcePolicy{PolicyName: in.PolicyName, PolicyDocument: in.PolicyDocument},
	}, nil
}

type describeResourcePoliciesInput struct {
	PolicyScope string `json:"policyScope,omitempty"`
	ResourceArn string `json:"resourceArn,omitempty"`
	NextToken   string `json:"nextToken,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type describeResourcePoliciesOutput struct {
	NextToken        string           `json:"nextToken,omitempty"`
	ResourcePolicies []ResourcePolicy `json:"resourcePolicies"`
}

func (h *Handler) handleDescribeResourcePolicies(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in describeResourcePoliciesInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		policies, next := b.DescribeResourcePolicies(in.PolicyScope, in.ResourceArn, in.NextToken, in.Limit)

		return &describeResourcePoliciesOutput{ResourcePolicies: policies, NextToken: next}, nil
	}

	return &describeResourcePoliciesOutput{ResourcePolicies: []ResourcePolicy{}}, nil
}

type deleteResourcePolicyInput struct {
	ExpectedRevisionID *string `json:"expectedRevisionId,omitempty"`
	PolicyName         string  `json:"policyName"`
	ResourceArn        string  `json:"resourceArn,omitempty"`
}

func (h *Handler) handleDeleteResourcePolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteResourcePolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteResourcePolicy(in.PolicyName, in.ResourceArn, in.ExpectedRevisionID); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}
