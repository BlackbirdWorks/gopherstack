package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"fmt"
)

type putIndexPolicyInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
	PolicyDocument     string `json:"policyDocument"`
}

type putIndexPolicyOutput struct {
	IndexPolicy *IndexPolicy `json:"indexPolicy,omitempty"`
}

func (h *Handler) handlePutIndexPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putIndexPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		p, err := b.PutIndexPolicy(in.LogGroupIdentifier, in.PolicyDocument)
		if err != nil {
			return nil, err
		}

		return &putIndexPolicyOutput{IndexPolicy: p}, nil
	}

	return &putIndexPolicyOutput{IndexPolicy: &IndexPolicy{}}, nil
}

type describeIndexPoliciesInput struct {
	NextToken           string   `json:"nextToken,omitempty"`
	LogGroupIdentifiers []string `json:"logGroupIdentifiers"`
}

type describeIndexPoliciesOutput struct {
	NextToken     string        `json:"nextToken,omitempty"`
	IndexPolicies []IndexPolicy `json:"indexPolicies"`
}

func (h *Handler) handleDescribeIndexPolicies(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in describeIndexPoliciesInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if len(in.LogGroupIdentifiers) == 0 {
		return nil, fmt.Errorf("%w: logGroupIdentifiers is required", ErrValidation)
	}

	if b := cwlBackend(h); b != nil {
		policies, next := b.DescribeIndexPolicies(in.LogGroupIdentifiers, in.NextToken, 0)

		return &describeIndexPoliciesOutput{IndexPolicies: policies, NextToken: next}, nil
	}

	return &describeIndexPoliciesOutput{IndexPolicies: []IndexPolicy{}}, nil
}

type deleteIndexPolicyInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
}

func (h *Handler) handleDeleteIndexPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteIndexPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteIndexPolicy(in.LogGroupIdentifier); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

func (h *Handler) handleDescribeConfigurationTemplates(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	return map[string]any{"configurationTemplates": []any{}}, nil
}

func (h *Handler) handleDescribeFieldIndexes(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	return map[string]any{"fieldIndexes": []any{}}, nil
}
