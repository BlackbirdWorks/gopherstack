package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"fmt"
)

type deleteAccountPolicyInput struct {
	PolicyName string `json:"policyName"`
	PolicyType string `json:"policyType"`
}

type deleteAccountPolicyOutput struct{}

// --- PutAccountPolicy ---.
type putAccountPolicyInput struct {
	PolicyDocument    string `json:"policyDocument"`
	PolicyName        string `json:"policyName"`
	PolicyType        string `json:"policyType"`
	Scope             string `json:"scope,omitempty"`
	SelectionCriteria string `json:"selectionCriteria,omitempty"`
}

type putAccountPolicyOutput struct {
	AccountPolicy *AccountPolicy `json:"accountPolicy,omitempty"`
}

// --- DescribeAccountPolicies ---.
type describeAccountPoliciesInput struct {
	PolicyName         string   `json:"policyName"`
	PolicyType         string   `json:"policyType"`
	NextToken          string   `json:"nextToken,omitempty"`
	AccountIdentifiers []string `json:"accountIdentifiers,omitempty"`
	MaxResults         int      `json:"maxResults,omitempty"`
}

type describeAccountPoliciesOutput struct {
	NextToken       string          `json:"nextToken,omitempty"`
	AccountPolicies []AccountPolicy `json:"accountPolicies"`
}

func (h *Handler) handleDeleteAccountPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input deleteAccountPolicyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteAccountPolicy(input.PolicyName, input.PolicyType); err != nil {
		return nil, err
	}

	return &deleteAccountPolicyOutput{}, nil
}

func (h *Handler) handlePutAccountPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input putAccountPolicyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	policy, err := h.Backend.PutAccountPolicy(
		input.PolicyName,
		input.PolicyType,
		input.PolicyDocument,
		input.Scope,
		input.SelectionCriteria,
	)
	if err != nil {
		return nil, err
	}

	return &putAccountPolicyOutput{AccountPolicy: policy}, nil
}

func (h *Handler) handleDescribeAccountPolicies(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input describeAccountPoliciesInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	policies, nextToken, err := h.Backend.DescribeAccountPolicies(
		input.PolicyType,
		input.PolicyName,
		input.AccountIdentifiers,
		input.MaxResults,
		input.NextToken,
	)
	if err != nil {
		return nil, err
	}

	return &describeAccountPoliciesOutput{AccountPolicies: policies, NextToken: nextToken}, nil
}

type deleteDataProtectionPolicyInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
}

func (h *Handler) handleDeleteDataProtectionPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteDataProtectionPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteDataProtectionPolicy(in.LogGroupIdentifier); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

type getDataProtectionPolicyInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
}

func (h *Handler) handleGetDataProtectionPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in getDataProtectionPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	policyDoc := "{}"
	var lastUpdatedTime int64
	if b := cwlBackend(h); b != nil {
		p, updated, err := b.GetDataProtectionPolicy(in.LogGroupIdentifier)
		if err != nil {
			return nil, err
		}
		policyDoc = p
		lastUpdatedTime = updated
	}

	resp := map[string]any{
		completenessKeyLogGroupIdentifier: in.LogGroupIdentifier,
		completenessKeyPolicyDocument:     policyDoc,
	}
	if lastUpdatedTime != 0 {
		resp[keyLastUpdatedTime] = lastUpdatedTime
	}

	return resp, nil
}

type putDataProtectionPolicyInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
	PolicyDocument     string `json:"policyDocument"`
}

func (h *Handler) handlePutDataProtectionPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putDataProtectionPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.PutDataProtectionPolicy(in.LogGroupIdentifier, in.PolicyDocument); err != nil {
			return nil, err
		}
	}

	return map[string]any{
		completenessKeyLogGroupIdentifier: in.LogGroupIdentifier,
		completenessKeyPolicyDocument:     in.PolicyDocument,
	}, nil
}
