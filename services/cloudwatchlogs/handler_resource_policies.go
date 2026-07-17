package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"fmt"
)

type putResourcePolicyInput struct {
	PolicyName     string `json:"policyName"`
	PolicyDocument string `json:"policyDocument"`
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
		p, err := b.PutResourcePolicy(in.PolicyName, in.PolicyDocument)
		if err != nil {
			return nil, err
		}

		return map[string]any{
			"resourcePolicy": map[string]any{
				keyPolicyName:                 p.PolicyName,
				completenessKeyPolicyDocument: p.PolicyDocument,
			},
		}, nil
	}

	return map[string]any{"resourcePolicy": map[string]any{
		keyPolicyName:                 in.PolicyName,
		completenessKeyPolicyDocument: in.PolicyDocument,
	}}, nil
}

func (h *Handler) handleDescribeResourcePolicies(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	if b := cwlBackend(h); b != nil {
		policies := b.DescribeResourcePolicies()
		out := make([]map[string]any, 0, len(policies))
		for _, p := range policies {
			out = append(out, map[string]any{
				keyPolicyName:                 p.PolicyName,
				completenessKeyPolicyDocument: p.PolicyDocument,
			})
		}

		return map[string]any{"resourcePolicies": out}, nil
	}

	return map[string]any{"resourcePolicies": []any{}}, nil
}

type deleteResourcePolicyInput struct {
	PolicyName string `json:"policyName"`
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
		if err := b.DeleteResourcePolicy(in.PolicyName); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}
