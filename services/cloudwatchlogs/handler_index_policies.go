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

		return map[string]any{"indexPolicy": map[string]any{
			completenessKeyLogGroupIdentifier: p.LogGroupIdentifier,
			completenessKeyPolicyDocument:     p.PolicyDocument,
		}}, nil
	}

	return map[string]any{"indexPolicy": map[string]any{}}, nil
}

func (h *Handler) handleDescribeIndexPolicies(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	if b := cwlBackend(h); b != nil {
		policies := b.DescribeIndexPolicies()
		out := make([]map[string]any, 0, len(policies))
		for _, p := range policies {
			out = append(out, map[string]any{
				completenessKeyLogGroupIdentifier: p.LogGroupIdentifier,
				completenessKeyPolicyDocument:     p.PolicyDocument,
			})
		}

		return map[string]any{"indexPolicies": out}, nil
	}

	return map[string]any{"indexPolicies": []any{}}, nil
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
