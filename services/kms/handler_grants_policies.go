package kms

import (
	"context"
	"encoding/json"
	"fmt"
)

// buildGrantPolicyActions returns dispatch entries for grant and key policy operations.
func (h *Handler) buildGrantPolicyActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		opCreateGrant: func(ctx context.Context, b []byte) (any, error) {
			var input CreateGrantInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateGrant(ctx, &input)
		},
		"ListGrants": func(ctx context.Context, b []byte) (any, error) {
			var input ListGrantsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListGrants(ctx, &input)
		},
		"RevokeGrant": func(ctx context.Context, b []byte) (any, error) {
			var input RevokeGrantInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.RevokeGrant(ctx, &input)
		},
		opRetireGrant: func(ctx context.Context, b []byte) (any, error) {
			var input RetireGrantInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.RetireGrant(ctx, &input)
		},
		"ListRetirableGrants": func(ctx context.Context, b []byte) (any, error) {
			var input ListRetirableGrantsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListRetirableGrants(ctx, &input)
		},
		"PutKeyPolicy": func(ctx context.Context, b []byte) (any, error) {
			var input PutKeyPolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			// AWS KMS only supports the "default" policy name.
			if input.PolicyName != "" && input.PolicyName != defaultKeyPolicyName {
				return nil, fmt.Errorf(
					"%w: PolicyName must be %q; got %q",
					ErrValidation, defaultKeyPolicyName, input.PolicyName,
				)
			}

			if input.PolicyName == "" {
				input.PolicyName = defaultKeyPolicyName
			}

			return struct{}{}, h.Backend.PutKeyPolicy(ctx, &input)
		},
		"GetKeyPolicy": func(ctx context.Context, b []byte) (any, error) {
			var input GetKeyPolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetKeyPolicy(ctx, &input)
		},
	}
}
