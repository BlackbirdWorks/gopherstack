package kms

import (
	"context"
	"encoding/json"
)

// buildAliasRotationActions returns dispatch entries for alias management and key rotation.
func (h *Handler) buildAliasRotationActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"CreateAlias": func(ctx context.Context, b []byte) (any, error) {
			var input CreateAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.CreateAlias(ctx, &input)
		},
		"UpdateAlias": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateAlias(ctx, &input)
		},
		"DeleteAlias": func(ctx context.Context, b []byte) (any, error) {
			var input DeleteAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DeleteAlias(ctx, &input)
		},
		"ListAliases": func(ctx context.Context, b []byte) (any, error) {
			var input ListAliasesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListAliases(ctx, &input)
		},
		"EnableKeyRotation": func(ctx context.Context, b []byte) (any, error) {
			var input EnableKeyRotationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.EnableKeyRotation(ctx, &input)
		},
		"DisableKeyRotation": func(ctx context.Context, b []byte) (any, error) {
			var input DisableKeyRotationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DisableKeyRotation(ctx, &input)
		},
		"GetKeyRotationStatus": func(ctx context.Context, b []byte) (any, error) {
			var input GetKeyRotationStatusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetKeyRotationStatus(ctx, &input)
		},
	}
}
