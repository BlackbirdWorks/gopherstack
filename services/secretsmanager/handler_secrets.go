package secretsmanager

import (
	"context"
	"encoding/json"
)

func (h *Handler) smSecretsActions() map[string]smActionFn {
	return map[string]smActionFn{
		"CreateSecret": func(ctx context.Context, region string, b []byte) (any, error) {
			var input CreateSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			input.Region = region

			return h.Backend.CreateSecret(ctx, &input)
		},
		"GetSecretValue": func(ctx context.Context, _ string, b []byte) (any, error) {
			var input GetSecretValueInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetSecretValue(ctx, &input)
		},
		"PutSecretValue": func(ctx context.Context, _ string, b []byte) (any, error) {
			var input PutSecretValueInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.PutSecretValue(ctx, &input)
		},
		"DeleteSecret": func(ctx context.Context, _ string, b []byte) (any, error) {
			var input DeleteSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteSecret(ctx, &input)
		},
		opListSecrets: func(ctx context.Context, _ string, b []byte) (any, error) {
			var input ListSecretsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListSecrets(ctx, &input)
		},
		opDescribeSecret: func(ctx context.Context, _ string, b []byte) (any, error) {
			var input DescribeSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeSecret(ctx, &input)
		},
		"UpdateSecret": func(ctx context.Context, _ string, b []byte) (any, error) {
			var input UpdateSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateSecret(ctx, &input)
		},
		"RestoreSecret": func(ctx context.Context, _ string, b []byte) (any, error) {
			var input RestoreSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.RestoreSecret(ctx, &input)
		},
	}
}
