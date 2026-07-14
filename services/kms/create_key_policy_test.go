package kms_test

// create_key_policy_test.go — regression for CreateKey honoring an inline Policy.
// Terraform's aws_kms_key sends the policy in CreateKey and then polls GetKeyPolicy
// after create until the configured policy propagates; if CreateKey drops the policy,
// GetKeyPolicy returns the synthesized default forever and the poll never converges.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func Test_CreateKey_InlinePolicy(t *testing.T) {
	t.Parallel()

	const customPolicy = `{"Version":"2012-10-17","Statement":[{"Sid":"Custom",` +
		`"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:role/admin"},` +
		`"Action":"kms:*","Resource":"*"}]}`

	tests := []struct {
		name       string
		policy     string
		wantPolicy string // empty => expect the synthesized default (non-empty, != customPolicy)
		wantErr    bool
	}{
		{
			name:       "inline policy stored and returned verbatim",
			policy:     customPolicy,
			wantPolicy: customPolicy,
		},
		{
			name:       "no policy falls back to default",
			policy:     "",
			wantPolicy: "",
		},
		{
			name:    "malformed policy rejected at create",
			policy:  `{"not":"a policy"}`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			ctx := context.Background()

			out, err := b.CreateKey(ctx, &kms.CreateKeyInput{Policy: tc.policy})
			if tc.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)

			got, err := b.GetKeyPolicy(ctx, &kms.GetKeyPolicyInput{
				KeyID: out.KeyMetadata.KeyID,
			})
			require.NoError(t, err)
			assert.Equal(t, "default", got.PolicyName)

			if tc.wantPolicy != "" {
				assert.Equal(t, tc.wantPolicy, got.Policy,
					"GetKeyPolicy must return the policy supplied to CreateKey")
			} else {
				assert.NotEmpty(t, got.Policy, "default policy must be non-empty")
				assert.NotEqual(t, customPolicy, got.Policy)
			}
		})
	}
}
