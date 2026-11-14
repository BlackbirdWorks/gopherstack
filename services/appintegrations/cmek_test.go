package appintegrations_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/appintegrations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func TestCreateDataIntegrationKmsKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, b *kms.InMemoryBackend, id string)
		name     string
		usage    string
		key      func(id, arn string) string
		wantCode string
		unwired  bool
	}{
		{name: "key arn", key: func(_, arn string) string { return arn }},
		{name: "key id", key: func(id, _ string) string { return id }},
		{
			name:     "missing",
			key:      func(string, string) string { return "arn:aws:kms:us-east-1:123456789012:key/missing" },
			wantCode: "InvalidRequestException",
		},
		{
			name: "disabled", key: func(id, _ string) string { return id }, wantCode: "InvalidRequestException",
			setup: func(t *testing.T, b *kms.InMemoryBackend, id string) {
				t.Helper()
				require.NoError(t, b.DisableKey(t.Context(), &kms.DisableKeyInput{KeyID: id}))
			},
		},
		{
			name: "pending deletion", key: func(id, _ string) string { return id }, wantCode: "InvalidRequestException",
			setup: func(t *testing.T, b *kms.InMemoryBackend, id string) {
				t.Helper()

				_, err := b.ScheduleKeyDeletion(
					t.Context(),
					&kms.ScheduleKeyDeletionInput{KeyID: id, PendingWindowInDays: 7},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "signing key", usage: kms.KeyUsageSignVerify, key: func(id, _ string) string { return id },
			wantCode: "InvalidRequestException",
		},
		{
			name: "unwired missing", unwired: true,
			key: func(string, string) string { return "arn:aws:kms:us-east-1:123456789012:key/missing" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			kb := kms.NewInMemoryBackendWithConfig(testAccount, "us-east-1")

			if !tt.unwired {
				f.backend.SetKeyChecker(kms.NewKeyChecker(kb))
			}

			in := &kms.CreateKeyInput{KeyUsage: tt.usage}
			if tt.usage == kms.KeyUsageSignVerify {
				in.KeySpec = "ECC_NIST_P256"
			}

			k, err := kb.CreateKey(t.Context(), in)
			require.NoError(t, err)

			if tt.setup != nil {
				tt.setup(t, kb, k.KeyMetadata.KeyID)
			}

			_, err = f.client.CreateDataIntegration(t.Context(), &sdk.CreateDataIntegrationInput{
				Name:      aws.String("d1"),
				KmsKey:    aws.String(tt.key(k.KeyMetadata.KeyID, k.KeyMetadata.Arn)),
				SourceURI: aws.String("Salesforce://AppFlow/d1"),
			})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
		})
	}
}
