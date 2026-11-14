package bedrockagentcorecontrol_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func TestCreateMemoryEncryptionKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, b *kms.InMemoryBackend, id string)
		key      func(arn string) string
		name     string
		usage    string
		wantCode string
		unwired  bool
	}{
		{name: "enabled", key: func(arn string) string { return arn }},
		{
			name:     "missing",
			key:      func(string) string { return "arn:aws:kms:us-east-1:" + testAccount + ":key/missing" },
			wantCode: "ValidationException",
		},
		{
			name: "disabled", key: func(arn string) string { return arn }, wantCode: "ValidationException",
			setup: func(t *testing.T, b *kms.InMemoryBackend, id string) {
				t.Helper()
				require.NoError(t, b.DisableKey(t.Context(), &kms.DisableKeyInput{KeyID: id}))
			},
		},
		{
			name: "signing key", usage: kms.KeyUsageSignVerify, key: func(arn string) string { return arn },
			wantCode: "ValidationException",
		},
		{
			name: "unwired missing", unwired: true,
			key: func(string) string { return "arn:aws:kms:us-east-1:" + testAccount + ":key/missing" },
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

			_, err = f.client.CreateMemory(t.Context(), &sdk.CreateMemoryInput{
				Name: aws.String("mem_one"), EventExpiryDuration: aws.Int32(30),
				EncryptionKeyArn: aws.String(tt.key(k.KeyMetadata.Arn)),
			})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
		})
	}
}
