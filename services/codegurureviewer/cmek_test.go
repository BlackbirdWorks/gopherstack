package codegurureviewer_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/codegurureviewer"
	"github.com/aws/aws-sdk-go-v2/service/codegurureviewer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func TestAssociateRepositoryCustomerKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, b *kms.InMemoryBackend, id string)
		name     string
		usage    string
		key      func(id, arn string) string
		wantCode string
		unwired  bool
	}{
		{name: "key id", key: func(id, _ string) string { return id }},
		{name: "key arn", key: func(_, arn string) string { return arn }},
		{name: "missing", key: func(string, string) string { return "missing" }, wantCode: "ValidationException"},
		{
			name: "disabled", key: func(id, _ string) string { return id }, wantCode: "ValidationException",
			setup: func(t *testing.T, b *kms.InMemoryBackend, id string) {
				t.Helper()
				require.NoError(t, b.DisableKey(t.Context(), &kms.DisableKeyInput{KeyID: id}))
			},
		},
		{
			name: "signing key", usage: kms.KeyUsageSignVerify, key: func(id, _ string) string { return id },
			wantCode: "ValidationException",
		},
		{name: "unwired missing", unwired: true, key: func(string, string) string { return "missing" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.commitRepo(t, "src", map[string]string{"a.txt": "x"})

			kb := kms.NewInMemoryBackendWithConfig(testAccount, testRegion)
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

			_, err = f.client.AssociateRepository(t.Context(), &sdk.AssociateRepositoryInput{
				Repository: &types.Repository{CodeCommit: &types.CodeCommitRepository{Name: aws.String("src")}},
				KMSKeyDetails: &types.KMSKeyDetails{
					EncryptionOption: types.EncryptionOptionCmCmk,
					KMSKeyId:         aws.String(tt.key(k.KeyMetadata.KeyID, k.KeyMetadata.Arn)),
				},
			})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
		})
	}
}
