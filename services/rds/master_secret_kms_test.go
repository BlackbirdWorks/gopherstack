package rds_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

func TestMasterSecretKMSKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		key     func(id, arn string) string
		setup   func(t *testing.T, kb *kms.InMemoryBackend, id string)
		wantErr error
		name    string
		usage   string
		unwired bool
		managed bool
	}{
		{name: "default key minted", managed: true, key: func(string, string) string { return "" }},
		{name: "custom key id", key: func(id, _ string) string { return id }},
		{name: "custom key arn", key: func(_, arn string) string { return arn }},
		{
			name: "missing", key: func(string, string) string { return "00000000-0000-0000-0000-000000000000" },
			wantErr: rds.ErrKMSKeyNotAccessible,
		},
		{
			name: "disabled", key: func(id, _ string) string { return id }, wantErr: rds.ErrKMSKeyNotAccessible,
			setup: func(t *testing.T, kb *kms.InMemoryBackend, id string) {
				t.Helper()
				require.NoError(t, kb.DisableKey(t.Context(), &kms.DisableKeyInput{KeyID: id}))
			},
		},
		{
			name: "signing key", usage: kms.KeyUsageSignVerify, key: func(id, _ string) string { return id },
			wantErr: rds.ErrKMSKeyNotAccessible,
		},
		{name: "unwired missing", unwired: true, key: func(string, string) string { return "missing" }},
		{name: "unwired default", unwired: true, key: func(string, string) string { return "" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			kb := kms.NewInMemoryBackendWithConfig("000000000000", "us-east-1")

			in := &kms.CreateKeyInput{KeyUsage: tt.usage}
			if tt.usage == kms.KeyUsageSignVerify {
				in.KeySpec = "ECC_NIST_P256"
			}

			k, err := kb.CreateKey(t.Context(), in)
			require.NoError(t, err)

			if tt.setup != nil {
				tt.setup(t, kb, k.KeyMetadata.KeyID)
			}

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			b.SetSecretsStore(newFakeSecretsStore())

			if !tt.unwired {
				b.SetKeyChecker(kms.NewKeyChecker(kb))
			}

			keyID := tt.key(k.KeyMetadata.KeyID, k.KeyMetadata.Arn)

			inst, err := b.CreateDBInstance("kms-i", "mysql", "db.t3.micro", "db", "admin", "", 20,
				rds.DBInstanceOptions{
					ManageMasterUserPassword: true, ManageSet: true, MasterUserSecretKmsKeyID: keyID,
				})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			if !tt.managed {
				assert.Equal(t, keyID, inst.MasterUserSecretKmsKeyID)

				return
			}

			managedKey, err := kb.DescribeKey(t.Context(), &kms.DescribeKeyInput{KeyID: "alias/aws/secretsmanager"})
			require.NoError(t, err)
			assert.Equal(t, managedKey.KeyMetadata.Arn, inst.MasterUserSecretKmsKeyID)

			_, err = b.ModifyDBInstance("kms-i", "", 0, rds.DBInstanceOptions{
				ManageMasterUserPassword: true, ManageSet: true, MasterUserSecretKmsKeyID: "alias/aws/secretsmanager",
			})
			require.NoError(t, err)
		})
	}
}
