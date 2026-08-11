package ec2_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyPairOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string
		keyName string
		wantErr bool
	}{
		{
			name:    "create_keypair",
			op:      "create",
			keyName: "my-key",
			wantErr: false,
		},
		{
			name:    "create_keypair_empty_name",
			op:      "create",
			keyName: "",
			wantErr: true,
		},
		{
			name:    "create_duplicate",
			op:      "create_duplicate",
			keyName: "dup-key",
			wantErr: true,
		},
		{
			name:    "describe_all",
			op:      "describe_all",
			keyName: "desc-key",
			wantErr: false,
		},
		{
			name:    "describe_by_name",
			op:      "describe_by_name",
			keyName: "desc-by-name-key",
			wantErr: false,
		},
		{
			name:    "delete_keypair",
			op:      "delete",
			keyName: "del-key",
			wantErr: false,
		},
		{
			name:    "delete_nonexistent",
			op:      "delete_nonexistent",
			keyName: "nonexistent-key",
			wantErr: true,
		},
		{
			name:    "import_keypair",
			op:      "import",
			keyName: "imported-key",
			wantErr: false,
		},
		{
			name:    "import_keypair_empty_name",
			op:      "import",
			keyName: "",
			wantErr: true,
		},
		{
			name:    "import_keypair_duplicate",
			op:      "import_duplicate",
			keyName: "dup-import-key",
			wantErr: true,
		},
		{
			name:    "import_keypair_retrievable",
			op:      "import_retrievable",
			keyName: "retrievable-key",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			switch tt.op {
			case "create":
				kp, err := b.CreateKeyPair(tt.keyName, nil)
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.keyName, kp.Name)
					assert.NotEmpty(t, kp.Fingerprint)
					assert.NotEmpty(t, kp.Material)
				}

			case "create_duplicate":
				_, err := b.CreateKeyPair(tt.keyName, nil)
				require.NoError(t, err)
				_, err = b.CreateKeyPair(tt.keyName, nil)
				require.Error(t, err)

			case "describe_all":
				_, err := b.CreateKeyPair(tt.keyName, nil)
				require.NoError(t, err)
				kps := b.DescribeKeyPairs(nil)
				assert.NotEmpty(t, kps)
				assert.Empty(t, kps[0].Material, "material should be stripped on describe")

			case "describe_by_name":
				_, err := b.CreateKeyPair(tt.keyName, nil)
				require.NoError(t, err)
				kps := b.DescribeKeyPairs([]string{tt.keyName})
				require.Len(t, kps, 1)
				assert.Equal(t, tt.keyName, kps[0].Name)

			case "delete":
				_, err := b.CreateKeyPair(tt.keyName, nil)
				require.NoError(t, err)
				err = b.DeleteKeyPair(tt.keyName)
				require.NoError(t, err)
				kps := b.DescribeKeyPairs([]string{tt.keyName})
				assert.Empty(t, kps)

			case "delete_nonexistent":
				err := b.DeleteKeyPair(tt.keyName)
				require.Error(t, err)

			case "import":
				kp, err := b.ImportKeyPair(tt.keyName, "", nil)
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.keyName, kp.Name)
					assert.NotEmpty(t, kp.Fingerprint)
					assert.Empty(t, kp.Material, "import should not set private key material")
				}

			case "import_duplicate":
				_, err := b.ImportKeyPair(tt.keyName, "", nil)
				require.NoError(t, err)
				_, err = b.ImportKeyPair(tt.keyName, "", nil)
				require.ErrorIs(t, err, ec2.ErrDuplicateKeyPairName)

			case "import_retrievable":
				_, err := b.ImportKeyPair(tt.keyName, "", nil)
				require.NoError(t, err)
				kps := b.DescribeKeyPairs([]string{tt.keyName})
				require.Len(t, kps, 1)
				assert.Equal(t, tt.keyName, kps[0].Name)
				assert.NotEmpty(t, kps[0].Fingerprint)
			}
		})
	}
}
