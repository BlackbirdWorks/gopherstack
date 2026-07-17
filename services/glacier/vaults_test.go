package glacier_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/glacier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVaultCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
		wantErr   bool
	}{
		{
			name:      "create_and_describe",
			vaultName: "test-vault",
		},
		{
			name:      "delete_nonexistent",
			vaultName: "does-not-exist",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()

			if tt.wantErr {
				err := bk.DeleteVault(testAccountID, testRegion, tt.vaultName)
				require.Error(t, err)

				return
			}

			v, err := bk.CreateVault(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, err)
			assert.Equal(t, tt.vaultName, v.VaultName)
			assert.NotEmpty(t, v.VaultARN)
			assert.NotEmpty(t, v.CreationDate)

			got, err := bk.DescribeVault(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, err)
			assert.Equal(t, v.VaultName, got.VaultName)
			assert.Equal(t, v.VaultARN, got.VaultARN)

			err = bk.DeleteVault(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, err)

			_, err = bk.DescribeVault(testAccountID, testRegion, tt.vaultName)
			require.Error(t, err)
		})
	}
}

func TestListVaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		vaultNames []string
		wantCount  int
	}{
		{
			name:       "empty",
			vaultNames: nil,
			wantCount:  0,
		},
		{
			name:       "multiple_vaults",
			vaultNames: []string{"vault-a", "vault-b", "vault-c"},
			wantCount:  3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()

			for _, name := range tt.vaultNames {
				_, err := bk.CreateVault(testAccountID, testRegion, name)
				require.NoError(t, err)
			}

			vaults := bk.ListVaults(testAccountID, testRegion)
			assert.Len(t, vaults, tt.wantCount)
		})
	}
}

// TestSortedListVaults verifies ListVaults returns vaults in lexicographic name order.
func TestSortedListVaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		vaultNames []string
		wantOrder  []string
	}{
		{
			name:       "three_vaults_sorted",
			vaultNames: []string{"zoo", "alpha", "middle"},
			wantOrder:  []string{"alpha", "middle", "zoo"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()

			for _, vn := range tt.vaultNames {
				_, err := b.CreateVault(testAccountID, testRegion, vn)
				require.NoError(t, err)
			}

			vaults := b.ListVaults(testAccountID, testRegion)
			require.Len(t, vaults, len(tt.wantOrder))

			for i, want := range tt.wantOrder {
				assert.Equal(t, want, vaults[i].VaultName)
			}
		})
	}
}

// TestErrValidationMapping verifies ErrValidation is returned from the backend.
func TestErrValidationMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		vaultName string
	}{
		{name: "empty_vault_name_returns_ErrValidation", vaultName: "", wantErr: glacier.ErrValidation},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			_, err := b.CreateVault(testAccountID, testRegion, tt.vaultName)

			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestDeleteVault_CascadesMultipartUploads verifies multipart uploads are removed when vault is deleted.
func TestDeleteVault_CascadesMultipartUploads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantUploadCount int
	}{
		{name: "delete_vault_removes_uploads", wantUploadCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			_, err := b.CreateVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			_, err = b.InitiateMultipartUpload(testAccountID, testRegion, "vault", "desc", 1024*1024)
			require.NoError(t, err)
			assert.Equal(t, 1, glacier.MultipartUploadCount(b))

			err = b.DeleteVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			assert.Equal(t, tt.wantUploadCount, glacier.MultipartUploadCount(b))
		})
	}
}
