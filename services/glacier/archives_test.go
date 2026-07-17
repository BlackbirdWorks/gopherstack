package glacier_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/glacier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArchiveCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setup       func(*testing.T, *glacier.InMemoryBackend)
		vaultName   string
		description string
		wantErr     bool
	}{
		{
			name: "upload_and_delete",
			setup: func(t *testing.T, bk *glacier.InMemoryBackend) {
				t.Helper()
				_, err := bk.CreateVault(testAccountID, testRegion, "vault")
				require.NoError(t, err)
			},
			vaultName:   "vault",
			description: "test archive",
		},
		{
			name:      "vault_not_found",
			vaultName: "no-such-vault",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, bk)
			}

			a, err := bk.UploadArchive(
				testAccountID,
				testRegion,
				tt.vaultName,
				tt.description,
				"checksum",
				1024,
				[]byte("data"),
			)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, a.ArchiveID)
			assert.Equal(t, tt.description, a.Description)

			v, err := bk.DescribeVault(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, err)
			assert.Equal(t, int64(1), v.NumberOfArchives)
			assert.Equal(t, int64(1024), v.SizeInBytes)

			err = bk.DeleteArchive(testAccountID, testRegion, tt.vaultName, a.ArchiveID)
			require.NoError(t, err)

			v, err = bk.DescribeVault(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, err)
			assert.Equal(t, int64(0), v.NumberOfArchives)
		})
	}
}
