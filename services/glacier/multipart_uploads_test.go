package glacier_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/glacier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSortedListMultipartUploads verifies ListMultipartUploads returns sorted by MultipartUploadID.
func TestSortedListMultipartUploads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		uploadCount int
	}{
		{name: "uploads_sorted_by_id", uploadCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			_, err := b.CreateVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			for range tt.uploadCount {
				_, err = b.InitiateMultipartUpload(testAccountID, testRegion, "vault", "desc", 1024*1024)
				require.NoError(t, err)
			}

			uploads := b.ListMultipartUploads(testAccountID, testRegion, "vault")
			require.Len(t, uploads, tt.uploadCount)

			for i := 1; i < len(uploads); i++ {
				assert.LessOrEqual(t, uploads[i-1].MultipartUploadID, uploads[i].MultipartUploadID)
			}
		})
	}
}

// TestNonNilListMultipartUploads verifies ListMultipartUploads returns non-nil empty slice.
func TestNonNilListMultipartUploads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
	}{
		{name: "empty_vault_non_nil_uploads", vaultName: "empty-vault"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			_, err := b.CreateVault(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, err)

			uploads := b.ListMultipartUploads(testAccountID, testRegion, tt.vaultName)

			assert.NotNil(t, uploads)
			assert.Empty(t, uploads)
		})
	}
}
