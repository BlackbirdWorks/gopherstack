package s3tables_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_DeleteTableBucketEncryptionClearsConfig verifies that
// DeleteTableBucketEncryption actually clears the encryption configuration
// so that a subsequent GetTableBucketEncryption returns 404. The emulator
// previously logged the deletion but left the config intact, so Get continued
// to return the old config — diverging from real AWS behaviour.
func TestParity_DeleteTableBucketEncryptionClearsConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		putFirst     bool
		wantGetAfter int
	}{
		{
			name:         "delete_after_put_returns_not_found_on_get",
			putFirst:     true,
			wantGetAfter: http.StatusNotFound,
		},
		{
			name:         "delete_on_bucket_without_encryption_returns_not_found_on_get",
			putFirst:     false,
			wantGetAfter: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bucketARN := createBucketHelper(t, h, "parity-enc-"+tt.name)
			encodedARN := url.PathEscape(bucketARN)
			encPath := "/buckets/" + encodedARN + "/encryption"

			if tt.putFirst {
				rec := doS3TablesRequest(t, h, http.MethodPut, encPath, map[string]any{
					"encryptionConfiguration": map[string]any{
						"sseAlgorithm": "AES256",
					},
				})
				require.Equal(t, http.StatusNoContent, rec.Code)

				rec = doS3TablesRequest(t, h, http.MethodGet, encPath, nil)
				require.Equal(t, http.StatusOK, rec.Code, "encryption should be present before delete")
			}

			rec := doS3TablesRequest(t, h, http.MethodDelete, encPath, nil)
			assert.Equal(t, http.StatusNoContent, rec.Code, "DeleteTableBucketEncryption should succeed")

			rec = doS3TablesRequest(t, h, http.MethodGet, encPath, nil)
			assert.Equal(t, tt.wantGetAfter, rec.Code,
				"GetTableBucketEncryption after delete should return %d", tt.wantGetAfter)
		})
	}
}
