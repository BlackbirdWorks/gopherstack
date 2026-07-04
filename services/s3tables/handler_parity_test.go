package s3tables_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------
// Parity sweep: real mutate/read round-trips for table bucket
// encryption and metrics configuration.
// ----------------------------------------

// TestParity_TableBucketEncryption_PutGetDeleteRoundTrip verifies that
// PutTableBucketEncryption actually persists the configuration, that Get
// reflects it, and that Delete really clears it (as opposed to merely
// validating bucket existence and no-oping).
func TestParity_TableBucketEncryption_PutGetDeleteRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "enc-roundtrip-bucket")
	encodedARN := url.PathEscape(bucketARN)
	path := "/buckets/" + encodedARN + "/encryption"

	// Initially unconfigured: Get returns NotFound.
	rec := doS3TablesRequest(t, h, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Put a real encryption configuration.
	rec = doS3TablesRequest(t, h, http.MethodPut, path, map[string]any{
		"encryptionConfiguration": map[string]any{
			"sseAlgorithm": "aws:kms",
			"kmsKeyArn":    "arn:aws:kms:us-east-1:000000000000:key/test",
		},
	})
	require.Equal(t, http.StatusNoContent, rec.Code)

	// Get now reflects the stored configuration.
	rec = doS3TablesRequest(t, h, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)
	encCfg, ok := result["encryptionConfiguration"].(map[string]any)
	require.True(t, ok, "expected encryptionConfiguration to be an object")
	assert.Equal(t, "aws:kms", encCfg["sseAlgorithm"])

	// Delete must actually clear the stored configuration, not just validate
	// that the bucket exists.
	rec = doS3TablesRequest(t, h, http.MethodDelete, path, nil)
	require.Equal(t, http.StatusNoContent, rec.Code)

	rec = doS3TablesRequest(t, h, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code, "encryption config must be cleared after delete")
}

// TestParity_TableBucketMetricsConfiguration_PutGetDeleteRoundTrip verifies
// that PutTableBucketMetricsConfiguration is reflected by a subsequent Get,
// and that Delete actually clears the stored metrics configuration state.
func TestParity_TableBucketMetricsConfiguration_PutGetDeleteRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "metrics-roundtrip-bucket")
	encodedARN := url.PathEscape(bucketARN)
	path := "/buckets/" + encodedARN + "/metrics"

	// Before any Put, metrics configuration is unconfigured: no "id" field.
	rec := doS3TablesRequest(t, h, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)
	assert.Equal(t, bucketARN, result["tableBucketARN"])
	_, hasID := result["id"]
	assert.False(t, hasID, "expected no metrics configuration id before Put")

	// Put enables metrics and assigns a configuration id.
	rec = doS3TablesRequest(t, h, http.MethodPut, path, nil)
	require.Equal(t, http.StatusNoContent, rec.Code)

	// Get now reflects the enabled configuration with a non-empty id.
	rec = doS3TablesRequest(t, h, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	result = parseResponse(t, rec)
	assert.Equal(t, bucketARN, result["tableBucketARN"])

	firstID, ok := result["id"].(string)
	require.True(t, ok, "expected metrics configuration id after Put")
	assert.NotEmpty(t, firstID)

	// Delete must actually clear the stored metrics configuration.
	rec = doS3TablesRequest(t, h, http.MethodDelete, path, nil)
	require.Equal(t, http.StatusNoContent, rec.Code)

	rec = doS3TablesRequest(t, h, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	result = parseResponse(t, rec)
	_, hasID = result["id"]
	assert.False(t, hasID, "expected metrics configuration id to be cleared after delete")

	// Re-enabling must assign a fresh id, proving the backend state actually
	// mutates rather than being a hardcoded stub value.
	rec = doS3TablesRequest(t, h, http.MethodPut, path, nil)
	require.Equal(t, http.StatusNoContent, rec.Code)

	rec = doS3TablesRequest(t, h, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	result = parseResponse(t, rec)

	secondID, ok := result["id"].(string)
	require.True(t, ok)
	assert.NotEqual(t, firstID, secondID, "expected a fresh metrics configuration id on re-enable")
}

// TestParity_TableBucketEncryptionDelete_NotFoundBucket verifies the
// not-found error path for a bucket that never existed.
func TestParity_TableBucketEncryptionDelete_NotFoundBucket(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := "arn:aws:s3tables:us-east-1:000000000000:bucket/does-not-exist"
	encodedARN := url.PathEscape(bucketARN)

	rec := doS3TablesRequest(t, h, http.MethodDelete, "/buckets/"+encodedARN+"/encryption", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestParity_TableBucketMetricsConfigurationDelete_NotFoundBucket verifies the
// not-found error path for a bucket that never existed.
func TestParity_TableBucketMetricsConfigurationDelete_NotFoundBucket(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := "arn:aws:s3tables:us-east-1:000000000000:bucket/does-not-exist"
	encodedARN := url.PathEscape(bucketARN)

	rec := doS3TablesRequest(t, h, http.MethodDelete, "/buckets/"+encodedARN+"/metrics", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestParity_TableBucketMetricsConfigurationGet_NotFoundBucket verifies the
// not-found error path for a bucket that never existed.
func TestParity_TableBucketMetricsConfigurationGet_NotFoundBucket(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := "arn:aws:s3tables:us-east-1:000000000000:bucket/does-not-exist"
	encodedARN := url.PathEscape(bucketARN)

	rec := doS3TablesRequest(t, h, http.MethodGet, "/buckets/"+encodedARN+"/metrics", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
