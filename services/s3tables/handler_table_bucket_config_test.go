package s3tables_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/s3tables"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_TableBucketEncryption_PutGetDeleteRoundTrip(t *testing.T) {
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

func TestHandler_TableBucketMetricsConfiguration_PutGetDeleteRoundTrip(t *testing.T) {
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

func TestHandler_TableBucketEncryptionDelete_NotFoundBucket(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := "arn:aws:s3tables:us-east-1:000000000000:bucket/does-not-exist"
	encodedARN := url.PathEscape(bucketARN)

	rec := doS3TablesRequest(t, h, http.MethodDelete, "/buckets/"+encodedARN+"/encryption", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestParity_TableBucketMetricsConfigurationDelete_NotFoundBucket verifies the

func TestHandler_TableBucketMetricsConfigurationDelete_NotFoundBucket(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := "arn:aws:s3tables:us-east-1:000000000000:bucket/does-not-exist"
	encodedARN := url.PathEscape(bucketARN)

	rec := doS3TablesRequest(t, h, http.MethodDelete, "/buckets/"+encodedARN+"/metrics", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestParity_TableBucketMetricsConfigurationGet_NotFoundBucket verifies the

func TestHandler_TableBucketMetricsConfigurationGet_NotFoundBucket(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := "arn:aws:s3tables:us-east-1:000000000000:bucket/does-not-exist"
	encodedARN := url.PathEscape(bucketARN)

	rec := doS3TablesRequest(t, h, http.MethodGet, "/buckets/"+encodedARN+"/metrics", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_DeleteTableBucketEncryptionClearsConfig(t *testing.T) {
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

func TestHandler_GetTableBucketIncludesType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "type-field-bucket")
	encodedARN := url.PathEscape(bucketARN)

	rec := doS3TablesRequest(t, h, http.MethodGet, "/buckets/"+encodedARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)
	assert.Equal(t, "customer", result["type"],
		"GetTableBucket response must include type='customer' for customer-owned buckets")
}

func TestHandler_ListTableBucketsIncludesType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createBucketHelper(t, h, "list-type-bucket")

	rec := doS3TablesRequest(t, h, http.MethodGet, "/buckets", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	buckets, ok := out["tableBuckets"].([]any)
	require.True(t, ok)
	require.Len(t, buckets, 1)

	entry := buckets[0].(map[string]any)
	assert.Equal(t, "customer", entry["type"],
		"ListTableBuckets summary must include type='customer'")
}

// ======================================================================
// Gap 2b: GetNamespace / ListNamespaces include tableBucketArn
// ======================================================================

func TestHandler_GetTableBucketReplicationWrapsDestinations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "repl-wrap-bucket")

	q := url.Values{}
	q.Set("tableBucketARN", bucketARN)

	// Seed directly via backend for reliability.
	s3tables.AddBucketReplicationInternal(h.Backend, bucketARN, &s3tables.BucketReplicationConfig{
		Destinations: []s3tables.ReplicationDestination{
			{DestinationBucketARN: "arn:aws:s3tables:us-east-1:000000000000:bucket/dest"},
		},
	})

	getRec := doS3TablesRequest(t, h, http.MethodGet, "/table-bucket-replication?"+q.Encode(), nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))

	// Real AWS nests destinations inside replicationConfiguration.
	_, hasTopLevelDests := out["destinations"]
	replCfg, hasReplCfg := out["replicationConfiguration"].(map[string]any)

	assert.False(t, hasTopLevelDests,
		"destinations must NOT appear at the top level — should be nested in replicationConfiguration")
	assert.True(t, hasReplCfg,
		"response must include replicationConfiguration wrapper object")

	if hasReplCfg {
		dests, ok := replCfg["destinations"].([]any)
		assert.True(t, ok, "replicationConfiguration must include destinations array")
		assert.Len(t, dests, 1)
	}
}

// ======================================================================
// Gap 4: PutTableReplication returns 204 No Content (no body)
// ======================================================================

func TestHandler_DeleteTableBucketEncryption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bucketName string
		wantCode   int
	}{
		{
			name:       "existing bucket",
			bucketName: "enc-bucket",
			wantCode:   http.StatusNoContent,
		},
		{
			name:     "missing bucket",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var bucketARN string
			if tt.bucketName != "" {
				bucketARN = createBucketHelper(t, h, tt.bucketName)
			} else {
				bucketARN = "arn:aws:s3tables:us-east-1:000000000000:bucket/nonexistent"
			}

			encodedARN := url.PathEscape(bucketARN)
			rec := doS3TablesRequest(t, h, http.MethodDelete, "/buckets/"+encodedARN+"/encryption", nil)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_GetTableBucketMetricsConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bucketName string
		wantCode   int
	}{
		{
			name:       "existing bucket",
			bucketName: "metrics-bucket",
			wantCode:   http.StatusOK,
		},
		{
			name:     "missing bucket",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var bucketARN string
			if tt.bucketName != "" {
				bucketARN = createBucketHelper(t, h, tt.bucketName)
			} else {
				bucketARN = "arn:aws:s3tables:us-east-1:000000000000:bucket/nonexistent"
			}

			encodedARN := url.PathEscape(bucketARN)
			rec := doS3TablesRequest(t, h, http.MethodGet, "/buckets/"+encodedARN+"/metrics", nil)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				result := parseResponse(t, rec)
				assert.Equal(t, bucketARN, result["tableBucketARN"])
			}
		})
	}
}

func TestHandler_DeleteTableBucketMetricsConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bucketName string
		wantCode   int
	}{
		{
			name:       "existing bucket",
			bucketName: "metrics-delete-bucket",
			wantCode:   http.StatusNoContent,
		},
		{
			name:     "missing bucket",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var bucketARN string
			if tt.bucketName != "" {
				bucketARN = createBucketHelper(t, h, tt.bucketName)
			} else {
				bucketARN = "arn:aws:s3tables:us-east-1:000000000000:bucket/nonexistent"
			}

			encodedARN := url.PathEscape(bucketARN)
			rec := doS3TablesRequest(t, h, http.MethodDelete, "/buckets/"+encodedARN+"/metrics", nil)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_GetTableBucketStorageClass(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bucketName string
		wantClass  string
		wantCode   int
	}{
		{
			name:       "existing bucket",
			bucketName: "sc-bucket",
			wantCode:   http.StatusOK,
			wantClass:  "STANDARD",
		},
		{
			name:     "missing bucket",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var bucketARN string
			if tt.bucketName != "" {
				bucketARN = createBucketHelper(t, h, tt.bucketName)
			} else {
				bucketARN = "arn:aws:s3tables:us-east-1:000000000000:bucket/nonexistent"
			}

			encodedARN := url.PathEscape(bucketARN)
			rec := doS3TablesRequest(t, h, http.MethodGet, "/buckets/"+encodedARN+"/storage-class", nil)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				result := parseResponse(t, rec)
				assert.Equal(t, tt.wantClass, result["storageClass"])
			}
		})
	}
}

func TestHandler_GetTableBucketReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucketName     string
		hasReplication bool
		wantCode       int
	}{
		{
			name:           "existing replication",
			bucketName:     "repl-bucket",
			hasReplication: true,
			wantCode:       http.StatusOK,
		},
		{
			name:       "no replication config",
			bucketName: "no-repl-bucket",
			wantCode:   http.StatusNotFound,
		},
		{
			name:     "missing bucket ARN param",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.bucketName != "" {
				bucketARN := createBucketHelper(t, h, tt.bucketName)

				if tt.hasReplication {
					s3tables.AddBucketReplicationInternal(h.Backend, bucketARN, &s3tables.BucketReplicationConfig{
						Destinations: []s3tables.ReplicationDestination{
							{DestinationBucketARN: "arn:aws:s3tables:us-east-1:000000000000:bucket/dest"},
						},
					})
				}

				q := url.Values{}
				q.Set("tableBucketARN", bucketARN)
				rec := doS3TablesRequest(t, h, http.MethodGet, "/table-bucket-replication?"+q.Encode(), nil)
				assert.Equal(t, tt.wantCode, rec.Code)

				if tt.wantCode == http.StatusOK {
					result := parseResponse(t, rec)
					assert.Equal(t, bucketARN, result["tableBucketARN"])
					replCfg, ok := result["replicationConfiguration"].(map[string]any)
					require.True(t, ok)
					dests, ok := replCfg["destinations"].([]any)
					require.True(t, ok)
					assert.Len(t, dests, 1)
				}
			} else {
				rec := doS3TablesRequest(t, h, http.MethodGet, "/table-bucket-replication", nil)
				assert.Equal(t, tt.wantCode, rec.Code)
			}
		})
	}
}

func TestHandler_DeleteTableBucketReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucketName     string
		hasReplication bool
		noARNParam     bool
		wantCode       int
	}{
		{
			name:           "delete existing replication",
			bucketName:     "repl-del-bucket",
			hasReplication: true,
			wantCode:       http.StatusNoContent,
		},
		{
			name:       "missing bucket",
			bucketName: "missing",
			wantCode:   http.StatusNotFound,
		},
		{
			name:       "missing ARN param",
			noARNParam: true,
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.noARNParam {
				rec := doS3TablesRequest(t, h, http.MethodDelete, "/table-bucket-replication", nil)
				assert.Equal(t, tt.wantCode, rec.Code)

				return
			}

			var bucketARN string
			if tt.bucketName == "missing" {
				bucketARN = "arn:aws:s3tables:us-east-1:000000000000:bucket/nonexistent"
			} else {
				bucketARN = createBucketHelper(t, h, tt.bucketName)
			}

			if tt.hasReplication {
				s3tables.AddBucketReplicationInternal(h.Backend, bucketARN, &s3tables.BucketReplicationConfig{})
			}

			q := url.Values{}
			q.Set("tableBucketARN", bucketARN)
			rec := doS3TablesRequest(t, h, http.MethodDelete, "/table-bucket-replication?"+q.Encode(), nil)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusNoContent {
				assert.Equal(t, 0, s3tables.BucketReplicationCount(h.Backend))
			}
		})
	}
}

func TestHandler_BucketEncryptionAndMetricsRoute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		subPath    string
		wantOp     string
		bucketName string
		wantCode   int
	}{
		{
			name:       "GET encryption",
			method:     http.MethodGet,
			subPath:    "encryption",
			wantCode:   http.StatusNotFound,
			bucketName: "enc-get",
		},
		{
			name:       "DELETE encryption",
			method:     http.MethodDelete,
			subPath:    "encryption",
			wantCode:   http.StatusNoContent,
			bucketName: "enc-del",
		},
		{
			name:       "GET metrics",
			method:     http.MethodGet,
			subPath:    "metrics",
			wantCode:   http.StatusOK,
			bucketName: "met-get",
		},
		{
			name:       "DELETE metrics",
			method:     http.MethodDelete,
			subPath:    "metrics",
			wantCode:   http.StatusNoContent,
			bucketName: "met-del",
		},
		{
			name:       "GET storage-class",
			method:     http.MethodGet,
			subPath:    "storage-class",
			wantCode:   http.StatusOK,
			bucketName: "sc-get",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bucketARN := createBucketHelper(t, h, tt.bucketName)
			encodedARN := url.PathEscape(bucketARN)
			rec := doS3TablesRequest(t, h, tt.method, "/buckets/"+encodedARN+"/"+tt.subPath, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
