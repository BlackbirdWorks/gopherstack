package s3tables_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3tables"
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
			name:         "delete_without_encryption_returns_not_found",
			putFirst:     false,
			wantGetAfter: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bucketARN := createBucketHelper(t, h, "parity-enc-"+bucketSuffix(tt.name))
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

// TestHandler_GetTableBucketReplication_WireShape locks in the real
// GetTableBucketReplicationOutput shape: {configuration: {role, rules:
// [{destinations: [{destinationTableBucketARN}]}]}, versionToken}, NOT the
// gopherstack-invented {tableBucketARN, replicationConfiguration:
// {destinations}} shape this test previously asserted as correct (verified
// against deserializeOpDocumentGetTableBucketReplicationOutput /
// deserializeDocumentTableBucketReplicationConfiguration in
// aws-sdk-go-v2/service/s3tables's deserializers.go).
func TestHandler_GetTableBucketReplication_WireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "repl-wrap-bucket")

	q := url.Values{}
	q.Set("tableBucketARN", bucketARN)

	destARN := "arn:aws:s3tables:us-east-1:000000000000:bucket/dest"

	// Seed directly via backend for reliability.
	s3tables.AddBucketReplicationInternal(h.Backend, bucketARN, &s3tables.BucketReplicationConfig{
		Role: "arn:aws:iam::000000000000:role/repl",
		Rules: []s3tables.ReplicationRule{
			{Destinations: []s3tables.ReplicationDestination{{DestinationTableBucketARN: destARN}}},
		},
		VersionToken: "seed-token",
	})

	getRec := doS3TablesRequest(t, h, http.MethodGet, "/table-bucket-replication?"+q.Encode(), nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))

	_, hasTopLevelDests := out["destinations"]
	_, hasFabricatedWrapper := out["replicationConfiguration"]
	assert.False(t, hasTopLevelDests, "destinations must not appear at the top level")
	assert.False(
		t,
		hasFabricatedWrapper,
		"replicationConfiguration is not a real GetTableBucketReplicationOutput field -- the real field is configuration",
	)

	assert.Equal(t, "seed-token", out["versionToken"],
		"GetTableBucketReplicationOutput.versionToken is a required response member")

	cfg, ok := out["configuration"].(map[string]any)
	require.True(t, ok, "response must include the real configuration field")
	assert.Equal(t, "arn:aws:iam::000000000000:role/repl", cfg["role"])

	rules, ok := cfg["rules"].([]any)
	require.True(t, ok)
	require.Len(t, rules, 1)

	rule, ok := rules[0].(map[string]any)
	require.True(t, ok)

	dests, ok := rule["destinations"].([]any)
	require.True(t, ok, "each rule nests its own destinations array")
	require.Len(t, dests, 1)

	dest, ok := dests[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, destARN, dest["destinationTableBucketARN"],
		"the wire field is destinationTableBucketARN, not destinationBucketARN")
}

func TestHandler_PutTableBucketReplication(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "put-bucket-repl-bucket")

	q := url.Values{}
	q.Set("tableBucketARN", bucketARN)

	rec := doS3TablesRequest(t, h, http.MethodPut, "/table-bucket-replication?"+q.Encode(), map[string]any{
		"configuration": map[string]any{
			"role": "arn:aws:iam::000000000000:role/repl",
			"rules": []any{
				map[string]any{
					"destinations": []any{
						map[string]any{
							"destinationTableBucketARN": "arn:aws:s3tables:us-east-1:000000000000:bucket/dest",
						},
					},
				},
			},
		},
	})

	require.Equal(
		t,
		http.StatusOK,
		rec.Code,
		"PutTableBucketReplicationOutput requires status and versionToken, so the response must be a 200, not 204",
	)

	result := parseResponse(t, rec)
	assert.Equal(t, "COMPLETED", result["status"])
	assert.NotEmpty(t, result["versionToken"])
}

func TestHandler_PutTableBucketReplication_StaleVersionTokenConflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "put-bucket-repl-conflict-bucket")

	s3tables.AddBucketReplicationInternal(h.Backend, bucketARN, &s3tables.BucketReplicationConfig{
		Role:         "arn:aws:iam::000000000000:role/repl",
		VersionToken: "real-token",
	})

	q := url.Values{}
	q.Set("tableBucketARN", bucketARN)
	q.Set("versionToken", "stale-token")

	rec := doS3TablesRequest(t, h, http.MethodPut, "/table-bucket-replication?"+q.Encode(), map[string]any{
		"configuration": map[string]any{"role": "arn:aws:iam::000000000000:role/repl2"},
	})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

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
						Role: "arn:aws:iam::000000000000:role/repl",
						Rules: []s3tables.ReplicationRule{
							{Destinations: []s3tables.ReplicationDestination{
								{DestinationTableBucketARN: "arn:aws:s3tables:us-east-1:000000000000:bucket/dest"},
							}},
						},
						VersionToken: "seed-token",
					})
				}

				q := url.Values{}
				q.Set("tableBucketARN", bucketARN)
				rec := doS3TablesRequest(t, h, http.MethodGet, "/table-bucket-replication?"+q.Encode(), nil)
				assert.Equal(t, tt.wantCode, rec.Code)

				if tt.wantCode == http.StatusOK {
					result := parseResponse(t, rec)
					assert.Equal(t, "seed-token", result["versionToken"])
					cfg, ok := result["configuration"].(map[string]any)
					require.True(t, ok)
					rules, ok := cfg["rules"].([]any)
					require.True(t, ok)
					assert.Len(t, rules, 1)
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
