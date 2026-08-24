package s3tables_test

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_TableBucket_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantARN    bool
	}{
		{
			name:       "create_bucket",
			body:       map[string]any{"name": "my-bucket"},
			wantStatus: http.StatusOK,
			wantARN:    true,
		},
		{
			name:       "missing_name",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty_body",
			body:       nil,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doS3TablesRequest(t, h, http.MethodPut, "/buckets", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantARN {
				result := parseResponse(t, rec)
				assert.NotEmpty(t, result["arn"])
			}
		})
	}
}

func TestHandler_TableBucket_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_ = createBucketHelper(t, h, "duplicate-bucket")

	rec := doS3TablesRequest(t, h, http.MethodPut, "/buckets", map[string]any{"name": "duplicate-bucket"})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandler_TableBucket_GetAndDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		checkName  string
		wantStatus int
		exists     bool
	}{
		{
			name:       "get_existing",
			method:     http.MethodGet,
			exists:     true,
			wantStatus: http.StatusOK,
			checkName:  "test-get-bucket",
		},
		{
			name:       "get_nonexistent",
			method:     http.MethodGet,
			exists:     false,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_existing",
			method:     http.MethodDelete,
			exists:     true,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "delete_nonexistent",
			method:     http.MethodDelete,
			exists:     false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			path := "/buckets/arn:aws:s3tables:us-east-1:000000000000:bucket/nonexistent"

			if tt.exists {
				bucketARN := createBucketHelper(t, h, "test-get-bucket")
				path = "/buckets/" + url.PathEscape(bucketARN)
			}

			rec := doS3TablesRequest(t, h, tt.method, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkName != "" && rec.Code == http.StatusOK {
				result := parseResponse(t, rec)
				assert.Equal(t, tt.checkName, result["name"])
			}
		})
	}
}

func TestHandler_ListTableBuckets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		numBuckets int
		wantStatus int
	}{
		{
			name:       "empty_list",
			numBuckets: 0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "list_buckets",
			numBuckets: 2,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tt.numBuckets {
				_ = createBucketHelper(t, h, fmt.Sprintf("list-bucket-%d", i))
			}

			rec := doS3TablesRequest(t, h, http.MethodGet, "/buckets", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			result := parseResponse(t, rec)
			buckets, ok := result["tableBuckets"].([]any)
			require.True(t, ok)
			assert.Len(t, buckets, tt.numBuckets)
		})
	}
}

func TestHandler_TableBucketPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "policy-bucket")
	encodedARN := url.PathEscape(bucketARN)
	policy := `{"Version":"2012-10-17","Statement":[]}`
	path := "/buckets/" + encodedARN + "/policy"

	// Put policy
	rec := doS3TablesRequest(t, h, http.MethodPut, path, map[string]any{"resourcePolicy": policy})
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get policy
	rec = doS3TablesRequest(t, h, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete policy
	rec = doS3TablesRequest(t, h, http.MethodDelete, path, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get policy after delete - should be 404
	rec = doS3TablesRequest(t, h, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CreateTableBucket_AppliesEncryptionStorageClassAndTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doS3TablesRequest(t, h, http.MethodPut, "/buckets", map[string]any{
		"name": "wire-opts-bucket",
		"encryptionConfiguration": map[string]any{
			"sseAlgorithm": "aws:kms",
			"kmsKeyArn":    "arn:aws:kms:us-east-1:000000000000:key/test",
		},
		"storageClassConfiguration": map[string]any{
			"storageClass": "INTELLIGENT_TIERING",
		},
		"tags": map[string]any{"env": "prod"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)
	bucketARN, ok := result["arn"].(string)
	require.True(t, ok)

	encodedARN := url.PathEscape(bucketARN)

	encRec := doS3TablesRequest(t, h, http.MethodGet, "/buckets/"+encodedARN+"/encryption", nil)
	require.Equal(t, http.StatusOK, encRec.Code)
	encResult := parseResponse(t, encRec)
	encCfg, ok := encResult["encryptionConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "aws:kms", encCfg["sseAlgorithm"])

	scRec := doS3TablesRequest(t, h, http.MethodGet, "/buckets/"+encodedARN+"/storage-class", nil)
	require.Equal(t, http.StatusOK, scRec.Code)
	scResult := parseResponse(t, scRec)
	scCfg, ok := scResult["storageClassConfiguration"].(map[string]any)
	require.True(t, ok, "expected storageClassConfiguration to be an object")
	assert.Equal(t, "INTELLIGENT_TIERING", scCfg["storageClass"])

	tagRec := doS3TablesRequest(t, h, http.MethodGet, "/tag/"+encodedARN, nil)
	require.Equal(t, http.StatusOK, tagRec.Code)
	tagResult := parseResponse(t, tagRec)
	tags, ok := tagResult["tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", tags["env"])
}

func TestHandler_ListTableBuckets_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		createBucketHelper(t, h, fmt.Sprintf("wire-page-bucket-%d", i))
	}

	rec := doS3TablesRequest(t, h, http.MethodGet, "/buckets?maxBuckets=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)
	buckets, ok := result["tableBuckets"].([]any)
	require.True(t, ok)
	assert.Len(t, buckets, 2)

	token, ok := result[keyContinuationTokenTestKey].(string)
	require.True(t, ok, "expected a continuationToken when more buckets remain")
	require.NotEmpty(t, token)

	q := url.Values{}
	q.Set("continuationToken", token)
	rec = doS3TablesRequest(t, h, http.MethodGet, "/buckets?"+q.Encode(), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	result = parseResponse(t, rec)
	buckets, ok = result["tableBuckets"].([]any)
	require.True(t, ok)
	assert.Len(t, buckets, 1, "the final page must contain the one remaining bucket")
	assert.NotContains(t, result, keyContinuationTokenTestKey, "no more pages means no continuationToken")
}
