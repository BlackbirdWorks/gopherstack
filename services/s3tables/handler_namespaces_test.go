package s3tables_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_Namespace_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		wantStatus int
		nsExists   bool
	}{
		{
			name:       "create_namespace",
			method:     http.MethodPut,
			wantStatus: http.StatusOK,
		},
		{
			name:       "create_namespace_missing_name",
			method:     http.MethodPut,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "list_namespaces",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
			nsExists:   true,
		},
		{
			name:       "get_namespace",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
			nsExists:   true,
		},
		{
			name:       "get_nonexistent_namespace",
			method:     http.MethodGet,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_namespace",
			method:     http.MethodDelete,
			wantStatus: http.StatusNoContent,
			nsExists:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bucketARN := createBucketHelper(t, h, "ns-bucket-"+bucketSuffix(tt.name))
			encodedARN := url.PathEscape(bucketARN)

			if tt.nsExists {
				createNamespaceHelper(t, h, bucketARN, []string{"my_ns"})
			}

			var path string
			var body map[string]any

			switch tt.name {
			case "create_namespace":
				path = "/namespaces/" + encodedARN
				body = map[string]any{"namespace": []string{"my_ns"}}
			case "create_namespace_missing_name":
				path = "/namespaces/" + encodedARN
				body = map[string]any{"namespace": []string{}}
			case "list_namespaces":
				path = "/namespaces/" + encodedARN
			case "get_namespace":
				path = "/namespaces/" + encodedARN + "/my_ns"
			case "get_nonexistent_namespace":
				path = "/namespaces/" + encodedARN + "/not_found"
			case "delete_namespace":
				path = "/namespaces/" + encodedARN + "/my_ns"
			}

			rec := doS3TablesRequest(t, h, tt.method, path, body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListNamespaces_MaxNamespacesLimitsPage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "wire-ns-page-bucket")

	for _, ns := range []string{"a", "b", "c"} {
		createNamespaceHelper(t, h, bucketARN, []string{ns})
	}

	encodedARN := url.PathEscape(bucketARN)
	rec := doS3TablesRequest(t, h, http.MethodGet, "/namespaces/"+encodedARN+"?maxNamespaces=1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)
	namespaces, ok := result["namespaces"].([]any)
	require.True(t, ok)
	assert.Len(t, namespaces, 1)
	assert.Contains(t, result, keyContinuationTokenTestKey)
}

func TestHandler_GetNamespaceIncludesTableBucketArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "ns-arn-bucket")
	createNamespaceHelper(t, h, bucketARN, []string{"ns1"})

	encodedARN := url.PathEscape(bucketARN)
	rec := doS3TablesRequest(t, h, http.MethodGet, "/namespaces/"+encodedARN+"/ns1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)
	assert.Equal(t, bucketARN, result["tableBucketARN"],
		"GetNamespace response must include tableBucketARN")
}

func TestHandler_ListNamespacesIncludesTableBucketArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "list-ns-arn-bucket")
	createNamespaceHelper(t, h, bucketARN, []string{"ns1"})

	encodedARN := url.PathEscape(bucketARN)
	rec := doS3TablesRequest(t, h, http.MethodGet, "/namespaces/"+encodedARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	namespaces, ok := out["namespaces"].([]any)
	require.True(t, ok)
	require.Len(t, namespaces, 1)

	entry := namespaces[0].(map[string]any)
	assert.Equal(t, bucketARN, entry["tableBucketARN"],
		"ListNamespaces summary must include tableBucketARN")
}

// ======================================================================
// Gap 3: GetTableBucketReplication wraps destinations in replicationConfiguration
// ======================================================================
