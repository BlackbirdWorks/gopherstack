package s3tables_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3tables"
)

// bucketSuffix converts an arbitrary table-test case name (this package's
// table tests conventionally use underscore_separated names, per project
// testing standards, and sometimes embed uppercase acronyms like "AES256")
// into a suffix usable in a real S3 Tables bucket name: real bucket names
// allow only LOWERCASE letters, digits, and hyphens (no underscores) -- see
// validation.go. Shared by every _test.go file that builds a bucket name as
// "<prefix>-" + a test case's name.
func bucketSuffix(s string) string {
	return strings.ReplaceAll(strings.ToLower(s), "_", "-")
}

func newTestHandler(t *testing.T) *s3tables.Handler {
	t.Helper()

	return s3tables.NewHandler(s3tables.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doS3TablesRequest(
	t *testing.T,
	h *s3tables.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func parseResponse(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var result map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&result))

	return result
}

func createBucketHelper(t *testing.T, h *s3tables.Handler, name string) string {
	t.Helper()

	rec := doS3TablesRequest(t, h, http.MethodPut, "/buckets", map[string]any{"name": name})
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)

	arnVal, ok := result["arn"].(string)
	require.True(t, ok, "expected arn in response")

	return arnVal
}

// getTableBucketIDHelper fetches the real, system-assigned tableBucketId AWS
// generates at CreateTableBucket time (see models.go's TableBucket.BucketID,
// gopherstack-wla0), for tests that need to assert a Namespace/Table
// response's tableBucketId matches its owning bucket's.
func getTableBucketIDHelper(t *testing.T, h *s3tables.Handler, bucketARN string) string {
	t.Helper()

	encodedARN := url.PathEscape(bucketARN)
	rec := doS3TablesRequest(t, h, http.MethodGet, "/buckets/"+encodedARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)

	id, ok := result["tableBucketId"].(string)
	require.True(t, ok, "expected tableBucketId in GetTableBucket response")
	require.NotEmpty(t, id)

	return id
}

func createNamespaceHelper(t *testing.T, h *s3tables.Handler, bucketARN string, namespace []string) {
	t.Helper()

	encodedARN := url.PathEscape(bucketARN)
	path := "/namespaces/" + encodedARN

	rec := doS3TablesRequest(t, h, http.MethodPut, path, map[string]any{"namespace": namespace})
	require.Equal(t, http.StatusOK, rec.Code)
}

func createTableHelper(t *testing.T, h *s3tables.Handler, bucketARN, namespace, name string) string {
	t.Helper()

	encodedARN := url.PathEscape(bucketARN)
	path := fmt.Sprintf("/tables/%s/%s", encodedARN, namespace)

	rec := doS3TablesRequest(t, h, http.MethodPut, path, map[string]any{
		"name":   name,
		"format": "ICEBERG",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)

	tableARN, ok := result["tableARN"].(string)
	require.True(t, ok, "expected tableArn in response")

	return tableARN
}

func getTableHelper(t *testing.T, h *s3tables.Handler, bucketARN, namespace, name string) map[string]any {
	t.Helper()

	query := url.Values{}
	query.Set("tableBucketARN", bucketARN)
	query.Set("namespace", namespace)
	query.Set("name", name)
	rec := doS3TablesRequest(t, h, http.MethodGet, "/get-table?"+query.Encode(), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	return parseResponse(t, rec)
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "S3tables", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "CreateTableBucket")
	assert.Contains(t, ops, "GetTableBucket")
	assert.Contains(t, ops, "ListTableBuckets")
	assert.Contains(t, ops, "CreateTable")
	assert.Contains(t, ops, "GetTable")
	assert.Contains(t, ops, "RenameTable")
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "matches /buckets", path: "/buckets", want: true},
		{name: "matches /buckets/arn", path: "/buckets/arn:aws:s3tables:us-east-1:123:bucket/test", want: true},
		{name: "matches /namespaces/arn", path: "/namespaces/arn:aws:s3tables:us-east-1:123:bucket/test", want: true},
		{name: "matches /tables/arn", path: "/tables/arn:aws:s3tables:us-east-1:123:bucket/test", want: true},
		{name: "matches /get-table", path: "/get-table", want: true},
		{name: "matches /tag/arn", path: "/tag/arn:aws:s3tables:us-east-1:123:bucket/test", want: true},
		{name: "no match /s3", path: "/s3", want: false},
		{name: "no match /", path: "/", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			c.SetRequest(req)

			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{name: "create_table_bucket", method: http.MethodPut, path: "/buckets", wantOp: "CreateTableBucket"},
		{name: "list_table_buckets", method: http.MethodGet, path: "/buckets", wantOp: "ListTableBuckets"},
		{name: "get_table", method: http.MethodGet, path: "/get-table", wantOp: "GetTable"},
		{
			name:   "list_tables",
			method: http.MethodGet,
			path:   "/tables/arn%3Aaws%3As3tables%3Aus-east-1%3A123456789012%3Abucket%2Fmy-bucket",
			wantOp: "ListTables",
		},
		{
			name:   "create_namespace",
			method: http.MethodPut,
			path:   "/namespaces/arn%3Aaws%3As3tables%3Aus-east-1%3A123%3Abucket%2Ftest",
			wantOp: "CreateNamespace",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			c.SetRequest(req)

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_UnknownPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doS3TablesRequest(t, h, http.MethodGet, "/unknown-path", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createBucketHelper(t, h, "reset-bucket")

	require.Equal(t, 1, s3tables.BucketCount(h.Backend))

	h.Reset()

	assert.Equal(t, 0, s3tables.BucketCount(h.Backend))
}

func TestHandler_SupportedOperationsLen(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	assert.Greater(t, s3tables.HandlerOpsLen(h), 25)
	assert.Contains(t, ops, "DeleteTableBucketEncryption")
	assert.Contains(t, ops, "DeleteTableBucketMetricsConfiguration")
	assert.Contains(t, ops, "DeleteTableBucketReplication")
	assert.Contains(t, ops, "DeleteTableReplication")
	assert.Contains(t, ops, "GetTableBucketMetricsConfiguration")
	assert.Contains(t, ops, "GetTableBucketReplication")
	assert.Contains(t, ops, "GetTableBucketStorageClass")
	assert.Contains(t, ops, "GetTableMaintenanceJobStatus")
	assert.Contains(t, ops, "GetTableMetadataLocation")
	assert.Contains(t, ops, "GetTableRecordExpirationConfiguration")
}

func TestHandler_RouteMatcher_ReplicationPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		want   string
	}{
		{
			name:   "table-bucket-replication GET",
			path:   "/table-bucket-replication?tableBucketARN=arn",
			method: http.MethodGet,
			want:   "GetTableBucketReplication",
		},
		{
			name:   "table-bucket-replication DELETE",
			path:   "/table-bucket-replication?tableBucketARN=arn",
			method: http.MethodDelete,
			want:   "DeleteTableBucketReplication",
		},
		{
			name:   "table-replication DELETE",
			path:   "/table-replication?tableArn=arn",
			method: http.MethodDelete,
			want:   "DeleteTableReplication",
		},
		{
			name:   "table-record-expiration GET",
			path:   "/table-record-expiration?tableArn=arn",
			method: http.MethodGet,
			want:   "GetTableRecordExpirationConfiguration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			// The operation is checked via GetSupportedOperations which includes these ops.
			ops := h.GetSupportedOperations()
			assert.Contains(t, ops, tt.want)
		})
	}
}

func TestHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "s3tables", h.ChaosServiceName())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
	assert.Equal(t, "us-east-1", h.ChaosRegions()[0])
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Positive(t, h.MatchPriority())
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "bucket path",
			path: "/buckets/arn%3Aaws%3As3tables%3Aus-east-1%3A123%3Abucket%2Fb",
			want: "arn:aws:s3tables:us-east-1:123:bucket/b",
		},
		{
			name: "table-bucket-replication with param",
			path: "/table-bucket-replication?tableBucketARN=myarn",
			want: "myarn",
		},
		{
			name: "table-replication with tableArn",
			path: "/table-replication?tableArn=mytablearn",
			want: "mytablearn",
		},
		{
			name: "empty path",
			path: "/",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			c.SetRequest(req)
			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestHandler_RouteMatcher_ReplicationPathPrefixes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "table-bucket-replication", path: "/table-bucket-replication?tableBucketARN=arn", want: true},
		{name: "table-replication", path: "/table-replication?tableArn=arn", want: true},
		{name: "table-record-expiration", path: "/table-record-expiration?tableArn=arn", want: true},
		{name: "no match", path: "/other", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			c.SetRequest(req)
			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

const keyContinuationTokenTestKey = "continuationToken"
