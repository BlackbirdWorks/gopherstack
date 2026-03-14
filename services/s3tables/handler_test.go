package s3tables_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3tables"
)

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
	require.True(t, ok, "expected tableARN in response")

	return tableARN
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
		{name: "matches /tag/arn", path: "/tag/arn:aws:s3tables:us-east-1:123:bucket/test", want: false},
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
			bucketARN := createBucketHelper(t, h, "ns-bucket-"+tt.name)
			encodedARN := url.PathEscape(bucketARN)

			if tt.nsExists {
				createNamespaceHelper(t, h, bucketARN, []string{"my-ns"})
			}

			var path string
			var body map[string]any

			switch tt.name {
			case "create_namespace":
				path = "/namespaces/" + encodedARN
				body = map[string]any{"namespace": []string{"my-ns"}}
			case "create_namespace_missing_name":
				path = "/namespaces/" + encodedARN
				body = map[string]any{"namespace": []string{}}
			case "list_namespaces":
				path = "/namespaces/" + encodedARN
			case "get_namespace":
				path = "/namespaces/" + encodedARN + "/my-ns"
			case "get_nonexistent_namespace":
				path = "/namespaces/" + encodedARN + "/not-found"
			case "delete_namespace":
				path = "/namespaces/" + encodedARN + "/my-ns"
			}

			rec := doS3TablesRequest(t, h, tt.method, path, body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_Table_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		checkField string
		wantStatus int
	}{
		{
			name: "create_table",
			body: map[string]any{
				"name":   "my-table",
				"format": "ICEBERG",
			},
			wantStatus: http.StatusOK,
			checkField: "tableARN",
		},
		{
			name:       "create_table_missing_name",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create_table_default_format",
			body:       map[string]any{"name": "default-format-table"},
			wantStatus: http.StatusOK,
			checkField: "tableARN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bucketARN := createBucketHelper(t, h, "table-create-bucket-"+tt.name)
			encodedARN := url.PathEscape(bucketARN)
			createNamespaceHelper(t, h, bucketARN, []string{"test-ns"})

			path := "/tables/" + encodedARN + "/test-ns"
			rec := doS3TablesRequest(t, h, http.MethodPut, path, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkField != "" && rec.Code == http.StatusOK {
				result := parseResponse(t, rec)
				assert.NotEmpty(t, result[tt.checkField])
			}
		})
	}
}

func TestHandler_Table_GetAndList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pathFn     func(bucketARN, encodedARN string) string
		name       string
		method     string
		checkField string
		wantStatus int
	}{
		{
			name:   "get_table",
			method: http.MethodGet,
			pathFn: func(bucketARN, _ string) string {
				return fmt.Sprintf(
					"/get-table?tableBucketARN=%s&namespace=test-ns&name=test-table",
					url.QueryEscape(bucketARN),
				)
			},
			wantStatus: http.StatusOK,
			checkField: "tableARN",
		},
		{
			name:   "get_table_not_found",
			method: http.MethodGet,
			pathFn: func(bucketARN, _ string) string {
				return fmt.Sprintf(
					"/get-table?tableBucketARN=%s&namespace=test-ns&name=nope",
					url.QueryEscape(bucketARN),
				)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "list_tables",
			method:     http.MethodGet,
			pathFn:     func(_, encodedARN string) string { return "/tables/" + encodedARN },
			wantStatus: http.StatusOK,
		},
		{
			name:       "list_tables_with_namespace",
			method:     http.MethodGet,
			pathFn:     func(_, encodedARN string) string { return "/tables/" + encodedARN + "?namespace=test-ns" },
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bucketARN := createBucketHelper(t, h, "get-list-bucket-"+tt.name)
			encodedARN := url.PathEscape(bucketARN)
			createNamespaceHelper(t, h, bucketARN, []string{"test-ns"})
			_ = createTableHelper(t, h, bucketARN, "test-ns", "test-table")

			path := tt.pathFn(bucketARN, encodedARN)
			rec := doS3TablesRequest(t, h, tt.method, path, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkField != "" && rec.Code == http.StatusOK {
				result := parseResponse(t, rec)
				assert.NotEmpty(t, result[tt.checkField])
			}
		})
	}
}

func TestHandler_Table_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		tableExists bool
		wantStatus  int
	}{
		{
			name:        "delete_existing",
			tableExists: true,
			wantStatus:  http.StatusNoContent,
		},
		{
			name:        "delete_not_found",
			tableExists: false,
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bucketARN := createBucketHelper(t, h, "delete-table-bucket-"+tt.name)
			encodedARN := url.PathEscape(bucketARN)
			createNamespaceHelper(t, h, bucketARN, []string{"test-ns"})

			if tt.tableExists {
				_ = createTableHelper(t, h, bucketARN, "test-ns", "my-table")
			}

			rec := doS3TablesRequest(t, h, http.MethodDelete, "/tables/"+encodedARN+"/test-ns/my-table", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_Table_Rename(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "rename_table",
			body:       map[string]any{"newName": "renamed-table"},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "rename_table_not_found",
			body:       map[string]any{"newName": "renamed"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bucketARN := createBucketHelper(t, h, "rename-bucket-"+tt.name)
			encodedARN := url.PathEscape(bucketARN)
			createNamespaceHelper(t, h, bucketARN, []string{"rename-ns"})

			tableName := "orig-table"
			if tt.name == "rename_table" {
				_ = createTableHelper(t, h, bucketARN, "rename-ns", tableName)
			} else {
				tableName = "not-exist"
			}

			path := "/tables/" + encodedARN + "/rename-ns/" + tableName + "/rename"
			rec := doS3TablesRequest(t, h, http.MethodPut, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_Table_UpdateMetadataLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "update_metadata_location",
			body:       map[string]any{"metadataLocation": "s3://bucket/path/metadata.json", "versionToken": "v1"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "table_not_found",
			body:       map[string]any{"metadataLocation": "s3://bucket/path/metadata.json", "versionToken": "v1"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bucketARN := createBucketHelper(t, h, "meta-bucket-"+tt.name)
			encodedARN := url.PathEscape(bucketARN)
			createNamespaceHelper(t, h, bucketARN, []string{"meta-ns"})

			tableName := "meta-table"
			if tt.name == "update_metadata_location" {
				_ = createTableHelper(t, h, bucketARN, "meta-ns", tableName)
			} else {
				tableName = "not-exist"
			}

			path := "/tables/" + encodedARN + "/meta-ns/" + tableName + "/metadata-location"
			rec := doS3TablesRequest(t, h, http.MethodPut, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
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

func TestHandler_TablePolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "table-policy-bucket")
	encodedARN := url.PathEscape(bucketARN)
	createNamespaceHelper(t, h, bucketARN, []string{"policy-ns"})
	_ = createTableHelper(t, h, bucketARN, "policy-ns", "policy-table")
	policy := `{"Version":"2012-10-17","Statement":[]}`
	path := "/tables/" + encodedARN + "/policy-ns/policy-table/policy"

	// Put table policy
	rec := doS3TablesRequest(t, h, http.MethodPut, path, map[string]any{"resourcePolicy": policy})
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get table policy
	rec = doS3TablesRequest(t, h, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete table policy
	rec = doS3TablesRequest(t, h, http.MethodDelete, path, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_MaintenanceConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		method     string
		pathType   string // "bucket_get", "bucket_put", "table_get", "table_put"
		wantStatus int
	}{
		{
			name:       "get_bucket_maintenance",
			method:     http.MethodGet,
			pathType:   "bucket_get",
			wantStatus: http.StatusOK,
		},
		{
			name:     "put_bucket_maintenance",
			method:   http.MethodPut,
			pathType: "bucket_put",
			body: map[string]any{
				"value": map[string]any{"status": "enabled"},
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "get_table_maintenance",
			method:     http.MethodGet,
			pathType:   "table_get",
			wantStatus: http.StatusOK,
		},
		{
			name:     "put_table_maintenance",
			method:   http.MethodPut,
			pathType: "table_put",
			body: map[string]any{
				"value": map[string]any{"status": "enabled"},
			},
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bucketARN := createBucketHelper(t, h, "maint-bucket-"+tt.name)
			encodedARN := url.PathEscape(bucketARN)
			createNamespaceHelper(t, h, bucketARN, []string{"maint-ns"})
			_ = createTableHelper(t, h, bucketARN, "maint-ns", "maint-table")

			var path string

			switch tt.pathType {
			case "bucket_get":
				path = "/buckets/" + encodedARN + "/maintenance"
			case "bucket_put":
				path = "/buckets/" + encodedARN + "/maintenance/icebergUnreferencedFileRemoval"
			case "table_get":
				path = "/tables/" + encodedARN + "/maint-ns/maint-table/maintenance"
			case "table_put":
				path = "/tables/" + encodedARN + "/maint-ns/maint-table/maintenance/icebergCompaction"
			}

			rec := doS3TablesRequest(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_Encryption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		pathType   string // "bucket" or "table"
		wantStatus int
	}{
		{
			name:       "get_bucket_encryption_returns_not_found",
			pathType:   "bucket",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "get_table_encryption_returns_aes256",
			pathType:   "table",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bucketARN := createBucketHelper(t, h, "enc-bucket-"+tt.name)
			encodedARN := url.PathEscape(bucketARN)

			var path string

			if tt.pathType == "bucket" {
				path = "/buckets/" + encodedARN + "/encryption"
			} else {
				createNamespaceHelper(t, h, bucketARN, []string{"enc-ns"})
				_ = createTableHelper(t, h, bucketARN, "enc-ns", "enc-table")
				path = "/tables/" + encodedARN + "/enc-ns/enc-table/encryption"
			}

			rec := doS3TablesRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				result := parseResponse(t, rec)
				encCfg, ok := result["encryptionConfiguration"].(map[string]any)
				require.True(t, ok, "expected encryptionConfiguration to be an object")
				assert.Equal(t, "AES256", encCfg["sseAlgorithm"])
			}
		})
	}
}

func TestHandler_UnknownPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doS3TablesRequest(t, h, http.MethodGet, "/unknown-path", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CreateTable_WithURLEncodedARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "encoded-bucket")
	encodedARN := url.PathEscape(bucketARN)
	createNamespaceHelper(t, h, bucketARN, []string{"encoded-ns"})

	path := "/tables/" + encodedARN + "/encoded-ns"
	rec := doS3TablesRequest(t, h, http.MethodPut, path, map[string]any{
		"name":   "encoded-table",
		"format": "ICEBERG",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)
	assert.NotEmpty(t, result["tableARN"])
}
