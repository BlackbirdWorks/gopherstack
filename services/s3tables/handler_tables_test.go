package s3tables_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
				"name":   "my_table",
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
			body:       map[string]any{"name": "default_format_table"},
			wantStatus: http.StatusOK,
			checkField: "tableARN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bucketARN := createBucketHelper(t, h, "table-create-bucket-"+bucketSuffix(tt.name))
			encodedARN := url.PathEscape(bucketARN)
			createNamespaceHelper(t, h, bucketARN, []string{"test_ns"})

			path := "/tables/" + encodedARN + "/test_ns"
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
					"/get-table?tableBucketARN=%s&namespace=test_ns&name=test_table",
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
					"/get-table?tableBucketARN=%s&namespace=test_ns&name=nope",
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
			pathFn:     func(_, encodedARN string) string { return "/tables/" + encodedARN + "?namespace=test_ns" },
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bucketARN := createBucketHelper(t, h, "get-list-bucket-"+bucketSuffix(tt.name))
			encodedARN := url.PathEscape(bucketARN)
			createNamespaceHelper(t, h, bucketARN, []string{"test_ns"})
			_ = createTableHelper(t, h, bucketARN, "test_ns", "test_table")

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
			bucketARN := createBucketHelper(t, h, "delete-table-bucket-"+bucketSuffix(tt.name))
			encodedARN := url.PathEscape(bucketARN)
			createNamespaceHelper(t, h, bucketARN, []string{"test_ns"})

			if tt.tableExists {
				_ = createTableHelper(t, h, bucketARN, "test_ns", "my_table")
			}

			rec := doS3TablesRequest(t, h, http.MethodDelete, "/tables/"+encodedARN+"/test_ns/my_table", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_Table_Rename(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		createTable   bool
		useStaleToken bool
		wantStatus    int
	}{
		{
			name:        "rename_table",
			body:        map[string]any{"newName": "renamed_table"},
			createTable: true,
			wantStatus:  http.StatusNoContent,
		},
		{
			name:          "reject_stale_version_token",
			body:          map[string]any{"newName": "renamed_table"},
			createTable:   true,
			useStaleToken: true,
			wantStatus:    http.StatusConflict,
		},
		{
			name:        "reject_missing_destination_namespace",
			body:        map[string]any{"newNamespaceName": "missing_ns"},
			createTable: true,
			wantStatus:  http.StatusNotFound,
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
			bucketARN := createBucketHelper(t, h, "rename-bucket-"+bucketSuffix(tt.name))
			encodedARN := url.PathEscape(bucketARN)
			createNamespaceHelper(t, h, bucketARN, []string{"rename_ns"})

			tableName := "orig_table"
			if tt.createTable {
				_ = createTableHelper(t, h, bucketARN, "rename_ns", tableName)
				table := getTableHelper(t, h, bucketARN, "rename_ns", tableName)
				tt.body["versionToken"] = table["versionToken"]
			} else {
				tableName = "not_exist"
			}

			if tt.useStaleToken {
				tt.body["versionToken"] = "stale-version-token"
			}

			path := "/tables/" + encodedARN + "/rename_ns/" + tableName + "/rename"
			rec := doS3TablesRequest(t, h, http.MethodPut, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_Table_UpdateMetadataLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		metadataLocation string
		useStaleToken    bool
		createTable      bool
		wantStatus       int
	}{
		{
			name:             "update_metadata_location",
			metadataLocation: "s3://meta-bucket-update_metadata_location/meta-ns/meta-table/v1.metadata.json",
			createTable:      true,
			wantStatus:       http.StatusOK,
		},
		{
			name:             "reject_stale_version_token",
			metadataLocation: "s3://meta-bucket-reject_stale_version_token/meta-ns/meta-table/v1.metadata.json.gz",
			useStaleToken:    true,
			createTable:      true,
			wantStatus:       http.StatusConflict,
		},
		{
			name:             "table_not_found",
			metadataLocation: "s3://meta-bucket-table_not_found/meta-ns/not-exist/v1.metadata.json",
			wantStatus:       http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bucketARN := createBucketHelper(t, h, "meta-bucket-"+bucketSuffix(tt.name))
			encodedARN := url.PathEscape(bucketARN)
			createNamespaceHelper(t, h, bucketARN, []string{"meta_ns"})

			tableName := "meta_table"
			versionToken := "missing-table-token"
			if tt.createTable {
				_ = createTableHelper(t, h, bucketARN, "meta_ns", tableName)
				table := getTableHelper(t, h, bucketARN, "meta_ns", tableName)
				versionToken = table["versionToken"].(string)
			} else {
				tableName = "not_exist"
			}

			if tt.useStaleToken {
				versionToken = "stale-version-token"
			}

			path := "/tables/" + encodedARN + "/meta_ns/" + tableName + "/metadata-location"
			rec := doS3TablesRequest(t, h, http.MethodPut, path, map[string]any{
				"metadataLocation": tt.metadataLocation,
				"versionToken":     versionToken,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_TablePolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "table-policy-bucket")
	encodedARN := url.PathEscape(bucketARN)
	createNamespaceHelper(t, h, bucketARN, []string{"policy_ns"})
	_ = createTableHelper(t, h, bucketARN, "policy_ns", "policy_table")
	policy := `{"Version":"2012-10-17","Statement":[]}`
	path := "/tables/" + encodedARN + "/policy_ns/policy_table/policy"

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

func TestHandler_CreateTable_WithURLEncodedARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "encoded-bucket")
	encodedARN := url.PathEscape(bucketARN)
	createNamespaceHelper(t, h, bucketARN, []string{"encoded_ns"})

	path := "/tables/" + encodedARN + "/encoded_ns"
	rec := doS3TablesRequest(t, h, http.MethodPut, path, map[string]any{
		"name":   "encoded_table",
		"format": "ICEBERG",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)
	assert.NotEmpty(t, result["tableARN"])
}

// ----------------------------------------
// GetTable via tableArn alone (real GetTableInput accepts either tableArn
// OR the tableBucketARN+namespace+name triple -- see api_op_GetTable.go).
// ----------------------------------------

func TestHandler_GetTable_ByTableArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "get-table-arn-bucket")
	createNamespaceHelper(t, h, bucketARN, []string{"ns1"})
	tableARN := createTableHelper(t, h, bucketARN, "ns1", "tbl")

	q := url.Values{}
	q.Set("tableArn", tableARN)
	rec := doS3TablesRequest(t, h, http.MethodGet, "/get-table?"+q.Encode(), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)
	assert.Equal(t, "tbl", result["name"])
	assert.Equal(t, tableARN, result["tableARN"])
}

func TestHandler_GetTable_MissingAllIdentifiers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doS3TablesRequest(t, h, http.MethodGet, "/get-table", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ----------------------------------------
// CreateTableBucket / CreateTable honor encryptionConfiguration,
// storageClassConfiguration, and tags from the request body instead of
// silently discarding them.
// ----------------------------------------

func TestHandler_CreateTable_AppliesEncryptionStorageClassAndTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "wire-table-opts-bucket")
	createNamespaceHelper(t, h, bucketARN, []string{"ns1"})
	encodedARN := url.PathEscape(bucketARN)

	rec := doS3TablesRequest(t, h, http.MethodPut, "/tables/"+encodedARN+"/ns1", map[string]any{
		"name":   "opts_table",
		"format": "ICEBERG",
		"encryptionConfiguration": map[string]any{
			"sseAlgorithm": "aws:kms",
			"kmsKeyArn":    "arn:aws:kms:us-east-1:000000000000:key/tbl",
		},
		"tags": map[string]any{"team": "data"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)
	tableARN, ok := result["tableARN"].(string)
	require.True(t, ok)

	encRec := doS3TablesRequest(t, h, http.MethodGet, "/tables/"+encodedARN+"/ns1/opts_table/encryption", nil)
	require.Equal(t, http.StatusOK, encRec.Code)
	encResult := parseResponse(t, encRec)
	encCfg, ok := encResult["encryptionConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "aws:kms", encCfg["sseAlgorithm"])

	tagRec := doS3TablesRequest(t, h, http.MethodGet, "/tag/"+url.PathEscape(tableARN), nil)
	require.Equal(t, http.StatusOK, tagRec.Code)
	tagResult := parseResponse(t, tagRec)
	tags, ok := tagResult["tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "data", tags["team"])
}

// ----------------------------------------
// List* pagination: continuationToken/maxX/prefix are honored, not silently
// ignored -- previously every List op always returned every resource
// regardless of maxBuckets/maxNamespaces/maxTables.
// ----------------------------------------

func TestHandler_ListTables_MaxTablesLimitsPage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "wire-tbl-page-bucket")
	createNamespaceHelper(t, h, bucketARN, []string{"ns1"})

	for _, name := range []string{"a", "b", "c"} {
		createTableHelper(t, h, bucketARN, "ns1", name)
	}

	encodedARN := url.PathEscape(bucketARN)
	rec := doS3TablesRequest(t, h, http.MethodGet, "/tables/"+encodedARN+"?maxTables=1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)
	tables, ok := result["tables"].([]any)
	require.True(t, ok)
	assert.Len(t, tables, 1)
	assert.Contains(t, result, keyContinuationTokenTestKey)
}

func TestHandler_CreateTableResponseUsesLowercaseTableArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "arn-casing-bucket")
	createNamespaceHelper(t, h, bucketARN, []string{"ns1"})
	encodedARN := url.PathEscape(bucketARN)

	rec := doS3TablesRequest(t, h, http.MethodPut,
		"/tables/"+encodedARN+"/ns1",
		map[string]any{"name": "tbl", "format": "ICEBERG"})
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)

	_, hasCorrect := result["tableARN"]
	_, hasWrong := result["tableArn"]

	assert.True(t, hasCorrect, "response must contain 'tableARN' (uppercase, per SDK)")
	assert.False(t, hasWrong, "response must not use lowercase 'tableArn'")
}

func TestHandler_GetTableResponseUsesLowercaseArns(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "get-table-arn-bucket")
	createNamespaceHelper(t, h, bucketARN, []string{"ns1"})
	createTableHelper(t, h, bucketARN, "ns1", "tbl")

	q := url.Values{}
	q.Set("tableBucketARN", bucketARN)
	q.Set("namespace", "ns1")
	q.Set("name", "tbl")
	rec := doS3TablesRequest(t, h, http.MethodGet, "/get-table?"+q.Encode(), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)

	_, hasTableARN := result["tableARN"]
	_, hasWrongTableArn := result["tableArn"]
	_, hasFabricatedBucketARN := result["tableBucketARN"]

	assert.True(t, hasTableARN, "GetTable response must include 'tableARN'")
	assert.False(t, hasWrongTableArn, "GetTable must not use lowercase 'tableArn'")
	// GetTableOutput has no tableBucketARN member at all -- its
	// bucket-identifying field is the real, system-assigned tableBucketId
	// (gopherstack-wla0; confirmed via
	// awsRestjson1_deserializeOpDocumentGetTableOutput in
	// aws-sdk-go-v2/service/s3tables@v1.18.4's deserializers.go).
	assert.False(t, hasFabricatedBucketARN, "GetTableOutput has no tableBucketARN member on the real API")
	assert.Equal(t, getTableBucketIDHelper(t, h, bucketARN), result["tableBucketId"],
		"GetTable response must include the real tableBucketId")
}

func TestHandler_ListTablesResponseUsesLowercaseArns(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "list-tables-arn-bucket")
	createNamespaceHelper(t, h, bucketARN, []string{"ns1"})
	tableARN := createTableHelper(t, h, bucketARN, "ns1", "tbl")
	_ = tableARN

	encodedARN := url.PathEscape(bucketARN)
	rec := doS3TablesRequest(t, h, http.MethodGet, "/tables/"+encodedARN+"?namespace=ns1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tables, ok := out["tables"].([]any)
	require.True(t, ok)
	require.Len(t, tables, 1)

	entry := tables[0].(map[string]any)
	_, hasTableARN := entry["tableARN"]
	_, hasWrong := entry["tableArn"]
	_, hasFabricatedBucketARN := entry["tableBucketARN"]

	assert.True(t, hasTableARN, "ListTables entry must include 'tableARN'")
	assert.False(t, hasWrong, "ListTables entry must not use lowercase 'tableArn'")
	// TableSummary has no tableBucketARN member either -- same fix as
	// GetTable above (gopherstack-wla0).
	assert.False(t, hasFabricatedBucketARN, "TableSummary has no tableBucketARN member on the real API")
	assert.Equal(t, getTableBucketIDHelper(t, h, bucketARN), entry["tableBucketId"],
		"ListTables entry must include the real tableBucketId")
}
