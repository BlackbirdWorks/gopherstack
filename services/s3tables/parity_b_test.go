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

// ======================================================================
// Gap 1: ARN key casing — "tableArn" and "tableBucketArn" (not ARN)
// ======================================================================

func TestParity_CreateTableResponseUsesLowercaseTableArn(t *testing.T) {
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

	_, hasCorrect := result["tableArn"]
	_, hasWrong := result["tableARN"]

	assert.True(t, hasCorrect, "response must contain 'tableArn' (lowercase 'rn')")
	assert.False(t, hasWrong, "response must not contain misspelled 'tableARN'")
}

func TestParity_GetTableResponseUsesLowercaseArns(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "get-table-arn-bucket")
	createNamespaceHelper(t, h, bucketARN, []string{"ns1"})
	createTableHelper(t, h, bucketARN, "ns1", "tbl")

	q := url.Values{}
	q.Set("tableBucketArn", bucketARN)
	q.Set("namespace", "ns1")
	q.Set("name", "tbl")
	rec := doS3TablesRequest(t, h, http.MethodGet, "/get-table?"+q.Encode(), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)

	_, hasTableArn := result["tableArn"]
	_, hasTableBucketArn := result["tableBucketArn"]
	_, hasWrongTableARN := result["tableARN"]
	_, hasWrongBucketARN := result["tableBucketARN"]

	assert.True(t, hasTableArn, "GetTable response must include 'tableArn'")
	assert.True(t, hasTableBucketArn, "GetTable response must include 'tableBucketArn'")
	assert.False(t, hasWrongTableARN, "GetTable must not use 'tableARN'")
	assert.False(t, hasWrongBucketARN, "GetTable must not use 'tableBucketARN'")
}

func TestParity_ListTablesResponseUsesLowercaseArns(t *testing.T) {
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
	_, hasTableArn := entry["tableArn"]
	_, hasTableBucketArn := entry["tableBucketArn"]
	_, hasWrong := entry["tableARN"]

	assert.True(t, hasTableArn, "ListTables entry must include 'tableArn'")
	assert.True(t, hasTableBucketArn, "ListTables entry must include 'tableBucketArn'")
	assert.False(t, hasWrong, "ListTables entry must not use 'tableARN'")
}

// ======================================================================
// Gap 2a: GetTableBucket / ListTableBuckets include "type": "customer"
// ======================================================================

func TestParity_GetTableBucketIncludesType(t *testing.T) {
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

func TestParity_ListTableBucketsIncludesType(t *testing.T) {
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

func TestParity_GetNamespaceIncludesTableBucketArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "ns-arn-bucket")
	createNamespaceHelper(t, h, bucketARN, []string{"ns1"})

	encodedARN := url.PathEscape(bucketARN)
	rec := doS3TablesRequest(t, h, http.MethodGet, "/namespaces/"+encodedARN+"/ns1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseResponse(t, rec)
	assert.Equal(t, bucketARN, result["tableBucketArn"],
		"GetNamespace response must include tableBucketArn")
}

func TestParity_ListNamespacesIncludesTableBucketArn(t *testing.T) {
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
	assert.Equal(t, bucketARN, entry["tableBucketArn"],
		"ListNamespaces summary must include tableBucketArn")
}

// ======================================================================
// Gap 3: GetTableBucketReplication wraps destinations in replicationConfiguration
// ======================================================================

func TestParity_GetTableBucketReplicationWrapsDestinations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "repl-wrap-bucket")

	q := url.Values{}
	q.Set("tableBucketArn", bucketARN)

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

func TestParity_PutTableReplicationReturns204(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "put-repl-204-bucket")
	createNamespaceHelper(t, h, bucketARN, []string{"ns1"})
	tableARN := createTableHelper(t, h, bucketARN, "ns1", "tbl")

	q := url.Values{}
	q.Set("tableArn", tableARN)
	rec := doS3TablesRequest(t, h, http.MethodPut, "/table-replication?"+q.Encode(),
		map[string]any{"replicationConfiguration": map[string]any{}})

	assert.Equal(t, http.StatusNoContent, rec.Code,
		"PutTableReplication must return 204 No Content per real AWS API")
	assert.Empty(t, rec.Body.String(), "PutTableReplication response body must be empty")
}
