package timestreamwrite_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

// assertInt64Field asserts that a field in a map[string]any equals the expected
// int64 value, converting from the JSON-unmarshalled float64 representation.
func assertInt64Field(t *testing.T, m map[string]any, key string, expected int64) {
	t.Helper()

	raw, ok := m[key]
	require.True(t, ok, "field %q missing from map", key)
	assert.Equal(t, expected, int64(raw.(float64)))
}

func TestHandler_CreateTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mydb"})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]string{"DatabaseName": "mydb", "TableName": "tbl"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing table name",
			body:       map[string]string{"DatabaseName": "mydb"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "database not found",
			body:       map[string]string{"DatabaseName": "missing", "TableName": "tbl"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := doRequest(t, h, "CreateTable", tt.body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_DescribeTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mydb"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateTable", map[string]string{"DatabaseName": "mydb", "TableName": "tbl"})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]string{"DatabaseName": "mydb", "TableName": "tbl"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			body:       map[string]string{"DatabaseName": "mydb", "TableName": "missing"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := doRequest(t, h, "DescribeTable", tt.body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_ListTables(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mydb"})
	require.Equal(t, http.StatusOK, rec.Code)

	for _, name := range []string{"t1", "t2"} {
		rec = doRequest(t, h, "CreateTable", map[string]string{"DatabaseName": "mydb", "TableName": name})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec = doRequest(t, h, "ListTables", map[string]string{"DatabaseName": "mydb"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	tbls, ok := resp["Tables"].([]any)
	assert.True(t, ok)
	assert.Len(t, tbls, 2)
}

// TestHandler_ListTables_DatabaseNameOptional verifies a request with no
// DatabaseName lists tables across every database. ListTablesInput marks no
// member required, DatabaseName included (api_op_ListTables.go,
// timestreamwrite@v1.38.4). A prior version of this test
// (TestHandler_ListTables_MissingDBName) asserted a 400 here, which was
// itself wrong: gopherstack-4ly2.
func TestHandler_ListTables_DatabaseNameOptional(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, db := range []string{"db-a", "db-b"} {
		rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": db})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "CreateTable", map[string]string{"DatabaseName": "db-a", "TableName": "t1"})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doRequest(t, h, "CreateTable", map[string]string{"DatabaseName": "db-b", "TableName": "t2"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListTables", map[string]string{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	tbls, ok := resp["Tables"].([]any)
	require.True(t, ok)
	assert.Len(t, tbls, 2)
}

func TestHandler_DeleteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mydb"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateTable", map[string]string{"DatabaseName": "mydb", "TableName": "tbl"})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]string{"DatabaseName": "mydb", "TableName": "tbl"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			body:       map[string]string{"DatabaseName": "mydb", "TableName": "missing"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing names",
			body:       map[string]string{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := doRequest(t, h, "DeleteTable", tt.body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_UpdateTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mydb"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateTable", map[string]string{"DatabaseName": "mydb", "TableName": "tbl"})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]string{"DatabaseName": "mydb", "TableName": "tbl"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			body:       map[string]string{"DatabaseName": "mydb", "TableName": "missing"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing names",
			body:       map[string]string{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := doRequest(t, h, "UpdateTable", tt.body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

// TestHandler_ListTables_Sorted verifies tables are sorted by name.
func TestHandler_ListTables_Sorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "srt-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "srt-db", "TableName": "ztbl"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "srt-db", "TableName": "atbl"})

	rec := doRequest(t, h, "ListTables", map[string]any{"DatabaseName": "srt-db"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbls := out["Tables"].([]any)
	require.Len(t, tbls, 2)
	assert.Equal(t, "atbl", tbls[0].(map[string]any)["TableName"])
	assert.Equal(t, "ztbl", tbls[1].(map[string]any)["TableName"])
}

// TestHandler_ListTables_Pagination verifies NextToken pagination for ListTables.
func TestHandler_ListTables_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "pg-db"})

	for _, name := range []string{"t1", "t2", "t3"} {
		doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "pg-db", "TableName": name})
	}

	rec1 := doRequest(t, h, "ListTables", map[string]any{"DatabaseName": "pg-db", "MaxResults": 2})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))

	tbls1 := out1["Tables"].([]any)
	assert.Len(t, tbls1, 2)

	nextToken := out1["NextToken"].(string)
	assert.NotEmpty(t, nextToken)

	rec2 := doRequest(t, h, "ListTables", map[string]any{
		"DatabaseName": "pg-db",
		"MaxResults":   2,
		"NextToken":    nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))

	tbls2 := out2["Tables"].([]any)
	assert.Len(t, tbls2, 1)
	assert.Empty(t, out2["NextToken"])
}

// TestHandler_CreateTable_WithSchema verifies that a composite partition key
// schema is stored and returned by CreateTable per the AWS API.
func TestHandler_CreateTable_WithSchema(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "schema-db"})

	rec := doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "schema-db",
		"TableName":    "schema-tbl",
		"Schema": map[string]any{
			"CompositePartitionKey": []map[string]any{
				{"Type": "DIMENSION", "Name": "region", "EnforcementInRecord": "REQUIRED"},
				{"Type": "MEASURE", "EnforcementInRecord": "OPTIONAL"},
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	schema, ok := tbl["Schema"].(map[string]any)
	require.True(t, ok, "Schema should be present in response")

	keys := schema["CompositePartitionKey"].([]any)
	require.Len(t, keys, 2)

	first := keys[0].(map[string]any)
	assert.Equal(t, "DIMENSION", first["Type"])
	assert.Equal(t, "region", first["Name"])
	assert.Equal(t, "REQUIRED", first["EnforcementInRecord"])
}

// TestHandler_DescribeTable_ReturnsSchema verifies Schema survives DescribeTable.
func TestHandler_DescribeTable_ReturnsSchema(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "desc-schema-db"})
	doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "desc-schema-db",
		"TableName":    "desc-schema-tbl",
		"Schema": map[string]any{
			"CompositePartitionKey": []map[string]any{
				{"Type": "MEASURE"},
			},
		},
	})

	rec := doRequest(t, h, "DescribeTable", map[string]any{
		"DatabaseName": "desc-schema-db",
		"TableName":    "desc-schema-tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	schema, ok := tbl["Schema"].(map[string]any)
	require.True(t, ok, "Schema should be present in DescribeTable response")
	keys := schema["CompositePartitionKey"].([]any)
	assert.Len(t, keys, 1)
	assert.Equal(t, "MEASURE", keys[0].(map[string]any)["Type"])
}

// TestHandler_UpdateTable_WithSchema verifies Schema can be set via UpdateTable.
func TestHandler_UpdateTable_WithSchema(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "upd-schema-db"})
	doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "upd-schema-db",
		"TableName":    "upd-schema-tbl",
	})

	rec := doRequest(t, h, "UpdateTable", map[string]any{
		"DatabaseName": "upd-schema-db",
		"TableName":    "upd-schema-tbl",
		"Schema": map[string]any{
			"CompositePartitionKey": []map[string]any{
				{"Type": "DIMENSION", "Name": "account_id", "EnforcementInRecord": "REQUIRED"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	schema := tbl["Schema"].(map[string]any)
	keys := schema["CompositePartitionKey"].([]any)
	require.Len(t, keys, 1)
	assert.Equal(t, "account_id", keys[0].(map[string]any)["Name"])
}

// TestHandler_ListTables_IncludesSchema verifies Schema appears in ListTables output.
func TestHandler_ListTables_IncludesSchema(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "ls-schema-db"})
	doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "ls-schema-db",
		"TableName":    "ls-schema-tbl",
		"Schema": map[string]any{
			"CompositePartitionKey": []map[string]any{
				{"Type": "DIMENSION", "Name": "host"},
			},
		},
	})

	rec := doRequest(t, h, "ListTables", map[string]string{"DatabaseName": "ls-schema-db"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tables := out["Tables"].([]any)
	require.Len(t, tables, 1)

	schema := tables[0].(map[string]any)["Schema"].(map[string]any)
	keys := schema["CompositePartitionKey"].([]any)
	assert.Equal(t, "host", keys[0].(map[string]any)["Name"])
}

// TestHandler_CreateTable_MagneticStoreRejectedDataLocation verifies S3
// config for rejected records is stored and returned in the table view.
func TestHandler_CreateTable_MagneticStoreRejectedDataLocation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "msrdl-db"})

	rec := doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "msrdl-db",
		"TableName":    "msrdl-tbl",
		"MagneticStoreWriteProperties": map[string]any{
			"EnableMagneticStoreWrites": true,
			"MagneticStoreRejectedDataLocation": map[string]any{
				"S3Configuration": map[string]any{
					"BucketName":       "rejected-records-bucket",
					"ObjectKeyPrefix":  "errors/",
					"EncryptionOption": "SSE_S3",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	mswp := tbl["MagneticStoreWriteProperties"].(map[string]any)
	assert.True(t, mswp["EnableMagneticStoreWrites"].(bool))

	loc := mswp["MagneticStoreRejectedDataLocation"].(map[string]any)
	s3cfg := loc["S3Configuration"].(map[string]any)
	assert.Equal(t, "rejected-records-bucket", s3cfg["BucketName"])
	assert.Equal(t, "errors/", s3cfg["ObjectKeyPrefix"])
	assert.Equal(t, "SSE_S3", s3cfg["EncryptionOption"])
}

// TestHandler_DescribeTable_MagneticStoreRejectedDataLocationRoundTrip
// verifies MagneticStoreRejectedDataLocation survives DescribeTable.
func TestHandler_DescribeTable_MagneticStoreRejectedDataLocationRoundTrip(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	h := timestreamwrite.NewHandler(b)

	_, err := b.CreateDatabase("msrdl2-db", "", nil)
	require.NoError(t, err)
	_, err = b.CreateTable("msrdl2-db", "msrdl2-tbl", nil, &timestreamwrite.CreateTableInput{
		MagneticStoreWriteProperties: &timestreamwrite.MagneticStoreWriteProperties{
			EnableMagneticStoreWrites: true,
			MagneticStoreRejectedDataLocation: &timestreamwrite.MagneticStoreRejectedDataLocation{
				S3Configuration: &timestreamwrite.S3Configuration{
					BucketName: "err-bucket",
					KmsKeyID:   "arn:aws:kms:us-east-1:000000000000:key/abc",
				},
			},
		},
	})
	require.NoError(t, err)

	rec := doRequest(t, h, "DescribeTable", map[string]any{
		"DatabaseName": "msrdl2-db",
		"TableName":    "msrdl2-tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	mswp := tbl["MagneticStoreWriteProperties"].(map[string]any)
	loc := mswp["MagneticStoreRejectedDataLocation"].(map[string]any)
	s3cfg := loc["S3Configuration"].(map[string]any)
	assert.Equal(t, "err-bucket", s3cfg["BucketName"])
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/abc", s3cfg["KmsKeyId"])
}

// TestHandler_CreateTable_NameFormatValidation verifies that CreateTable
// rejects TableName values with invalid characters or excessive length.
func TestHandler_CreateTable_NameFormatValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tableName  string
		wantStatus int
	}{
		{
			name:       "valid name",
			tableName:  "metrics-tbl",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid with underscores",
			tableName:  "cpu_metrics",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid with dots",
			tableName:  "app.metrics.v2",
			wantStatus: http.StatusOK,
		},
		{
			name:       "space in name",
			tableName:  "bad table",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "bang character",
			tableName:  "bad!table",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "hash character",
			tableName:  "bad#table",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "name exactly at max length (256 chars)",
			tableName:  strings.Repeat("x", 256),
			wantStatus: http.StatusOK,
		},
		{
			name:       "name exceeds max length (257 chars)",
			tableName:  strings.Repeat("x", 257),
			wantStatus: http.StatusBadRequest,
		},
	}

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "namecheck-db"})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "CreateTable", map[string]any{
				"DatabaseName": "namecheck-db",
				"TableName":    tt.tableName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "TableName=%q", tt.tableName)

			if tt.wantStatus == http.StatusBadRequest {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "ValidationException", body["__type"])
			}
		})
	}
}

// TestHandler_UpdateTable_NameLookupAcceptsExistingNames verifies that
// UpdateTable's identity lookup still accepts known table names (the name
// format check applies to new tables only).
func TestHandler_UpdateTable_NameLookupAcceptsExistingNames(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "lookup-db"})
	doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "lookup-db",
		"TableName":    "lookup-tbl",
	})

	// UpdateTable with a valid table name should succeed.
	rec := doRequest(t, h, "UpdateTable", map[string]any{
		"DatabaseName": "lookup-db",
		"TableName":    "lookup-tbl",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandler_CreateTable_RetentionPropertiesRangeValidation verifies that
// CreateTable rejects out-of-range retention period values per the AWS API.
func TestHandler_CreateTable_RetentionPropertiesRangeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		memoryHours  int64
		magneticDays int64
		wantStatus   int
	}{
		{
			name:         "valid min values",
			memoryHours:  1,
			magneticDays: 1,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "valid max values",
			memoryHours:  8766,
			magneticDays: 73000,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "valid mid-range",
			memoryHours:  24,
			magneticDays: 365,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "memory hours too low (0)",
			memoryHours:  0,
			magneticDays: 365,
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "memory hours too high (8767)",
			memoryHours:  8767,
			magneticDays: 365,
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "magnetic days too low (0)",
			memoryHours:  24,
			magneticDays: 0,
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "magnetic days too high (73001)",
			memoryHours:  24,
			magneticDays: 73001,
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "both out of range",
			memoryHours:  0,
			magneticDays: 0,
			wantStatus:   http.StatusBadRequest,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			dbName := fmt.Sprintf("ret-db-%d", i)
			tblName := fmt.Sprintf("ret-tbl-%d", i)

			doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": dbName})

			rec := doRequest(t, h, "CreateTable", map[string]any{
				"DatabaseName": dbName,
				"TableName":    tblName,
				"RetentionProperties": map[string]any{
					"MemoryStoreRetentionPeriodInHours":  tt.memoryHours,
					"MagneticStoreRetentionPeriodInDays": tt.magneticDays,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code,
				"memory=%d magnetic=%d", tt.memoryHours, tt.magneticDays)

			if tt.wantStatus == http.StatusBadRequest {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "ValidationException", body["__type"])
			}
		})
	}
}

// TestHandler_UpdateTable_RetentionPropertiesRangeValidation verifies that
// UpdateTable also enforces retention range constraints.
func TestHandler_UpdateTable_RetentionPropertiesRangeValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "upd-ret-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "upd-ret-db", "TableName": "upd-ret-tbl"})

	tests := []struct {
		name         string
		memoryHours  int64
		magneticDays int64
		wantStatus   int
	}{
		{
			name:         "valid update",
			memoryHours:  48,
			magneticDays: 730,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "invalid memory hours on update",
			memoryHours:  9999,
			magneticDays: 30,
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "invalid magnetic days on update",
			memoryHours:  24,
			magneticDays: 99999,
			wantStatus:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "UpdateTable", map[string]any{
				"DatabaseName": "upd-ret-db",
				"TableName":    "upd-ret-tbl",
				"RetentionProperties": map[string]any{
					"MemoryStoreRetentionPeriodInHours":  tt.memoryHours,
					"MagneticStoreRetentionPeriodInDays": tt.magneticDays,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_CreateTable_PartitionKeyTypeValidation verifies that the
// handler rejects invalid PartitionKey configurations per AWS rules.
func TestHandler_CreateTable_PartitionKeyTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		key        map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "DIMENSION with Name is valid",
			key:        map[string]any{"Type": "DIMENSION", "Name": "region", "EnforcementInRecord": "REQUIRED"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "MEASURE without Name is valid",
			key:        map[string]any{"Type": "MEASURE"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "MEASURE with EnforcementInRecord is valid",
			key:        map[string]any{"Type": "MEASURE", "EnforcementInRecord": "OPTIONAL"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "DIMENSION without Name is invalid",
			key:        map[string]any{"Type": "DIMENSION"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "DIMENSION with empty Name is invalid",
			key:        map[string]any{"Type": "DIMENSION", "Name": ""},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unknown type is invalid",
			key:        map[string]any{"Type": "UNKNOWN_TYPE"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing type is invalid",
			key:        map[string]any{"Name": "some-name"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			dbName := fmt.Sprintf("pk-db-%d", i)
			tblName := fmt.Sprintf("pk-tbl-%d", i)

			doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": dbName})

			rec := doRequest(t, h, "CreateTable", map[string]any{
				"DatabaseName": dbName,
				"TableName":    tblName,
				"Schema": map[string]any{
					"CompositePartitionKey": []map[string]any{tt.key},
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "ValidationException", body["__type"])
			}
		})
	}
}

// TestHandler_UpdateTable_PartitionKeyValidation verifies that UpdateTable
// also validates schema partition keys.
func TestHandler_UpdateTable_PartitionKeyValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "pk-upd-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "pk-upd-db", "TableName": "pk-upd-tbl"})

	// DIMENSION without Name should be rejected.
	rec := doRequest(t, h, "UpdateTable", map[string]any{
		"DatabaseName": "pk-upd-db",
		"TableName":    "pk-upd-tbl",
		"Schema": map[string]any{
			"CompositePartitionKey": []map[string]any{
				{"Type": "DIMENSION"}, // missing Name
			},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ValidationException", body["__type"])

	// Valid DIMENSION with Name should succeed.
	rec2 := doRequest(t, h, "UpdateTable", map[string]any{
		"DatabaseName": "pk-upd-db",
		"TableName":    "pk-upd-tbl",
		"Schema": map[string]any{
			"CompositePartitionKey": []map[string]any{
				{"Type": "DIMENSION", "Name": "az"},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// TestHandler_CreateTable_TagValidation verifies that tag validation applies
// on CreateTable as well.
func TestHandler_CreateTable_TagValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "tbl-tag-db"})

	// Create table with an empty tag key should fail.
	rec := doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "tbl-tag-db",
		"TableName":    "tbl-tag-tbl",
		"Tags":         []map[string]string{{"Key": "", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ValidationException", body["__type"])
}

// TestHandler_CreateTable_ConflictException verifies that creating a table
// with a duplicate name in the same database returns HTTP 400 (awsJson1.0
// reports all client faults as 400) with __type=ConflictException.
func TestHandler_CreateTable_ConflictException(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "dup-tbl-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "dup-tbl-db", "TableName": "dup-tbl"})

	rec := doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "dup-tbl-db", "TableName": "dup-tbl"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ConflictException", body["__type"])
}

// TestHandler_CreateTable_StatusActive verifies that newly created tables
// have TableStatus = "ACTIVE" per the AWS API.
func TestHandler_CreateTable_StatusActive(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "status-db"})

	rec := doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "status-db",
		"TableName":    "status-tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	assert.Equal(t, "ACTIVE", tbl["TableStatus"])
}

// TestHandler_DescribeTable_StatusActive verifies that DescribeTable also
// returns ACTIVE status for a newly created table.
func TestHandler_DescribeTable_StatusActive(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "status2-db"})
	doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "status2-db",
		"TableName":    "status2-tbl",
	})

	rec := doRequest(t, h, "DescribeTable", map[string]any{
		"DatabaseName": "status2-db",
		"TableName":    "status2-tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	assert.Equal(t, "ACTIVE", tbl["TableStatus"])
}

// TestHandler_ListTables_StatusActive verifies that ListTables includes the
// ACTIVE status on each returned table.
func TestHandler_ListTables_StatusActive(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "status3-db"})

	for _, name := range []string{"t-a", "t-b", "t-c"} {
		doRequest(t, h, "CreateTable", map[string]any{
			"DatabaseName": "status3-db",
			"TableName":    name,
		})
	}

	rec := doRequest(t, h, "ListTables", map[string]string{"DatabaseName": "status3-db"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tables := out["Tables"].([]any)
	require.Len(t, tables, 3)

	for _, rawTbl := range tables {
		tbl := rawTbl.(map[string]any)
		assert.Equal(t, "ACTIVE", tbl["TableStatus"],
			"all tables should have ACTIVE status in list output")
	}
}

// TestHandler_CreateTable_ARNFormat verifies that the table ARN in
// create/describe responses is correctly formed with both database and table
// name segments.
func TestHandler_CreateTable_ARNFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "arn-tbl-db"})

	rec := doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "arn-tbl-db",
		"TableName":    "arn-tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	arn := tbl["Arn"].(string)

	assert.True(t, strings.HasPrefix(arn, "arn:aws:timestream:"),
		"Table ARN should start with arn:aws:timestream:")
	assert.Contains(t, arn, "database/arn-tbl-db/table/arn-tbl",
		"Table ARN should contain database and table name segments")
}

// TestHandler_CreateTable_TimestampsAreFloats verifies table timestamps are float64.
func TestHandler_CreateTable_TimestampsAreFloats(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "ts-tbl-db"})
	rec := doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "ts-tbl-db",
		"TableName":    "ts-tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	tbl := raw["Table"].(map[string]any)
	ct, ok := tbl["CreationTime"].(float64)
	require.True(t, ok, "table CreationTime should be float64, got %T", tbl["CreationTime"])
	assert.Greater(t, ct, float64(0))
}

// TestHandler_CreateTable_WithTags verifies tags are stored on table creation.
func TestHandler_CreateTable_WithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "tagged-tbl-db"})

	rec := doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "tagged-tbl-db",
		"TableName":    "tagged-tbl",
		"Tags": []map[string]string{
			{"Key": "tier", "Value": "silver"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	arn := tbl["Arn"].(string)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsOut))

	tags := tagsOut["Tags"].([]any)
	assert.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "tier", tag["Key"])
	assert.Equal(t, "silver", tag["Value"])
}

// TestHandler_CreateTable_RetentionProperties verifies that RetentionProperties
// are stored and returned when creating a table.
func TestHandler_CreateTable_RetentionProperties(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "ret-db"})

	rec := doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "ret-db",
		"TableName":    "ret-tbl",
		"RetentionProperties": map[string]any{
			"MemoryStoreRetentionPeriodInHours":  24,
			"MagneticStoreRetentionPeriodInDays": 365,
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	rp := tbl["RetentionProperties"].(map[string]any)
	assertInt64Field(t, rp, "MemoryStoreRetentionPeriodInHours", 24)
	assertInt64Field(t, rp, "MagneticStoreRetentionPeriodInDays", 365)
}

// TestHandler_CreateTable_MagneticStoreWriteProperties verifies that
// MagneticStoreWriteProperties are stored and returned when creating a table.
func TestHandler_CreateTable_MagneticStoreWriteProperties(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mag-db"})

	rec := doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "mag-db",
		"TableName":    "mag-tbl",
		"MagneticStoreWriteProperties": map[string]any{
			"EnableMagneticStoreWrites": true,
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	mswp := tbl["MagneticStoreWriteProperties"].(map[string]any)
	assert.Equal(t, true, mswp["EnableMagneticStoreWrites"])
}

// TestHandler_UpdateTable_RetentionProperties verifies that UpdateTable
// updates RetentionProperties correctly.
func TestHandler_UpdateTable_RetentionProperties(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "upd-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "upd-db", "TableName": "upd-tbl"})

	rec := doRequest(t, h, "UpdateTable", map[string]any{
		"DatabaseName": "upd-db",
		"TableName":    "upd-tbl",
		"RetentionProperties": map[string]any{
			"MemoryStoreRetentionPeriodInHours":  48,
			"MagneticStoreRetentionPeriodInDays": 730,
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	rp := tbl["RetentionProperties"].(map[string]any)
	assertInt64Field(t, rp, "MemoryStoreRetentionPeriodInHours", 48)
	assertInt64Field(t, rp, "MagneticStoreRetentionPeriodInDays", 730)
}

// TestHandler_UpdateTable_MagneticStoreWriteProperties verifies
// MagneticStoreWriteProperties are preserved through UpdateTable.
func TestHandler_UpdateTable_MagneticStoreWriteProperties(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "ms-db"})
	doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "ms-db",
		"TableName":    "ms-tbl",
		"MagneticStoreWriteProperties": map[string]any{
			"EnableMagneticStoreWrites": false,
		},
	})

	rec := doRequest(t, h, "UpdateTable", map[string]any{
		"DatabaseName": "ms-db",
		"TableName":    "ms-tbl",
		"MagneticStoreWriteProperties": map[string]any{
			"EnableMagneticStoreWrites": true,
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	tbl := out["Table"].(map[string]any)
	mswp := tbl["MagneticStoreWriteProperties"].(map[string]any)
	assert.Equal(t, true, mswp["EnableMagneticStoreWrites"])
}

// TestHandler_CreateTable_DefaultRetentionProperties verifies default (6h /
// 73d) and explicit RetentionProperties on CreateTable.
func TestHandler_CreateTable_DefaultRetentionProperties(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name       string
		wantHours  int64
		wantDays   int64
		createBody map[string]any
	}{
		{
			name:       "no_retention_specified_returns_defaults",
			wantHours:  6,
			wantDays:   73,
			createBody: map[string]any{},
		},
		{
			name:      "explicit_retention_overrides_defaults",
			wantHours: 12,
			wantDays:  180,
			createBody: map[string]any{
				"RetentionProperties": map[string]any{
					"MemoryStoreRetentionPeriodInHours":  12,
					"MagneticStoreRetentionPeriodInDays": 180,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "def-ret-db"})

			body := map[string]any{
				"DatabaseName": "def-ret-db",
				"TableName":    "def-ret-tbl",
			}

			maps.Copy(body, tt.createBody)

			rec := doRequest(t, h, "CreateTable", body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			tbl := out["Table"].(map[string]any)
			rp := tbl["RetentionProperties"].(map[string]any)
			assert.InDelta(t, float64(tt.wantHours), rp["MemoryStoreRetentionPeriodInHours"], 0)
			assert.InDelta(t, float64(tt.wantDays), rp["MagneticStoreRetentionPeriodInDays"], 0)
		})
	}
}

// TestHandler_CreateTable_NoPropertiesReturnsDefaults verifies that when no
// RetentionProperties are specified, real AWS defaults are applied (6h / 73d)
// and that MagneticStoreWriteProperties is omitted (not defaulted).
func TestHandler_CreateTable_NoPropertiesReturnsDefaults(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "np-db"})

	rec := doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "np-db",
		"TableName":    "np-tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	rp, hasRP := tbl["RetentionProperties"]
	assert.True(t, hasRP, "RetentionProperties should be set to AWS defaults")

	rpMap, ok := rp.(map[string]any)
	assert.True(t, ok)
	assert.InDelta(t, float64(6), rpMap["MemoryStoreRetentionPeriodInHours"], 0)
	assert.InDelta(t, float64(73), rpMap["MagneticStoreRetentionPeriodInDays"], 0)

	_, hasMSWP := tbl["MagneticStoreWriteProperties"]
	assert.False(t, hasMSWP)
}

// TestHandler_DescribeTable_RetentionAfterUpdate verifies that after an
// UpdateTable call the DescribeTable response reflects updated properties.
func TestHandler_DescribeTable_RetentionAfterUpdate(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "upd2-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "upd2-db", "TableName": "upd2-tbl"})

	doRequest(t, h, "UpdateTable", map[string]any{
		"DatabaseName": "upd2-db",
		"TableName":    "upd2-tbl",
		"RetentionProperties": map[string]any{
			"MemoryStoreRetentionPeriodInHours":  6,
			"MagneticStoreRetentionPeriodInDays": 90,
		},
	})

	rec := doRequest(t, h, "DescribeTable", map[string]any{
		"DatabaseName": "upd2-db",
		"TableName":    "upd2-tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	rp := tbl["RetentionProperties"].(map[string]any)
	assertInt64Field(t, rp, "MemoryStoreRetentionPeriodInHours", 6)
	assertInt64Field(t, rp, "MagneticStoreRetentionPeriodInDays", 90)
}

// TestHandler_ListTables_IncludesRetentionProperties verifies that the
// ListTables response includes RetentionProperties when set.
func TestHandler_ListTables_IncludesRetentionProperties(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "lt-db"})
	doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "lt-db",
		"TableName":    "lt-tbl",
		"RetentionProperties": map[string]any{
			"MemoryStoreRetentionPeriodInHours":  8,
			"MagneticStoreRetentionPeriodInDays": 60,
		},
	})

	rec := doRequest(t, h, "ListTables", map[string]string{"DatabaseName": "lt-db"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tables := out["Tables"].([]any)
	require.Len(t, tables, 1)
	tbl := tables[0].(map[string]any)
	rp := tbl["RetentionProperties"].(map[string]any)
	assertInt64Field(t, rp, "MemoryStoreRetentionPeriodInHours", 8)
}
