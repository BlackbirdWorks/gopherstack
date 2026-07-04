package timestreamwrite_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

// ---------------------------------------------------------------------------
// Gap A: DatabaseName format and length validation
// ---------------------------------------------------------------------------

// TestAccuracy2_DatabaseNameFormatValidation verifies that the handler rejects
// DatabaseName values that violate AWS character-set and length constraints.
func TestAccuracy2_DatabaseNameFormatValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		dbName     string
		wantStatus int
	}{
		{
			name:       "valid alphanumeric",
			dbName:     "valid-db-01",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid with underscores and dots",
			dbName:     "my_db.prod",
			wantStatus: http.StatusOK,
		},
		{
			name:       "space in name",
			dbName:     "bad db",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "special chars ampersand",
			dbName:     "bad&db",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "special chars slash",
			dbName:     "bad/db",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "at-sign not allowed",
			dbName:     "bad@db",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "name exactly at max length (64 chars)",
			// 64 lowercase a's
			dbName:     strings.Repeat("a", 64),
			wantStatus: http.StatusOK,
		},
		{
			name: "name exceeds max length (65 chars)",
			// 65 lowercase a's
			dbName:     strings.Repeat("a", 65),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": tt.dbName})
			assert.Equal(t, tt.wantStatus, rec.Code, "DatabaseName=%q", tt.dbName)

			if tt.wantStatus == http.StatusBadRequest {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "ValidationException", body["__type"])
			}
		})
	}
}

// TestAccuracy2_DatabaseNameHTTPErrorBody verifies that a name-validation failure
// returns __type=ValidationException with a descriptive message.
func TestAccuracy2_DatabaseNameHTTPErrorBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateDatabase", map[string]string{
		"DatabaseName": "has space",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ValidationException", body["__type"])
	assert.NotEmpty(t, body["message"])
}

// ---------------------------------------------------------------------------
// Gap B: TableName format and length validation
// ---------------------------------------------------------------------------

// TestAccuracy2_TableNameFormatValidation verifies that CreateTable rejects
// TableName values with invalid characters or excessive length.
func TestAccuracy2_TableNameFormatValidation(t *testing.T) {
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

// TestAccuracy2_TableNameValidationAppliesOnUpdateTable verifies that UpdateTable also
// rejects a request when the table name (looked-up existing) remains valid while the
// format check applies to new tables only. This test verifies that validation of the
// existing table's identity lookup still accepts known table names.
func TestAccuracy2_TableNameLookupAcceptsExistingNames(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Gap C: RetentionProperties range validation
// ---------------------------------------------------------------------------

// TestAccuracy2_RetentionPropertiesRangeValidation verifies that CreateTable and
// UpdateTable reject out-of-range retention period values per the AWS API.
func TestAccuracy2_RetentionPropertiesRangeValidation(t *testing.T) {
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

// TestAccuracy2_RetentionPropertiesUpdateTableRangeValidation verifies that UpdateTable
// also enforces retention range constraints.
func TestAccuracy2_RetentionPropertiesUpdateTableRangeValidation(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Gap D: WriteRecords max 100 records per request
// ---------------------------------------------------------------------------

// TestAccuracy2_WriteRecordsMaxRecordsLimit verifies that the handler enforces the
// AWS limit of 100 records per WriteRecords request.
func TestAccuracy2_WriteRecordsMaxRecordsLimit(t *testing.T) {
	t.Parallel()

	makeRecords := func(n int) []map[string]any {
		records := make([]map[string]any, n)
		for i := range records {
			records[i] = map[string]any{
				"MeasureName":      fmt.Sprintf("metric-%d", i),
				"MeasureValue":     "1.0",
				"MeasureValueType": "DOUBLE",
				"Time":             strconv.FormatInt(1609459200000+int64(i)*1000, 10),
				"TimeUnit":         "MILLISECONDS",
			}
		}

		return records
	}

	tests := []struct {
		name       string
		count      int
		wantStatus int
	}{
		{
			name:       "exactly 100 records (at limit)",
			count:      100,
			wantStatus: http.StatusOK,
		},
		{
			name:       "99 records (below limit)",
			count:      99,
			wantStatus: http.StatusOK,
		},
		{
			name:       "101 records (exceeds limit)",
			count:      101,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "200 records (well over limit)",
			count:      200,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Each sub-test gets its own handler to avoid record conflicts.
			h := newTestHandler(t)
			doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "limit-db"})
			doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "limit-db", "TableName": "limit-tbl"})

			rec := doRequest(t, h, "WriteRecords", map[string]any{
				"DatabaseName": "limit-db",
				"TableName":    "limit-tbl",
				"Records":      makeRecords(tt.count),
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "count=%d", tt.count)

			if tt.wantStatus == http.StatusBadRequest {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "ValidationException", body["__type"])
			}
		})
	}
}

// TestAccuracy2_WriteRecordsEmptyBatch verifies that writing zero records is accepted
// and returns a zero ingestion count.
func TestAccuracy2_WriteRecordsEmptyBatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "empty-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "empty-db", "TableName": "empty-tbl"})

	rec := doRequest(t, h, "WriteRecords", map[string]any{
		"DatabaseName": "empty-db",
		"TableName":    "empty-tbl",
		"Records":      []map[string]any{},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	ingested := out["RecordsIngested"].(map[string]any)
	assert.Equal(t, 0, int(ingested["Total"].(float64)))
	assert.Equal(t, 0, int(ingested["MemoryStore"].(float64)))
}

// ---------------------------------------------------------------------------
// Gap E: PartitionKey type validation
// ---------------------------------------------------------------------------

// TestAccuracy2_PartitionKeyTypeValidation verifies that the handler rejects invalid
// PartitionKey configurations per AWS rules.
func TestAccuracy2_PartitionKeyTypeValidation(t *testing.T) {
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

// TestAccuracy2_PartitionKeyValidationOnUpdateTable verifies that UpdateTable also
// validates schema partition keys.
func TestAccuracy2_PartitionKeyValidationOnUpdateTable(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Gap F: Tag key/value validation
// ---------------------------------------------------------------------------

// TestAccuracy2_TagKeyValidation verifies that empty and oversized tag keys are rejected.
func TestAccuracy2_TagKeyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tags       []map[string]string
		wantStatus int
	}{
		{
			name:       "valid tag",
			tags:       []map[string]string{{"Key": "env", "Value": "prod"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty tag key rejected",
			tags:       []map[string]string{{"Key": "", "Value": "some-value"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "tag key exactly 128 chars",
			tags:       []map[string]string{{"Key": strings.Repeat("k", 128), "Value": "v"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "tag key 129 chars rejected",
			tags:       []map[string]string{{"Key": strings.Repeat("k", 129), "Value": "v"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "tag value exactly 256 chars",
			tags:       []map[string]string{{"Key": "k", "Value": strings.Repeat("v", 256)}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "tag value 257 chars rejected",
			tags:       []map[string]string{{"Key": "k", "Value": strings.Repeat("v", 257)}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty tag value is valid",
			tags:       []map[string]string{{"Key": "k", "Value": ""}},
			wantStatus: http.StatusOK,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			dbName := fmt.Sprintf("tagval-db-%d", i)

			rec := doRequest(t, h, "CreateDatabase", map[string]any{
				"DatabaseName": dbName,
				"Tags":         tt.tags,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "case: %s", tt.name)

			if tt.wantStatus == http.StatusBadRequest {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "ValidationException", body["__type"])
			}
		})
	}
}

// TestAccuracy2_TagResourceKeyValidation verifies that TagResource validates tag keys.
func TestAccuracy2_TagResourceKeyValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "tagres-db"})

	arn := "arn:aws:timestream:us-east-1:000000000000:database/tagres-db"

	tests := []struct {
		name       string
		tags       []map[string]string
		wantStatus int
	}{
		{
			name:       "valid tags",
			tags:       []map[string]string{{"Key": "team", "Value": "platform"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty key rejected",
			tags:       []map[string]string{{"Key": "", "Value": "v"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "oversized key rejected",
			tags:       []map[string]string{{"Key": strings.Repeat("x", 129), "Value": "v"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "oversized value rejected",
			tags:       []map[string]string{{"Key": "k", "Value": strings.Repeat("v", 257)}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "TagResource", map[string]any{
				"ResourceARN": arn,
				"Tags":        tt.tags,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAccuracy2_CreateTableTagValidation verifies that tag validation applies on
// CreateTable as well.
func TestAccuracy2_CreateTableTagValidation(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Gap G: KmsKeyId on CreateDatabase
// ---------------------------------------------------------------------------

// TestAccuracy2_CreateDatabaseWithKmsKeyId verifies that a KmsKeyId provided at
// database creation time is stored and reflected in DescribeDatabase.
func TestAccuracy2_CreateDatabaseWithKmsKeyId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	kmsKey := "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012"

	rec := doRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseName": "kms-db",
		"KmsKeyId":     kmsKey,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))

	db := createOut["Database"].(map[string]any)
	assert.Equal(t, kmsKey, db["KmsKeyId"])
}

// TestAccuracy2_CreateDatabaseKmsKeyRoundTrip verifies that a KmsKeyId provided at
// creation time is preserved through a DescribeDatabase call.
func TestAccuracy2_CreateDatabaseKmsKeyRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	kmsKey := "arn:aws:kms:us-east-1:000000000000:key/abcdef00-dead-beef-cafe-000000000000"

	doRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseName": "kms-rt-db",
		"KmsKeyId":     kmsKey,
	})

	rec := doRequest(t, h, "DescribeDatabase", map[string]string{"DatabaseName": "kms-rt-db"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	db := out["Database"].(map[string]any)
	assert.Equal(t, kmsKey, db["KmsKeyId"], "KmsKeyId should survive DescribeDatabase")
}

// TestAccuracy2_CreateDatabaseKmsKeyInListDatabases verifies that a KmsKeyId
// provided at creation time is present in ListDatabases output.
func TestAccuracy2_CreateDatabaseKmsKeyInListDatabases(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	kmsKey := "arn:aws:kms:us-east-1:000000000000:key/99999999-0000-0000-0000-000000000000"

	doRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseName": "kms-list-db",
		"KmsKeyId":     kmsKey,
	})

	rec := doRequest(t, h, "ListDatabases", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	dbs := out["Databases"].([]any)
	require.Len(t, dbs, 1)

	db := dbs[0].(map[string]any)
	assert.Equal(t, kmsKey, db["KmsKeyId"])
}

// TestAccuracy2_CreateDatabaseWithoutKmsKeyIdOmitsField verifies that when no
// KmsKeyId is provided, the field is absent from the response (not empty string).
func TestAccuracy2_CreateDatabaseWithoutKmsKeyIdOmitsField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "no-kms-db"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Parse raw to check presence/absence of key.
	body := rec.Body.String()
	// KmsKeyId should be absent (omitempty) when not set.
	assert.NotContains(t, body, `"KmsKeyId":""`, "empty KmsKeyId should be omitted")
}

// ---------------------------------------------------------------------------
// Gap H: DeleteDatabase requires empty database
// ---------------------------------------------------------------------------

// TestAccuracy2_DeleteDatabaseRequiresEmpty verifies that deleting a database that
// still contains tables returns a ValidationException, matching AWS API behaviour.
func TestAccuracy2_DeleteDatabaseRequiresEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createTbls []string
		wantStatus int
	}{
		{
			name:       "empty database can be deleted",
			createTbls: nil,
			wantStatus: http.StatusOK,
		},
		{
			name:       "database with one table cannot be deleted",
			createTbls: []string{"tbl"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "database with multiple tables cannot be deleted",
			createTbls: []string{"tbl-a", "tbl-b", "tbl-c"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			dbName := fmt.Sprintf("del-req-db-%d", i)

			doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": dbName})

			for _, tblName := range tt.createTbls {
				doRequest(t, h, "CreateTable", map[string]any{
					"DatabaseName": dbName,
					"TableName":    tblName,
				})
			}

			rec := doRequest(t, h, "DeleteDatabase", map[string]string{"DatabaseName": dbName})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "ValidationException", body["__type"])
			}
		})
	}
}

// TestAccuracy2_DeleteDatabaseAfterDeletingTables verifies the happy path: if all
// tables are deleted first, then the database can be deleted.
func TestAccuracy2_DeleteDatabaseAfterDeletingTables(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "del-after-db"})
	doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "del-after-db",
		"TableName":    "del-after-tbl",
	})

	// Deleting database with a table fails.
	rec := doRequest(t, h, "DeleteDatabase", map[string]string{"DatabaseName": "del-after-db"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Delete the table first.
	rec = doRequest(t, h, "DeleteTable", map[string]any{
		"DatabaseName": "del-after-db",
		"TableName":    "del-after-tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Now database can be deleted.
	rec = doRequest(t, h, "DeleteDatabase", map[string]string{"DatabaseName": "del-after-db"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Confirm it's gone.
	rec = doRequest(t, h, "DescribeDatabase", map[string]string{"DatabaseName": "del-after-db"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Gap I: MagneticStore routing in WriteRecords
// ---------------------------------------------------------------------------

// TestAccuracy2_WriteRecordsMagneticStoreRouting verifies that records whose
// timestamps fall outside the memory retention window are counted toward the
// magnetic store (when EnableMagneticStoreWrites is true).
func TestAccuracy2_WriteRecordsMagneticStoreRouting(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("mag-route-db", nil)
	require.NoError(t, err)

	// Create a table with 1-hour memory retention and magnetic store writes enabled.
	_, err = b.CreateTable("mag-route-db", "mag-route-tbl", nil, &timestreamwrite.CreateTableInput{
		RetentionProperties: &timestreamwrite.RetentionProperties{
			MemoryStoreRetentionPeriodInHours:  1,
			MagneticStoreRetentionPeriodInDays: 365,
		},
		MagneticStoreWriteProperties: &timestreamwrite.MagneticStoreWriteProperties{
			EnableMagneticStoreWrites: true,
		},
	})
	require.NoError(t, err)

	// A timestamp from 2 hours ago, well outside the 1-hour retention window.
	twoHoursAgo := time.Now().UTC().Add(-2 * time.Hour)
	oldTS := strconv.FormatInt(twoHoursAgo.UnixMilli(), 10)

	out, err := b.WriteRecords("mag-route-db", "mag-route-tbl", []timestreamwrite.Record{
		{
			MeasureName:      "cpu",
			MeasureValue:     "55.0",
			MeasureValueType: "DOUBLE",
			Time:             oldTS,
			TimeUnit:         "MILLISECONDS",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), out.Total)
	assert.Equal(t, int32(0), out.MemoryStore, "old record should go to magnetic store")
	assert.Equal(t, int32(1), out.MagneticStore, "old record should be counted in magnetic store")
}

// TestAccuracy2_WriteRecordsMemoryStoreCountWhenMagneticDisabled verifies that
// when magnetic store writes are disabled, all records go to memory store regardless
// of their timestamp age.
func TestAccuracy2_WriteRecordsMemoryStoreCountWhenMagneticDisabled(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("mag-off-db", nil)
	require.NoError(t, err)

	_, err = b.CreateTable("mag-off-db", "mag-off-tbl", nil, &timestreamwrite.CreateTableInput{
		RetentionProperties: &timestreamwrite.RetentionProperties{
			MemoryStoreRetentionPeriodInHours:  1,
			MagneticStoreRetentionPeriodInDays: 365,
		},
		MagneticStoreWriteProperties: &timestreamwrite.MagneticStoreWriteProperties{
			EnableMagneticStoreWrites: false, // magnetic store writes disabled
		},
	})
	require.NoError(t, err)

	// Even a very old timestamp should go to memory store because magnetic writes are off.
	veryOldTS := strconv.FormatInt(time.Now().UTC().Add(-72*time.Hour).UnixMilli(), 10)

	out, err := b.WriteRecords("mag-off-db", "mag-off-tbl", []timestreamwrite.Record{
		{
			MeasureName:      "mem",
			MeasureValue:     "8192",
			MeasureValueType: "BIGINT",
			Time:             veryOldTS,
			TimeUnit:         "MILLISECONDS",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), out.Total)
	assert.Equal(t, int32(1), out.MemoryStore, "records should go to memory store when magnetic disabled")
	assert.Equal(t, int32(0), out.MagneticStore)
}

// TestAccuracy2_WriteRecordsMixedStoreRouting verifies that a batch containing both
// recent and old records routes them to the correct stores.
func TestAccuracy2_WriteRecordsMixedStoreRouting(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("mixed-store-db", nil)
	require.NoError(t, err)

	_, err = b.CreateTable("mixed-store-db", "mixed-store-tbl", nil, &timestreamwrite.CreateTableInput{
		RetentionProperties: &timestreamwrite.RetentionProperties{
			MemoryStoreRetentionPeriodInHours:  1,
			MagneticStoreRetentionPeriodInDays: 365,
		},
		MagneticStoreWriteProperties: &timestreamwrite.MagneticStoreWriteProperties{
			EnableMagneticStoreWrites: true,
		},
	})
	require.NoError(t, err)

	recentTS := strconv.FormatInt(time.Now().UTC().UnixMilli(), 10)
	oldTS := strconv.FormatInt(time.Now().UTC().Add(-3*time.Hour).UnixMilli(), 10)

	out, err := b.WriteRecords("mixed-store-db", "mixed-store-tbl", []timestreamwrite.Record{
		{
			MeasureName: "m1", MeasureValue: "1.0", MeasureValueType: "DOUBLE",
			Time: recentTS, TimeUnit: "MILLISECONDS",
		},
		{
			MeasureName: "m2", MeasureValue: "2.0", MeasureValueType: "DOUBLE",
			Time: oldTS, TimeUnit: "MILLISECONDS",
		},
		{
			MeasureName: "m3", MeasureValue: "3.0", MeasureValueType: "DOUBLE",
			Time: recentTS, TimeUnit: "MILLISECONDS",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(3), out.Total)
	assert.Equal(t, int32(2), out.MemoryStore, "two recent records should go to memory store")
	assert.Equal(t, int32(1), out.MagneticStore, "one old record should go to magnetic store")
}

// TestAccuracy2_WriteRecordsMagneticStoreInHTTPResponse verifies that the
// MagneticStore count is propagated through the HTTP layer.
func TestAccuracy2_WriteRecordsMagneticStoreInHTTPResponse(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	b := h.Backend

	_, err := b.CreateDatabase("mag-http-db", nil)
	require.NoError(t, err)

	_, err = b.CreateTable("mag-http-db", "mag-http-tbl", nil, &timestreamwrite.CreateTableInput{
		RetentionProperties: &timestreamwrite.RetentionProperties{
			MemoryStoreRetentionPeriodInHours:  1,
			MagneticStoreRetentionPeriodInDays: 365,
		},
		MagneticStoreWriteProperties: &timestreamwrite.MagneticStoreWriteProperties{
			EnableMagneticStoreWrites: true,
		},
	})
	require.NoError(t, err)

	oldTS := strconv.FormatInt(time.Now().UTC().Add(-5*time.Hour).UnixMilli(), 10)

	rec := doRequest(t, h, "WriteRecords", map[string]any{
		"DatabaseName": "mag-http-db",
		"TableName":    "mag-http-tbl",
		"Records": []map[string]any{
			{
				"MeasureName":      "latency",
				"MeasureValue":     "42",
				"MeasureValueType": "BIGINT",
				"Time":             oldTS,
				"TimeUnit":         "MILLISECONDS",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	ingested := out["RecordsIngested"].(map[string]any)
	assert.Equal(t, 1, int(ingested["Total"].(float64)))
	assert.Equal(t, 0, int(ingested["MemoryStore"].(float64)))
	assert.Equal(t, 1, int(ingested["MagneticStore"].(float64)),
		"old record should appear in MagneticStore count")
}

// TestAccuracy2_WriteRecordsMagneticStoreDefaultRetentionOldRecordGoesToMagnetic verifies that
// when no explicit retention is configured, AWS defaults (6h memory) are applied,
// so very old records go to magnetic store (not memory).
func TestAccuracy2_WriteRecordsMagneticStoreDefaultRetentionOldRecordGoesToMagnetic(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("no-ret-mag-db", nil)
	require.NoError(t, err)

	// Table has magnetic store enabled; RetentionProperties defaults to {6h, 73d}.
	_, err = b.CreateTable("no-ret-mag-db", "no-ret-mag-tbl", nil, &timestreamwrite.CreateTableInput{
		MagneticStoreWriteProperties: &timestreamwrite.MagneticStoreWriteProperties{
			EnableMagneticStoreWrites: true,
		},
	})
	require.NoError(t, err)

	veryOldTS := strconv.FormatInt(time.Now().UTC().Add(-1000*24*time.Hour).UnixMilli(), 10)

	out, err := b.WriteRecords("no-ret-mag-db", "no-ret-mag-tbl", []timestreamwrite.Record{
		{
			MeasureName: "m", MeasureValue: "1", MeasureValueType: "BIGINT",
			Time: veryOldTS, TimeUnit: "MILLISECONDS",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), out.Total)
	// Default 6h memory retention: 1000-day-old record exceeds memory window → magnetic store.
	assert.Equal(t, int32(0), out.MemoryStore)
	assert.Equal(t, int32(1), out.MagneticStore, "default 6h retention → old record goes to magnetic store")
}

// ---------------------------------------------------------------------------
// Gap J: UntagResource idempotency and ListTagsForResource for unknown ARN
// ---------------------------------------------------------------------------

// TestAccuracy2_UntagResourceIdempotent verifies that UntagResource succeeds
// even when the tag key does not exist on the resource.
func TestAccuracy2_UntagResourceIdempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "untag-idem-db"})

	arn := "arn:aws:timestream:us-east-1:000000000000:database/untag-idem-db"

	// Untag a key that was never tagged — should succeed without error.
	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": arn,
		"TagKeys":     []string{"nonexistent-key"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestAccuracy2_ListTagsForResourceUnknownARNReturnsEmpty verifies that
// ListTagsForResource returns an empty list (not an error) for an ARN that has no
// tags stored.
func TestAccuracy2_ListTagsForResourceUnknownTagsReturnsEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "empty-tags-db"})

	arn := "arn:aws:timestream:us-east-1:000000000000:database/empty-tags-db"

	rec := doRequest(t, h, "ListTagsForResource", map[string]string{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	// Tags field should be present and empty (not absent).
	tags, ok := out["Tags"]
	require.True(t, ok, "Tags field should be present even when empty")
	assert.Empty(t, tags, "Tags list should be empty for a resource with no tags")
}

// TestAccuracy2_UntagResourceFromKnownARN verifies that tagging then untagging works
// correctly and leaves no residual tags.
func TestAccuracy2_UntagResourceFromKnownARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "untag-known-db"})

	arn := "arn:aws:timestream:us-east-1:000000000000:database/untag-known-db"

	doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags": []map[string]string{
			{"Key": "alpha", "Value": "1"},
			{"Key": "beta", "Value": "2"},
		},
	})

	// Untag one key.
	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": arn,
		"TagKeys":     []string{"alpha"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List remaining tags.
	listRec := doRequest(t, h, "ListTagsForResource", map[string]string{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))

	tags := out["Tags"].([]any)
	require.Len(t, tags, 1)
	assert.Equal(t, "beta", tags[0].(map[string]any)["Key"])
}

// ---------------------------------------------------------------------------
// Gap K: ConflictException on duplicate resources
// ---------------------------------------------------------------------------

// TestAccuracy2_CreateDatabaseConflict verifies that creating a database with a
// duplicate name returns HTTP 409 with __type=ConflictException.
func TestAccuracy2_CreateDatabaseConflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "dup-conf-db"})

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "dup-conf-db"})
	assert.Equal(t, http.StatusConflict, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ConflictException", body["__type"])
	assert.NotEmpty(t, body["message"])
}

// TestAccuracy2_CreateTableConflict verifies that creating a table with a duplicate
// name in the same database returns HTTP 409 with __type=ConflictException.
func TestAccuracy2_CreateTableConflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "dup-tbl-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "dup-tbl-db", "TableName": "dup-tbl"})

	rec := doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "dup-tbl-db", "TableName": "dup-tbl"})
	assert.Equal(t, http.StatusConflict, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ConflictException", body["__type"])
}

// ---------------------------------------------------------------------------
// Gap L: TableStatus is always ACTIVE after create
// ---------------------------------------------------------------------------

// TestAccuracy2_TableStatusActiveAfterCreate verifies that newly created tables
// have TableStatus = "ACTIVE" per the AWS API.
func TestAccuracy2_TableStatusActiveAfterCreate(t *testing.T) {
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

// TestAccuracy2_TableStatusActiveAfterDescribe verifies that DescribeTable also
// returns ACTIVE status for a newly created table.
func TestAccuracy2_TableStatusActiveAfterDescribe(t *testing.T) {
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

// TestAccuracy2_TableStatusActiveInListTables verifies that ListTables includes the
// ACTIVE status on each returned table.
func TestAccuracy2_TableStatusActiveInListTables(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Gap M: Database TableCount tracking
// ---------------------------------------------------------------------------

// TestAccuracy2_DatabaseTableCountTracking verifies that TableCount in the database
// object reflects the current number of tables after create and delete operations.
func TestAccuracy2_DatabaseTableCountTracking(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "cnt-track-db"})

	getCount := func() int {
		rec := doRequest(t, h, "DescribeDatabase", map[string]string{"DatabaseName": "cnt-track-db"})
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

		db := out["Database"].(map[string]any)

		return int(db["TableCount"].(float64))
	}

	assert.Equal(t, 0, getCount(), "new database should have zero tables")

	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "cnt-track-db", "TableName": "t-1"})
	assert.Equal(t, 1, getCount(), "after creating one table count should be 1")

	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "cnt-track-db", "TableName": "t-2"})
	assert.Equal(t, 2, getCount(), "after creating two tables count should be 2")

	doRequest(t, h, "DeleteTable", map[string]any{"DatabaseName": "cnt-track-db", "TableName": "t-1"})
	assert.Equal(t, 1, getCount(), "after deleting one table count should be 1")

	doRequest(t, h, "DeleteTable", map[string]any{"DatabaseName": "cnt-track-db", "TableName": "t-2"})
	assert.Equal(t, 0, getCount(), "after deleting all tables count should be 0")
}

// ---------------------------------------------------------------------------
// Gap N: ARN format in create responses
// ---------------------------------------------------------------------------

// TestAccuracy2_DatabaseARNFormat verifies that the database ARN in create/describe
// responses contains the region and account segments.
func TestAccuracy2_DatabaseARNFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "arn-db"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	db := out["Database"].(map[string]any)
	arn := db["Arn"].(string)

	assert.True(t, strings.HasPrefix(arn, "arn:aws:timestream:"),
		"ARN should start with arn:aws:timestream:")
	assert.Contains(t, arn, "database/arn-db",
		"ARN should contain the database name")
}

// TestAccuracy2_TableARNFormat verifies that the table ARN in create/describe
// responses is correctly formed with both database and table name segments.
func TestAccuracy2_TableARNFormat(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Gap O: Concurrent WriteRecords goroutine safety
// ---------------------------------------------------------------------------

// TestAccuracy2_ConcurrentWriteRecordsToSameTable verifies that simultaneous
// WriteRecords calls to the same table do not produce data races or corrupt state.
func TestAccuracy2_ConcurrentWriteRecordsToSameTable(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("conc-db", nil)
	require.NoError(t, err)
	_, err = b.CreateTable("conc-db", "conc-tbl", nil, nil)
	require.NoError(t, err)

	const goroutines = 20

	var wg sync.WaitGroup
	wg.Add(goroutines)
	errors := make([]error, goroutines)

	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()

			_, errors[idx] = b.WriteRecords("conc-db", "conc-tbl", []timestreamwrite.Record{
				{
					MeasureName:      fmt.Sprintf("metric-%d", idx),
					MeasureValue:     fmt.Sprintf("%d.0", idx),
					MeasureValueType: "DOUBLE",
					Time:             strconv.FormatInt(1609459200000+int64(idx)*1000, 10),
					TimeUnit:         "MILLISECONDS",
				},
			})
		}(i)
	}

	wg.Wait()

	for i, err := range errors {
		require.NoError(t, err, "goroutine %d should not error", i)
	}

	// All goroutines wrote distinct records (different measure names and times).
	assert.Equal(t, goroutines, timestreamwrite.RecordCount(b, "conc-db", "conc-tbl"),
		"all goroutine-written records should be stored")
}

// TestAccuracy2_ConcurrentWriteRecordsToDifferentTables verifies that concurrent
// writes to different tables in the same database do not interfere.
func TestAccuracy2_ConcurrentWriteRecordsToDifferentTables(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("conc2-db", nil)
	require.NoError(t, err)

	tables := []string{"tbl-a", "tbl-b", "tbl-c", "tbl-d"}
	for _, tbl := range tables {
		_, err = b.CreateTable("conc2-db", tbl, nil, nil)
		require.NoError(t, err)
	}

	const recordsPerTable = 5

	var wg sync.WaitGroup
	wg.Add(len(tables) * recordsPerTable)

	for _, tblName := range tables {
		for j := range recordsPerTable {
			go func(tbl string, idx int) {
				defer wg.Done()

				_, _ = b.WriteRecords("conc2-db", tbl, []timestreamwrite.Record{
					{
						MeasureName:      fmt.Sprintf("m-%d", idx),
						MeasureValue:     "1",
						MeasureValueType: "BIGINT",
						Time:             strconv.FormatInt(1609459200000+int64(idx)*1000, 10),
						TimeUnit:         "MILLISECONDS",
					},
				})
			}(tblName, j)
		}
	}

	wg.Wait()

	for _, tbl := range tables {
		count := timestreamwrite.RecordCount(b, "conc2-db", tbl)
		assert.Equal(t, recordsPerTable, count,
			"table %s should have %d records", tbl, recordsPerTable)
	}
}

// ---------------------------------------------------------------------------
// Gap P: RecordCount export helper
// ---------------------------------------------------------------------------

// TestAccuracy2_RecordCountExport verifies that the RecordCount export correctly
// reflects the number of records stored in a table.
func TestAccuracy2_RecordCountExport(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("rcnt-db", nil)
	require.NoError(t, err)
	_, err = b.CreateTable("rcnt-db", "rcnt-tbl", nil, nil)
	require.NoError(t, err)

	assert.Equal(t, 0, timestreamwrite.RecordCount(b, "rcnt-db", "rcnt-tbl"),
		"new table should have zero records")

	_, err = b.WriteRecords("rcnt-db", "rcnt-tbl", []timestreamwrite.Record{
		{MeasureName: "m1", MeasureValue: "1", Time: "1609459200000", TimeUnit: "MILLISECONDS"},
		{MeasureName: "m2", MeasureValue: "2", Time: "1609459200001", TimeUnit: "MILLISECONDS"},
		{MeasureName: "m3", MeasureValue: "3", Time: "1609459200002", TimeUnit: "MILLISECONDS"},
	})
	require.NoError(t, err)

	assert.Equal(t, 3, timestreamwrite.RecordCount(b, "rcnt-db", "rcnt-tbl"),
		"record count should reflect ingested records")
}

// TestAccuracy2_RecordCountNonexistentTable verifies that RecordCount returns zero
// for a table that does not exist.
func TestAccuracy2_RecordCountNonexistentTable(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()

	count := timestreamwrite.RecordCount(b, "ghost-db", "ghost-tbl")
	assert.Equal(t, 0, count, "non-existent table should return zero records")
}

// ---------------------------------------------------------------------------
// Gap Q: Backend WriteRecords MagneticStore output field exposed via backend
// ---------------------------------------------------------------------------

// TestAccuracy2_WriteRecordsOutputMagneticStoreFieldExists verifies that the
// WriteRecordsOutput struct exposes a MagneticStore field that can be read.
func TestAccuracy2_WriteRecordsOutputMagneticStoreFieldExists(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("ms-field-db", nil)
	require.NoError(t, err)
	_, err = b.CreateTable("ms-field-db", "ms-field-tbl", nil, nil)
	require.NoError(t, err)

	out, err := b.WriteRecords("ms-field-db", "ms-field-tbl", []timestreamwrite.Record{
		{MeasureName: "m", MeasureValue: "1", Time: "1609459200000", TimeUnit: "MILLISECONDS"},
	})
	require.NoError(t, err)
	// Without magnetic store write config, MagneticStore should be 0.
	assert.Equal(t, int32(0), out.MagneticStore,
		"MagneticStore should be 0 when magnetic store writes are not enabled")
	assert.Equal(t, int32(1), out.MemoryStore,
		"MemoryStore should equal Total when magnetic store writes are not enabled")
	assert.Equal(t, out.MemoryStore+out.MagneticStore, out.Total,
		"Total must equal MemoryStore + MagneticStore")
}

// TestAccuracy2_WriteRecordsTotalEqualsMemoryPlusMagnetic verifies the accounting
// invariant: Total == MemoryStore + MagneticStore for all WriteRecords calls.
func TestAccuracy2_WriteRecordsTotalEqualsMemoryPlusMagnetic(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("inv-db", nil)
	require.NoError(t, err)

	_, err = b.CreateTable("inv-db", "inv-tbl", nil, &timestreamwrite.CreateTableInput{
		RetentionProperties: &timestreamwrite.RetentionProperties{
			MemoryStoreRetentionPeriodInHours:  2,
			MagneticStoreRetentionPeriodInDays: 365,
		},
		MagneticStoreWriteProperties: &timestreamwrite.MagneticStoreWriteProperties{
			EnableMagneticStoreWrites: true,
		},
	})
	require.NoError(t, err)

	recentTS := strconv.FormatInt(time.Now().UTC().UnixMilli(), 10)
	oldTS := strconv.FormatInt(time.Now().UTC().Add(-10*time.Hour).UnixMilli(), 10)

	records := []timestreamwrite.Record{
		{MeasureName: "m1", MeasureValue: "1", MeasureValueType: "DOUBLE", Time: recentTS, TimeUnit: "MILLISECONDS"},
		{MeasureName: "m2", MeasureValue: "2", MeasureValueType: "DOUBLE", Time: oldTS, TimeUnit: "MILLISECONDS"},
		{MeasureName: "m3", MeasureValue: "3", MeasureValueType: "DOUBLE", Time: recentTS, TimeUnit: "MILLISECONDS"},
		{MeasureName: "m4", MeasureValue: "4", MeasureValueType: "DOUBLE", Time: oldTS, TimeUnit: "MILLISECONDS"},
	}

	out, err := b.WriteRecords("inv-db", "inv-tbl", records)
	require.NoError(t, err)

	assert.Equal(t, out.MemoryStore+out.MagneticStore, out.Total,
		"Total must equal MemoryStore + MagneticStore invariant")
	assert.Equal(t, int32(len(records)), out.Total, "Total should equal number of records")
}

// ---------------------------------------------------------------------------
// Gap R: Cumulative tag updates via TagResource
// ---------------------------------------------------------------------------

// TestAccuracy2_TagResourceCumulativeUpdates verifies that calling TagResource
// multiple times accumulates tags on the resource (existing tags are preserved and
// new tags are added).
func TestAccuracy2_TagResourceCumulativeUpdates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "cumtag-db"})

	arn := "arn:aws:timestream:us-east-1:000000000000:database/cumtag-db"

	// First TagResource call.
	doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags":        []map[string]string{{"Key": "env", "Value": "staging"}},
	})

	// Second TagResource call adds a new tag.
	doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags":        []map[string]string{{"Key": "team", "Value": "platform"}},
	})

	// Third TagResource call updates an existing tag.
	doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags":        []map[string]string{{"Key": "env", "Value": "prod"}},
	})

	listRec := doRequest(t, h, "ListTagsForResource", map[string]string{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))

	tags := out["Tags"].([]any)
	require.Len(t, tags, 2, "should have two distinct tags")

	tagMap := make(map[string]string, 2)
	for _, rawTag := range tags {
		tag := rawTag.(map[string]any)
		tagMap[tag["Key"].(string)] = tag["Value"].(string)
	}

	assert.Equal(t, "prod", tagMap["env"], "env tag should be updated to prod")
	assert.Equal(t, "platform", tagMap["team"], "team tag should be present")
}

// ---------------------------------------------------------------------------
// Gap S: BatchLoadTask RecordVersion field
// ---------------------------------------------------------------------------

// TestAccuracy2_BatchLoadTaskRecordVersionInDescribe verifies that the RecordVersion
// field on a BatchLoadTask description view is populated when set on the task.
func TestAccuracy2_BatchLoadTaskRecordVersionInDescribe(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	now := time.Now()

	b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
		TaskID:             "rv-task",
		TargetDatabaseName: "rv-db",
		TargetTableName:    "rv-tbl",
		TaskStatus:         timestreamwrite.BatchLoadStatusCreated,
		CreationTime:       now,
		LastUpdatedTime:    now,
		RecordVersion:      42,
	})

	h := timestreamwrite.NewHandler(b)
	rec := doRequest(t, h, "DescribeBatchLoadTask", map[string]string{"TaskId": "rv-task"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	desc := out["BatchLoadTaskDescription"].(map[string]any)
	assert.InDelta(t, float64(42), desc["RecordVersion"], 0,
		"RecordVersion should be returned in DescribeBatchLoadTask response")
}

// TestAccuracy2_BatchLoadTaskRecordVersionZeroOmitted verifies that RecordVersion
// is omitted from the response when it is zero.
func TestAccuracy2_BatchLoadTaskRecordVersionZeroOmitted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "rv0-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "rv0-db", "TableName": "rv0-tbl"})

	cr := doRequest(t, h, "CreateBatchLoadTask", map[string]any{
		"TargetDatabaseName": "rv0-db",
		"TargetTableName":    "rv0-tbl",
		"DataSourceConfiguration": map[string]any{
			"DataFormat":                "CSV",
			"DataSourceS3Configuration": map[string]any{"BucketName": "my-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, cr.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(cr.Body.Bytes(), &createOut))
	taskID := createOut["TaskId"].(string)

	dr := doRequest(t, h, "DescribeBatchLoadTask", map[string]string{"TaskId": taskID})
	require.Equal(t, http.StatusOK, dr.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(dr.Body.Bytes(), &out))

	desc := out["BatchLoadTaskDescription"].(map[string]any)
	_, hasRV := desc["RecordVersion"]
	// RecordVersion of 0 should be omitted (omitempty).
	assert.False(t, hasRV, "RecordVersion of 0 should be omitted from response")
}

// ---------------------------------------------------------------------------
// Gap T: DescribeEndpoints CachePeriodInMinutes
// ---------------------------------------------------------------------------

// TestAccuracy2_DescribeEndpointsCachePeriod verifies that the endpoint response
// includes a positive CachePeriodInMinutes per the AWS API.
func TestAccuracy2_DescribeEndpointsCachePeriod(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeEndpoints", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	endpoints, ok := out["Endpoints"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, endpoints)

	ep := endpoints[0].(map[string]any)
	cache := ep["CachePeriodInMinutes"].(float64)
	assert.Greater(t, cache, float64(0), "CachePeriodInMinutes should be positive")
	assert.LessOrEqual(t, cache, float64(1440), "CachePeriodInMinutes should not exceed one day")
}

// ---------------------------------------------------------------------------
// Gap U: Snapshot/Restore preserves KmsKeyId
// ---------------------------------------------------------------------------

// TestAccuracy2_SnapshotRestorePreservesKmsKeyId verifies that a database's KmsKeyId
// survives a snapshot/restore cycle.
func TestAccuracy2_SnapshotRestorePreservesKmsKeyId(t *testing.T) {
	t.Parallel()

	b1 := timestreamwrite.NewInMemoryBackend()
	_, err := b1.CreateDatabase("snap-kms-db", nil)
	require.NoError(t, err)

	kmsKey := "arn:aws:kms:us-east-1:000000000000:key/snap-key"
	_, err = b1.UpdateDatabase("snap-kms-db", kmsKey)
	require.NoError(t, err)

	data, err := b1.Snapshot()
	require.NoError(t, err)

	b2 := timestreamwrite.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), data))

	db, err := b2.DescribeDatabase("snap-kms-db")
	require.NoError(t, err)
	assert.Equal(t, kmsKey, db.KmsKeyID, "KmsKeyId should survive snapshot/restore")
}

// TestAccuracy2_SnapshotRestorePreservesBatchLoadTaskProgressReport verifies that
// a BatchLoadTask's ProgressReport survives a snapshot/restore cycle.
func TestAccuracy2_SnapshotRestorePreservesBatchLoadTaskProgressReport(t *testing.T) {
	t.Parallel()

	b1 := timestreamwrite.NewInMemoryBackend()
	now := time.Now()

	pr := &timestreamwrite.BatchLoadProgressReport{
		RecordsProcessed:        2000,
		RecordsIngested:         1950,
		RecordIngestionFailures: 50,
		FileFailures:            2,
		BytesMetered:            1024 * 512,
	}

	b1.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
		TaskID:             "snap-pr-task",
		TargetDatabaseName: "snap-pr-db",
		TargetTableName:    "snap-pr-tbl",
		TaskStatus:         timestreamwrite.BatchLoadStatusInProgress,
		CreationTime:       now,
		LastUpdatedTime:    now,
		ProgressReport:     pr,
	})

	data, err := b1.Snapshot()
	require.NoError(t, err)

	b2 := timestreamwrite.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), data))

	task, err := b2.DescribeBatchLoadTask("snap-pr-task")
	require.NoError(t, err)
	require.NotNil(t, task.ProgressReport, "ProgressReport should survive snapshot/restore")
	assert.Equal(t, int64(2000), task.ProgressReport.RecordsProcessed)
	assert.Equal(t, int64(1950), task.ProgressReport.RecordsIngested)
	assert.Equal(t, int64(50), task.ProgressReport.RecordIngestionFailures)
}

// ---------------------------------------------------------------------------
// Gap V: UpdateDatabase KmsKeyId clears when set to empty string
// ---------------------------------------------------------------------------

// TestAccuracy2_UpdateDatabaseClearKmsKeyId verifies that calling UpdateDatabase
// with an empty KmsKeyId clears the existing key.
func TestAccuracy2_UpdateDatabaseClearKmsKeyId(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("clr-kms-db", nil)
	require.NoError(t, err)

	// First set a KMS key.
	_, err = b.UpdateDatabase("clr-kms-db", "arn:aws:kms:us-east-1:000000000000:key/abc")
	require.NoError(t, err)

	db, err := b.DescribeDatabase("clr-kms-db")
	require.NoError(t, err)
	assert.NotEmpty(t, db.KmsKeyID, "KmsKeyID should be set after UpdateDatabase")

	// Now clear it.
	_, err = b.UpdateDatabase("clr-kms-db", "")
	require.NoError(t, err)

	db, err = b.DescribeDatabase("clr-kms-db")
	require.NoError(t, err)
	assert.Empty(t, db.KmsKeyID, "KmsKeyID should be cleared after UpdateDatabase with empty string")
}

// ---------------------------------------------------------------------------
// Gap W: ListTables without DatabaseName returns ValidationException
// ---------------------------------------------------------------------------

// TestAccuracy2_ListTablesRequiresDatabaseName verifies that omitting DatabaseName
// in the ListTables request returns a ValidationException (HTTP 400).
func TestAccuracy2_ListTablesRequiresDatabaseName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListTables", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ValidationException", body["__type"])
}

// ---------------------------------------------------------------------------
// Gap X: WriteRecords MeasureValueType field JSON roundtrip
// ---------------------------------------------------------------------------

// TestAccuracy2_WriteRecordsResponseContainsCorrectFields verifies that the
// WriteRecords HTTP response body contains the expected RecordsIngested structure
// with all three count fields.
func TestAccuracy2_WriteRecordsResponseContainsCorrectFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "fields-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "fields-db", "TableName": "fields-tbl"})

	rec := doRequest(t, h, "WriteRecords", map[string]any{
		"DatabaseName": "fields-db",
		"TableName":    "fields-tbl",
		"Records": []map[string]any{
			{
				"MeasureName":      "cpu",
				"MeasureValue":     "45.0",
				"MeasureValueType": "DOUBLE",
				"Time":             "1609459200000",
				"TimeUnit":         "MILLISECONDS",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, `"Total"`, "response should contain Total field")
	assert.Contains(t, body, `"MemoryStore"`, "response should contain MemoryStore field")
	assert.Contains(t, body, `"MagneticStore"`, "response should contain MagneticStore field")
	assert.Contains(t, body, `"RecordsIngested"`, "response should contain RecordsIngested wrapper")
}
