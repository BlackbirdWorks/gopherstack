package timestreamwrite_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]string{"DatabaseName": "my-db"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing name",
			body:       map[string]string{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateDatabase", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				db, ok := resp["Database"].(map[string]any)
				assert.True(t, ok)
				assert.Equal(t, "my-db", db["DatabaseName"])
			}
		})
	}
}

func TestHandler_CreateDatabase_Conflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{"DatabaseName": "dup-db"}

	rec := doRequest(t, h, "CreateDatabase", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateDatabase", body)
	// awsJson1.0 reports every client-fault error (including ConflictException) as
	// HTTP 400; the SDK resolves the concrete exception from the body's __type field.
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errBody map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errBody))
	assert.Equal(t, "ConflictException", errBody["__type"])
}

func TestHandler_DescribeDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupDB    string
		queryDB    string
		wantStatus int
	}{
		{
			name:       "success",
			setupDB:    "my-db",
			queryDB:    "my-db",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			setupDB:    "",
			queryDB:    "missing-db",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setupDB != "" {
				rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": tt.setupDB})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "DescribeDatabase", map[string]string{"DatabaseName": tt.queryDB})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListDatabases(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"db-a", "db-b"} {
		rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": name})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListDatabases", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	dbs, ok := resp["Databases"].([]any)
	assert.True(t, ok)
	assert.Len(t, dbs, 2)
}

// TestHandler_ListDatabases_Sorted verifies databases are sorted by name.
func TestHandler_ListDatabases_Sorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "zdb"})
	doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "adb"})
	doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "mdb"})

	rec := doRequest(t, h, "ListDatabases", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	dbs := out["Databases"].([]any)
	require.Len(t, dbs, 3)
	assert.Equal(t, "adb", dbs[0].(map[string]any)["DatabaseName"])
	assert.Equal(t, "mdb", dbs[1].(map[string]any)["DatabaseName"])
	assert.Equal(t, "zdb", dbs[2].(map[string]any)["DatabaseName"])
}

// TestHandler_ListDatabases_Pagination verifies NextToken pagination for ListDatabases.
func TestHandler_ListDatabases_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"aaa", "bbb", "ccc", "ddd", "eee"} {
		doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": name})
	}

	// First page: MaxResults=2.
	rec1 := doRequest(t, h, "ListDatabases", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))

	dbs1 := out1["Databases"].([]any)
	assert.Len(t, dbs1, 2)

	nextToken, ok := out1["NextToken"].(string)
	require.True(t, ok && nextToken != "", "NextToken should be non-empty when more pages exist")

	// Second page.
	rec2 := doRequest(t, h, "ListDatabases", map[string]any{"MaxResults": 2, "NextToken": nextToken})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))

	dbs2 := out2["Databases"].([]any)
	assert.Len(t, dbs2, 2)

	// Third page (final).
	nextToken2 := out2["NextToken"].(string)
	rec3 := doRequest(t, h, "ListDatabases", map[string]any{"MaxResults": 2, "NextToken": nextToken2})
	require.Equal(t, http.StatusOK, rec3.Code)

	var out3 map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &out3))

	dbs3 := out3["Databases"].([]any)
	assert.Len(t, dbs3, 1)
	// No NextToken on final page.
	assert.Empty(t, out3["NextToken"])
}

// TestHandler_ListDatabases_NoNextTokenWhenFits verifies no NextToken when all
// results fit on a single page.
func TestHandler_ListDatabases_NoNextTokenWhenFits(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "only-db"})

	rec := doRequest(t, h, "ListDatabases", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Empty(t, out["NextToken"])
}

func TestHandler_DeleteDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupDB    string
		deleteDB   string
		wantStatus int
	}{
		{
			name:       "success",
			setupDB:    "del-db",
			deleteDB:   "del-db",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			setupDB:    "",
			deleteDB:   "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setupDB != "" {
				rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": tt.setupDB})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "DeleteDatabase", map[string]string{"DatabaseName": tt.deleteDB})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DeleteDatabase_RequiresEmpty verifies that deleting a database
// that still contains tables returns a ValidationException, matching AWS API
// behaviour.
func TestHandler_DeleteDatabase_RequiresEmpty(t *testing.T) {
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

// TestHandler_DeleteDatabase_AfterDeletingTables verifies the happy path: if
// all tables are deleted first, then the database can be deleted.
func TestHandler_DeleteDatabase_AfterDeletingTables(t *testing.T) {
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

func TestHandler_UpdateDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupDB    string
		updateDB   string
		kmsKey     string
		wantStatus int
	}{
		{
			name:       "success",
			setupDB:    "my-db",
			updateDB:   "my-db",
			kmsKey:     "arn:aws:kms:us-east-1:000000000000:key/1234",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			setupDB:    "",
			updateDB:   "missing",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing name",
			setupDB:    "",
			updateDB:   "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setupDB != "" {
				rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": tt.setupDB})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "UpdateDatabase", map[string]string{
				"DatabaseName": tt.updateDB,
				"KmsKeyId":     tt.kmsKey,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_CreateDatabase_MinimumNameLength verifies the AWS-mandated
// 3-character minimum for DatabaseName.
func TestHandler_CreateDatabase_MinimumNameLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		dbName     string
		wantStatus int
	}{
		{name: "one_char_rejected", dbName: "a", wantStatus: http.StatusBadRequest},
		{name: "two_char_rejected", dbName: "ab", wantStatus: http.StatusBadRequest},
		{name: "three_char_accepted", dbName: "abc", wantStatus: http.StatusOK},
		{name: "four_char_accepted", dbName: "abcd", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": tt.dbName})
			assert.Equal(t, tt.wantStatus, rec.Code, "DatabaseName=%q", tt.dbName)

			if tt.wantStatus == http.StatusBadRequest {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "ValidationException", body["__type"])
			}
		})
	}
}

// TestHandler_CreateDatabase_NameFormatValidation verifies that the handler
// rejects DatabaseName values that violate AWS character-set and length
// constraints.
func TestHandler_CreateDatabase_NameFormatValidation(t *testing.T) {
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

// TestHandler_CreateDatabase_NameValidationErrorBody verifies that a
// name-validation failure returns __type=ValidationException with a
// descriptive message.
func TestHandler_CreateDatabase_NameValidationErrorBody(t *testing.T) {
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

// TestHandler_CreateDatabase_TagKeyValidation verifies that empty and
// oversized tag keys/values are rejected.
func TestHandler_CreateDatabase_TagKeyValidation(t *testing.T) {
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

// TestHandler_CreateDatabase_WithKmsKeyID verifies that a KmsKeyId provided at
// database creation time is stored and reflected in the create response.
func TestHandler_CreateDatabase_WithKmsKeyID(t *testing.T) {
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

// TestHandler_CreateDatabase_KmsKeyRoundTrip verifies that a KmsKeyId provided
// at creation time is preserved through a DescribeDatabase call.
func TestHandler_CreateDatabase_KmsKeyRoundTrip(t *testing.T) {
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

// TestHandler_ListDatabases_IncludesKmsKeyID verifies that a KmsKeyId provided
// at creation time is present in ListDatabases output.
func TestHandler_ListDatabases_IncludesKmsKeyID(t *testing.T) {
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

// TestHandler_CreateDatabase_WithoutKmsKeyIDOmitsField verifies that when no
// KmsKeyId is provided, the field is absent from the response (not empty
// string).
func TestHandler_CreateDatabase_WithoutKmsKeyIDOmitsField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "no-kms-db"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Parse raw to check presence/absence of key.
	body := rec.Body.String()
	// KmsKeyId should be absent (omitempty) when not set.
	assert.NotContains(t, body, `"KmsKeyId":""`, "empty KmsKeyId should be omitted")
}

// TestHandler_CreateDatabase_ConflictException verifies that creating a
// database with a duplicate name returns HTTP 400 (awsJson1.0 reports all
// client faults as 400) with __type=ConflictException.
func TestHandler_CreateDatabase_ConflictException(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "dup-conf-db"})

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "dup-conf-db"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ConflictException", body["__type"])
	assert.NotEmpty(t, body["message"])
}

// TestHandler_DescribeDatabase_TableCountTracking verifies that TableCount in
// the database object reflects the current number of tables after create and
// delete operations.
func TestHandler_DescribeDatabase_TableCountTracking(t *testing.T) {
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

// TestHandler_CreateDatabase_ARNFormat verifies that the database ARN in
// create/describe responses contains the region and account segments.
func TestHandler_CreateDatabase_ARNFormat(t *testing.T) {
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

// TestHandler_CreateDatabase_TimestampsAreFloats verifies that timestamps are
// returned as float64 Unix epoch seconds (not strings), which is required by
// the AWS SDK v2.
func TestHandler_CreateDatabase_TimestampsAreFloats(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseName": "ts-float-db",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	db := raw["Database"].(map[string]any)
	ct, ok := db["CreationTime"].(float64)
	require.True(t, ok, "CreationTime should be float64, got %T", db["CreationTime"])
	assert.Greater(t, ct, float64(0))

	lu, ok := db["LastUpdatedTime"].(float64)
	require.True(t, ok, "LastUpdatedTime should be float64, got %T", db["LastUpdatedTime"])
	assert.Greater(t, lu, float64(0))
}

// TestHandler_CreateDatabase_WithTags verifies tags are stored on creation.
func TestHandler_CreateDatabase_WithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseName": "tagged-db",
		"Tags": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "data"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	// Fetch the ARN then list tags
	db := out["Database"].(map[string]any)
	arn := db["Arn"].(string)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": arn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsOut))

	tags := tagsOut["Tags"].([]any)
	assert.Len(t, tags, 2)
}
