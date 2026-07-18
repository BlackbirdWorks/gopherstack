package glue_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// Test_CreatePartition_RequiresExistingTable verifies that CreatePartition
// (and BatchCreatePartition) reject a nonexistent database/table with
// EntityNotFoundException instead of silently storing an orphaned partition.
// Previously BatchCreatePartition never checked that the table existed at
// all.
func Test_CreatePartition_RequiresExistingTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreatePartition", map[string]any{
		"DatabaseName":   "no-such-db",
		"TableName":      "no-such-table",
		"PartitionInput": map[string]any{"Values": []string{"2024-01-01"}},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "EntityNotFoundException")
}

// Test_CreatePartition_DuplicateReportsAlreadyExists verifies that creating a
// duplicate partition surfaces AlreadyExistsException, not
// EntityNotFoundException. awserrFromDetail previously wrapped every
// batch-partition ErrorDetail as ErrNotFound regardless of its ErrorCode, so
// this case was misreported.
func Test_CreatePartition_DuplicateReportsAlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	require.Equal(t, http.StatusOK, doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "db1"},
	}).Code)
	require.Equal(t, http.StatusOK, doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "db1",
		"TableInput":   map[string]any{"Name": "tbl1"},
	}).Code)

	partitionInput := map[string]any{
		"DatabaseName":   "db1",
		"TableName":      "tbl1",
		"PartitionInput": map[string]any{"Values": []string{"2024-01-01"}},
	}
	require.Equal(t, http.StatusOK, doGlueRequest(t, h, "CreatePartition", partitionInput).Code)

	rec := doGlueRequest(t, h, "CreatePartition", partitionInput)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "AlreadyExistsException")
	assert.NotContains(t, rec.Body.String(), "EntityNotFoundException")
}

// Test_BatchGetPartition_ReturnsRealPartitions verifies BatchGetPartition
// actually looks up requested partitions against backend state. It
// previously always returned an empty Partitions list regardless of what
// existed — a disguised stub masked by a stale comment claiming the backend
// "has no partition storage".
func Test_BatchGetPartition_ReturnsRealPartitions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	require.Equal(t, http.StatusOK, doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "db1"},
	}).Code)
	require.Equal(t, http.StatusOK, doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "db1",
		"TableInput":   map[string]any{"Name": "tbl1"},
	}).Code)
	require.Equal(t, http.StatusOK, doGlueRequest(t, h, "CreatePartition", map[string]any{
		"DatabaseName": "db1",
		"TableName":    "tbl1",
		"PartitionInput": map[string]any{
			"Values":            []string{"2024-01-01"},
			"StorageDescriptor": map[string]any{"Location": "s3://bucket/2024-01-01/"},
		},
	}).Code)

	rec := doGlueRequest(t, h, "BatchGetPartition", map[string]any{
		"DatabaseName": "db1",
		"TableName":    "tbl1",
		"PartitionsToGet": []map[string]any{
			{"Values": []string{"2024-01-01"}},
			{"Values": []string{"2024-01-02"}}, // does not exist
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Partitions []struct {
			StorageDescriptor struct {
				Location string `json:"Location"`
			} `json:"StorageDescriptor"`
			Values []string `json:"Values"`
		} `json:"Partitions"`
		UnprocessedKeys []struct {
			Values []string `json:"Values"`
		} `json:"UnprocessedKeys"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	require.Len(t, out.Partitions, 1)
	assert.Equal(t, []string{"2024-01-01"}, out.Partitions[0].Values)
	assert.Equal(t, "s3://bucket/2024-01-01/", out.Partitions[0].StorageDescriptor.Location)
	require.Len(t, out.UnprocessedKeys, 1)
	assert.Equal(t, []string{"2024-01-02"}, out.UnprocessedKeys[0].Values)
}

// TestAuditGlue_GetPartitions_Expression tests GetPartitions expression filtering.
func TestAuditGlue_GetPartitions_Expression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		partitions [][]string
		expr       string
		name       string
		wantValues [][]string
		wantCode   int
	}{
		{
			name:       "eq_match",
			partitions: [][]string{{"2023", "01"}, {"2023", "02"}, {"2024", "01"}},
			expr:       "year = '2023'",
			wantValues: [][]string{{"2023", "01"}, {"2023", "02"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "eq_no_match",
			partitions: [][]string{{"2023", "01"}, {"2024", "01"}},
			expr:       "year = '2025'",
			wantValues: [][]string{},
			wantCode:   http.StatusOK,
		},
		{
			name:       "neq",
			partitions: [][]string{{"2023", "01"}, {"2024", "01"}},
			expr:       "year <> '2023'",
			wantValues: [][]string{{"2024", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "and",
			partitions: [][]string{{"2023", "01"}, {"2023", "02"}, {"2024", "01"}},
			expr:       "year = '2023' AND month = '01'",
			wantValues: [][]string{{"2023", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "or",
			partitions: [][]string{{"2023", "01"}, {"2023", "02"}, {"2024", "01"}},
			expr:       "month = '01' OR month = '02'",
			wantValues: [][]string{{"2023", "01"}, {"2023", "02"}, {"2024", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "in_list",
			partitions: [][]string{{"2023", "01"}, {"2023", "06"}, {"2024", "01"}},
			expr:       "year IN ('2023', '2024')",
			wantValues: [][]string{{"2023", "01"}, {"2023", "06"}, {"2024", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "not_in_list",
			partitions: [][]string{{"2023", "01"}, {"2023", "06"}, {"2024", "01"}},
			expr:       "year NOT IN ('2023')",
			wantValues: [][]string{{"2024", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "gt",
			partitions: [][]string{{"2021", "01"}, {"2022", "01"}, {"2023", "01"}},
			expr:       "year > '2021'",
			wantValues: [][]string{{"2022", "01"}, {"2023", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "gte",
			partitions: [][]string{{"2021", "01"}, {"2022", "01"}, {"2023", "01"}},
			expr:       "year >= '2022'",
			wantValues: [][]string{{"2022", "01"}, {"2023", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "lt",
			partitions: [][]string{{"2021", "01"}, {"2022", "01"}, {"2023", "01"}},
			expr:       "year < '2023'",
			wantValues: [][]string{{"2021", "01"}, {"2022", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "lte",
			partitions: [][]string{{"2021", "01"}, {"2022", "01"}, {"2023", "01"}},
			expr:       "year <= '2022'",
			wantValues: [][]string{{"2021", "01"}, {"2022", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "like_prefix",
			partitions: [][]string{{"2023", "01"}, {"2023", "12"}, {"2024", "01"}},
			expr:       "year LIKE '202%'",
			wantValues: [][]string{{"2023", "01"}, {"2023", "12"}, {"2024", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "like_exact",
			partitions: [][]string{{"2023", "01"}, {"2024", "01"}},
			expr:       "year LIKE '2023'",
			wantValues: [][]string{{"2023", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "no_expression_returns_all",
			partitions: [][]string{{"2023", "01"}, {"2024", "01"}},
			expr:       "",
			wantValues: [][]string{{"2023", "01"}, {"2024", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "compound_and_or",
			partitions: [][]string{{"2023", "01"}, {"2023", "06"}, {"2024", "03"}},
			expr:       "(year = '2023' AND month = '06') OR year = '2024'",
			wantValues: [][]string{{"2023", "06"}, {"2024", "03"}},
			wantCode:   http.StatusOK,
		},
		{
			name:     "invalid_expression_returns_error",
			expr:     "BADBADBAD ~~~ ~~~",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			dbName := "exprdb_" + tc.name
			tableName := "exprtbl_" + tc.name

			rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
				"DatabaseInput": map[string]any{"Name": dbName},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doGlueRequest(t, h, "CreateTable", map[string]any{
				"DatabaseName": dbName,
				"TableInput": map[string]any{
					"Name": tableName,
					"PartitionKeys": []map[string]any{
						{"Name": "year", "Type": "string"},
						{"Name": "month", "Type": "string"},
					},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			for _, vals := range tc.partitions {
				rec = doGlueRequest(t, h, "CreatePartition", map[string]any{
					"DatabaseName": dbName,
					"TableName":    tableName,
					"PartitionInput": map[string]any{
						"Values": vals,
						"StorageDescriptor": map[string]any{
							"Location": "s3://bucket/" + tableName,
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{
				"DatabaseName": dbName,
				"TableName":    tableName,
			}
			if tc.expr != "" {
				body["Expression"] = tc.expr
			}

			rec = doGlueRequest(t, h, "GetPartitions", body)
			assert.Equal(t, tc.wantCode, rec.Code, "response body: %s", rec.Body.String())

			if tc.wantCode != http.StatusOK {
				return
			}

			var out struct {
				Partitions []struct {
					Values []string `json:"Values"`
				} `json:"Partitions"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			got := make([][]string, len(out.Partitions))
			for i, p := range out.Partitions {
				got[i] = p.Values
			}

			if len(tc.wantValues) == 0 {
				assert.Empty(t, got)
			} else {
				assert.ElementsMatch(t, tc.wantValues, got)
			}
		})
	}
}

// TestBatchCreatePartition_Limit tests the 100-partition-per-call limit.
func TestBatchCreatePartition_Limit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		partCount int
		wantCode  int
	}{
		{
			name:      "exactly_100_ok",
			partCount: 100,
			wantCode:  http.StatusOK,
		},
		{
			name:      "101_partitions_rejected",
			partCount: 101,
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestDB(t, h, "bcp-db-"+tt.name, "bcp-tbl")

			parts := make([]map[string]any, tt.partCount)
			for i := range tt.partCount {
				parts[i] = map[string]any{
					"Values": []string{string(rune('a' + i%26))},
				}
			}

			rec := doGlueRequest(t, h, "BatchCreatePartition", map[string]any{
				"DatabaseName":       "bcp-db-" + tt.name,
				"TableName":          "bcp-tbl",
				"PartitionInputList": parts,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHTTPBatchCreatePartition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a database and table first via HTTP.
	postGlue(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "testdb"},
	})
	postGlue(t, h, "CreateTable", map[string]any{
		"DatabaseName": "testdb",
		"TableInput":   map[string]any{"Name": "testtbl"},
	})

	rec := postGlue(t, h, "BatchCreatePartition", map[string]any{
		"DatabaseName": "testdb",
		"TableName":    "testtbl",
		"PartitionInputList": []map[string]any{
			{"Values": []string{"2024", "01"}},
			{"Values": []string{"2024", "02"}},
		},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotNil(t, out["Partitions"])
}

// postGlue is a helper for HTTP tests - reuses handler_test.go's infrastructure.
func postGlue(t *testing.T, h *glue.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	bs, err := json.Marshal(body)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bs))
	req.Header.Set("X-Amz-Target", "AWSGlue."+action)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err = h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// createTestDB is a helper that creates a database and a table for partition tests.
func createTestDB(t *testing.T, h *glue.Handler, dbName, tableName string) {
	t.Helper()

	rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": dbName},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": dbName,
		"TableInput":   map[string]any{"Name": tableName},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// createTestPartition creates a single partition via the API.
func createTestPartition(t *testing.T, h *glue.Handler, dbName, tableName string, values []string) {
	t.Helper()

	rec := doGlueRequest(t, h, "CreatePartition", map[string]any{
		"DatabaseName": dbName,
		"TableName":    tableName,
		"PartitionInput": map[string]any{
			"Values": values,
			"StorageDescriptor": map[string]any{
				"Location": "s3://bucket/" + tableName,
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestGetPartition_Found(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDB(t, h, "db1", "tbl1")
	createTestPartition(t, h, "db1", "tbl1", []string{"2024", "01"})

	rec := doGlueRequest(t, h, "GetPartition", map[string]any{
		"DatabaseName":    "db1",
		"TableName":       "tbl1",
		"PartitionValues": []string{"2024", "01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Partition struct {
			Values []string `json:"Values"`
		} `json:"Partition"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, []string{"2024", "01"}, out.Partition.Values)
}

func TestGetPartition_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "GetPartition", map[string]any{
		"DatabaseName":    "nodb",
		"TableName":       "notbl",
		"PartitionValues": []string{"x"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetPartitions_List(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDB(t, h, "db2", "tbl2")
	createTestPartition(t, h, "db2", "tbl2", []string{"2024"})
	createTestPartition(t, h, "db2", "tbl2", []string{"2025"})

	rec := doGlueRequest(t, h, "GetPartitions", map[string]any{
		"DatabaseName": "db2",
		"TableName":    "tbl2",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Partitions []struct {
			Values []string `json:"Values"`
		} `json:"Partitions"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.Partitions, 2)
}

func TestGetPartitions_TableNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "GetPartitions", map[string]any{
		"DatabaseName": "nodb",
		"TableName":    "notbl",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestUpdatePartition_Updates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDB(t, h, "db3", "tbl3")
	createTestPartition(t, h, "db3", "tbl3", []string{"v1"})

	rec := doGlueRequest(t, h, "UpdatePartition", map[string]any{
		"DatabaseName":       "db3",
		"TableName":          "tbl3",
		"PartitionValueList": []string{"v1"},
		"PartitionInput": map[string]any{
			"Values": []string{"v1"},
			"StorageDescriptor": map[string]any{
				"Location": "s3://updated-bucket/tbl3",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetPartition", map[string]any{
		"DatabaseName":    "db3",
		"TableName":       "tbl3",
		"PartitionValues": []string{"v1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Partition struct {
			StorageDescriptor struct {
				Location string `json:"Location"`
			} `json:"StorageDescriptor"`
		} `json:"Partition"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "s3://updated-bucket/tbl3", out.Partition.StorageDescriptor.Location)
}

func TestUpdatePartition_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "UpdatePartition", map[string]any{
		"DatabaseName":       "nodb",
		"TableName":          "notbl",
		"PartitionValueList": []string{"x"},
		"PartitionInput":     map[string]any{"Values": []string{"x"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatchUpdatePartition_Updates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDB(t, h, "db4", "tbl4")
	createTestPartition(t, h, "db4", "tbl4", []string{"p1"})
	createTestPartition(t, h, "db4", "tbl4", []string{"p2"})

	rec := doGlueRequest(t, h, "BatchUpdatePartition", map[string]any{
		"DatabaseName": "db4",
		"TableName":    "tbl4",
		"Entries": []map[string]any{
			{
				"PartitionValueList": []string{"p1"},
				"PartitionInput": map[string]any{
					"Values": []string{"p1"},
					"StorageDescriptor": map[string]any{
						"Location": "s3://new/p1",
					},
				},
			},
			{
				"PartitionValueList": []string{"p2"},
				"PartitionInput": map[string]any{
					"Values": []string{"p2"},
					"StorageDescriptor": map[string]any{
						"Location": "s3://new/p2",
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Errors []any `json:"Errors"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out.Errors)
}

// TestBatchGetPartition_TableValidation verifies that BatchGetPartition mirrors
// the AWS Glue contract: missing required identifiers and unknown tables both
// produce client errors instead of an empty success.
func TestBatchGetPartition_TableValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		setupTable bool
		wantOK     bool
	}{
		{
			name:       "missing_database_and_table",
			body:       map[string]any{},
			setupTable: false,
			wantOK:     false,
		},
		{
			name:       "unknown_table_returns_client_error",
			body:       map[string]any{"DatabaseName": "db1", "TableName": "missing"},
			setupTable: false,
			wantOK:     false,
		},
		{
			name:       "existing_table_returns_empty_partitions",
			body:       map[string]any{"DatabaseName": "db1", "TableName": "tbl1"},
			setupTable: true,
			wantOK:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setupTable {
				rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
					"DatabaseInput": map[string]any{"Name": "db1"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doGlueRequest(t, h, "CreateTable", map[string]any{
					"DatabaseName": "db1",
					"TableInput":   map[string]any{"Name": "tbl1"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doGlueRequest(t, h, "BatchGetPartition", tt.body)
			if tt.wantOK {
				assert.Equal(t, http.StatusOK, rec.Code)
			} else {
				assert.NotEqual(t, http.StatusOK, rec.Code)
				assert.NotEqual(t, http.StatusInternalServerError, rec.Code)
			}
		})
	}
}
