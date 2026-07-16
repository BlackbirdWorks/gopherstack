package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// Test_TableStorageDescriptor_RoundTrip verifies that CreateTable/GetTable
// round-trip the full StorageDescriptor shape (InputFormat, OutputFormat,
// SerdeInfo, Parameters, BucketColumns, SortColumns, Compressed,
// NumberOfBuckets) plus Table-level Parameters/Owner/Retention and
// Column.Parameters — fields that aws-sdk-go-v2/service/glue/types.
// StorageDescriptor/Table declare but this backend previously dropped
// entirely (silently discarded on every CreateTable/UpdateTable call).
func Test_TableStorageDescriptor_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "db1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	tableInput := map[string]any{
		"Name":      "tbl1",
		"Owner":     "hive",
		"Retention": 7,
		"Parameters": map[string]any{
			"classification": "parquet",
			"EXTERNAL":       "TRUE",
		},
		"StorageDescriptor": map[string]any{
			"Location":        "s3://bucket/tbl1/",
			"InputFormat":     "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
			"OutputFormat":    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
			"Compressed":      true,
			"NumberOfBuckets": 4,
			"BucketColumns":   []string{"id"},
			"SortColumns": []map[string]any{
				{"Column": "id", "SortOrder": 1},
			},
			"Parameters": map[string]any{"serialization.format": "1"},
			"SerdeInfo": map[string]any{
				"Name":                 "parquet-serde",
				"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
				"Parameters":           map[string]any{"foo": "bar"},
			},
			"Columns": []map[string]any{
				{"Name": "id", "Type": "int", "Parameters": map[string]any{"comment": "pk"}},
			},
		},
	}

	rec = doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "db1",
		"TableInput":   tableInput,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetTable", map[string]any{
		"DatabaseName": "db1",
		"Name":         "tbl1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Table struct {
			Parameters        map[string]string `json:"Parameters"`
			Owner             string            `json:"Owner"`
			StorageDescriptor struct {
				SerdeInfo struct {
					Parameters           map[string]string `json:"Parameters"`
					Name                 string            `json:"Name"`
					SerializationLibrary string            `json:"SerializationLibrary"`
				} `json:"SerdeInfo"`
				Parameters    map[string]string `json:"Parameters"`
				Location      string            `json:"Location"`
				InputFormat   string            `json:"InputFormat"`
				OutputFormat  string            `json:"OutputFormat"`
				BucketColumns []string          `json:"BucketColumns"`
				SortColumns   []struct {
					Column    string `json:"Column"`
					SortOrder int    `json:"SortOrder"`
				} `json:"SortColumns"`
				Columns []struct {
					Parameters map[string]string `json:"Parameters"`
					Name       string            `json:"Name"`
					Type       string            `json:"Type"`
				} `json:"Columns"`
				NumberOfBuckets int  `json:"NumberOfBuckets"`
				Compressed      bool `json:"Compressed"`
			} `json:"StorageDescriptor"`
			Retention int `json:"Retention"`
		} `json:"Table"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Equal(t, "hive", out.Table.Owner)
	assert.Equal(t, 7, out.Table.Retention)
	assert.Equal(t, "parquet", out.Table.Parameters["classification"])
	sd := out.Table.StorageDescriptor
	assert.Equal(t, "s3://bucket/tbl1/", sd.Location)
	assert.True(t, sd.Compressed)
	assert.Equal(t, 4, sd.NumberOfBuckets)
	assert.Equal(t, []string{"id"}, sd.BucketColumns)
	assert.Equal(t, "1", sd.Parameters["serialization.format"])
	require.Len(t, sd.SortColumns, 1)
	assert.Equal(t, "id", sd.SortColumns[0].Column)
	require.NotNil(t, sd.SerdeInfo)
	assert.Equal(t, "parquet-serde", sd.SerdeInfo.Name)
	assert.Equal(t, "bar", sd.SerdeInfo.Parameters["foo"])
	require.Len(t, sd.Columns, 1)
	assert.Equal(t, "pk", sd.Columns[0].Parameters["comment"])
}

// TestAuditGlue_GetTables_Expression tests GetTables regex expression filtering.
func TestAuditGlue_GetTables_Expression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tableNames []string
		expr       string
		name       string
		wantNames  []string
		wantCode   int
	}{
		{
			name:       "prefix_match",
			tableNames: []string{"sales_2023", "sales_2024", "inventory_2023"},
			expr:       "^sales_.*",
			wantNames:  []string{"sales_2023", "sales_2024"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "exact_match",
			tableNames: []string{"orders", "order_items", "customers"},
			expr:       "^orders$",
			wantNames:  []string{"orders"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "no_match",
			tableNames: []string{"sales_2023", "inventory"},
			expr:       "^nonexistent.*",
			wantNames:  []string{},
			wantCode:   http.StatusOK,
		},
		{
			name:       "no_expression_returns_all",
			tableNames: []string{"alpha", "beta"},
			expr:       "",
			wantNames:  []string{"alpha", "beta"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "pattern_suffix",
			tableNames: []string{"raw_2023", "processed_2023", "raw_2024"},
			expr:       ".*_2023$",
			wantNames:  []string{"raw_2023", "processed_2023"},
			wantCode:   http.StatusOK,
		},
		{
			name:     "invalid_regex_returns_error",
			expr:     "[invalid",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			dbName := "tblexprdb_" + tc.name

			rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
				"DatabaseInput": map[string]any{"Name": dbName},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			for _, name := range tc.tableNames {
				rec = doGlueRequest(t, h, "CreateTable", map[string]any{
					"DatabaseName": dbName,
					"TableInput":   map[string]any{"Name": name},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{
				"DatabaseName": dbName,
			}
			if tc.expr != "" {
				body["Expression"] = tc.expr
			}

			rec = doGlueRequest(t, h, "GetTables", body)
			assert.Equal(t, tc.wantCode, rec.Code, "response body: %s", rec.Body.String())

			if tc.wantCode != http.StatusOK {
				return
			}

			var out struct {
				TableList []struct {
					Name string `json:"Name"`
				} `json:"TableList"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			gotNames := make([]string, len(out.TableList))
			for i, tbl := range out.TableList {
				gotNames[i] = tbl.Name
			}

			if len(tc.wantNames) == 0 {
				assert.Empty(t, gotNames)
			} else {
				assert.ElementsMatch(t, tc.wantNames, gotNames)
			}
		})
	}
}

func TestRefinement1_HTTPBatchDeleteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	postGlue(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "db1"},
	})
	postGlue(t, h, "CreateTable", map[string]any{
		"DatabaseName": "db1",
		"TableInput":   map[string]any{"Name": "tbl1"},
	})
	postGlue(t, h, "CreateTable", map[string]any{
		"DatabaseName": "db1",
		"TableInput":   map[string]any{"Name": "tbl2"},
	})

	rec := postGlue(t, h, "BatchDeleteTable", map[string]any{
		"DatabaseName":   "db1",
		"TablesToDelete": []string{"tbl1", "tbl2"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	errs, ok := out["Errors"].([]any)
	require.True(t, ok)
	assert.Empty(t, errs)
}

// TestRefinement2_CloneTableWithColumns tests deep copy of tables with columns.
func TestRefinement2_CloneTableWithColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		columns       []glue.Column
		partitionKeys []glue.Column
	}{
		{
			name:          "table_with_columns",
			columns:       []glue.Column{{Name: "id", Type: "int"}, {Name: "name", Type: "string"}},
			partitionKeys: []glue.Column{{Name: "year", Type: "int"}},
		},
		{
			name:          "table_no_columns",
			columns:       nil,
			partitionKeys: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			doGlueRequest(t, h, "CreateTable", map[string]any{
				"DatabaseName": "db",
				"TableInput": map[string]any{
					"Name":              "tbl",
					"StorageDescriptor": map[string]any{"Columns": tt.columns},
					"PartitionKeys":     tt.partitionKeys,
				},
			})

			rec := doGlueRequest(t, h, "GetTable", map[string]any{"DatabaseName": "db", "Name": "tbl"})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			tbl := out["Table"].(map[string]any)
			assert.NotNil(t, tbl)

			if len(tt.columns) > 0 {
				sd := tbl["StorageDescriptor"].(map[string]any)
				cols, _ := sd["Columns"].([]any)
				assert.Len(t, cols, len(tt.columns))
			}
		})
	}
}

// TestNewOps_DeleteTableVersion tests DeleteTableVersion.
func TestNewOps_DeleteTableVersion(t *testing.T) {
	t.Parallel()
	b := glue.NewInMemoryBackend("123456789012", "us-east-1")
	h := glue.NewHandler(b)

	// Seed a table version directly.
	b.AddTableVersionInternal("mydb", "mytable", &glue.TableVersion{VersionID: "1"})

	// DeleteTableVersion
	dispatchNewOp(t, h, "DeleteTableVersion", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"VersionId":    "1",
	})

	// Verify deletion (second delete should fail).
	dispatchNewOpExpectError(t, h, "DeleteTableVersion", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"VersionId":    "1",
	})
}

func TestAccuracy_SearchTables_EmptyFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDB(t, h, "searchdb", "alpha")

	rec := doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "searchdb",
		"TableInput":   map[string]any{"Name": "beta"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "SearchTables", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		TableList []struct {
			Name string `json:"Name"`
		} `json:"TableList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.TableList, 2)
}

func TestAccuracy_SearchTables_WithFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDB(t, h, "filterdb", "orders")

	rec := doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "filterdb",
		"TableInput":   map[string]any{"Name": "customers"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "SearchTables", map[string]any{
		"SearchText": "order",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		TableList []struct {
			Name string `json:"Name"`
		} `json:"TableList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.TableList, 1)
	assert.Equal(t, "orders", out.TableList[0].Name)
}
