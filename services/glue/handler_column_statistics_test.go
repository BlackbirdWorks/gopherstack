package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestStopColumnStatisticsTaskRun_NotFound verifies that
// StopColumnStatisticsTaskRun raises EntityNotFoundException for an unknown run ID.
func TestStopColumnStatisticsTaskRun_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		runID     string
		wantError string
		wantCode  int
		create    bool
	}{
		{
			name:      "stop_missing_run_returns_entity_not_found",
			runID:     "no-such-run",
			create:    false,
			wantCode:  http.StatusBadRequest,
			wantError: "EntityNotFoundException",
		},
		{
			name:     "stop_existing_run_succeeds",
			create:   true,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			runID := tt.runID
			if tt.create {
				rec := doGlueRequest(t, h, "StartColumnStatisticsTaskRun", map[string]any{
					"DatabaseName": "mydb",
					"TableName":    "mytable",
					"RoleArn":      "arn:aws:iam::123456789012:role/GlueRole",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					RunID string `json:"ColumnStatisticsTaskRunId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.NotEmpty(t, out.RunID, "expected ColumnStatisticsTaskRunId in response")
				runID = out.RunID
			}

			rec := doGlueRequest(t, h, "StopColumnStatisticsTaskRun", map[string]any{
				"ColumnStatisticsTaskRunId": runID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

func TestColumnStatisticsTask(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Start a run
	rec := doGlueRequest(t, h, "StartColumnStatisticsTaskRun", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ColumnStatisticsTaskRunId")

	// List runs
	rec = doGlueRequest(t, h, "ListColumnStatisticsTaskRuns", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get task settings
	rec = doGlueRequest(t, h, "GetColumnStatisticsTaskSettings", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestColumnStatistics_Table(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "csdb"}})
	doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "csdb",
		"TableInput":   map[string]any{"Name": "cst"},
	})

	colStats := []map[string]any{
		{
			"ColumnName": "id",
			"ColumnType": "int",
			"StatisticsData": map[string]any{
				"Type": "LONG",
				"LongColumnStatisticsData": map[string]any{
					"NumberOfNulls":          0,
					"NumberOfDistinctValues": 100,
				},
			},
		},
		{
			"ColumnName": "name",
			"ColumnType": "string",
			"StatisticsData": map[string]any{
				"Type": "STRING",
				"StringColumnStatisticsData": map[string]any{
					"MaximumLength":          50,
					"NumberOfNulls":          5,
					"NumberOfDistinctValues": 90,
				},
			},
		},
	}

	tests := []struct {
		body     map[string]any
		check    func(t *testing.T, body string)
		name     string
		op       string
		wantCode int
	}{
		{
			name: "update-col-stats-for-table",
			op:   "UpdateColumnStatisticsForTable",
			body: map[string]any{
				"DatabaseName":         "csdb",
				"TableName":            "cst",
				"ColumnStatisticsList": colStats,
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "Errors")
			},
		},
		{
			name:     "get-col-stats-for-table",
			op:       "GetColumnStatisticsForTable",
			body:     map[string]any{"DatabaseName": "csdb", "TableName": "cst"},
			wantCode: http.StatusOK,
		},
		{
			name: "get-col-stats-for-table-specific-columns",
			op:   "GetColumnStatisticsForTable",
			body: map[string]any{
				"DatabaseName": "csdb",
				"TableName":    "cst",
				"ColumnNames":  []string{"id"},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "delete-col-stats-for-table",
			op:   "DeleteColumnStatisticsForTable",
			body: map[string]any{
				"DatabaseName": "csdb",
				"TableName":    "cst",
				"ColumnName":   "id",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Set up state before each test.
			h2 := newTestHandler(t)
			doGlueRequest(t, h2, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "csdb"}})
			doGlueRequest(t, h2, "CreateTable", map[string]any{
				"DatabaseName": "csdb",
				"TableInput":   map[string]any{"Name": "cst"},
			})
			doGlueRequest(t, h2, "UpdateColumnStatisticsForTable", map[string]any{
				"DatabaseName":         "csdb",
				"TableName":            "cst",
				"ColumnStatisticsList": colStats,
			})

			rec := doGlueRequest(t, h2, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.check != nil {
				tt.check(t, rec.Body.String())
			}
		})
	}
}

func TestColumnStatistics_Partition(t *testing.T) {
	t.Parallel()

	partitionValues := []string{"2024-01-01"}
	colStats := []map[string]any{
		{
			"ColumnName": "val",
			"ColumnType": "double",
			"StatisticsData": map[string]any{
				"Type": "DOUBLE",
				"DoubleColumnStatisticsData": map[string]any{
					"NumberOfNulls":          0,
					"NumberOfDistinctValues": 50,
				},
			},
		},
	}

	setupPartitionHandler := func(t *testing.T) *glue.Handler {
		t.Helper()
		h := newTestHandler(t)
		doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "pdb"}})
		doGlueRequest(t, h, "CreateTable", map[string]any{
			"DatabaseName": "pdb",
			"TableInput": map[string]any{
				"Name":          "pt",
				"PartitionKeys": []map[string]any{{"Name": "dt", "Type": "string"}},
			},
		})
		doGlueRequest(t, h, "CreatePartition", map[string]any{
			"DatabaseName": "pdb",
			"TableName":    "pt",
			"PartitionInput": map[string]any{
				"Values": partitionValues,
			},
		})

		return h
	}

	tests := []struct {
		body     map[string]any
		name     string
		op       string
		wantCode int
	}{
		{
			name: "update-col-stats-for-partition",
			op:   "UpdateColumnStatisticsForPartition",
			body: map[string]any{
				"DatabaseName":         "pdb",
				"TableName":            "pt",
				"PartitionValues":      partitionValues,
				"ColumnStatisticsList": colStats,
			},
			wantCode: http.StatusOK,
		},
		{
			name: "get-col-stats-for-partition",
			op:   "GetColumnStatisticsForPartition",
			body: map[string]any{
				"DatabaseName":    "pdb",
				"TableName":       "pt",
				"PartitionValues": partitionValues,
			},
			wantCode: http.StatusOK,
		},
		{
			name: "delete-col-stats-for-partition",
			op:   "DeleteColumnStatisticsForPartition",
			body: map[string]any{
				"DatabaseName":    "pdb",
				"TableName":       "pt",
				"PartitionValues": partitionValues,
				"ColumnName":      "val",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ph := setupPartitionHandler(t)
			doGlueRequest(t, ph, "UpdateColumnStatisticsForPartition", map[string]any{
				"DatabaseName":         "pdb",
				"TableName":            "pt",
				"PartitionValues":      partitionValues,
				"ColumnStatisticsList": colStats,
			})

			rec := doGlueRequest(t, ph, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestColumnStatisticsTaskSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "cstdb"}})
	doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "cstdb",
		"TableInput":   map[string]any{"Name": "csttbl"},
	})

	createCSTRec := doGlueRequest(t, h, "CreateColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName":   "cstdb",
		"TableName":      "csttbl",
		"Role":           "arn:aws:iam::123456789012:role/GlueRole",
		"ColumnNameList": []string{"col1", "col2"},
	})
	assert.Equal(t, http.StatusOK, createCSTRec.Code)

	doGlueRequest(t, h, "CreateColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName": "cstdb",
		"TableName":    "csttbl",
		"Role":         "arn:aws:iam::123456789012:role/GlueRole",
	})
	getCSTRec := doGlueRequest(t, h, "GetColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName": "cstdb",
		"TableName":    "csttbl",
	})
	require.Equal(t, http.StatusOK, getCSTRec.Code)
	assert.Contains(t, getCSTRec.Body.String(), "ColumnStatisticsTaskSettings")

	startCSTRec := doGlueRequest(t, h, "StartColumnStatisticsTaskRun", map[string]any{
		"DatabaseName": "cstdb",
		"TableName":    "csttbl",
	})
	require.Equal(t, http.StatusOK, startCSTRec.Code)
	var startCSTOut map[string]any
	require.NoError(t, json.Unmarshal(startCSTRec.Body.Bytes(), &startCSTOut))
	assert.NotEmpty(t, startCSTOut["ColumnStatisticsTaskRunId"])

	doGlueRequest(t, h, "StartColumnStatisticsTaskRun", map[string]any{
		"DatabaseName": "cstdb",
		"TableName":    "csttbl",
	})
	listCSTRec := doGlueRequest(t, h, "ListColumnStatisticsTaskRuns", map[string]any{})
	require.Equal(t, http.StatusOK, listCSTRec.Code)
	assert.Contains(t, listCSTRec.Body.String(), "ColumnStatisticsTaskRunIds")

	startCSTRec2 := doGlueRequest(t, h, "StartColumnStatisticsTaskRun", map[string]any{
		"DatabaseName": "cstdb",
		"TableName":    "csttbl",
	})
	require.Equal(t, http.StatusOK, startCSTRec2.Code)
	var startCSTOut2 map[string]any
	require.NoError(t, json.Unmarshal(startCSTRec2.Body.Bytes(), &startCSTOut2))
	cstRunID := startCSTOut2["ColumnStatisticsTaskRunId"].(string)

	getCSTRunRec := doGlueRequest(t, h, "GetColumnStatisticsTaskRun", map[string]any{
		"ColumnStatisticsTaskRunId": cstRunID,
	})
	require.Equal(t, http.StatusOK, getCSTRunRec.Code)

	stopCSTRec := doGlueRequest(t, h, "StopColumnStatisticsTaskRun", map[string]any{
		"ColumnStatisticsTaskRunId": cstRunID,
	})
	assert.Equal(t, http.StatusOK, stopCSTRec.Code)

	doGlueRequest(t, h, "CreateColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName": "cstdb",
		"TableName":    "csttbl",
	})
	deleteCSTRec := doGlueRequest(t, h, "DeleteColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName": "cstdb",
		"TableName":    "csttbl",
	})
	assert.Equal(t, http.StatusOK, deleteCSTRec.Code)
}

// TestColumnStatisticsTaskRunSchedule_RequiresExistingSettings verifies
// Start/StopColumnStatisticsTaskRunSchedule mutate real schedule state tied to
// existing task settings instead of no-op'ing.
func TestColumnStatisticsTaskRunSchedule_RequiresExistingSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "db"}})
	doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "db",
		"TableInput":   map[string]any{"Name": "tbl"},
	})

	rec := doGlueRequest(t, h, "StartColumnStatisticsTaskRunSchedule", map[string]any{
		"DatabaseName": "db", "TableName": "tbl",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "no task settings created yet")

	doGlueRequest(t, h, "CreateColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName": "db",
		"TableName":    "tbl",
		"Role":         "arn:aws:iam::123456789012:role/GlueRole",
	})

	rec = doGlueRequest(t, h, "StartColumnStatisticsTaskRunSchedule", map[string]any{
		"DatabaseName": "db", "TableName": "tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName": "db", "TableName": "tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ColumnStatisticsTaskSettings struct {
			Schedule struct {
				State string `json:"State"`
			} `json:"Schedule"`
		} `json:"ColumnStatisticsTaskSettings"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "SCHEDULED", out.ColumnStatisticsTaskSettings.Schedule.State)

	rec = doGlueRequest(t, h, "StopColumnStatisticsTaskRunSchedule", map[string]any{
		"DatabaseName": "db", "TableName": "tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName": "db", "TableName": "tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "NOT_SCHEDULED", out.ColumnStatisticsTaskSettings.Schedule.State)
}

// TestColumnStatistics tests column statistics for table and partition.
func TestColumnStatistics(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// Setup: create db and table.
	dispatchNewOp(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "mydb"},
	})
	dispatchNewOp(t, h, "CreateTable", map[string]any{
		"DatabaseName": "mydb",
		"TableInput":   map[string]any{"Name": "mytable"},
	})

	// UpdateColumnStatisticsForTable
	dispatchNewOp(t, h, "UpdateColumnStatisticsForTable", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"ColumnStatisticsList": []any{
			map[string]any{
				"ColumnName": "col1",
				"ColumnType": "string",
				"StatisticsData": map[string]any{
					"Type": "STRING",
				},
			},
		},
	})

	// GetColumnStatisticsForTable
	out := dispatchNewOp(t, h, "GetColumnStatisticsForTable", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"ColumnNames":  []any{"col1"},
	})
	stats, _ := out["ColumnStatisticsList"].([]any)
	if len(stats) != 1 {
		t.Errorf("expected 1 column stat, got %d", len(stats))
	}

	// DeleteColumnStatisticsForTable
	dispatchNewOp(t, h, "DeleteColumnStatisticsForTable", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"ColumnName":   "col1",
	})

	// Verify deletion
	out2 := dispatchNewOp(t, h, "GetColumnStatisticsForTable", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
	})
	stats2, _ := out2["ColumnStatisticsList"].([]any)
	if len(stats2) != 0 {
		t.Errorf("expected 0 column stats after delete, got %d", len(stats2))
	}

	// UpdateColumnStatisticsForPartition
	dispatchNewOp(t, h, "UpdateColumnStatisticsForPartition", map[string]any{
		"DatabaseName":    "mydb",
		"TableName":       "mytable",
		"PartitionValues": []any{"2023"},
		"ColumnStatisticsList": []any{
			map[string]any{
				"ColumnName": "col2",
				"ColumnType": "bigint",
				"StatisticsData": map[string]any{
					"Type": "LONG",
				},
			},
		},
	})

	// GetColumnStatisticsForPartition
	out3 := dispatchNewOp(t, h, "GetColumnStatisticsForPartition", map[string]any{
		"DatabaseName":    "mydb",
		"TableName":       "mytable",
		"PartitionValues": []any{"2023"},
	})
	stats3, _ := out3["ColumnStatisticsList"].([]any)
	if len(stats3) != 1 {
		t.Errorf("expected 1 partition stat, got %d", len(stats3))
	}

	// DeleteColumnStatisticsForPartition
	dispatchNewOp(t, h, "DeleteColumnStatisticsForPartition", map[string]any{
		"DatabaseName":    "mydb",
		"TableName":       "mytable",
		"PartitionValues": []any{"2023"},
		"ColumnName":      "col2",
	})
}

// TestColumnStatisticsTaskSettings_Stateful verifies column statistics task settings lifecycle.
func TestColumnStatisticsTaskSettings_Stateful(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doGlueRequest(t, h, "CreateColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"Role":         "arn:aws:iam::000000000000:role/GlueRole",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	updateRec := doGlueRequest(t, h, "UpdateColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"Role":         "arn:aws:iam::000000000000:role/NewGlueRole",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	getRec := doGlueRequest(t, h, "GetColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	deleteRec := doGlueRequest(t, h, "DeleteColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
	})
	require.Equal(t, http.StatusOK, deleteRec.Code)
}

// TestColumnStatisticsTaskSettings_WireRoleName pins the request member name
// to the botocore model (glue/2017-03-31): Role, not RoleArn.
func TestColumnStatisticsTaskSettings_WireRoleName(t *testing.T) {
	t.Parallel()

	const role = "arn:aws:iam::000000000000:role/WireRole"

	tests := []struct {
		name   string
		action string
		seed   bool
	}{
		{name: "create", action: "CreateColumnStatisticsTaskSettings"},
		{name: "update", action: "UpdateColumnStatisticsTaskSettings", seed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			db, tbl := "wiredb-"+tt.name, "wiretbl"

			if tt.seed {
				seedRec := doGlueRequest(t, h, "CreateColumnStatisticsTaskSettings", map[string]any{
					"DatabaseName": db,
					"TableName":    tbl,
				})
				require.Equal(t, http.StatusOK, seedRec.Code)
			}

			rec := doGlueRequest(t, h, tt.action, map[string]any{
				"DatabaseName": db,
				"TableName":    tbl,
				"Role":         role,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			getRec := doGlueRequest(t, h, "GetColumnStatisticsTaskSettings", map[string]any{
				"DatabaseName": db,
				"TableName":    tbl,
			})
			require.Equal(t, http.StatusOK, getRec.Code)
			assert.Contains(t, getRec.Body.String(), role)
		})
	}
}
