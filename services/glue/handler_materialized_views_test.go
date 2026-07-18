package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStopMaterializedViewRefreshTaskRun_NotFound verifies that
// StopMaterializedViewRefreshTaskRun raises EntityNotFoundException for an unknown task run ID.
func TestStopMaterializedViewRefreshTaskRun_NotFound(t *testing.T) {
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
			runID:     "no-such-mvr",
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
				rec := doGlueRequest(t, h, "StartMaterializedViewRefreshTaskRun", map[string]any{
					"DatabaseName": "mydb",
					"TableName":    "myview",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					RunID string `json:"RunId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.NotEmpty(t, out.RunID, "expected RunId in response")
				runID = out.RunID
			}

			rec := doGlueRequest(t, h, "StopMaterializedViewRefreshTaskRun", map[string]any{
				"RunId": runID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

func TestMaterializedViewRefresh(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Start
	rec := doGlueRequest(t, h, "StartMaterializedViewRefreshTaskRun", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "RunId")

	// List
	rec = doGlueRequest(t, h, "ListMaterializedViewRefreshTaskRuns", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Runs")
}

func TestMaterializedViewRefreshTaskRun(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startRefreshRec1 := doGlueRequest(t, h, "StartMaterializedViewRefreshTaskRun", map[string]any{})
	require.Equal(t, http.StatusOK, startRefreshRec1.Code)
	var startRefreshOut1 map[string]any
	require.NoError(t, json.Unmarshal(startRefreshRec1.Body.Bytes(), &startRefreshOut1))
	assert.NotEmpty(t, startRefreshOut1["RunId"])

	startRec := doGlueRequest(t, h, "StartMaterializedViewRefreshTaskRun", map[string]any{})
	require.Equal(t, http.StatusOK, startRec.Code)
	var startOut map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	runID := startOut["RunId"].(string)

	getRefreshRec := doGlueRequest(t, h, "GetMaterializedViewRefreshTaskRun", map[string]any{})
	require.Equal(t, http.StatusOK, getRefreshRec.Code)

	listRefreshRec := doGlueRequest(t, h, "ListMaterializedViewRefreshTaskRuns", map[string]any{})
	require.Equal(t, http.StatusOK, listRefreshRec.Code)
	assert.Contains(t, listRefreshRec.Body.String(), "Runs")

	stopRefreshRec := doGlueRequest(t, h, "StopMaterializedViewRefreshTaskRun", map[string]any{"RunId": runID})
	assert.Equal(t, http.StatusOK, stopRefreshRec.Code)
}

// TestMaterializedViewRefresh_Stateful verifies materialized view refresh lifecycle.
func TestMaterializedViewRefresh_Stateful(t *testing.T) {
	t.Parallel()

	t.Run("start_get_by_id_stop", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		startRec := doGlueRequest(t, h, "StartMaterializedViewRefreshTaskRun", map[string]any{
			"DatabaseName": "mydb",
			"TableName":    "myview",
		})
		require.Equal(t, http.StatusOK, startRec.Code)

		var startOut struct {
			RunID string `json:"RunId"`
		}
		require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
		assert.NotEmpty(t, startOut.RunID)

		getRec := doGlueRequest(t, h, "GetMaterializedViewRefreshTaskRun", map[string]any{
			"RunId": startOut.RunID,
		})
		require.Equal(t, http.StatusOK, getRec.Code)

		var getOut struct {
			RunID  string `json:"RunId"`
			Status string `json:"Status"`
		}
		require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
		assert.Equal(t, startOut.RunID, getOut.RunID)
		assert.Equal(t, "RUNNING", getOut.Status)

		stopRec := doGlueRequest(t, h, "StopMaterializedViewRefreshTaskRun", map[string]any{
			"RunId": startOut.RunID,
		})
		require.Equal(t, http.StatusOK, stopRec.Code)

		getRec2 := doGlueRequest(t, h, "GetMaterializedViewRefreshTaskRun", map[string]any{
			"RunId": startOut.RunID,
		})
		require.Equal(t, http.StatusOK, getRec2.Code)

		var getOut2 struct {
			Status string `json:"Status"`
		}
		require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &getOut2))
		assert.Equal(t, "STOPPED", getOut2.Status)
	})

	t.Run("get_not_found_returns_400", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doGlueRequest(t, h, "GetMaterializedViewRefreshTaskRun", map[string]any{
			"RunId": "mvr-not-found",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("empty_run_id_falls_back_to_first", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		doGlueRequest(t, h, "StartMaterializedViewRefreshTaskRun", map[string]any{
			"DatabaseName": "db1",
			"TableName":    "tbl1",
		})

		rec := doGlueRequest(t, h, "GetMaterializedViewRefreshTaskRun", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Status string `json:"Status"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Equal(t, "RUNNING", out.Status)
	})
}
