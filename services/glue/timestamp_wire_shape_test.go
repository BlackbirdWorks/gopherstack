package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStartedOn_IsEpochSecondsNumber locks down a fix for a systemic bug
// class: glue is awsjson1.1, which serializes timestamps as JSON NUMBERS
// (seconds since the Unix epoch), never RFC3339 strings. BlueprintRun and
// ColumnStatisticsTaskRun previously modeled their StartedOn field as a raw
// time.Time, which encoding/json renders as an RFC3339 string
// (`"2024-01-01T00:00:00Z"`) — the real aws-sdk-go-v2 deserializer for these
// fields expects a JSON number and would fail to parse the response. Both
// models now use float64 (matching every other run/timestamp field in this
// package, e.g. JobRun.StartedOn, WorkflowRun.StartedOn).
func TestStartedOn_IsEpochSecondsNumber(t *testing.T) {
	t.Parallel()

	t.Run("BlueprintRun", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doGlueRequest(t, h, "CreateBlueprint", map[string]any{
			"Name":              "ts-bp",
			"BlueprintLocation": "s3://bucket/ts-bp",
		})

		startRec := doGlueRequest(t, h, "StartBlueprintRun", map[string]any{"BlueprintName": "ts-bp"})
		require.Equal(t, http.StatusOK, startRec.Code)

		var startOut map[string]any
		require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
		runID, ok := startOut["RunId"].(string)
		require.True(t, ok)

		getRec := doGlueRequest(t, h, "GetBlueprintRun", map[string]any{
			"BlueprintName": "ts-bp",
			"RunId":         runID,
		})
		require.Equal(t, http.StatusOK, getRec.Code)

		var getOut map[string]any
		require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
		run, ok := getOut["Run"].(map[string]any)
		require.True(t, ok)

		startedOn, present := run["StartedOn"]
		require.True(t, present, "StartedOn should be present on the wire")
		_, isNumber := startedOn.(float64)
		assert.True(
			t, isNumber, "StartedOn must serialize as a JSON number (epoch seconds), got %T: %v", startedOn, startedOn,
		)
	})

	t.Run("ColumnStatisticsTaskRun", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "ts-db"}})
		doGlueRequest(t, h, "CreateTable", map[string]any{
			"DatabaseName": "ts-db",
			"TableInput":   map[string]any{"Name": "ts-tbl"},
		})

		startRec := doGlueRequest(t, h, "StartColumnStatisticsTaskRun", map[string]any{
			"DatabaseName":   "ts-db",
			"TableName":      "ts-tbl",
			"Role":           "role",
			"ColumnNameList": []string{"col1"},
		})
		require.Equal(t, http.StatusOK, startRec.Code)

		var startOut map[string]any
		require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
		runID, ok := startOut["ColumnStatisticsTaskRunId"].(string)
		require.True(t, ok)

		getRec := doGlueRequest(t, h, "GetColumnStatisticsTaskRun", map[string]any{
			"ColumnStatisticsTaskRunId": runID,
		})
		require.Equal(t, http.StatusOK, getRec.Code)

		var getOut map[string]any
		require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
		run, ok := getOut["ColumnStatisticsTaskRun"].(map[string]any)
		require.True(t, ok)

		// The real member name is StartTime, not StartedOn (glue@v1.152.0
		// deserializers.go: awsAwsjson11_deserializeDocumentColumnStatisticsTaskRun's
		// case list has StartTime, no StartedOn key at all).
		startedOn, present := run["StartTime"]
		require.True(t, present, "StartTime should be present on the wire")
		_, isNumber := startedOn.(float64)
		assert.True(
			t, isNumber, "StartTime must serialize as a JSON number (epoch seconds), got %T: %v", startedOn, startedOn,
		)
	})
}
