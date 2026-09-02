package kinesisanalyticsv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKAV2_ApplicationOperationsLifecycle exercises
// DescribeApplicationOperation and ListApplicationOperations end to end.
// Before recordOperation wired StartApplication/StopApplication/
// UpdateApplication/RollbackApplication into the backend's operations map,
// both of these read ops were permanently empty / not-found regardless of
// how many lifecycle actions had run.
func TestKAV2_ApplicationOperationsLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "ops-lifecycle-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})

	startRec := doKAV2Request(t, h, "StartApplication", map[string]any{"ApplicationName": "ops-lifecycle-app"})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startOut map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	opID, ok := startOut["OperationId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, opID)

	descRec := doKAV2Request(t, h, "DescribeApplicationOperation", map[string]any{
		"ApplicationName": "ops-lifecycle-app",
		"OperationId":     opID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	details, ok := descOut["ApplicationOperationInfoDetails"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "StartApplication", details["Operation"])
	assert.Equal(t, "SUCCESSFUL", details["OperationStatus"])
	assert.Positive(t, details["StartTime"])
	assert.Positive(t, details["EndTime"])

	listRec := doKAV2Request(t, h, "ListApplicationOperations", map[string]any{
		"ApplicationName": "ops-lifecycle-app",
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	items, ok := listOut["ApplicationOperationInfoList"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)

	item, ok := items[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, opID, item["OperationId"])
	assert.Equal(t, "StartApplication", item["Operation"])

	// Unknown OperationId must 404, not silently succeed.
	missRec := doKAV2Request(t, h, "DescribeApplicationOperation", map[string]any{
		"ApplicationName": "ops-lifecycle-app",
		"OperationId":     "op-does-not-exist",
	})
	assert.Equal(t, http.StatusNotFound, missRec.Code)
}

// TestKAV2_RollbackApplication exercises RollbackApplication over HTTP --
// previously untested at any layer, and (before the version-history fix in
// store.go) permanently broken: RollbackApplication requires at least 2
// recorded versions, but nothing besides CreateApplication ever populated
// the version history, so this call always failed with InvalidArgumentException.
func TestKAV2_RollbackApplication(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":      "rollback-http-app",
		"RuntimeEnvironment":   "FLINK-1_18",
		"ServiceExecutionRole": "arn:aws:iam::000000000000:role/original",
	})

	updateRec := doKAV2Request(t, h, "UpdateApplication", map[string]any{
		"ApplicationName":             "rollback-http-app",
		"ServiceExecutionRoleUpdate":  "arn:aws:iam::000000000000:role/changed",
		"CurrentApplicationVersionId": 1,
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	rollbackRec := doKAV2Request(t, h, "RollbackApplication", map[string]any{
		"ApplicationName":             "rollback-http-app",
		"CurrentApplicationVersionId": 2,
	})
	require.Equal(t, http.StatusOK, rollbackRec.Code)

	var rollbackOut map[string]any
	require.NoError(t, json.Unmarshal(rollbackRec.Body.Bytes(), &rollbackOut))
	assert.NotEmpty(t, rollbackOut["OperationId"])

	detail, ok := rollbackOut["ApplicationDetail"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "arn:aws:iam::000000000000:role/original", detail["ServiceExecutionRole"])
	assert.InEpsilon(t, 3.0, detail["ApplicationVersionId"], 1e-9)
}
