package quicksight_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- DataSet refresh schedule CRUD round-trip and errors ----

func TestQuickSight_DataSetRefreshScheduleCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createDataSetRec := doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
		"DataSetId": "ds1", "Name": "DataSet1", "ImportMode": "SPICE", "PhysicalTableMap": testPhysicalTableMap(),
	})
	require.Equal(t, http.StatusCreated, createDataSetRec.Code)

	// Dataset doesn't exist -> 404.
	missingDataSetRec := doRequest(
		t, h, http.MethodPost, accountPath("/data-sets/notexist/refresh-schedules"),
		map[string]any{"Schedule": map[string]any{"ScheduleId": "s1", "RefreshType": "FULL_REFRESH"}},
	)
	assert.Equal(t, http.StatusNotFound, missingDataSetRec.Code)

	// Missing/invalid RefreshType -> validation error.
	invalidRec := doRequest(
		t, h, http.MethodPost, accountPath("/data-sets/ds1/refresh-schedules"),
		map[string]any{"Schedule": map[string]any{"ScheduleId": "s1"}},
	)
	assert.Equal(t, http.StatusBadRequest, invalidRec.Code)

	createRec := doRequest(
		t, h, http.MethodPost, accountPath("/data-sets/ds1/refresh-schedules"),
		map[string]any{"Schedule": map[string]any{
			"ScheduleId": "s1", "RefreshType": "FULL_REFRESH",
			"ScheduleFrequency": map[string]any{"Interval": "DAILY"},
		}},
	)
	require.Equal(t, http.StatusOK, createRec.Code)
	assert.Equal(t, "s1", parseBody(t, createRec)["ScheduleId"])

	dupRec := doRequest(
		t, h, http.MethodPost, accountPath("/data-sets/ds1/refresh-schedules"),
		map[string]any{"Schedule": map[string]any{"ScheduleId": "s1", "RefreshType": "FULL_REFRESH"}},
	)
	assert.Equal(t, http.StatusConflict, dupRec.Code)

	describeRec := doRequest(t, h, http.MethodGet, accountPath("/data-sets/ds1/refresh-schedules/s1"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	sched := parseBody(t, describeRec)["RefreshSchedule"].(map[string]any)
	assert.Equal(t, "FULL_REFRESH", sched["RefreshType"])

	missingScheduleRec := doRequest(t, h, http.MethodGet, accountPath("/data-sets/ds1/refresh-schedules/notexist"), nil)
	assert.Equal(t, http.StatusNotFound, missingScheduleRec.Code)

	updateRec := doRequest(
		t, h, http.MethodPut, accountPath("/data-sets/ds1/refresh-schedules"),
		map[string]any{"Schedule": map[string]any{"ScheduleId": "s1", "RefreshType": "INCREMENTAL_REFRESH"}},
	)
	require.Equal(t, http.StatusOK, updateRec.Code)

	describeAfterUpdateRec := doRequest(t, h, http.MethodGet, accountPath("/data-sets/ds1/refresh-schedules/s1"), nil)
	assert.Equal(
		t, "INCREMENTAL_REFRESH",
		parseBody(t, describeAfterUpdateRec)["RefreshSchedule"].(map[string]any)["RefreshType"],
	)

	listRec := doRequest(t, h, http.MethodGet, accountPath("/data-sets/ds1/refresh-schedules"), nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	assert.Len(t, parseBody(t, listRec)["RefreshSchedules"].([]any), 1)

	deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/data-sets/ds1/refresh-schedules/s1"), nil)
	require.Equal(t, http.StatusOK, deleteRec.Code)

	deleteMissingRec := doRequest(t, h, http.MethodDelete, accountPath("/data-sets/ds1/refresh-schedules/s1"), nil)
	assert.Equal(t, http.StatusNotFound, deleteMissingRec.Code)
}

// ---- DataSet refresh properties CRUD round-trip and errors ----

func TestQuickSight_DataSetRefreshPropertiesCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createDataSetRec := doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
		"DataSetId": "ds1", "Name": "DataSet1", "ImportMode": "SPICE", "PhysicalTableMap": testPhysicalTableMap(),
	})
	require.Equal(t, http.StatusCreated, createDataSetRec.Code)

	// Not set yet -> 404.
	missingRec := doRequest(t, h, http.MethodGet, accountPath("/data-sets/ds1/refresh-properties"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)

	putRec := doRequest(t, h, http.MethodPut, accountPath("/data-sets/ds1/refresh-properties"), map[string]any{
		"DataSetRefreshProperties": map[string]any{
			"RefreshConfiguration": map[string]any{
				"IncrementalRefresh": map[string]any{"LookbackWindow": map[string]any{"Size": 1, "SizeUnit": "DAY"}},
			},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	describeRec := doRequest(t, h, http.MethodGet, accountPath("/data-sets/ds1/refresh-properties"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	props := parseBody(t, describeRec)["DataSetRefreshProperties"].(map[string]any)
	refreshConfig := props["RefreshConfiguration"].(map[string]any)
	assert.Contains(t, refreshConfig, "IncrementalRefresh")

	deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/data-sets/ds1/refresh-properties"), nil)
	require.Equal(t, http.StatusOK, deleteRec.Code)

	deleteMissingRec := doRequest(t, h, http.MethodDelete, accountPath("/data-sets/ds1/refresh-properties"), nil)
	assert.Equal(t, http.StatusNotFound, deleteMissingRec.Code)

	// Dataset itself missing -> 404 too.
	noDataSetRec := doRequest(t, h, http.MethodGet, accountPath("/data-sets/notexist/refresh-properties"), nil)
	assert.Equal(t, http.StatusNotFound, noDataSetRec.Code)
}

// ---- StartAfterDateTime wire format: real AWS's unixTimestamp protocol
// sends/expects seconds-since-epoch as a JSON number, not a string. ----

func TestQuickSight_RefreshSchedule_StartAfterDateTime(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createDataSetRec := doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
		"DataSetId": "ds1", "Name": "DataSet1", "ImportMode": "SPICE", "PhysicalTableMap": testPhysicalTableMap(),
	})
	require.Equal(t, http.StatusCreated, createDataSetRec.Code)

	const epochSeconds = 1735689600 // 2025-01-01T00:00:00Z

	createRec := doRequest(
		t, h, http.MethodPost, accountPath("/data-sets/ds1/refresh-schedules"),
		map[string]any{"Schedule": map[string]any{
			"ScheduleId": "s1", "RefreshType": "FULL_REFRESH", "StartAfterDateTime": epochSeconds,
		}},
	)
	require.Equal(t, http.StatusOK, createRec.Code)

	describeRec := doRequest(t, h, http.MethodGet, accountPath("/data-sets/ds1/refresh-schedules/s1"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	body := parseBody(t, describeRec)

	// DescribeRefreshScheduleOutput carries a top-level Arn in addition to the
	// nested RefreshSchedule.Arn.
	sched := body["RefreshSchedule"].(map[string]any)
	require.NotEmpty(t, body["Arn"])
	assert.Equal(t, body["Arn"], sched["Arn"])

	got, ok := sched["StartAfterDateTime"].(float64)
	require.True(t, ok, "StartAfterDateTime must round-trip as a JSON number, not a string")
	assert.InDelta(t, epochSeconds, got, 0.001)
}
