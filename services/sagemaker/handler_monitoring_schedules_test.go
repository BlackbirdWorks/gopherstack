package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateMonitoringSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{
		"MonitoringScheduleName": "my-schedule",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["MonitoringScheduleArn"], "my-schedule")
}

func TestHandler_DescribeMonitoringSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-1"})

	rec := doSageMakerRequest(t, h, "DescribeMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "sched-1", resp["MonitoringScheduleName"])
	assert.Equal(t, "Scheduled", resp["MonitoringScheduleStatus"])
}

func TestHandler_StopMonitoringSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-stop"})
	rec := doSageMakerRequest(t, h, "StopMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-stop"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-stop"})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "Stopped", resp["MonitoringScheduleStatus"])
}

func TestHandler_StartMonitoringSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-start"})
	doSageMakerRequest(t, h, "StopMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-start"})
	rec := doSageMakerRequest(t, h, "StartMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-start"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(
		t,
		h,
		"DescribeMonitoringSchedule",
		map[string]any{"MonitoringScheduleName": "sched-start"},
	)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "Scheduled", resp["MonitoringScheduleStatus"])
}

func TestHandler_DeleteMonitoringSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-del"})
	rec := doSageMakerRequest(t, h, "DeleteMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListMonitoringSchedules(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-a"})
	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-b"})

	rec := doSageMakerRequest(t, h, "ListMonitoringSchedules", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["MonitoringScheduleSummaries"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// Workteam
// ---------------------------------------------------------------------------

func TestStopMonitoringSchedule_AlreadyStopped_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{
		"MonitoringScheduleName": "sched-stop-twice",
	})
	doSageMakerRequest(t, h, "StopMonitoringSchedule", map[string]any{
		"MonitoringScheduleName": "sched-stop-twice",
	})

	rec := doSageMakerRequest(t, h, "StopMonitoringSchedule", map[string]any{
		"MonitoringScheduleName": "sched-stop-twice",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestStartMonitoringSchedule_AlreadyScheduled_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{
		"MonitoringScheduleName": "sched-start-running",
	})

	rec := doSageMakerRequest(t, h, "StartMonitoringSchedule", map[string]any{
		"MonitoringScheduleName": "sched-start-running",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
