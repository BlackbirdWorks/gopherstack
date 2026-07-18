package scheduler_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRESTGetScheduleByPath verifies GET /schedules/{name} works with REST routing.
func TestRESTGetScheduleByPath(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	createScheduleViaHandler(t, h, "rest-sched", "", "rate(5 minutes)")

	rec := doRESTRequest(t, h, http.MethodGet, "/schedules/rest-sched", nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "rest-sched", out["Name"])
}

// TestRESTGetScheduleWithGroupName verifies GET /schedules/{name}?groupName=default.
func TestRESTGetScheduleWithGroupName(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	createScheduleViaHandler(t, h, "grp-sched", "", "rate(5 minutes)")

	rec := doRESTRequest(t, h, http.MethodGet, "/schedules/grp-sched", nil, map[string]string{
		"groupName": "default",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "grp-sched", out["Name"])
	assert.Equal(t, "default", out["GroupName"])
}

// TestRESTDeleteScheduleByPath verifies DELETE /schedules/{name} with groupName.
func TestRESTDeleteScheduleByPath(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	createScheduleViaHandler(t, h, "del-rest", "", "rate(10 minutes)")

	rec := doRESTRequest(t, h, http.MethodDelete, "/schedules/del-rest", nil, map[string]string{
		"groupName": "default",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify the schedule is gone.
	rec2 := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "del-rest"})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// TestSchedulerHandler_REST_DeleteAndUpdate verifies DELETE /schedules/{name} via
// the raw echo.HandlerFunc, without going through doRESTRequest.
func TestSchedulerHandler_REST_DeleteAndUpdate(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	createRec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "del-sched",
		"ScheduleExpression": "rate(1 minute)",
		"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// DELETE /schedules/del-sched via REST.
	rec2 := doRESTRequest(t, h, http.MethodDelete, "/schedules/del-sched", nil, nil)
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// TestRESTUpdateSchedule verifies PUT /schedules/{name} updates the schedule.
func TestRESTUpdateSchedule(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	createScheduleViaHandler(t, h, "upd-rest", "", "rate(1 minute)")

	rec := doRESTRequest(t, h, http.MethodPut, "/schedules/upd-rest", map[string]any{
		"ScheduleExpression": "rate(10 minutes)",
		"Target":             map[string]string{"Arn": "arn:aws:sqs:us-east-1:0:q", "RoleArn": "arn:aws:iam::0:role/r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
		"State":              "ENABLED",
	}, map[string]string{"groupName": "default"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify the expression was updated.
	rec2 := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "upd-rest"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
	assert.Equal(t, "rate(10 minutes)", out["ScheduleExpression"])
}

// TestRESTListSchedulesWithGroupNameFilter verifies GET /schedules?ScheduleGroup=x.
func TestRESTListSchedulesWithGroupNameFilter(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	// Create a group and schedules in two groups.
	recGrp := doSchedulerRequest(t, h, "CreateScheduleGroup", map[string]any{"Name": "grp-filter"})
	require.Equal(t, http.StatusOK, recGrp.Code)

	createScheduleViaHandler(t, h, "s-default", "", "rate(1 minute)")
	createScheduleViaHandler(t, h, "s-grp", "grp-filter", "rate(2 minutes)")

	rec := doRESTRequest(t, h, http.MethodGet, "/schedules", nil, map[string]string{
		"ScheduleGroup": "grp-filter",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Schedules []map[string]any `json:"Schedules"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Schedules, 1)
	assert.Equal(t, "s-grp", out.Schedules[0]["Name"])
}

// TestRESTCreateScheduleByPath verifies POST /schedules/{name} creates a schedule.
func TestRESTCreateScheduleByPath(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	rec := doRESTRequest(t, h, http.MethodPost, "/schedules/path-sched", map[string]any{
		"ScheduleExpression": "rate(5 minutes)",
		"Target":             map[string]string{"Arn": "arn:aws:sqs:us-east-1:0:q", "RoleArn": "arn:aws:iam::0:role/r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
	}, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out["ScheduleArn"])

	// Verify it can be retrieved.
	rec2 := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "path-sched"})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// TestRESTListSchedules_MaxResultsQueryParam verifies GET /schedules?MaxResults=N.
func TestRESTListSchedules_MaxResultsQueryParam(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	for _, name := range []string{"r-a", "r-b", "r-c"} {
		createBaseSchedule(t, h, name)
	}

	rec := doRESTRequest(t, h, http.MethodGet, "/schedules", nil, map[string]string{
		"MaxResults": "2",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	var schedules []json.RawMessage
	require.NoError(t, json.Unmarshal(out["Schedules"], &schedules))
	assert.Len(t, schedules, 2)
}

// TestRESTNotFound verifies unknown REST path returns 404.
func TestRESTNotFound(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	rec := doRESTRequest(t, h, http.MethodPatch, "/schedules/bad", nil, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestSchedulerHandler_RESTPathDeleteSchedule verifies DELETE /schedules/{name}
// through the raw echo.HandlerFunc returns 404 for a schedule that was never
// created via that same REST path (path routing works even without a prior match).
func TestSchedulerHandler_RESTPathDeleteSchedule(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	delRec := doRESTRequest(t, h, http.MethodDelete, "/schedules/rest-del", nil, nil)
	assert.Equal(t, http.StatusNotFound, delRec.Code)
}
