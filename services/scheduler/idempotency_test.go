package scheduler_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateSchedule_ClientToken_ReplaysOnRetry verifies that retrying a
// CreateSchedule call with the same Name/GroupName/ClientToken (simulating a
// client resending a request whose response was lost in transit) replays the
// original ScheduleArn instead of failing with ConflictException on the
// now-existing name.
func TestCreateSchedule_ClientToken_ReplaysOnRetry(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	req := map[string]any{
		"Name":               "idem-sched",
		"ScheduleExpression": "rate(1 hour)",
		"Target":             map[string]string{"Arn": "arn:aws:sqs:us-east-1:0:q", "RoleArn": "arn:aws:iam::0:role/r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
		"ClientToken":        "retry-token-1",
	}

	rec1 := doSchedulerRequest(t, h, "CreateSchedule", req)
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]string
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))

	rec2 := doSchedulerRequest(t, h, "CreateSchedule", req)
	require.Equal(t, http.StatusOK, rec2.Code, "retry with the same ClientToken must replay, not 409")

	var out2 map[string]string
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))

	assert.Equal(t, out1["ScheduleArn"], out2["ScheduleArn"])

	// Exactly one schedule was actually created by the backend.
	rec3 := doSchedulerRequest(t, h, "ListSchedules", map[string]any{})
	require.Equal(t, http.StatusOK, rec3.Code)

	var list struct {
		Schedules []map[string]any `json:"Schedules"`
	}
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &list))
	assert.Len(t, list.Schedules, 1)
}

// TestCreateSchedule_NoClientToken_DuplicateNameStillConflicts verifies idempotent
// replay is scoped strictly to a matching ClientToken: creating the same name
// twice without one still returns ConflictException, matching pre-existing
// name-uniqueness semantics.
func TestCreateSchedule_NoClientToken_DuplicateNameStillConflicts(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	req := map[string]any{
		"Name":               "dup-sched",
		"ScheduleExpression": "rate(1 hour)",
		"Target":             map[string]string{"Arn": "arn:aws:sqs:us-east-1:0:q", "RoleArn": "arn:aws:iam::0:role/r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
	}

	rec1 := doSchedulerRequest(t, h, "CreateSchedule", req)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doSchedulerRequest(t, h, "CreateSchedule", req)
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

// TestCreateSchedule_DifferentClientToken_DuplicateNameStillConflicts verifies that
// a *different* ClientToken on a colliding name is treated as a genuine conflict,
// not a replay.
func TestCreateSchedule_DifferentClientToken_DuplicateNameStillConflicts(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	base := map[string]any{
		"Name":               "dup-sched-2",
		"ScheduleExpression": "rate(1 hour)",
		"Target":             map[string]string{"Arn": "arn:aws:sqs:us-east-1:0:q", "RoleArn": "arn:aws:iam::0:role/r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
	}

	req1 := map[string]any{"ClientToken": "token-a"}
	maps.Copy(req1, base)

	req2 := map[string]any{"ClientToken": "token-b"}
	maps.Copy(req2, base)

	rec1 := doSchedulerRequest(t, h, "CreateSchedule", req1)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doSchedulerRequest(t, h, "CreateSchedule", req2)
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

// TestCreateScheduleGroup_ClientToken_ReplaysOnRetry mirrors the CreateSchedule
// idempotency behaviour for schedule groups.
func TestCreateScheduleGroup_ClientToken_ReplaysOnRetry(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	req := map[string]any{
		"Name":        "idem-group",
		"ClientToken": "group-retry-token",
	}

	rec1 := doSchedulerRequest(t, h, "CreateScheduleGroup", req)
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]string
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))

	rec2 := doSchedulerRequest(t, h, "CreateScheduleGroup", req)
	require.Equal(t, http.StatusOK, rec2.Code, "retry with the same ClientToken must replay, not 409")

	var out2 map[string]string
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))

	assert.Equal(t, out1["ScheduleGroupArn"], out2["ScheduleGroupArn"])
}

// TestSchedulerHandler_Reset_ClearsIdempotencyCache lives in whitebox_test.go:
// it needs direct access to the unexported idempotency cache.
