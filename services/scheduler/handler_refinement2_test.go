package scheduler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/scheduler"
)

// doRESTRequest sends a REST request to the scheduler handler and returns the recorder.
func doRESTRequest(
	t *testing.T,
	h *scheduler.Handler,
	method, path string,
	body any,
	query map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	if len(query) > 0 {
		q := req.URL.Query()
		for k, v := range query {
			q.Set(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// TestRefinement2_TimestampsAreEpochSeconds verifies that GetSchedule returns CreationDate
// and LastModificationDate as JSON numbers (Unix epoch seconds), as required by the AWS SDK.
func TestRefinement2_TimestampsAreEpochSeconds(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "ts-schedule",
		"ScheduleExpression": "rate(1 hour)",
		"Target":             map[string]string{"Arn": "arn:aws:sqs:us-east-1:0:q", "RoleArn": "arn:aws:iam::0:role/r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "ts-schedule"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))

	// CreationDate must be a JSON number (float64), not a string.
	var creationDate float64
	require.NoError(t, json.Unmarshal(out["CreationDate"], &creationDate), "CreationDate must be a number")
	assert.Greater(t, creationDate, float64(0), "CreationDate must be a positive epoch value")

	var lastModDate float64
	require.NoError(
		t,
		json.Unmarshal(out["LastModificationDate"], &lastModDate),
		"LastModificationDate must be a number",
	)
	assert.GreaterOrEqual(t, lastModDate, creationDate)
}

// TestRefinement2_ListSchedulesTimestampsAreEpochSeconds verifies ListSchedules returns epoch seconds.
func TestRefinement2_ListSchedulesTimestampsAreEpochSeconds(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	createScheduleViaHandler(t, h, "list-ts-s", "", "rate(1 minute)")

	rec := doSchedulerRequest(t, h, "ListSchedules", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Schedules []map[string]json.RawMessage `json:"Schedules"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.Schedules)

	var cd float64
	require.NoError(t, json.Unmarshal(out.Schedules[0]["CreationDate"], &cd))
	assert.Greater(t, cd, float64(0))
}

// TestRefinement2_GetScheduleGroupTimestampsAreEpochSeconds verifies GetScheduleGroup returns epoch seconds.
func TestRefinement2_GetScheduleGroupTimestampsAreEpochSeconds(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	rec := doSchedulerRequest(t, h, "GetScheduleGroup", map[string]any{"Name": "default"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	var cd float64
	require.NoError(t, json.Unmarshal(out["CreationDate"], &cd), "CreationDate must be a JSON number")
	assert.Greater(t, cd, float64(0))
}

// TestRefinement2_ListScheduleGroupsTimestampsAreEpochSeconds verifies ListScheduleGroups returns epoch seconds.
func TestRefinement2_ListScheduleGroupsTimestampsAreEpochSeconds(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	rec := doSchedulerRequest(t, h, "ListScheduleGroups", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ScheduleGroups []map[string]json.RawMessage `json:"ScheduleGroups"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.ScheduleGroups)

	var cd float64
	require.NoError(t, json.Unmarshal(out.ScheduleGroups[0]["CreationDate"], &cd))
	assert.Greater(t, cd, float64(0))
}

// TestRefinement2_RESTGetScheduleByPath verifies GET /schedules/{name} works with REST routing.
func TestRefinement2_RESTGetScheduleByPath(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	createScheduleViaHandler(t, h, "rest-sched", "", "rate(5 minutes)")

	rec := doRESTRequest(t, h, http.MethodGet, "/schedules/rest-sched", nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "rest-sched", out["Name"])
}

// TestRefinement2_RESTGetScheduleWithGroupName verifies GET /schedules/{name}?groupName=default.
func TestRefinement2_RESTGetScheduleWithGroupName(t *testing.T) {
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

// TestRefinement2_RESTDeleteScheduleByPath verifies DELETE /schedules/{name} with groupName.
func TestRefinement2_RESTDeleteScheduleByPath(t *testing.T) {
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

// TestRefinement2_RESTUpdateSchedule verifies PUT /schedules/{name} updates the schedule.
func TestRefinement2_RESTUpdateSchedule(t *testing.T) {
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

// TestRefinement2_RESTListSchedulesWithGroupNameFilter verifies GET /schedules?ScheduleGroup=x.
func TestRefinement2_RESTListSchedulesWithGroupNameFilter(t *testing.T) {
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

// TestRefinement2_RESTCreateScheduleByPath verifies POST /schedules/{name} creates a schedule.
func TestRefinement2_RESTCreateScheduleByPath(t *testing.T) {
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

// TestRefinement2_RESTTagResource verifies POST /tags/{resourceArn} tags a resource.
func TestRefinement2_RESTTagResource(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	// Create a schedule and grab its ARN.
	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "tag-via-rest",
		"ScheduleExpression": "rate(1 minute)",
		"Target":             map[string]string{"Arn": "arn:aws:sqs:us-east-1:0:q", "RoleArn": "arn:aws:iam::0:role/r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	schedARN := createOut["ScheduleArn"]
	require.NotEmpty(t, schedARN)

	// Tag via REST path.
	rec2 := doRESTRequest(t, h, http.MethodPost, "/tags/"+schedARN, map[string]any{
		"Tags": map[string]string{"env": "prod", "team": "platform"},
	}, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	// Verify tags via ListTagsForResource.
	rec3 := doSchedulerRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": schedARN})
	require.Equal(t, http.StatusOK, rec3.Code)

	var tagsOut struct {
		Tags map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &tagsOut))
	assert.Equal(t, "prod", tagsOut.Tags["env"])
	assert.Equal(t, "platform", tagsOut.Tags["team"])
}

// TestRefinement2_RESTListTagsForResource verifies GET /tags/{resourceArn}.
func TestRefinement2_RESTListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "list-tags-rest",
		"ScheduleExpression": "rate(1 minute)",
		"Target":             map[string]string{"Arn": "arn:aws:sqs:us-east-1:0:q", "RoleArn": "arn:aws:iam::0:role/r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
		"Tags":               map[string]string{"key1": "val1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	schedARN := createOut["ScheduleArn"]

	// Tag the resource first via the standard operation.
	_ = doSchedulerRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": schedARN,
		"Tags":        map[string]string{"key1": "val1"},
	})

	rec2 := doRESTRequest(t, h, http.MethodGet, "/tags/"+schedARN, nil, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var tagsOut struct {
		Tags map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &tagsOut))
	assert.Equal(t, "val1", tagsOut.Tags["key1"])
}

// TestRefinement2_RESTUntagResource verifies DELETE /tags/{resourceArn}?tagKeys=...
func TestRefinement2_RESTUntagResource(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "untag-rest",
		"ScheduleExpression": "rate(1 minute)",
		"Target":             map[string]string{"Arn": "arn:aws:sqs:us-east-1:0:q", "RoleArn": "arn:aws:iam::0:role/r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	schedARN := createOut["ScheduleArn"]

	_ = doSchedulerRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": schedARN,
		"Tags":        map[string]string{"key1": "val1", "key2": "val2"},
	})

	rec2 := doRESTRequest(t, h, http.MethodDelete, "/tags/"+schedARN, nil, map[string]string{
		"tagKeys": "key1",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	rec3 := doSchedulerRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": schedARN})
	var tagsOut struct {
		Tags map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &tagsOut))
	assert.NotContains(t, tagsOut.Tags, "key1")
	assert.Equal(t, "val2", tagsOut.Tags["key2"])
}

// TestRefinement2_RESTNotFound verifies unknown REST path returns 404.
func TestRefinement2_RESTNotFound(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	rec := doRESTRequest(t, h, http.MethodPatch, "/schedules/bad", nil, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement2_ValidateState_CreateSchedule verifies invalid state is rejected.
func TestRefinement2_ValidateState_CreateSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		state   string
		wantErr bool
	}{
		{name: "enabled", state: "ENABLED", wantErr: false},
		{name: "disabled", state: "DISABLED", wantErr: false},
		{name: "empty_defaults_enabled", state: "", wantErr: false},
		{name: "invalid", state: "RUNNING", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)
			body := map[string]any{
				"Name":               "state-test",
				"ScheduleExpression": "rate(1 minute)",
				"Target": map[string]string{
					"Arn":     "arn:aws:sqs:us-east-1:0:q",
					"RoleArn": "arn:aws:iam::0:role/r",
				},
				"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
			}

			if tt.state != "" {
				body["State"] = tt.state
			}

			rec := doSchedulerRequest(t, h, "CreateSchedule", body)
			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

// TestRefinement2_ValidateFlexibleTimeWindowMode verifies invalid mode is rejected
// and FLEXIBLE mode requires MaximumWindowInMinutes > 0.
func TestRefinement2_ValidateFlexibleTimeWindowMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ftw     map[string]any
		name    string
		wantErr bool
	}{
		{name: "off", ftw: map[string]any{"Mode": "OFF"}, wantErr: false},
		{name: "flexible", ftw: map[string]any{"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 15}, wantErr: false},
		{name: "flexible_no_window", ftw: map[string]any{"Mode": "FLEXIBLE"}, wantErr: true},
		{name: "invalid", ftw: map[string]any{"Mode": "STRICT"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)
			rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
				"Name":               "mode-test-" + tt.name,
				"ScheduleExpression": "rate(1 minute)",
				"Target": map[string]string{
					"Arn":     "arn:aws:sqs:us-east-1:0:q",
					"RoleArn": "arn:aws:iam::0:role/r",
				},
				"FlexibleTimeWindow": tt.ftw,
			})
			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

// TestRefinement2_DeleteScheduleGroupCascade verifies schedules are deleted when their group is deleted.
func TestRefinement2_DeleteScheduleGroupCascade(t *testing.T) {
	t.Parallel()

	b := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	h := scheduler.NewHandler(b)

	// Create a group and schedules within it.
	_, err := b.CreateScheduleGroup(context.Background(), "grp-cascade", "", nil)
	require.NoError(t, err)

	createScheduleViaHandler(t, h, "s1", "grp-cascade", "rate(1 minute)")
	createScheduleViaHandler(t, h, "s2", "grp-cascade", "rate(2 minutes)")
	assert.Equal(t, 2, scheduler.ScheduleCount(b))

	// Delete the group - all schedules in the group should cascade-delete.
	rec := doSchedulerRequest(t, h, "DeleteScheduleGroup", map[string]any{"Name": "grp-cascade"})
	require.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, 0, scheduler.ScheduleCount(b))
	assert.Equal(t, 0, scheduler.ScheduleGroupCount(b)-1) // default group remains
}

// TestRefinement2_DeleteDefaultGroupForbidden verifies the default group cannot be deleted.
func TestRefinement2_DeleteDefaultGroupForbidden(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	rec := doSchedulerRequest(t, h, "DeleteScheduleGroup", map[string]any{"Name": "default"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement2_ActionAfterCompletion verifies the ActionAfterCompletion field is persisted.
func TestRefinement2_ActionAfterCompletion(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "aac-sched",
		"ScheduleExpression": "rate(1 minute)",
		"Target": map[string]string{
			"Arn":     "arn:aws:sqs:us-east-1:0:q",
			"RoleArn": "arn:aws:iam::0:role/r",
		},
		"FlexibleTimeWindow":    map[string]string{"Mode": "OFF"},
		"ActionAfterCompletion": "DELETE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "aac-sched"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
	assert.Equal(t, "DELETE", out["ActionAfterCompletion"])
}

// TestRefinement2_KmsKeyArn verifies the KmsKeyArn field is persisted.
func TestRefinement2_KmsKeyArn(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	kmsARN := "arn:aws:kms:us-east-1:000000000000:key/12345678-abcd-ef01-2345-6789abcdef01"

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "kms-sched",
		"ScheduleExpression": "rate(1 minute)",
		"Target":             map[string]string{"Arn": "arn:aws:sqs:us-east-1:0:q", "RoleArn": "arn:aws:iam::0:role/r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
		"KmsKeyArn":          kmsARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "kms-sched"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
	assert.Equal(t, kmsARN, out["KmsKeyArn"])
}

// TestRefinement2_StartDateEndDate verifies optional StartDate and EndDate on schedules.
func TestRefinement2_StartDateEndDate(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	startEpoch := float64(time.Now().Unix())
	endEpoch := float64(time.Now().Add(24 * time.Hour).Unix())

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "dated-sched",
		"ScheduleExpression": "rate(1 minute)",
		"Target":             map[string]string{"Arn": "arn:aws:sqs:us-east-1:0:q", "RoleArn": "arn:aws:iam::0:role/r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
		"StartDate":          startEpoch,
		"EndDate":            endEpoch,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "dated-sched"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))

	_, hasStart := out["StartDate"]
	_, hasEnd := out["EndDate"]
	assert.True(t, hasStart, "StartDate should be present in GetSchedule response")
	assert.True(t, hasEnd, "EndDate should be present in GetSchedule response")
}

// TestRefinement2_Persistence_NewFields verifies new fields survive Snapshot/Restore.
func TestRefinement2_Persistence_NewFields(t *testing.T) {
	t.Parallel()

	b := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	kmsARN := "arn:aws:kms:us-east-1:000000000000:key/abc"

	_, err := b.CreateSchedule(
		context.Background(),
		"persist-sched", "", "rate(1 minute)", "desc", "",
		scheduler.Target{ARN: "arn:aws:sqs:us-east-1:0:q", RoleARN: "arn:aws:iam::0:role/r"},
		"ENABLED",
		scheduler.FlexibleTimeWindow{Mode: "OFF"},
		scheduler.WithActionAfterCompletion("DELETE"),
		scheduler.WithKmsKeyArn(kmsARN),
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	s, err := b2.GetSchedule(context.Background(), "persist-sched", "default")
	require.NoError(t, err)
	assert.Equal(t, "DELETE", s.ActionAfterCompletion)
	assert.Equal(t, kmsARN, s.KmsKeyArn)
}

// TestRefinement2_GetScheduleGroupIncludesTags verifies GetScheduleGroup includes Tags.
func TestRefinement2_GetScheduleGroupIncludesTags(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	rec := doSchedulerRequest(t, h, "CreateScheduleGroup", map[string]any{
		"Name": "grp-with-tags",
		"Tags": map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSchedulerRequest(t, h, "GetScheduleGroup", map[string]any{"Name": "grp-with-tags"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out struct {
		Tags map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
	assert.Equal(t, "test", out.Tags["env"])
}

// TestRefinement2_ListScheduleGroupsIncludesTags verifies ListScheduleGroups includes Tags.
func TestRefinement2_ListScheduleGroupsIncludesTags(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	rec := doSchedulerRequest(t, h, "CreateScheduleGroup", map[string]any{
		"Name": "list-grp-tags",
		"Tags": map[string]string{"x": "y"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSchedulerRequest(t, h, "ListScheduleGroups", map[string]any{})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out struct {
		ScheduleGroups []struct {
			Tags map[string]string `json:"Tags"`
			Name string            `json:"Name"`
		} `json:"ScheduleGroups"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))

	for _, g := range out.ScheduleGroups {
		if g.Name == "list-grp-tags" {
			assert.Equal(t, "y", g.Tags["x"])

			return
		}
	}
	t.Fatal("group list-grp-tags not found in ListScheduleGroups")
}

// TestRefinement2_UpdateScheduleValidatesState verifies invalid state is rejected on update.
func TestRefinement2_UpdateScheduleValidatesState(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	createScheduleViaHandler(t, h, "upd-state", "", "rate(1 minute)")

	rec := doSchedulerRequest(t, h, "UpdateSchedule", map[string]any{
		"Name":               "upd-state",
		"ScheduleExpression": "rate(5 minutes)",
		"Target":             map[string]string{"Arn": "arn:aws:sqs:us-east-1:0:q", "RoleArn": "arn:aws:iam::0:role/r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
		"State":              "INVALID",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement2_RouteMatcherHandlesTagsPath verifies /tags/{arn} is routed to scheduler.
func TestRefinement2_RouteMatcherHandlesTagsPath(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	m := h.RouteMatcher()

	e := echo.New()

	makeCtx := func(path string) *echo.Context {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		return c
	}

	assert.True(t, m(makeCtx("/schedules")), "/schedules should match")
	assert.True(t, m(makeCtx("/schedule-groups/foo")), "/schedule-groups/foo should match")
	assert.True(
		t,
		m(makeCtx("/tags/arn:aws:scheduler:us-east-1:123:schedule/default/my-sched")),
		"/tags/arn:aws:scheduler:... should match",
	)
	assert.True(
		t,
		m(makeCtx("/tags/arn:aws:scheduler:us-east-1:000000000000:schedule-group/default")),
		"/tags/arn:aws:scheduler:...:schedule-group/... should match",
	)
	// Non-scheduler ARNs must NOT be claimed by the Scheduler route matcher to
	// avoid intercepting tag requests destined for other services (QLDB, Pipes, FIS, etc.).
	assert.False(
		t,
		m(makeCtx("/tags/arn:aws:qldb:us-east-1:000000000000:ledger/my-ledger")),
		"/tags/arn:aws:qldb:... should NOT match",
	)
	assert.False(
		t,
		m(makeCtx("/tags/arn:aws:pipes:us-east-1:000000000000:pipe/my-pipe")),
		"/tags/arn:aws:pipes:... should NOT match",
	)
	assert.False(
		t,
		m(makeCtx("/tags/arn:aws:fis:us-east-1:000000000000:experiment-template/abc")),
		"/tags/arn:aws:fis:... should NOT match",
	)
	assert.False(t, m(makeCtx("/tags")), "bare /tags should NOT match")
	assert.False(t, m(makeCtx("/other/path")), "/other/path should not match")
}

// TestRefinement2_ExtractResourceTagsPath verifies ExtractResource returns ARN for /tags/{arn}.
func TestRefinement2_ExtractResourceTagsPath(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	e := echo.New()
	arn := "arn:aws:scheduler:us-east-1:000000000000:schedule/default/my-sched"

	req := httptest.NewRequest(http.MethodGet, "/tags/"+arn, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	resource := h.ExtractResource(c)
	assert.Equal(t, arn, resource)
}

// TestRefinement2_ListSchedulesWithNextTokenPresent verifies NextToken is omitted when all results fit.
func TestRefinement2_ListSchedulesWithNextTokenPresent(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	createScheduleViaHandler(t, h, "nt-s1", "", "rate(1 minute)")

	rec := doSchedulerRequest(t, h, "ListSchedules", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		NextToken string `json:"NextToken"`
		Schedules []any  `json:"Schedules"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	// NextToken should be empty when all results fit.
	assert.Empty(t, out.NextToken)
}

// TestRefinement2_ScheduleGroupNotFound verifies GetScheduleGroup returns 404 for unknown group.
func TestRefinement2_ScheduleGroupNotFound(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	rec := doSchedulerRequest(t, h, "GetScheduleGroup", map[string]any{"Name": "no-such-group"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement2_FlexibleTimeWindowMaximumWindowInMinutes verifies MaximumWindowInMinutes is persisted.
func TestRefinement2_FlexibleTimeWindowMaximumWindowInMinutes(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "ftw-sched",
		"ScheduleExpression": "rate(1 minute)",
		"Target":             map[string]string{"Arn": "arn:aws:sqs:us-east-1:0:q", "RoleArn": "arn:aws:iam::0:role/r"},
		"FlexibleTimeWindow": map[string]any{"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 15},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "ftw-sched"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out struct {
		FlexibleTimeWindow struct {
			Mode                   string `json:"Mode"`
			MaximumWindowInMinutes int    `json:"MaximumWindowInMinutes"`
		} `json:"FlexibleTimeWindow"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
	assert.Equal(t, "FLEXIBLE", out.FlexibleTimeWindow.Mode)
	assert.Equal(t, 15, out.FlexibleTimeWindow.MaximumWindowInMinutes)
}

// TestRefinement2_UpdateScheduleWithActionAfterCompletion verifies ActionAfterCompletion on update.
func TestRefinement2_UpdateScheduleWithActionAfterCompletion(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	createScheduleViaHandler(t, h, "upd-aac", "", "rate(1 minute)")

	rec := doSchedulerRequest(t, h, "UpdateSchedule", map[string]any{
		"Name":               "upd-aac",
		"ScheduleExpression": "rate(5 minutes)",
		"Target": map[string]string{
			"Arn":     "arn:aws:sqs:us-east-1:0:q",
			"RoleArn": "arn:aws:iam::0:role/r",
		},
		"FlexibleTimeWindow":    map[string]string{"Mode": "OFF"},
		"State":                 "ENABLED",
		"ActionAfterCompletion": "NONE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "upd-aac"})
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
	assert.Equal(t, "NONE", out["ActionAfterCompletion"])
}

// TestRefinement2_RESTListScheduleGroups verifies GET /schedule-groups returns groups.
func TestRefinement2_RESTListScheduleGroups(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	rec := doRESTRequest(t, h, http.MethodGet, "/schedule-groups", nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ScheduleGroups []map[string]any `json:"ScheduleGroups"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.ScheduleGroups, "default group should always be present")
}

// TestRefinement2_EpochSeconds_ConvertBack verifies epochSecondsToTime converts correctly.
func TestRefinement2_EpochSeconds_ConvertBack(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	now := time.Now().UTC().Truncate(time.Second)
	epoch := float64(now.Unix())

	// Store and retrieve to test round-trip through epoch conversion.
	rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "epoch-test",
		"ScheduleExpression": "rate(1 minute)",
		"Target":             map[string]string{"Arn": "arn:aws:sqs:us-east-1:0:q", "RoleArn": "arn:aws:iam::0:role/r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
		"StartDate":          epoch,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "epoch-test"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))

	var startDate float64
	require.NoError(t, json.Unmarshal(out["StartDate"], &startDate))
	// The stored epoch should round-trip back to the same second.
	assert.InDelta(t, epoch, startDate, 1.0)
}

// TestRefinement2_MissingRequiredFields verifies required field errors.
func TestRefinement2_MissingRequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "missing_name",
			body: map[string]any{
				"ScheduleExpression": "rate(1 minute)",
				"Target": map[string]string{
					"Arn":     "arn:aws:sqs:us-east-1:0:q",
					"RoleArn": "arn:aws:iam::0:role/r",
				},
				"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
			},
		},
		{
			name: "missing_expression",
			body: map[string]any{
				"Name": "no-expr",
				"Target": map[string]string{
					"Arn":     "arn:aws:sqs:us-east-1:0:q",
					"RoleArn": "arn:aws:iam::0:role/r",
				},
				"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
			},
		},
		{
			name: "missing_target_arn",
			body: map[string]any{
				"Name":               "no-tgt-arn",
				"ScheduleExpression": "rate(1 minute)",
				"Target":             map[string]string{"RoleArn": "arn:aws:iam::0:role/r"},
				"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)
			rec := doSchedulerRequest(t, h, "CreateSchedule", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}
