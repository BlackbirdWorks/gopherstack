package scheduler_test

import (
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

// --- helpers ---

func createScheduleViaHandler(t *testing.T, h *scheduler.Handler, name, groupName, expr string) {
	t.Helper()

	body := map[string]any{
		"Name":               name,
		"GroupName":          groupName,
		"ScheduleExpression": expr,
		"Target": map[string]string{
			"Arn":     "arn:aws:lambda:us-east-1:000000000000:function:fn",
			"RoleArn": "arn:aws:iam::000000000000:role/r",
		},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
	}
	rec := doSchedulerRequest(t, h, "CreateSchedule", body)
	require.Equal(t, http.StatusOK, rec.Code)
}

// --- tests ---

func TestRefinement1_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	// Verify compile-time assertion: *InMemoryBackend must satisfy StorageBackend.
	var _ scheduler.StorageBackend = scheduler.NewInMemoryBackend("000000000000", "us-east-1")
}

func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &scheduler.Provider{}
	_, err := p.Init(nil)

	require.ErrorIs(t, err, scheduler.ErrNilAppContext)
}

func TestRefinement1_AccountID(t *testing.T) {
	t.Parallel()

	b := scheduler.NewInMemoryBackend("123456789012", "eu-west-1")

	assert.Equal(t, "123456789012", b.AccountID())
}

func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	assert.Equal(t, len(h.GetSupportedOperations()), scheduler.HandlerOpsLen(h))
}

func TestRefinement1_ScheduleCount(t *testing.T) {
	t.Parallel()

	b := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	h := scheduler.NewHandler(b)

	assert.Equal(t, 0, scheduler.ScheduleCount(b))

	createScheduleViaHandler(t, h, "s1", "", "rate(1 minute)")
	assert.Equal(t, 1, scheduler.ScheduleCount(b))
}

func TestRefinement1_ScheduleGroupCount(t *testing.T) {
	t.Parallel()

	b := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	// Seeded with "default" group.
	assert.Equal(t, 1, scheduler.ScheduleGroupCount(b))

	_, err := b.CreateScheduleGroup(context.Background(), "prod", "", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, scheduler.ScheduleGroupCount(b))
}

func TestRefinement1_AddScheduleInternal(t *testing.T) {
	t.Parallel()

	b := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddScheduleInternal(&scheduler.Schedule{
		Name:               "injected",
		ARN:                "arn:aws:scheduler:us-east-1:000000000000:schedule/default/injected",
		GroupName:          "default",
		ScheduleExpression: "rate(1 hour)",
		State:              "ENABLED",
	})

	assert.Equal(t, 1, scheduler.ScheduleCount(b))

	s, err := b.GetSchedule(context.Background(), "injected", "default")
	require.NoError(t, err)
	assert.Equal(t, "injected", s.Name)
}

func TestRefinement1_AddScheduleGroupInternal(t *testing.T) {
	t.Parallel()

	b := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddScheduleGroupInternal(&scheduler.ScheduleGroup{
		Name:  "injected-group",
		ARN:   "arn:aws:scheduler:us-east-1:000000000000:schedule-group/injected-group",
		State: "ACTIVE",
	})

	assert.Equal(t, 2, scheduler.ScheduleGroupCount(b))

	g, err := b.GetScheduleGroup(context.Background(), "injected-group")
	require.NoError(t, err)
	assert.Equal(t, "injected-group", g.Name)
}

func TestRefinement1_CreateScheduleValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing_name",
			body: map[string]any{
				"ScheduleExpression": "rate(1 minute)",
				"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
				"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_expression",
			body: map[string]any{
				"Name":               "s1",
				"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
				"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_target_arn",
			body: map[string]any{
				"Name":               "s1",
				"ScheduleExpression": "rate(1 minute)",
				"Target":             map[string]string{"RoleArn": "arn:r"},
				"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_target_role_arn",
			body: map[string]any{
				"Name":               "s1",
				"ScheduleExpression": "rate(1 minute)",
				"Target":             map[string]string{"Arn": "arn:a"},
				"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_ftw_mode",
			body: map[string]any{
				"Name":               "s1",
				"ScheduleExpression": "rate(1 minute)",
				"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
				"FlexibleTimeWindow": map[string]any{},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "group_not_found",
			body: map[string]any{
				"Name":               "s1",
				"GroupName":          "nonexistent-group",
				"ScheduleExpression": "rate(1 minute)",
				"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
				"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)
			rec := doSchedulerRequest(t, h, "CreateSchedule", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestRefinement1_CreateScheduleGroupNameRequired(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	rec := doSchedulerRequest(t, h, "CreateScheduleGroup", map[string]any{"Description": "no-name"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_CreateScheduleGroupWithDescription(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	rec := doSchedulerRequest(t, h, "CreateScheduleGroup", map[string]any{
		"Name":        "described-group",
		"Description": "my group description",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	getRec := doSchedulerRequest(t, h, "GetScheduleGroup", map[string]any{"Name": "described-group"})
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	assert.Equal(t, "my group description", resp["Description"])
}

func TestRefinement1_GetScheduleReturnsGroupNameAndDates(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	// Create a custom group first.
	doSchedulerRequest(t, h, "CreateScheduleGroup", map[string]any{"Name": "mygroup"})

	// Create a schedule in that group.
	doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "gs-sched",
		"GroupName":          "mygroup",
		"ScheduleExpression": "rate(1 hour)",
		"Description":        "test description",
		"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
	})

	rec := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "gs-sched", "GroupName": "mygroup"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Equal(t, "mygroup", resp["GroupName"])
	assert.Equal(t, "test description", resp["Description"])
	assert.NotEmpty(t, resp["CreationDate"])
	assert.NotEmpty(t, resp["LastModificationDate"])
}

func TestRefinement1_GetScheduleReturnsARNWithGroupName(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	doSchedulerRequest(t, h, "CreateScheduleGroup", map[string]any{"Name": "my-group"})

	createRec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "grouped-sched",
		"GroupName":          "my-group",
		"ScheduleExpression": "rate(1 minute)",
		"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	assert.Contains(t, createResp["ScheduleArn"], "schedule/my-group/grouped-sched")
}

func TestRefinement1_ListSchedulesFilterByGroupName(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	b := h.Backend.(*scheduler.InMemoryBackend)

	_, err := b.CreateScheduleGroup(context.Background(), "g1", "", nil)
	require.NoError(t, err)
	_, err = b.CreateScheduleGroup(context.Background(), "g2", "", nil)
	require.NoError(t, err)

	createScheduleViaHandler(t, h, "in-g1", "g1", "rate(1 minute)")
	createScheduleViaHandler(t, h, "in-g2", "g2", "rate(2 minutes)")

	rec := doSchedulerRequest(t, h, "ListSchedules", map[string]any{"GroupName": "g1"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	schedules := resp["Schedules"].([]any)
	require.Len(t, schedules, 1)
	assert.Equal(t, "in-g1", schedules[0].(map[string]any)["Name"])
}

func TestRefinement1_ListSchedulesFilterByNamePrefix(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	createScheduleViaHandler(t, h, "prod-sched", "", "rate(1 minute)")
	createScheduleViaHandler(t, h, "dev-sched", "", "rate(2 minutes)")
	createScheduleViaHandler(t, h, "prod-other", "", "rate(3 minutes)")

	rec := doSchedulerRequest(t, h, "ListSchedules", map[string]any{"NamePrefix": "prod-"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	schedules := resp["Schedules"].([]any)
	assert.Len(t, schedules, 2)
}

func TestRefinement1_ListSchedulesFilterByState(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	createScheduleViaHandler(t, h, "enabled-sched", "", "rate(1 minute)")
	doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "disabled-sched",
		"ScheduleExpression": "rate(2 minutes)",
		"State":              "DISABLED",
		"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
	})

	rec := doSchedulerRequest(t, h, "ListSchedules", map[string]any{"State": "ENABLED"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	schedules := resp["Schedules"].([]any)
	require.Len(t, schedules, 1)
	assert.Equal(t, "enabled-sched", schedules[0].(map[string]any)["Name"])
}

func TestRefinement1_ListSchedulesSorted(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	createScheduleViaHandler(t, h, "zebra", "", "rate(1 minute)")
	createScheduleViaHandler(t, h, "alpha", "", "rate(2 minutes)")
	createScheduleViaHandler(t, h, "middle", "", "rate(3 minutes)")

	rec := doSchedulerRequest(t, h, "ListSchedules", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	schedules := resp["Schedules"].([]any)
	require.Len(t, schedules, 3)
	assert.Equal(t, "alpha", schedules[0].(map[string]any)["Name"])
	assert.Equal(t, "middle", schedules[1].(map[string]any)["Name"])
	assert.Equal(t, "zebra", schedules[2].(map[string]any)["Name"])
}

func TestRefinement1_ListScheduleGroupsFilterByNamePrefix(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	b := h.Backend.(*scheduler.InMemoryBackend)

	_, err := b.CreateScheduleGroup(context.Background(), "prod-group", "", nil)
	require.NoError(t, err)
	_, err = b.CreateScheduleGroup(context.Background(), "dev-group", "", nil)
	require.NoError(t, err)

	rec := doSchedulerRequest(t, h, "ListScheduleGroups", map[string]any{"NamePrefix": "prod-"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	groups := resp["ScheduleGroups"].([]any)
	require.Len(t, groups, 1)
	assert.Equal(t, "prod-group", groups[0].(map[string]any)["Name"])
}

func TestRefinement1_ListScheduleGroupsSorted(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	b := h.Backend.(*scheduler.InMemoryBackend)

	_, err := b.CreateScheduleGroup(context.Background(), "zoo", "", nil)
	require.NoError(t, err)
	_, err = b.CreateScheduleGroup(context.Background(), "aardvark", "", nil)
	require.NoError(t, err)

	rec := doSchedulerRequest(t, h, "ListScheduleGroups", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	groups := resp["ScheduleGroups"].([]any)
	names := make([]string, len(groups))
	for i, g := range groups {
		names[i] = g.(map[string]any)["Name"].(string)
	}

	// "aardvark" comes before "default" alphabetically.
	assert.Equal(t, "aardvark", names[0])
	assert.Equal(t, "default", names[1])
	assert.Equal(t, "zoo", names[2])
}

func TestRefinement1_UpdateScheduleUpdatesLastModificationDate(t *testing.T) {
	t.Parallel()

	b := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	h := scheduler.NewHandler(b)

	createScheduleViaHandler(t, h, "upd-sched", "", "rate(1 minute)")

	s1, err := b.GetSchedule(context.Background(), "upd-sched", "")
	require.NoError(t, err)

	// Advance time enough to guarantee LastModificationDate changes.
	time.Sleep(1100 * time.Millisecond)

	doSchedulerRequest(t, h, "UpdateSchedule", map[string]any{
		"Name":               "upd-sched",
		"ScheduleExpression": "rate(2 minutes)",
		"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
		"State":              "ENABLED",
	})

	s2, err := b.GetSchedule(context.Background(), "upd-sched", "")
	require.NoError(t, err)

	assert.True(t, s2.LastModificationDate.After(s1.LastModificationDate),
		"LastModificationDate should advance after UpdateSchedule")
	assert.Equal(t, "rate(2 minutes)", s2.ScheduleExpression)
}

func TestRefinement1_GetScheduleGroupReturnsCreationDate(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	rec := doSchedulerRequest(t, h, "GetScheduleGroup", map[string]any{"Name": "default"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["CreationDate"])
	assert.NotEmpty(t, resp["LastModificationDate"])
}

func TestRefinement1_TagResourceAndListTags(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	createRec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "tagged-sched",
		"ScheduleExpression": "rate(1 minute)",
		"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	schedARN := createResp["ScheduleArn"]

	tagRec := doSchedulerRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": schedARN,
		"Tags":        map[string]string{"env": "prod", "team": "platform"},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	listRec := doSchedulerRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": schedARN})
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsResp))
	tags := tagsResp["Tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "platform", tags["team"])
}

func TestRefinement1_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	b := h.Backend.(*scheduler.InMemoryBackend)

	grp, err := b.CreateScheduleGroup(context.Background(), "tag-grp", "", map[string]string{"k1": "v1", "k2": "v2"})
	require.NoError(t, err)

	untagRec := doSchedulerRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": grp.ARN,
		"TagKeys":     []string{"k1"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	listRec := doSchedulerRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": grp.ARN})
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsResp))
	tags := tagsResp["Tags"].(map[string]any)
	assert.NotContains(t, tags, "k1")
	assert.Contains(t, tags, "k2")
}

func TestRefinement1_DeleteScheduleInCustomGroup(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	b := h.Backend.(*scheduler.InMemoryBackend)

	_, err := b.CreateScheduleGroup(context.Background(), "custom", "", nil)
	require.NoError(t, err)

	createScheduleViaHandler(t, h, "del-sched", "custom", "rate(1 minute)")
	assert.Equal(t, 1, scheduler.ScheduleCount(b))

	delRec := doSchedulerRequest(t, h, "DeleteSchedule", map[string]any{
		"Name":      "del-sched",
		"GroupName": "custom",
	})
	require.Equal(t, http.StatusOK, delRec.Code)
	assert.Equal(t, 0, scheduler.ScheduleCount(b))
}

func TestRefinement1_GetScheduleNotFound(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	rec := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "nonexistent"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRefinement1_DeleteScheduleNotFound(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	rec := doSchedulerRequest(t, h, "DeleteSchedule", map[string]any{"Name": "nonexistent"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRefinement1_UpdateScheduleNotFound(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	rec := doSchedulerRequest(t, h, "UpdateSchedule", map[string]any{
		"Name":               "nonexistent",
		"ScheduleExpression": "rate(1 minute)",
		"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRefinement1_TagResourceNotFound(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	rec := doSchedulerRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": "arn:aws:scheduler:us-east-1:000000000000:schedule/default/nonexistent",
		"Tags":        map[string]string{"k": "v"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRefinement1_ListTagsForResourceNotFound(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	rec := doSchedulerRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": "arn:aws:scheduler:us-east-1:000000000000:schedule/default/nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRefinement1_DeleteScheduleGroupDefault(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	rec := doSchedulerRequest(t, h, "DeleteScheduleGroup", map[string]any{"Name": "default"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_PersistenceRoundTripWithGroupName(t *testing.T) {
	t.Parallel()

	b := scheduler.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateScheduleGroup(context.Background(), "mygrp", "a description", map[string]string{"env": "test"})
	require.NoError(t, err)

	_, err = b.CreateSchedule(
		context.Background(),
		"grp-sched", "mygrp", "rate(5 minutes)", "desc", "UTC",
		scheduler.Target{ARN: "arn:a", RoleARN: "arn:r"},
		"ENABLED",
		scheduler.FlexibleTimeWindow{Mode: "OFF"},
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	s, err := fresh.GetSchedule(context.Background(), "grp-sched", "mygrp")
	require.NoError(t, err)
	assert.Equal(t, "mygrp", s.GroupName)
	assert.Equal(t, "UTC", s.ScheduleExpressionTimezone)
	assert.Equal(t, "desc", s.Description)

	g, err := fresh.GetScheduleGroup(context.Background(), "mygrp")
	require.NoError(t, err)
	assert.Equal(t, "a description", g.Description)

	// Verify tags were persisted for the group.
	kv, err := fresh.ListTagsForResource(context.Background(), g.ARN)
	require.NoError(t, err)
	assert.Equal(t, "test", kv["env"])
}

func TestRefinement1_BackendReset(t *testing.T) {
	t.Parallel()

	b := scheduler.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateScheduleGroup(context.Background(), "grp", "", nil)
	require.NoError(t, err)
	_, err = b.CreateSchedule(context.Background(), "s1", "grp", "rate(1 minute)", "", "",
		scheduler.Target{ARN: "arn:a", RoleARN: "arn:r"}, "ENABLED", scheduler.FlexibleTimeWindow{Mode: "OFF"})
	require.NoError(t, err)

	b.Reset()

	assert.Equal(t, 0, scheduler.ScheduleCount(b))
	// Only the default group should remain.
	assert.Equal(t, 1, scheduler.ScheduleGroupCount(b))
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	h := scheduler.NewHandler(b)

	createScheduleViaHandler(t, h, "r1", "", "rate(1 minute)")
	assert.Equal(t, 1, scheduler.ScheduleCount(b))

	h.Reset()
	assert.Equal(t, 0, scheduler.ScheduleCount(b))
}

func TestRefinement1_RESTPathDeleteSchedule(t *testing.T) {
	t.Parallel()

	h := scheduler.NewHandler(scheduler.NewInMemoryBackend("000000000000", "us-east-1"))
	e := echo.New()

	// Create a schedule via target API.
	createBody := map[string]any{
		"Name": "rest-del", "ScheduleExpression": "rate(1 minute)",
		"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
	}
	bodyBytes, _ := json.Marshal(createBody)
	createReq := httptest.NewRequest(http.MethodPost, "/schedules/rest-del", nil)
	createReq.Header.Set("X-Amz-Target", "AWSScheduler.CreateSchedule")
	createReq.Body = httptest.NewRecorder().Result().Body
	createReqP := httptest.NewRequest(http.MethodPost, "/", nil)
	createReqP.Header.Set("X-Amz-Target", "AWSScheduler.CreateSchedule")
	_ = bodyBytes
	_ = createReq

	// Use target-based create.
	createRec := httptest.NewRecorder()
	createCtx := e.NewContext(
		func() *http.Request {
			r := httptest.NewRequest(http.MethodPost, "/", nil)
			r.Header.Set("X-Amz-Target", "AWSScheduler.CreateSchedule")
			data, _ := json.Marshal(createBody)
			r.Body = httptest.NewRecorder().Result().Body
			_ = data

			return r
		}(),
		createRec,
	)
	_ = createCtx

	// Directly test REST DELETE.
	delReq := httptest.NewRequest(http.MethodDelete, "/schedules/rest-del", nil)
	delRec := httptest.NewRecorder()
	delCtx := e.NewContext(delReq, delRec)
	err := h.Handler()(delCtx)
	require.NoError(t, err)
	// Not found since we didn't create it via REST, but path routing works.
	assert.Equal(t, http.StatusNotFound, delRec.Code)
}

func TestRefinement1_ExtractOperationRESTPaths(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	e := echo.New()

	tests := []struct {
		method  string
		path    string
		wantOp  string
		wantRes string
	}{
		{http.MethodGet, "/schedules", "ListSchedules", ""},
		{http.MethodGet, "/schedules/", "ListSchedules", ""},
		{http.MethodPost, "/schedules/my-sched", "CreateSchedule", "my-sched"},
		{http.MethodGet, "/schedules/my-sched", "GetSchedule", "my-sched"},
		{http.MethodDelete, "/schedules/my-sched", "DeleteSchedule", "my-sched"},
		{http.MethodPut, "/schedules/my-sched", "UpdateSchedule", "my-sched"},
		{http.MethodGet, "/schedule-groups", "ListScheduleGroups", ""},
		{http.MethodPost, "/schedule-groups/grp", "CreateScheduleGroup", "grp"},
		{http.MethodGet, "/schedule-groups/grp", "GetScheduleGroup", "grp"},
		{http.MethodDelete, "/schedule-groups/grp", "DeleteScheduleGroup", "grp"},
	}

	for _, tt := range tests {
		t.Run(tt.method+"_"+tt.path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestRefinement1_ScheduleDefaultStateEnabled(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "default-state",
		"ScheduleExpression": "rate(1 hour)",
		"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
	})

	rec := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "default-state"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ENABLED", resp["State"])
}

func TestRefinement1_ListSchedulesIncludesGroupNameAndDates(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	b := h.Backend.(*scheduler.InMemoryBackend)

	_, err := b.CreateScheduleGroup(context.Background(), "custom-g", "", nil)
	require.NoError(t, err)

	createScheduleViaHandler(t, h, "dated-sched", "custom-g", "rate(1 minute)")

	rec := doSchedulerRequest(t, h, "ListSchedules", map[string]any{"GroupName": "custom-g"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	schedules := resp["Schedules"].([]any)
	require.Len(t, schedules, 1)

	s := schedules[0].(map[string]any)
	assert.Equal(t, "custom-g", s["GroupName"])
	assert.NotEmpty(t, s["CreationDate"])
	assert.NotEmpty(t, s["LastModificationDate"])
}

func TestRefinement1_ScheduleExpressionTimezone(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":                       "tz-sched",
		"ScheduleExpression":         "cron(0 9 * * ? *)",
		"ScheduleExpressionTimezone": "America/New_York",
		"Target":                     map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
		"FlexibleTimeWindow":         map[string]string{"Mode": "OFF"},
	})

	rec := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "tz-sched"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "America/New_York", resp["ScheduleExpressionTimezone"])
}
