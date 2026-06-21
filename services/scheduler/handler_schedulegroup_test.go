package scheduler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/scheduler"
)

func TestSchedulerHandler_CreateScheduleGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantCode int
		wantARN  bool
	}{
		{
			name:     "success",
			body:     map[string]any{"Name": "my-group", "Tags": map[string]string{"env": "test"}},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name:     "duplicate",
			body:     map[string]any{"Name": "my-group"},
			wantCode: http.StatusConflict,
		},
		{
			name:     "invalid_json",
			body:     nil,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)
			if tt.name == "duplicate" {
				doSchedulerRequest(t, h, "CreateScheduleGroup", map[string]any{"Name": "my-group"})
			}

			var rec *httptest.ResponseRecorder
			if tt.body == nil {
				rec = doInvalidSchedulerRequest(t, h, "CreateScheduleGroup")
			} else {
				rec = doSchedulerRequest(t, h, "CreateScheduleGroup", tt.body)
			}

			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantARN {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["ScheduleGroupArn"], "arn:aws:scheduler:")
				assert.Contains(t, resp["ScheduleGroupArn"], "schedule-group/my-group")
			}
		})
	}
}

func TestSchedulerHandler_GetScheduleGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		group    string
		wantName string
		wantCode int
	}{
		{
			name:     "default_group_exists",
			group:    "default",
			wantCode: http.StatusOK,
			wantName: "default",
		},
		{
			name:     "created_group",
			group:    "my-group",
			wantCode: http.StatusOK,
			wantName: "my-group",
		},
		{
			name:     "not_found",
			group:    "nonexistent",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)
			if tt.group == "my-group" {
				doSchedulerRequest(t, h, "CreateScheduleGroup", map[string]any{"Name": "my-group"})
			}

			rec := doSchedulerRequest(t, h, "GetScheduleGroup", map[string]any{"Name": tt.group})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantName, resp["Name"])
				assert.Contains(t, resp["Arn"], "schedule-group/"+tt.wantName)
				assert.Equal(t, "ACTIVE", resp["State"])
				assert.NotEmpty(t, resp["CreationDate"])
				assert.NotEmpty(t, resp["LastModificationDate"])
			}
		})
	}
}

func TestSchedulerHandler_DeleteScheduleGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		group    string
		setup    bool
		wantCode int
	}{
		{
			name:     "success",
			group:    "my-group",
			setup:    true,
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			group:    "nonexistent",
			setup:    false,
			wantCode: http.StatusNotFound,
		},
		{
			name:     "cannot_delete_default",
			group:    "default",
			setup:    false,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)
			if tt.setup {
				doSchedulerRequest(t, h, "CreateScheduleGroup", map[string]any{"Name": tt.group})
			}

			rec := doSchedulerRequest(t, h, "DeleteScheduleGroup", map[string]any{"Name": tt.group})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				getRec := doSchedulerRequest(t, h, "GetScheduleGroup", map[string]any{"Name": tt.group})
				assert.Equal(t, http.StatusNotFound, getRec.Code)
			}
		})
	}
}

func TestSchedulerHandler_ListScheduleGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		groupsToCreate []string
		wantMinCount   int
	}{
		{
			name:         "default_group_always_present",
			wantMinCount: 1,
		},
		{
			name:           "lists_all_groups",
			groupsToCreate: []string{"group-a", "group-b"},
			wantMinCount:   3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)
			for _, g := range tt.groupsToCreate {
				doSchedulerRequest(t, h, "CreateScheduleGroup", map[string]any{"Name": g})
			}

			rec := doSchedulerRequest(t, h, "ListScheduleGroups", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Contains(t, resp, "ScheduleGroups")
			groups, ok := resp["ScheduleGroups"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(groups), tt.wantMinCount)
		})
	}
}

func TestSchedulerHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupTags   map[string]string
		name        string
		removeKeys  []string
		wantRemoved []string
		wantKept    []string
		wantCode    int
		notFound    bool
	}{
		{
			name:        "remove_one_tag_from_schedule",
			setupTags:   map[string]string{"env": "test", "team": "platform"},
			removeKeys:  []string{"env"},
			wantCode:    http.StatusOK,
			wantRemoved: []string{"env"},
			wantKept:    []string{"team"},
		},
		{
			name:       "remove_all_tags",
			setupTags:  map[string]string{"env": "test"},
			removeKeys: []string{"env"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "not_found",
			wantCode:   http.StatusNotFound,
			removeKeys: []string{"env"},
			notFound:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)

			var resourceARN string
			if !tt.notFound {
				createRec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
					"Name":               "tagged-schedule",
					"ScheduleExpression": "rate(1 minute)",
					"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
					"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				var createResp map[string]string
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				resourceARN = createResp["ScheduleArn"]

				if len(tt.setupTags) > 0 {
					doSchedulerRequest(t, h, "TagResource", map[string]any{
						"ResourceArn": resourceARN,
						"Tags":        tt.setupTags,
					})
				}
			} else {
				resourceARN = "arn:aws:scheduler:us-east-1:000000000000:schedule/default/nonexistent"
			}

			rec := doSchedulerRequest(t, h, "UntagResource", map[string]any{
				"ResourceArn": resourceARN,
				"TagKeys":     tt.removeKeys,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				listRec := doSchedulerRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": resourceARN})
				require.Equal(t, http.StatusOK, listRec.Code)
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				remaining, _ := listResp["Tags"].(map[string]any)
				for _, key := range tt.wantRemoved {
					assert.NotContains(t, remaining, key)
				}
				for _, key := range tt.wantKept {
					assert.Contains(t, remaining, key)
				}
			}
		})
	}
}

func TestSchedulerHandler_UntagResource_ScheduleGroup(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	doSchedulerRequest(t, h, "CreateScheduleGroup", map[string]any{
		"Name": "tagged-group",
		"Tags": map[string]string{"env": "test", "team": "platform"},
	})

	getRec := doSchedulerRequest(t, h, "GetScheduleGroup", map[string]any{"Name": "tagged-group"})
	require.Equal(t, http.StatusOK, getRec.Code)
	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	groupARN, _ := getResp["Arn"].(string)
	require.NotEmpty(t, groupARN)

	rec := doSchedulerRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": groupARN,
		"TagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	listRec := doSchedulerRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": groupARN})
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	remaining, _ := listResp["Tags"].(map[string]any)
	assert.NotContains(t, remaining, "env")
	assert.Contains(t, remaining, "team")
}

func TestSchedulerHandler_TagResource_ScheduleGroup(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	doSchedulerRequest(t, h, "CreateScheduleGroup", map[string]any{"Name": "tag-group"})
	getRec := doSchedulerRequest(t, h, "GetScheduleGroup", map[string]any{"Name": "tag-group"})
	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	groupARN, _ := getResp["Arn"].(string)

	rec := doSchedulerRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": groupARN,
		"Tags":        map[string]string{"owner": "team-a"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	listRec := doSchedulerRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": groupARN})
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tagsMap, _ := listResp["Tags"].(map[string]any)
	assert.Equal(t, "team-a", tagsMap["owner"])
}

func TestSchedulerHandler_ScheduleGroup_RESTRouting(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/schedule-groups/my-group", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := httptest.NewRequest(http.MethodGet, "/schedule-groups/my-group", nil)
	rec2 := httptest.NewRecorder()
	c2 := e.NewContext(req2, rec2)
	err = h.Handler()(c2)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec2.Code)
	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &getResp))
	assert.Equal(t, "my-group", getResp["Name"])

	req3 := httptest.NewRequest(http.MethodGet, "/schedule-groups", nil)
	rec3 := httptest.NewRecorder()
	c3 := e.NewContext(req3, rec3)
	err = h.Handler()(c3)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec3.Code)

	req4 := httptest.NewRequest(http.MethodDelete, "/schedule-groups/my-group", nil)
	rec4 := httptest.NewRecorder()
	c4 := e.NewContext(req4, rec4)
	err = h.Handler()(c4)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec4.Code)
}

func TestSchedulerHandler_RouteMatcher_ScheduleGroups(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	matcher := h.RouteMatcher()
	e := echo.New()

	req := httptest.NewRequest(http.MethodGet, "/schedule-groups", nil)
	c := e.NewContext(req, httptest.NewRecorder())
	assert.True(t, matcher(c))
}

func TestSchedulerHandler_GetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateScheduleGroup")
	assert.Contains(t, ops, "DeleteScheduleGroup")
	assert.Contains(t, ops, "GetScheduleGroup")
	assert.Contains(t, ops, "ListScheduleGroups")
	assert.Contains(t, ops, "UntagResource")
}

func TestSchedulerHandler_InvalidJSON_NewOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
	}{
		{name: "CreateScheduleGroup", action: "CreateScheduleGroup"},
		{name: "DeleteScheduleGroup", action: "DeleteScheduleGroup"},
		{name: "GetScheduleGroup", action: "GetScheduleGroup"},
		{name: "UntagResource", action: "UntagResource"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)
			rec := doInvalidSchedulerRequest(t, h, tt.action)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestSchedulerBackend_Reset(t *testing.T) {
	t.Parallel()

	b := scheduler.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateSchedule(context.Background(), "s1", "",
		"rate(1 minute)",
		"",
		"",
		scheduler.Target{ARN: "arn:a", RoleARN: "arn:r"},
		"ENABLED", scheduler.FlexibleTimeWindow{Mode: "OFF"})
	require.NoError(t, err)

	_, err = b.CreateScheduleGroup(context.Background(), "g1", "", nil)
	require.NoError(t, err)

	b.Reset()

	schedules, _ := b.ListSchedules(context.Background(), "", "", "", "", 0)
	assert.Empty(t, schedules)
	groups, _ := b.ListScheduleGroups(context.Background(), "", "", 0)
	require.Len(t, groups, 1)
	assert.Equal(t, "default", groups[0].Name)
}

func TestSchedulerBackend_SnapshotRestore_ScheduleGroups(t *testing.T) {
	t.Parallel()

	b := scheduler.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateScheduleGroup(context.Background(), "production", "", map[string]string{"env": "prod"})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	g, err := fresh.GetScheduleGroup(context.Background(), "production")
	require.NoError(t, err)
	assert.Equal(t, "production", g.Name)
	assert.Equal(t, "ACTIVE", g.State)

	def, err := fresh.GetScheduleGroup(context.Background(), "default")
	require.NoError(t, err)
	assert.Equal(t, "default", def.Name)
}

func TestSchedulerHandler_ScheduleGroup_ErrorStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "GetScheduleGroup_NotFound",
			action:   "GetScheduleGroup",
			body:     map[string]any{"Name": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteScheduleGroup_NotFound",
			action:   "DeleteScheduleGroup",
			body:     map[string]any{"Name": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteScheduleGroup_Default",
			action:   "DeleteScheduleGroup",
			body:     map[string]any{"Name": "default"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "UntagResource_NotFound",
			action: "UntagResource",
			body: map[string]any{
				"ResourceArn": "arn:aws:scheduler:us-east-1:000000000000:schedule/default/nope",
				"TagKeys":     []string{"k"},
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)
			rec := doSchedulerRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestSchedulerHandler_ExtractOperation_ScheduleGroups(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{name: "list_groups", method: http.MethodGet, path: "/schedule-groups", wantOp: "ListScheduleGroups"},
		{
			name:   "create_group",
			method: http.MethodPost,
			path:   "/schedule-groups/my-group",
			wantOp: "CreateScheduleGroup",
		},
		{name: "get_group", method: http.MethodGet, path: "/schedule-groups/my-group", wantOp: "GetScheduleGroup"},
		{
			name:   "delete_group",
			method: http.MethodDelete,
			path:   "/schedule-groups/my-group",
			wantOp: "DeleteScheduleGroup",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestSchedulerBackend_Region(t *testing.T) {
	t.Parallel()

	b := scheduler.NewInMemoryBackend("000000000000", "us-west-2")
	assert.Equal(t, "us-west-2", b.Region())
}

func TestSchedulerHandler_REST_DeleteAndUpdate(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	e := echo.New()

	createRec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "del-sched",
		"ScheduleExpression": "rate(1 minute)",
		"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// DELETE /schedules/del-sched via REST.
	req2 := httptest.NewRequest(http.MethodDelete, "/schedules/del-sched", nil)
	rec2 := httptest.NewRecorder()
	c2 := e.NewContext(req2, rec2)
	err := h.Handler()(c2)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestSchedulerHandler_REST_UnknownPath(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	e := echo.New()

	// PUT on /schedule-groups/{name} is not a valid REST operation → 404.
	req := httptest.NewRequest(http.MethodPut, "/schedule-groups/my-group", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
