package resourcegroups_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

func TestResourceGroupsHandler_StartTagSyncTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *resourcegroups.Handler)
		name         string
		group        string
		roleArn      string
		wantContains []string
		wantCode     int
	}{
		{
			name:  "success",
			group: "my-group",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				t.Helper()
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})
			},
			roleArn:      "arn:aws:iam::000000000000:role/my-role",
			wantCode:     http.StatusOK,
			wantContains: []string{"TaskArn", "GroupName"},
		},
		{
			name:     "missing_group",
			group:    "",
			roleArn:  "arn:aws:iam::000000000000:role/my-role",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_role",
			group:    "my-group",
			roleArn:  "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "not_found",
			group:    "nonexistent",
			roleArn:  "arn:aws:iam::000000000000:role/my-role",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			body := map[string]any{
				"Group":    tt.group,
				"RoleArn":  tt.roleArn,
				"TagKey":   "env",
				"TagValue": "prod",
			}
			rec := doResourceGroupsRequest(t, h, "StartTagSyncTask", body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestStartTagSyncTask_RequiredFields verifies required field validation.
func TestStartTagSyncTask_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *resourcegroups.Handler)
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "no_group",
			body:     map[string]any{"RoleArn": "arn:aws:iam::000000000000:role/r"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "no_role_arn",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				t.Helper()
				rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g1"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"Group": "g1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "success",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				t.Helper()
				rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g1"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: map[string]any{
				"Group":   "g1",
				"RoleArn": "arn:aws:iam::000000000000:role/r",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doResourceGroupsRequest(t, h, "StartTagSyncTask", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestTagSyncTask_RoundTrip tests the full task lifecycle.
func TestTagSyncTask_RoundTrip(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	h := resourcegroups.NewHandler(b)

	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "app-group"})

	startRec := doResourceGroupsRequest(t, h, "StartTagSyncTask", map[string]any{
		"Group":    "app-group",
		"RoleArn":  "arn:aws:iam::000000000000:role/sync-role",
		"TagKey":   "application",
		"TagValue": "my-app",
	})
	require.Equal(t, http.StatusOK, startRec.Code)
	assert.Contains(t, startRec.Body.String(), "TaskArn")

	assert.Equal(t, 1, resourcegroups.TagSyncTaskCount(b))

	// List tasks - should appear
	listRec := doResourceGroupsRequest(t, h, "ListTagSyncTasks", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)
	assert.Contains(t, listRec.Body.String(), "app-group")
}

func TestResourceGroupsHandler_GetTagSyncTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *resourcegroups.Handler) string
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *resourcegroups.Handler) string {
				t.Helper()
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})
				rec := doResourceGroupsRequest(t, h, "StartTagSyncTask", map[string]any{
					"Group":    "my-group",
					"RoleArn":  "arn:aws:iam::000000000000:role/r",
					"TagKey":   "env",
					"TagValue": "prod",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					TaskArn string `json:"TaskArn"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return out.TaskArn
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"TaskArn", "Status"},
		},
		{
			name: "missing_task_arn",
			setup: func(_ *testing.T, _ *resourcegroups.Handler) string {
				return ""
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "not_found",
			setup: func(_ *testing.T, _ *resourcegroups.Handler) string {
				return "arn:aws:resource-groups:us-east-1:000000000000:tag-sync-task/nonexistent"
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			taskARN := tt.setup(t, h)

			rec := doResourceGroupsRequest(t, h, "GetTagSyncTask", map[string]any{"TaskArn": taskARN})
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestGetTagSyncTask_RequiredFields verifies TaskArn is required.
func TestGetTagSyncTask_RequiredFields(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "GetTagSyncTask", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestResourceGroupsHandler_CancelTagSyncTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *resourcegroups.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *resourcegroups.Handler) string {
				t.Helper()
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})
				rec := doResourceGroupsRequest(t, h, "StartTagSyncTask", map[string]any{
					"Group":    "my-group",
					"RoleArn":  "arn:aws:iam::000000000000:role/r",
					"TagKey":   "env",
					"TagValue": "prod",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					TaskArn string `json:"TaskArn"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return out.TaskArn
			},
			wantCode: http.StatusOK,
		},
		{
			name: "missing_task_arn",
			setup: func(_ *testing.T, _ *resourcegroups.Handler) string {
				return ""
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "not_found",
			setup: func(_ *testing.T, _ *resourcegroups.Handler) string {
				return "arn:aws:resource-groups:us-east-1:000000000000:tag-sync-task/nonexistent"
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			taskARN := tt.setup(t, h)

			rec := doResourceGroupsRequest(t, h, "CancelTagSyncTask", map[string]any{"TaskArn": taskARN})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestCancelTagSyncTask_RequiredFields verifies TaskArn is required.
func TestCancelTagSyncTask_RequiredFields(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "CancelTagSyncTask", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestCancelTagSyncTaskState verifies that CancelTagSyncTask deletes the
// task rather than transitioning it to a fabricated CANCELLED status:
// TagSyncTaskStatus's only documented wire values are ACTIVE and ERROR, and
// AWS documents CancelTagSyncTask as taking "the TaskArn of the tag-sync
// task you want to delete".
func TestCancelTagSyncTaskState(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "sync-group"})

	startRec := doResourceGroupsRequest(t, h, "StartTagSyncTask", map[string]any{
		"Group":    "sync-group",
		"RoleArn":  "arn:aws:iam::000000000000:role/sync-role",
		"TagKey":   "app",
		"TagValue": "myapp",
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startOut map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	taskARN := startOut["TaskArn"].(string)

	cancelRec := doResourceGroupsRequest(
		t,
		h,
		"CancelTagSyncTask",
		map[string]any{"TaskArn": taskARN},
	)
	assert.Equal(t, http.StatusOK, cancelRec.Code)

	// Task must be gone after cancellation, not lingering with an invalid status.
	getRec := doResourceGroupsRequest(t, h, "GetTagSyncTask", map[string]any{"TaskArn": taskARN})
	assert.Equal(
		t,
		http.StatusNotFound,
		getRec.Code,
		"cancelled task must no longer be retrievable: %s",
		getRec.Body.String(),
	)

	// Must no longer appear in ListTagSyncTasks either.
	listRec := doResourceGroupsRequest(t, h, "ListTagSyncTasks", map[string]any{})
	assert.Equal(t, http.StatusOK, listRec.Code)
	assert.NotContains(t, listRec.Body.String(), taskARN)
}

// TestCancelTagSyncTask_NotFound verifies 404 for unknown task ARN.
func TestCancelTagSyncTask_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "CancelTagSyncTask", map[string]any{
		"TaskArn": "arn:aws:resource-groups:us-east-1:000000000000:tag-sync-task/nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestResourceGroupsHandler_ListTagSyncTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *resourcegroups.Handler)
		name         string
		filters      []map[string]any
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			wantCode:     http.StatusOK,
			wantContains: []string{"TagSyncTasks"},
		},
		{
			name: "with_tasks",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				t.Helper()
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})
				doResourceGroupsRequest(t, h, "StartTagSyncTask", map[string]any{
					"Group":    "my-group",
					"RoleArn":  "arn:aws:iam::000000000000:role/r",
					"TagKey":   "env",
					"TagValue": "prod",
				})
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"TagSyncTasks", "my-group"},
		},
		{
			name: "filtered_by_group_name",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				t.Helper()
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})
				doResourceGroupsRequest(t, h, "StartTagSyncTask", map[string]any{
					"Group":   "my-group",
					"RoleArn": "arn:aws:iam::000000000000:role/r",
				})
			},
			filters:      []map[string]any{{"GroupName": "my-group"}},
			wantCode:     http.StatusOK,
			wantContains: []string{"TagSyncTasks"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			body := map[string]any{}
			if tt.filters != nil {
				body["Filters"] = tt.filters
			}

			rec := doResourceGroupsRequest(t, h, "ListTagSyncTasks", body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestListTagSyncTasks_Pagination verifies NextToken pagination through the handler.
func TestListTagSyncTasks_PaginationViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)

	for i := range 4 {
		name := fmt.Sprintf("sync-grp-%d", i)
		doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": name})
		doResourceGroupsRequest(t, h, "StartTagSyncTask", map[string]any{
			"Group":   name,
			"RoleArn": "arn:aws:iam::000000000000:role/r",
			"TagKey":  "env",
		})
	}

	rec1 := doResourceGroupsRequest(t, h, "ListTagSyncTasks", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	tasks1 := out1["TagSyncTasks"].([]any)
	assert.Len(t, tasks1, 2)
	tok1, _ := out1["NextToken"].(string)
	require.NotEmpty(t, tok1)

	rec2 := doResourceGroupsRequest(t, h, "ListTagSyncTasks", map[string]any{
		"MaxResults": 10,
		"NextToken":  tok1,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	tasks2 := out2["TagSyncTasks"].([]any)
	assert.Len(t, tasks2, 2)
	assert.Empty(t, out2["NextToken"])
}

// TestResourceGroupsHandler_TagSyncTask_CreatedAtIsEpochSeconds verifies that
// CreatedAt is serialized as a JSON number of seconds since the Unix epoch
// (the rest-json unixTimestamp format), not an RFC3339/ISO8601 string, for
// both GetTagSyncTask and ListTagSyncTasks.
func TestResourceGroupsHandler_TagSyncTask_CreatedAtIsEpochSeconds(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "epoch-group"})
	startRec := doResourceGroupsRequest(t, h, "StartTagSyncTask", map[string]any{
		"Group":    "epoch-group",
		"RoleArn":  "arn:aws:iam::000000000000:role/r",
		"TagKey":   "env",
		"TagValue": "prod",
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startOut struct {
		TaskArn string `json:"TaskArn"`
	}
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))

	getRec := doResourceGroupsRequest(t, h, "GetTagSyncTask", map[string]any{"TaskArn": startOut.TaskArn})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	_, isNumber := getOut["CreatedAt"].(float64)
	assert.True(t, isNumber, "GetTagSyncTask CreatedAt must be a JSON number: %s", getRec.Body.String())

	listRec := doResourceGroupsRequest(t, h, "ListTagSyncTasks", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		TagSyncTasks []map[string]any `json:"TagSyncTasks"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.Len(t, listOut.TagSyncTasks, 1)
	_, isNumber = listOut.TagSyncTasks[0]["CreatedAt"].(float64)
	assert.True(t, isNumber, "ListTagSyncTasks CreatedAt must be a JSON number: %s", listRec.Body.String())
}
