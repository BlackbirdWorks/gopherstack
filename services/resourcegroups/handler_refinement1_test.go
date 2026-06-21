package resourcegroups_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

// doResourceGroupsRequestRaw sends a raw (possibly invalid JSON) body to the handler.
func doResourceGroupsRequestRaw(
	t *testing.T,
	h *resourcegroups.Handler,
	action string,
	body []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "ResourceGroups."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// TestRefinement1_HandlerOpsLen verifies the dispatch table is pre-built and
// contains the expected number of operations.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	assert.Equal(t, 20, resourcegroups.HandlerOpsLen(h))
}

// TestRefinement1_GetSupportedOperations verifies the list is sorted and complete.
func TestRefinement1_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i], "operations should be sorted")
	}

	assert.Contains(t, ops, "UpdateAccountSettings")
	assert.Contains(t, ops, "CancelTagSyncTask")
	assert.Contains(t, ops, "StartTagSyncTask")
}

// TestRefinement1_CreateGroup_NameRequired verifies that missing Name returns 400.
func TestRefinement1_CreateGroup_NameRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "empty_name",
			body:     map[string]any{"Name": ""},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_name",
			body:     map[string]any{"Description": "no name given"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "valid",
			body:     map[string]any{"Name": "my-group"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			rec := doResourceGroupsRequest(t, h, "CreateGroup", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestRefinement1_UpdateGroup_NameRequired verifies that missing group name returns 400.
func TestRefinement1_UpdateGroup_NameRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *resourcegroups.Handler)
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "no_name",
			body:     map[string]any{"Description": "updated"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "with_group_field",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				t.Helper()
				rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"Group": "my-group", "Description": "updated"},
			wantCode: http.StatusOK,
		},
		{
			name: "with_group_name_field",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				t.Helper()
				rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "other-group"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"GroupName": "other-group", "Description": "updated"},
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

			rec := doResourceGroupsRequest(t, h, "UpdateGroup", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestRefinement1_UpdateGroupQuery_NameRequired verifies that missing group name returns 400.
func TestRefinement1_UpdateGroupQuery_NameRequired(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "UpdateGroupQuery", map[string]any{
		"ResourceQuery": map[string]string{"Type": "TAG_FILTERS_1_0", "Query": "{}"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_GetAccountSettings verifies default settings are returned.
func TestRefinement1_GetAccountSettings(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "GetAccountSettings", nil)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AccountSettings")
}

// TestRefinement1_UpdateAccountSettings verifies the settings can be changed.
func TestRefinement1_UpdateAccountSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		desiredStatus  string
		wantStatusInDB string
		wantCode       int
	}{
		{
			name:           "set_active",
			desiredStatus:  "ACTIVE",
			wantCode:       http.StatusOK,
			wantStatusInDB: "ACTIVE",
		},
		{
			name:          "set_inactive",
			desiredStatus: "INACTIVE",
			wantCode:      http.StatusOK,
		},
		{
			name:          "invalid_status",
			desiredStatus: "BOGUS",
			wantCode:      http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			rec := doResourceGroupsRequest(t, h, "UpdateAccountSettings", map[string]any{
				"GroupLifecycleEventsDesiredStatus": tt.desiredStatus,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestRefinement1_DeleteGroup_Cascade verifies cascaded deletion of resources, config, and tasks.
func TestRefinement1_DeleteGroup_Cascade(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	h := resourcegroups.NewHandler(b)

	// Create a group and populate it.
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g1"})
	doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
		"Group":        "g1",
		"ResourceArns": []string{"arn:aws:s3:::bucket1"},
	})
	doResourceGroupsRequest(t, h, "PutGroupConfiguration", map[string]any{
		"Group":         "g1",
		"Configuration": []map[string]any{{"Type": "AWS::EC2::CapacityReservationPool"}},
	})
	doResourceGroupsRequest(t, h, "StartTagSyncTask", map[string]any{
		"Group":   "g1",
		"RoleArn": "arn:aws:iam::000000000000:role/role1",
	})

	assert.Equal(t, 1, resourcegroups.GroupCount(b))
	assert.Equal(t, 1, resourcegroups.GroupResourceCount(b))
	assert.Equal(t, 1, resourcegroups.GroupConfigurationCount(b))
	assert.Equal(t, 1, resourcegroups.TagSyncTaskCount(b))

	// Delete the group.
	rec := doResourceGroupsRequest(t, h, "DeleteGroup", map[string]any{"Group": "g1"})
	require.Equal(t, http.StatusOK, rec.Code)

	// All cascade-deleted.
	assert.Equal(t, 0, resourcegroups.GroupCount(b))
	assert.Equal(t, 0, resourcegroups.GroupResourceCount(b))
	assert.Equal(t, 0, resourcegroups.GroupConfigurationCount(b))
	assert.Equal(t, 0, resourcegroups.TagSyncTaskCount(b))
}

// TestRefinement1_ListGroups_Sorted verifies groups are returned sorted by name.
func TestRefinement1_ListGroups_Sorted(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	h := resourcegroups.NewHandler(b)

	for _, name := range []string{"z-group", "a-group", "m-group"} {
		doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": name})
	}

	groups, _ := b.ListGroups(context.Background(), nil, "", 0)
	require.Len(t, groups, 3)
	assert.Equal(t, "a-group", groups[0].Name)
	assert.Equal(t, "m-group", groups[1].Name)
	assert.Equal(t, "z-group", groups[2].Name)
}

// TestRefinement1_PutGroupConfiguration_DeepCopy verifies parameter values are
// properly deep-copied and mutations in the caller do not affect stored state.
func TestRefinement1_PutGroupConfiguration_DeepCopy(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateGroup(context.Background(), "g1", "", nil, nil, nil)

	params := []resourcegroups.GroupConfigurationParameter{
		{Name: "allowed-resource-types", Values: []string{"v1", "v2"}},
	}
	items := []resourcegroups.GroupConfigurationItem{
		{Type: "AWS::ResourceGroups::Generic", Parameters: params},
	}

	require.NoError(t, b.PutGroupConfiguration(context.Background(), "g1", items))

	// Mutate the original slice after storing.
	params[0].Values[0] = "mutated"

	got, err := b.GetGroupConfigurationItems(context.Background(), "g1")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Len(t, got[0].Parameters, 1)
	// Stored value should not be affected by the mutation.
	assert.Equal(t, "v1", got[0].Parameters[0].Values[0])
}

// TestRefinement1_GroupResources_NoDuplicates verifies duplicate ARNs are de-duped.
func TestRefinement1_GroupResources_NoDuplicates(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateGroup(context.Background(), "g1", "", nil, nil, nil)

	_, err := b.GroupResources(context.Background(), "g1", []string{"arn:aws:s3:::b1", "arn:aws:s3:::b1"})
	require.NoError(t, err)

	// Should only store one copy.
	assert.Equal(t, 1, resourcegroups.GroupResourceCount(b))
}

// TestRefinement1_GroupResources_NotFound verifies 404 for unknown group.
func TestRefinement1_GroupResources_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
		"Group":        "nonexistent",
		"ResourceArns": []string{"arn:aws:s3:::b1"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement1_ListGroupResources_Empty verifies empty list for group with no resources.
func TestRefinement1_ListGroupResources_Empty(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "empty-group"})

	rec := doResourceGroupsRequest(t, h, "ListGroupResources", map[string]any{
		"Group": "empty-group",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Resources")
}

// TestRefinement1_ListGroupResources_NotFound verifies 404 for unknown group.
func TestRefinement1_ListGroupResources_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "ListGroupResources", map[string]any{
		"Group": "ghost",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement1_ListGroupingStatuses_AfterGroupResources verifies status records
// are created when resources are grouped.
func TestRefinement1_ListGroupingStatuses_AfterGroupResources(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g1"})
	doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
		"Group":        "g1",
		"ResourceArns": []string{"arn:aws:s3:::b1"},
	})

	rec := doResourceGroupsRequest(t, h, "ListGroupingStatuses", map[string]any{
		"Group": "g1",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "SUCCESS")
}

// TestRefinement1_StartTagSyncTask_RequiredFields verifies required field validation.
func TestRefinement1_StartTagSyncTask_RequiredFields(t *testing.T) {
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

// TestRefinement1_GetTagSyncTask_RequiredFields verifies TaskArn is required.
func TestRefinement1_GetTagSyncTask_RequiredFields(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "GetTagSyncTask", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_CancelTagSyncTask_RequiredFields verifies TaskArn is required.
func TestRefinement1_CancelTagSyncTask_RequiredFields(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "CancelTagSyncTask", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_TagSyncTask_RoundTrip tests the full task lifecycle.
func TestRefinement1_TagSyncTask_RoundTrip(t *testing.T) {
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

// TestRefinement1_ListTagSyncTasks_Sorted verifies tasks are returned sorted.
func TestRefinement1_ListTagSyncTasks_Sorted(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	_, _ = b.CreateGroup(context.Background(), "g1", "", nil, nil, nil)
	_, _ = b.CreateGroup(context.Background(), "g2", "", nil, nil, nil)

	// Start multiple tasks for determinism check.
	_, err1 := b.StartTagSyncTask(context.Background(), "g1", "arn:aws:iam::000000000000:role/r", "k", "v", nil)
	_, err2 := b.StartTagSyncTask(context.Background(), "g2", "arn:aws:iam::000000000000:role/r", "k", "v", nil)
	require.NoError(t, err1)
	require.NoError(t, err2)

	tasks, _, err := b.ListTagSyncTasks(context.Background(), nil, "", 0)
	require.NoError(t, err)
	require.Len(t, tasks, 2)

	assert.LessOrEqual(t, tasks[0].TaskArn, tasks[1].TaskArn)
}

// TestRefinement1_SearchResources_DeduplicatesAcrossGroups verifies cross-group de-dup.
func TestRefinement1_SearchResources_DeduplicatesAcrossGroups(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateGroup(context.Background(), "g1", "", nil, nil, nil)
	_, _ = b.CreateGroup(context.Background(), "g2", "", nil, nil, nil)

	// Same ARN added to both groups.
	_, _ = b.GroupResources(context.Background(), "g1", []string{"arn:aws:s3:::shared"})
	_, _ = b.GroupResources(context.Background(), "g2", []string{"arn:aws:s3:::shared"})

	results, _, err := b.SearchResources(context.Background(), nil, "", 0)
	require.NoError(t, err)
	assert.Len(t, results, 1)
}

// TestRefinement1_AddGroupInternal verifies the seed helper works for tests.
func TestRefinement1_AddGroupInternal(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	g := resourcegroups.AddGroupInternal(b, "seeded-group", "seeded desc")

	assert.Equal(t, "seeded-group", g.Name)
	assert.Equal(t, 1, resourcegroups.GroupCount(b))
}

// TestRefinement1_ErrNilAppContext verifies provider returns error for nil context.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &resourcegroups.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroups.ErrNilAppContext)
}

// TestRefinement1_PersistenceIncludesNewState verifies snapshot/restore covers
// group resources, configurations, and tag-sync tasks.
func TestRefinement1_PersistenceIncludesNewState(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateGroup(context.Background(), "g1", "desc", nil, nil, nil)
	_, _ = b.GroupResources(context.Background(), "g1", []string{"arn:aws:s3:::b1"})
	_ = b.PutGroupConfiguration(context.Background(), "g1", []resourcegroups.GroupConfigurationItem{
		{Type: "AWS::EC2::CapacityReservationPool"},
	})
	_, _ = b.StartTagSyncTask(context.Background(), "g1", "arn:aws:iam::000000000000:role/r", "k", "v", nil)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	assert.Equal(t, 1, resourcegroups.GroupCount(b2))
	assert.Equal(t, 1, resourcegroups.GroupResourceCount(b2))
	assert.Equal(t, 1, resourcegroups.GroupConfigurationCount(b2))
	assert.Equal(t, 1, resourcegroups.TagSyncTaskCount(b2))
}

// TestRefinement1_PersistenceTagsRenamedAfterRestore verifies tags can be accessed
// after restore without using the "json.tags" name collision.
func TestRefinement1_PersistenceTagsRenamedAfterRestore(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	g, _ := b.CreateGroup(context.Background(), "tagged", "", nil, nil, nil)
	_, _ = b.AddTagsByARN(context.Background(), g.ARN, map[string]string{"owner": "alice"})

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	tags, err := b2.GetTagsByARN(context.Background(), g.ARN)
	require.NoError(t, err)
	assert.Equal(t, "alice", tags["owner"])
}

// TestRefinement1_GetGroupConfiguration_Empty verifies an empty list is returned when
// no configuration has been stored for a group.
func TestRefinement1_GetGroupConfiguration_Empty(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "no-config"})

	rec := doResourceGroupsRequest(t, h, "GetGroupConfiguration", map[string]any{
		"Group": "no-config",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GroupConfiguration")
}

// TestRefinement1_BadJSON verifies that malformed JSON returns 400.
func TestRefinement1_BadJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
	}{
		{name: "CreateGroup", action: "CreateGroup"},
		{name: "GroupResources", action: "GroupResources"},
		{name: "StartTagSyncTask", action: "StartTagSyncTask"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			rec := doResourceGroupsRequestRaw(t, h, tt.action, []byte("{invalid-json"))
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestRefinement1_UnknownOperation verifies that unknown operations return an error.
func TestRefinement1_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "NonExistentOperation", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_GroupCount_AfterCreateAndDelete verifies count helpers work.
func TestRefinement1_GroupCount_AfterCreateAndDelete(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	h := resourcegroups.NewHandler(b)

	assert.Equal(t, 0, resourcegroups.GroupCount(b))

	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g1"})
	assert.Equal(t, 1, resourcegroups.GroupCount(b))

	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g2"})
	assert.Equal(t, 2, resourcegroups.GroupCount(b))

	doResourceGroupsRequest(t, h, "DeleteGroup", map[string]any{"Group": "g1"})
	assert.Equal(t, 1, resourcegroups.GroupCount(b))
}

// TestRefinement1_ListTagSyncTasks_FilteredByGroupName verifies filter works.
func TestRefinement1_ListTagSyncTasks_FilteredByGroupName(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateGroup(context.Background(), "g1", "", nil, nil, nil)
	_, _ = b.CreateGroup(context.Background(), "g2", "", nil, nil, nil)
	_, _ = b.StartTagSyncTask(context.Background(), "g1", "arn:aws:iam::000000000000:role/r", "", "", nil)
	_, _ = b.StartTagSyncTask(context.Background(), "g2", "arn:aws:iam::000000000000:role/r", "", "", nil)

	tasks, _, err := b.ListTagSyncTasks(context.Background(), []resourcegroups.ListTagSyncTasksFilter{
		{GroupName: "g1"},
	}, "", 0)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, "g1", tasks[0].GroupName)
}

// TestRefinement1_CloneConfigItems_NilInput verifies nil config input returns empty slice.
func TestRefinement1_CloneConfigItems_NilInput(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateGroup(context.Background(), "g1", "", nil, nil, nil)

	items, err := b.GetGroupConfigurationItems(context.Background(), "g1")
	require.NoError(t, err)
	assert.NotNil(t, items)
	assert.Empty(t, items)
}

// TestRefinement1_Reset_ClearsAllState verifies Reset wipes all maps.
func TestRefinement1_Reset_ClearsAllState(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	h := resourcegroups.NewHandler(b)

	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g1"})
	doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
		"Group":        "g1",
		"ResourceArns": []string{"arn:aws:s3:::b1"},
	})
	doResourceGroupsRequest(t, h, "StartTagSyncTask", map[string]any{
		"Group":   "g1",
		"RoleArn": "arn:aws:iam::000000000000:role/r",
	})

	h.Reset()

	assert.Equal(t, 0, resourcegroups.GroupCount(b))
	assert.Equal(t, 0, resourcegroups.GroupResourceCount(b))
	assert.Equal(t, 0, resourcegroups.TagSyncTaskCount(b))
}
