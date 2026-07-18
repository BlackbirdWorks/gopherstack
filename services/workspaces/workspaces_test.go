package workspaces_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workspaces"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func createWorkspaceWithSpec(t *testing.T, h *workspaces.Handler, userID, dirID string) string {
	t.Helper()

	// Ensure the directory is registered; ignore duplicate-registration errors.
	doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{
		"DirectoryId": dirID,
	})

	rec := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": []map[string]any{
			{
				"UserName":    userID,
				"DirectoryId": dirID,
				"BundleId":    "wsb-bh8rsxt14",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pending, _ := resp["PendingRequests"].([]any)
	require.Len(t, pending, 1)

	return pending[0].(map[string]any)["WorkspaceId"].(string)
}

func describeWorkspacesPage(
	t *testing.T, h *workspaces.Handler, nextToken string, limit int,
) ([]string, string) {
	t.Helper()

	body := map[string]any{}
	if nextToken != "" {
		body["NextToken"] = nextToken
	}

	if limit > 0 {
		body["Limit"] = limit
	}

	rec := doTargetRequest(t, h, "DescribeWorkspaces", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	wsList, _ := resp["Workspaces"].([]any)
	ids := make([]string, 0, len(wsList))

	for _, w := range wsList {
		ids = append(ids, w.(map[string]any)["WorkspaceId"].(string))
	}

	nextPage, _ := resp["NextToken"].(string)

	return ids, nextPage
}

// ---------------------------------------------------------------------------
// CreateWorkspace / registered directory requirement
// ---------------------------------------------------------------------------

// TestCreateWorkspace_RequiresRegisteredDirectory verifies that CreateWorkspace
// returns an error for an unregistered directory and succeeds after registration.
func TestCreateWorkspace_RequiresRegisteredDirectory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		dirID     string
		register  bool
		wantError bool
	}{
		{
			name:      "unregistered directory returns error",
			dirID:     "d-unregistered",
			register:  false,
			wantError: true,
		},
		{
			name:      "registered directory succeeds",
			dirID:     "d-registered",
			register:  true,
			wantError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := workspaces.NewInMemoryBackend("000000000000", "us-east-1")

			if tc.register {
				require.NoError(t, b.RegisterWorkspaceDirectory(tc.dirID, nil))
			}

			_, err := b.CreateWorkspace(context.Background(), &workspaces.WorkspaceCreationSpec{
				UserName:    "alice",
				DirectoryID: tc.dirID,
				BundleID:    "wsb-bh8rsxt14",
			})

			if tc.wantError {
				assert.Error(t, err, "unregistered directory must return error")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestDescribeWorkspaces_FiltersByRegion verifies that workspaces created in
// one region are not returned when describing from another region.
func TestDescribeWorkspaces_FiltersByRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createRegion string
		queryRegion  string
		wantCount    int
	}{
		{
			name:         "same region returns workspace",
			createRegion: "us-east-1",
			queryRegion:  "us-east-1",
			wantCount:    1,
		},
		{
			name:         "different region returns nothing",
			createRegion: "us-east-1",
			queryRegion:  "eu-west-1",
			wantCount:    0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := workspaces.NewInMemoryBackend("000000000000", tc.createRegion)
			require.NoError(t, b.RegisterWorkspaceDirectory("d-test", nil))

			createCtx := ctxWithRegion(tc.createRegion)
			_, err := b.CreateWorkspace(createCtx, &workspaces.WorkspaceCreationSpec{
				UserName:    "alice",
				DirectoryID: "d-test",
				BundleID:    "wsb-bh8rsxt14",
			})
			require.NoError(t, err)

			queryCtx := ctxWithRegion(tc.queryRegion)
			wsList, _, err := b.DescribeWorkspaces(queryCtx, nil, nil, nil, nil, 0, "")
			require.NoError(t, err)
			assert.Len(t, wsList, tc.wantCount)
		})
	}
}

// ---------------------------------------------------------------------------
// WorkspaceId format
// ---------------------------------------------------------------------------

// TestWorkspaceIDFormat verifies that created workspace IDs match the AWS
// pattern: "ws-" followed by exactly 8 lowercase hex characters.
func TestWorkspaceIDFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	assert.True(t, strings.HasPrefix(wsID, "ws-"), "WorkspaceId must start with ws-, got %q", wsID)

	hexPart := strings.TrimPrefix(wsID, "ws-")
	assert.Len(
		t,
		hexPart,
		8,
		"WorkspaceId hex suffix must be 8 chars, got %d in %q",
		len(hexPart),
		wsID,
	)

	for _, ch := range hexPart {
		assert.True(t, (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f'),
			"WorkspaceId hex chars must be lowercase hex, got %q in %q", string(ch), wsID)
	}
}

// ---------------------------------------------------------------------------
// StopWorkspaces state transition: AVAILABLE -> STOPPED
// ---------------------------------------------------------------------------

// TestStopWorkspaces_TransitionsToStopped verifies that after StopWorkspaces
// the workspace state changes to STOPPED.
func TestStopWorkspaces_TransitionsToStopped(t *testing.T) {
	t.Parallel()

	backend := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
	h := workspaces.NewHandler(backend)
	wsID := createWorkspace(t, h)

	assert.Equal(
		t,
		"AVAILABLE",
		workspaces.WorkspaceState(backend, wsID),
		"initial state must be AVAILABLE",
	)

	rec := doTargetRequest(t, h, "StopWorkspaces", map[string]any{
		"StopWorkspaceRequests": []map[string]any{{"WorkspaceId": wsID}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	failures, _ := resp["FailedRequests"].([]any)
	assert.Empty(t, failures)

	assert.Equal(t, "STOPPED", workspaces.WorkspaceState(backend, wsID),
		"workspace state must be STOPPED after StopWorkspaces")
}

// TestStopWorkspaces_StateVisibleInDescribe verifies that the STOPPED state
// is reflected in DescribeWorkspaces.
func TestStopWorkspaces_StateVisibleInDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	doTargetRequest(t, h, "StopWorkspaces", map[string]any{
		"StopWorkspaceRequests": []map[string]any{{"WorkspaceId": wsID}},
	})

	rec := doTargetRequest(t, h, "DescribeWorkspaces", map[string]any{
		"WorkspaceIds": []string{wsID},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wsList, _ := resp["Workspaces"].([]any)
	require.Len(t, wsList, 1)
	ws := wsList[0].(map[string]any)
	assert.Equal(t, "STOPPED", ws["State"], "DescribeWorkspaces must reflect STOPPED state")
}

// ---------------------------------------------------------------------------
// StartWorkspaces state transition: STOPPED -> AVAILABLE
// ---------------------------------------------------------------------------

// TestStartWorkspaces_ResumesFromStopped verifies that after StartWorkspaces
// a STOPPED workspace returns to AVAILABLE.
func TestStartWorkspaces_ResumesFromStopped(t *testing.T) {
	t.Parallel()

	backend := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
	h := workspaces.NewHandler(backend)
	wsID := createWorkspace(t, h)

	doTargetRequest(t, h, "StopWorkspaces", map[string]any{
		"StopWorkspaceRequests": []map[string]any{{"WorkspaceId": wsID}},
	})
	require.Equal(t, "STOPPED", workspaces.WorkspaceState(backend, wsID))

	rec := doTargetRequest(t, h, "StartWorkspaces", map[string]any{
		"StartWorkspaceRequests": []map[string]any{{"WorkspaceId": wsID}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	failures, _ := resp["FailedRequests"].([]any)
	assert.Empty(t, failures)

	assert.Equal(t, "AVAILABLE", workspaces.WorkspaceState(backend, wsID),
		"workspace state must return to AVAILABLE after StartWorkspaces")
}

// ---------------------------------------------------------------------------
// Stop/Start idempotency
// ---------------------------------------------------------------------------

// TestStopWorkspaces_AlreadyStopped_Succeeds verifies that stopping an
// already-STOPPED workspace succeeds with no failures.
func TestStopWorkspaces_AlreadyStopped_Succeeds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	doTargetRequest(t, h, "StopWorkspaces", map[string]any{
		"StopWorkspaceRequests": []map[string]any{{"WorkspaceId": wsID}},
	})

	rec := doTargetRequest(t, h, "StopWorkspaces", map[string]any{
		"StopWorkspaceRequests": []map[string]any{{"WorkspaceId": wsID}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	failures, _ := resp["FailedRequests"].([]any)
	assert.Empty(t, failures, "stopping an already-STOPPED workspace must succeed")
}

// TestStartWorkspaces_AlreadyAvailable_Succeeds verifies that starting an
// already-AVAILABLE workspace succeeds with no failures.
func TestStartWorkspaces_AlreadyAvailable_Succeeds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	rec := doTargetRequest(t, h, "StartWorkspaces", map[string]any{
		"StartWorkspaceRequests": []map[string]any{{"WorkspaceId": wsID}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	failures, _ := resp["FailedRequests"].([]any)
	assert.Empty(t, failures, "starting an already-AVAILABLE workspace must succeed")
}

// ---------------------------------------------------------------------------
// Unknown workspace failures for Stop/Start
// ---------------------------------------------------------------------------

// TestStopWorkspaces_UnknownID_ReturnsFailure verifies that StopWorkspaces on
// a non-existent workspace returns a FailedRequest.
func TestStopWorkspaces_UnknownID_ReturnsFailure(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doTargetRequest(t, h, "StopWorkspaces", map[string]any{
		"StopWorkspaceRequests": []map[string]any{{"WorkspaceId": "ws-notexist"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	failures, _ := resp["FailedRequests"].([]any)
	assert.Len(t, failures, 1, "unknown workspace must produce one FailedRequest")

	failed := failures[0].(map[string]any)
	assert.Equal(t, "ws-notexist", failed["WorkspaceId"])
	assert.NotEmpty(t, failed["ErrorCode"])
}

// TestStartWorkspaces_UnknownID_ReturnsFailure verifies that StartWorkspaces
// on a non-existent workspace returns a FailedRequest.
func TestStartWorkspaces_UnknownID_ReturnsFailure(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doTargetRequest(t, h, "StartWorkspaces", map[string]any{
		"StartWorkspaceRequests": []map[string]any{{"WorkspaceId": "ws-notexist"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	failures, _ := resp["FailedRequests"].([]any)
	assert.Len(t, failures, 1, "unknown workspace must produce one FailedRequest")
}

// ---------------------------------------------------------------------------
// ModifyWorkspaceProperties persistence
// ---------------------------------------------------------------------------

// TestModifyWorkspaceProperties_Persisted verifies that properties set via
// ModifyWorkspaceProperties are persisted internally.
func TestModifyWorkspaceProperties_Persisted(t *testing.T) {
	t.Parallel()

	backend := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
	h := workspaces.NewHandler(backend)
	wsID := createWorkspace(t, h)

	assert.Nil(t, workspaces.WorkspaceProps(backend, wsID), "properties must be nil before modify")

	rec := doTargetRequest(t, h, "ModifyWorkspaceProperties", map[string]any{
		"WorkspaceId": wsID,
		"WorkspaceProperties": map[string]any{
			"RunningMode":       "AUTO_STOP",
			"ComputeTypeName":   "STANDARD",
			"UserVolumeSizeGib": 50,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	props := workspaces.WorkspaceProps(backend, wsID)
	require.NotNil(t, props, "properties must be stored after ModifyWorkspaceProperties")
	assert.Equal(t, "AUTO_STOP", props.RunningMode)
	assert.Equal(t, "STANDARD", props.ComputeTypeName)
	assert.Equal(t, int32(50), props.UserVolumeSizeGib)
}

// TestModifyWorkspaceProperties_VisibleInDescribe verifies that updated
// properties appear in DescribeWorkspaces under WorkspaceProperties.
func TestModifyWorkspaceProperties_VisibleInDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	doTargetRequest(t, h, "ModifyWorkspaceProperties", map[string]any{
		"WorkspaceId": wsID,
		"WorkspaceProperties": map[string]any{
			"RunningMode":     "AUTO_STOP",
			"ComputeTypeName": "VALUE",
		},
	})

	rec := doTargetRequest(t, h, "DescribeWorkspaces", map[string]any{
		"WorkspaceIds": []string{wsID},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wsList, _ := resp["Workspaces"].([]any)
	require.Len(t, wsList, 1)

	ws := wsList[0].(map[string]any)
	propsRaw, ok := ws["WorkspaceProperties"]
	require.True(t, ok, "WorkspaceProperties must be present in DescribeWorkspaces after modify")
	require.NotNil(t, propsRaw)

	props := propsRaw.(map[string]any)
	assert.Equal(t, "AUTO_STOP", props["RunningMode"])
	assert.Equal(t, "VALUE", props["ComputeTypeName"])
}

// TestWorkspaceProperties_AbsentBeforeModify verifies that WorkspaceProperties
// is absent from DescribeWorkspaces before any modify.
func TestWorkspaceProperties_AbsentBeforeModify(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	rec := doTargetRequest(t, h, "DescribeWorkspaces", map[string]any{
		"WorkspaceIds": []string{wsID},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wsList, _ := resp["Workspaces"].([]any)
	require.Len(t, wsList, 1)

	ws := wsList[0].(map[string]any)
	_, hasProps := ws["WorkspaceProperties"]
	assert.False(t, hasProps, "WorkspaceProperties must be absent when never set")
}

// ---------------------------------------------------------------------------
// TerminateWorkspaces removes from DescribeWorkspaces
// ---------------------------------------------------------------------------

// TestTerminateWorkspaces_RemovedFromDescribe verifies that terminated
// workspaces no longer appear in DescribeWorkspaces.
func TestTerminateWorkspaces_RemovedFromDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	doTargetRequest(t, h, "TerminateWorkspaces", map[string]any{
		"TerminateWorkspaceRequests": []map[string]any{{"WorkspaceId": wsID}},
	})

	rec := doTargetRequest(t, h, "DescribeWorkspaces", map[string]any{
		"WorkspaceIds": []string{wsID},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wsList, _ := resp["Workspaces"].([]any)
	assert.Empty(t, wsList, "terminated workspace must not appear in DescribeWorkspaces")
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

func TestPagination_Limit1(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 3 workspaces so we can paginate through them.
	createdIDs := make([]string, 0, 3)
	for i := range 3 {
		id := createWorkspaceWithSpec(t, h, fmt.Sprintf("user%d", i), "d-abc123")
		createdIDs = append(createdIDs, id)
	}

	sort.Strings(createdIDs)

	// First page: limit=1 -> one result, token present.
	page1, token1 := describeWorkspacesPage(t, h, "", 1)
	require.Len(t, page1, 1)
	assert.NotEmpty(t, token1, "NextToken must be set when there are more results")
	assert.Equal(t, createdIDs[0], page1[0])

	// Second page: continue from token1 -> second result, token present.
	page2, token2 := describeWorkspacesPage(t, h, token1, 1)
	require.Len(t, page2, 1)
	assert.NotEmpty(t, token2)
	assert.Equal(t, createdIDs[1], page2[0])

	// Third page: continue from token2 -> last result, no token.
	page3, token3 := describeWorkspacesPage(t, h, token2, 1)
	require.Len(t, page3, 1)
	assert.Empty(t, token3, "NextToken must be absent on the last page")
	assert.Equal(t, createdIDs[2], page3[0])
}

func TestPagination_DefaultLimit25(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create exactly 26 workspaces to trigger pagination.
	for i := range 26 {
		createWorkspaceWithSpec(t, h, fmt.Sprintf("user%d", i), "d-abc123")
	}

	// First page: no explicit limit -> defaults to 25.
	page1, token1 := describeWorkspacesPage(t, h, "", 0)
	assert.Len(t, page1, 25, "default page size must be 25")
	assert.NotEmpty(t, token1)

	// Second page: remaining 1 result.
	page2, token2 := describeWorkspacesPage(t, h, token1, 0)
	assert.Len(t, page2, 1)
	assert.Empty(t, token2)

	// No overlap between pages.
	combined := make([]string, 0, len(page1)+len(page2))
	combined = append(combined, page1...)
	combined = append(combined, page2...)
	seen := make(map[string]struct{})

	for _, id := range combined {
		_, already := seen[id]
		assert.False(t, already, "workspace %q appeared in both pages", id)
		seen[id] = struct{}{}
	}

	assert.Len(t, combined, 26)
}

func TestPagination_SortedByID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		createWorkspaceWithSpec(t, h, fmt.Sprintf("user%d", i), "d-abc123")
	}

	// Collect all IDs via 5 single-item pages.
	collected := make([]string, 0, 5)
	token := ""

	for range 5 {
		page, next := describeWorkspacesPage(t, h, token, 1)
		require.Len(t, page, 1)
		collected = append(collected, page[0])
		token = next
	}

	// Verify ascending order.
	for i := 1; i < len(collected); i++ {
		assert.Less(t, collected[i-1], collected[i],
			"page results must be in ascending WorkspaceId order")
	}
}

func TestPagination_ExplicitLimitCappedAt25(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 30 {
		createWorkspaceWithSpec(t, h, fmt.Sprintf("user%d", i), "d-abc123")
	}

	// Even if the client requests limit=100, we cap at 25.
	page1, _ := describeWorkspacesPage(t, h, "", 100)
	assert.LessOrEqual(t, len(page1), 25, "limit must be capped at 25")
}

func TestPagination_FilteredByDirectoryID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createWorkspaceWithSpec(t, h, "u1", "d-aaa")
	createWorkspaceWithSpec(t, h, "u2", "d-bbb")
	createWorkspaceWithSpec(t, h, "u3", "d-aaa")

	rec := doTargetRequest(t, h, "DescribeWorkspaces", map[string]any{
		"DirectoryId": "d-aaa",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wsList := resp["Workspaces"].([]any)
	assert.Len(t, wsList, 2, "filter by DirectoryId must return only matching workspaces")
}
