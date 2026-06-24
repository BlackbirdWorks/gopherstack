package workspaces_test

// Batch-2 AWS-accuracy tests for WorkSpaces (go-jnmy).
//
// Covers accuracy gaps not exercised by the batch-1 audit:
//
//  1. WorkspaceId format: IDs have "ws-" prefix + 8 hex characters, matching
//     real AWS workspace ID length.
//  2. StopWorkspaces state transition: AVAILABLE → STOPPED.
//  3. StartWorkspaces state transition: STOPPED → AVAILABLE.
//  4. Stop/Start idempotency: stopping an already-STOPPED workspace succeeds;
//     starting an already-AVAILABLE workspace succeeds.
//  5. ModifyWorkspaceProperties persistence: properties stored and returned by
//     DescribeWorkspaces in WorkspaceProperties field.
//  6. WorkspaceProperties in DescribeWorkspaces: field absent when unset,
//     present after ModifyWorkspaceProperties.
//  7. Tags via CreateTags visible in DescribeWorkspaces: post-creation tags
//     are reflected in the Workspaces response.
//  8. DeleteTags removes tags from DescribeWorkspaces response.
//  9. TerminateWorkspaces removes workspace from DescribeWorkspaces.
// 10. StopWorkspaces unknown workspace returns FailedRequest (no panic).
// 11. StartWorkspaces unknown workspace returns FailedRequest.

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workspaces"
)

// ---------------------------------------------------------------------------
// WorkspaceId format
// ---------------------------------------------------------------------------

// TestBatch2Accuracy_WorkspaceIDFormat verifies that created workspace IDs
// match the AWS pattern: "ws-" followed by exactly 8 lowercase hex characters.
func TestBatch2Accuracy_WorkspaceIDFormat(t *testing.T) {
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
// StopWorkspaces state transition: AVAILABLE → STOPPED
// ---------------------------------------------------------------------------

// TestBatch2Accuracy_StopWorkspaces_TransitionsToStopped verifies that after
// StopWorkspaces the workspace state changes to STOPPED.
func TestBatch2Accuracy_StopWorkspaces_TransitionsToStopped(t *testing.T) {
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

// TestBatch2Accuracy_StopWorkspaces_StateVisibleInDescribe verifies that the
// STOPPED state is reflected in DescribeWorkspaces.
func TestBatch2Accuracy_StopWorkspaces_StateVisibleInDescribe(t *testing.T) {
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
// StartWorkspaces state transition: STOPPED → AVAILABLE
// ---------------------------------------------------------------------------

// TestBatch2Accuracy_StartWorkspaces_ResumesFromStopped verifies that after
// StartWorkspaces a STOPPED workspace returns to AVAILABLE.
func TestBatch2Accuracy_StartWorkspaces_ResumesFromStopped(t *testing.T) {
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

// TestBatch2Accuracy_StopWorkspaces_AlreadyStopped_Succeeds verifies that
// stopping an already-STOPPED workspace succeeds with no failures.
func TestBatch2Accuracy_StopWorkspaces_AlreadyStopped_Succeeds(t *testing.T) {
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

// TestBatch2Accuracy_StartWorkspaces_AlreadyAvailable_Succeeds verifies that
// starting an already-AVAILABLE workspace succeeds with no failures.
func TestBatch2Accuracy_StartWorkspaces_AlreadyAvailable_Succeeds(t *testing.T) {
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

// TestBatch2Accuracy_StopWorkspaces_UnknownID_ReturnsFailure verifies that
// StopWorkspaces on a non-existent workspace returns a FailedRequest.
func TestBatch2Accuracy_StopWorkspaces_UnknownID_ReturnsFailure(t *testing.T) {
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

// TestBatch2Accuracy_StartWorkspaces_UnknownID_ReturnsFailure verifies that
// StartWorkspaces on a non-existent workspace returns a FailedRequest.
func TestBatch2Accuracy_StartWorkspaces_UnknownID_ReturnsFailure(t *testing.T) {
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

// TestBatch2Accuracy_ModifyWorkspaceProperties_Persisted verifies that
// properties set via ModifyWorkspaceProperties are persisted internally.
func TestBatch2Accuracy_ModifyWorkspaceProperties_Persisted(t *testing.T) {
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

// TestBatch2Accuracy_ModifyWorkspaceProperties_VisibleInDescribe verifies that
// updated properties appear in DescribeWorkspaces under WorkspaceProperties.
func TestBatch2Accuracy_ModifyWorkspaceProperties_VisibleInDescribe(t *testing.T) {
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

// TestBatch2Accuracy_WorkspaceProperties_AbsentBeforeModify verifies that
// WorkspaceProperties is absent from DescribeWorkspaces before any modify.
func TestBatch2Accuracy_WorkspaceProperties_AbsentBeforeModify(t *testing.T) {
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
// Tags via CreateTags visible in DescribeWorkspaces
// ---------------------------------------------------------------------------

// TestBatch2Accuracy_CreateTags_VisibleInDescribeWorkspaces verifies that
// tags added via CreateTags after workspace creation appear in DescribeWorkspaces.
func TestBatch2Accuracy_CreateTags_VisibleInDescribeWorkspaces(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	doTargetRequest(t, h, "CreateTags", map[string]any{
		"ResourceId": wsID,
		"Tags": []map[string]any{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
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
	tags, _ := ws["Tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"], "CreateTags changes must be visible in DescribeWorkspaces")
	assert.Equal(t, "platform", tags["team"])
}

// ---------------------------------------------------------------------------
// DeleteTags removes tags from DescribeWorkspaces
// ---------------------------------------------------------------------------

// TestBatch2Accuracy_DeleteTags_RemovedFromDescribeWorkspaces verifies that
// DeleteTags removes tags from the DescribeWorkspaces response.
func TestBatch2Accuracy_DeleteTags_RemovedFromDescribeWorkspaces(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	doTargetRequest(t, h, "CreateTags", map[string]any{
		"ResourceId": wsID,
		"Tags": []map[string]any{
			{"Key": "env", "Value": "prod"},
			{"Key": "keep", "Value": "yes"},
		},
	})

	doTargetRequest(t, h, "DeleteTags", map[string]any{
		"ResourceId": wsID,
		"TagKeys":    []string{"env"},
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
	tags, _ := ws["Tags"].(map[string]any)
	_, hasEnv := tags["env"]
	assert.False(t, hasEnv, "deleted tag must not appear in DescribeWorkspaces")
	assert.Equal(t, "yes", tags["keep"], "non-deleted tag must still appear")
}

// ---------------------------------------------------------------------------
// TerminateWorkspaces removes from DescribeWorkspaces
// ---------------------------------------------------------------------------

// TestBatch2Accuracy_TerminateWorkspaces_RemovedFromDescribe verifies that
// terminated workspaces no longer appear in DescribeWorkspaces.
func TestBatch2Accuracy_TerminateWorkspaces_RemovedFromDescribe(t *testing.T) {
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
// Tags from CreateWorkspaces visible in DescribeTags
// ---------------------------------------------------------------------------

// TestBatch2Accuracy_CreateWorkspaces_Tags_VisibleInDescribeTags verifies that
// tags provided at workspace creation time are retrievable via DescribeTags.
func TestBatch2Accuracy_CreateWorkspaces_Tags_VisibleInDescribeTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{"DirectoryId": "d-abc123"})

	rec := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": []map[string]any{
			{
				"UserName":    "alice",
				"DirectoryId": "d-abc123",
				"BundleId":    "wsb-bh8rsxt14",
				"Tags":        []map[string]any{{"Key": "project", "Value": "gamma"}},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	pending, _ := createResp["PendingRequests"].([]any)
	require.Len(t, pending, 1)
	wsID := pending[0].(map[string]any)["WorkspaceId"].(string)

	descRec := doTargetRequest(t, h, "DescribeTags", map[string]any{
		"ResourceId": wsID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var tagResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &tagResp))
	tagList, _ := tagResp["TagList"].([]any)

	found := false
	for _, item := range tagList {
		tag := item.(map[string]any)
		if tag["Key"] == "project" && tag["Value"] == "gamma" {
			found = true
		}
	}

	assert.True(t, found, "tags from CreateWorkspaces must be visible via DescribeTags")
}
