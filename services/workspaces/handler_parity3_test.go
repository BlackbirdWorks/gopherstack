package workspaces_test

// Parity-3 comprehensive accuracy tests for WorkSpaces.
//
// Covers the following behavioral gaps addressed in this deepening pass:
//
//  1. Pagination: DescribeWorkspaces honours Limit, returns NextToken cursor,
//     subsequent pages return non-overlapping results in ID order.
//  2. CreateWorkspaces input validation: empty UserName, DirectoryId, BundleId
//     each return 400; more than 25 workspaces per call returns 400.
//  3. Tag limit enforcement: adding >50 tags to a resource returns 400.
//  4. Tag key validation: empty tag key returns 400.
//  5. WorkspaceProperties in CreateWorkspaces: initial properties are stored
//     and returned in PendingRequests and DescribeWorkspaces.
//  6. SubnetId propagation: SubnetId set at creation is returned in Describe.
//  7. ModifyWorkspaceProperties validation: invalid ComputeTypeName → 400,
//     invalid RunningMode → 400, invalid AutoStop timeout → 400.
//  8. Valid compute type names and running modes accepted without error.
//  9. RunningModeAutoStopTimeoutInMinutes: must be a multiple of 60 between
//     60 and 600; exactly 60 and 600 are accepted; 30 and 601 are rejected.
// 10. Bundle response fidelity: ComputeType.Name, UserStorage.Capacity,
//     RootStorage.Capacity are all present in DescribeWorkspaceBundles.
// 11. Custom bundles appear in DescribeWorkspaceBundles without owner filter.
// 12. DescribeWorkspaceDirectories: registered directories are returned;
//     unregistered directories are not; SubnetIds are propagated.
// 13. GetWorkspacesConnectionStatus: AVAILABLE → DISCONNECTED, STOPPED →
//     NOT_CONNECTED; unfiltered call returns all workspaces.
// 14. CreateTags with empty ResourceId returns 400.
// 15. DescribeWorkspaces pagination produces correct, non-overlapping pages.
// 16. RebootWorkspaces/RebuildWorkspaces do not change workspace state.
// 17. MigrateWorkspace: source workspace is removed, new workspace gets bundleId.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workspaces"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func createWorkspaceWithSpec(t *testing.T, h *workspaces.Handler, userID, dirID, bundleID string) string {
	t.Helper()

	rec := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": []map[string]any{
			{
				"UserName":    userID,
				"DirectoryId": dirID,
				"BundleId":    bundleID,
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
) (ids []string, token string) {
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
	for _, w := range wsList {
		ids = append(ids, w.(map[string]any)["WorkspaceId"].(string))
	}

	token, _ = resp["NextToken"].(string)

	return ids, token
}

// ---------------------------------------------------------------------------
// 1. Pagination
// ---------------------------------------------------------------------------

func TestParity3_Pagination_Limit1(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 3 workspaces so we can paginate through them.
	var createdIDs []string
	for i := range 3 {
		id := createWorkspaceWithSpec(t, h, fmt.Sprintf("user%d", i), "d-abc123", "wsb-bh8rsxt14")
		createdIDs = append(createdIDs, id)
	}

	sort.Strings(createdIDs)

	// First page: limit=1 → one result, token present.
	page1, token1 := describeWorkspacesPage(t, h, "", 1)
	require.Len(t, page1, 1)
	assert.NotEmpty(t, token1, "NextToken must be set when there are more results")
	assert.Equal(t, createdIDs[0], page1[0])

	// Second page: continue from token1 → second result, token present.
	page2, token2 := describeWorkspacesPage(t, h, token1, 1)
	require.Len(t, page2, 1)
	assert.NotEmpty(t, token2)
	assert.Equal(t, createdIDs[1], page2[0])

	// Third page: continue from token2 → last result, no token.
	page3, token3 := describeWorkspacesPage(t, h, token2, 1)
	require.Len(t, page3, 1)
	assert.Empty(t, token3, "NextToken must be absent on the last page")
	assert.Equal(t, createdIDs[2], page3[0])
}

func TestParity3_Pagination_DefaultLimit25(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create exactly 26 workspaces to trigger pagination.
	for i := range 26 {
		createWorkspaceWithSpec(t, h, fmt.Sprintf("user%d", i), "d-abc123", "wsb-bh8rsxt14")
	}

	// First page: no explicit limit → defaults to 25.
	page1, token1 := describeWorkspacesPage(t, h, "", 0)
	assert.Len(t, page1, 25, "default page size must be 25")
	assert.NotEmpty(t, token1)

	// Second page: remaining 1 result.
	page2, token2 := describeWorkspacesPage(t, h, token1, 0)
	assert.Len(t, page2, 1)
	assert.Empty(t, token2)

	// No overlap between pages.
	combined := append(page1, page2...)
	seen := make(map[string]struct{})

	for _, id := range combined {
		_, already := seen[id]
		assert.False(t, already, "workspace %q appeared in both pages", id)
		seen[id] = struct{}{}
	}

	assert.Len(t, combined, 26)
}

func TestParity3_Pagination_SortedByID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		createWorkspaceWithSpec(t, h, fmt.Sprintf("user%d", i), "d-abc123", "wsb-bh8rsxt14")
	}

	// Collect all IDs via 5 single-item pages.
	var collected []string
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

func TestParity3_Pagination_ExplicitLimitCappedAt25(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 30 {
		createWorkspaceWithSpec(t, h, fmt.Sprintf("user%d", i), "d-abc123", "wsb-bh8rsxt14")
	}

	// Even if the client requests limit=100, we cap at 25.
	page1, _ := describeWorkspacesPage(t, h, "", 100)
	assert.LessOrEqual(t, len(page1), 25, "limit must be capped at 25")
}

func TestParity3_Pagination_FilteredByDirectoryID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createWorkspaceWithSpec(t, h, "u1", "d-aaa", "wsb-bh8rsxt14")
	createWorkspaceWithSpec(t, h, "u2", "d-bbb", "wsb-bh8rsxt14")
	createWorkspaceWithSpec(t, h, "u3", "d-aaa", "wsb-bh8rsxt14")

	rec := doTargetRequest(t, h, "DescribeWorkspaces", map[string]any{
		"DirectoryId": "d-aaa",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wsList := resp["Workspaces"].([]any)
	assert.Len(t, wsList, 2, "filter by DirectoryId must return only matching workspaces")
}

// ---------------------------------------------------------------------------
// 2. CreateWorkspaces input validation
// ---------------------------------------------------------------------------

func TestParity3_CreateWorkspaces_EmptyList_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": []any{},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestParity3_CreateWorkspaces_MissingUserName_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": []map[string]any{
			{"DirectoryId": "d-abc", "BundleId": "wsb-bh8rsxt14"},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "missing UserName must return 400")
}

func TestParity3_CreateWorkspaces_MissingDirectoryId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": []map[string]any{
			{"UserName": "alice", "BundleId": "wsb-bh8rsxt14"},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "missing DirectoryId must return 400")
}

func TestParity3_CreateWorkspaces_MissingBundleId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": []map[string]any{
			{"UserName": "alice", "DirectoryId": "d-abc"},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "missing BundleId must return 400")
}

func TestParity3_CreateWorkspaces_TooMany_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	specs := make([]map[string]any, 26)
	for i := range specs {
		specs[i] = map[string]any{
			"UserName":    fmt.Sprintf("user%d", i),
			"DirectoryId": "d-abc",
			"BundleId":    "wsb-bh8rsxt14",
		}
	}

	rec := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": specs,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "more than 25 workspaces per call must return 400")
}

func TestParity3_CreateWorkspaces_MaxAllowed_Returns200(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	specs := make([]map[string]any, 25)
	for i := range specs {
		specs[i] = map[string]any{
			"UserName":    fmt.Sprintf("user%d", i),
			"DirectoryId": "d-abc",
			"BundleId":    "wsb-bh8rsxt14",
		}
	}

	rec := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": specs,
	})
	assert.Equal(t, http.StatusOK, rec.Code, "exactly 25 workspaces per call is the max and must succeed")

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pending := resp["PendingRequests"].([]any)
	assert.Len(t, pending, 25)
}

// ---------------------------------------------------------------------------
// 3. Tag limit enforcement
// ---------------------------------------------------------------------------

func TestParity3_CreateTags_ExceedsLimit_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	// First batch: 50 tags.
	tags50 := make([]map[string]any, 50)
	for i := range tags50 {
		tags50[i] = map[string]any{"Key": fmt.Sprintf("key%d", i), "Value": "v"}
	}

	rec := doTargetRequest(t, h, "CreateTags", map[string]any{
		"ResourceId": wsID,
		"Tags":       tags50,
	})
	assert.Equal(t, http.StatusOK, rec.Code, "50 tags must be accepted")

	// One more tag should push over the limit.
	rec = doTargetRequest(t, h, "CreateTags", map[string]any{
		"ResourceId": wsID,
		"Tags":       []map[string]any{{"Key": "overflow", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "51st tag must be rejected")
}

func TestParity3_CreateTags_Update_DoesNotDoubleCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	// Add 50 tags.
	tags50 := make([]map[string]any, 50)
	for i := range tags50 {
		tags50[i] = map[string]any{"Key": fmt.Sprintf("key%d", i), "Value": "v"}
	}

	rec := doTargetRequest(t, h, "CreateTags", map[string]any{
		"ResourceId": wsID,
		"Tags":       tags50,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Updating an existing key should succeed (not counted as a new tag).
	rec = doTargetRequest(t, h, "CreateTags", map[string]any{
		"ResourceId": wsID,
		"Tags":       []map[string]any{{"Key": "key0", "Value": "updated"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code, "updating existing tag must succeed even at limit")
}

// ---------------------------------------------------------------------------
// 4. Tag key validation
// ---------------------------------------------------------------------------

func TestParity3_CreateTags_EmptyKey_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)
	rec := doTargetRequest(t, h, "CreateTags", map[string]any{
		"ResourceId": wsID,
		"Tags":       []map[string]any{{"Key": "", "Value": "val"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "empty tag key must return 400")
}

func TestParity3_CreateTags_EmptyResourceId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTargetRequest(t, h, "CreateTags", map[string]any{
		"ResourceId": "",
		"Tags":       []map[string]any{{"Key": "k", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "empty ResourceId must return 400")
}

// ---------------------------------------------------------------------------
// 5. WorkspaceProperties in CreateWorkspaces
// ---------------------------------------------------------------------------

func TestParity3_CreateWorkspaces_WithProperties_Stored(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": []map[string]any{
			{
				"UserName":    "alice",
				"DirectoryId": "d-abc",
				"BundleId":    "wsb-bh8rsxt14",
				"WorkspaceProperties": map[string]any{
					"RunningMode":       "AUTO_STOP",
					"ComputeTypeName":   "STANDARD",
					"UserVolumeSizeGib": 50,
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	pending := createResp["PendingRequests"].([]any)
	require.Len(t, pending, 1)

	ws := pending[0].(map[string]any)
	wsID := ws["WorkspaceId"].(string)

	propsRaw, hasProps := ws["WorkspaceProperties"]
	assert.True(t, hasProps, "PendingRequests must include WorkspaceProperties when set at creation")
	require.NotNil(t, propsRaw)

	props := propsRaw.(map[string]any)
	assert.Equal(t, "AUTO_STOP", props["RunningMode"])
	assert.Equal(t, "STANDARD", props["ComputeTypeName"])

	// Confirm properties also appear in DescribeWorkspaces.
	rec2 := doTargetRequest(t, h, "DescribeWorkspaces", map[string]any{
		"WorkspaceIds": []string{wsID},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &descResp))
	wsList := descResp["Workspaces"].([]any)
	require.Len(t, wsList, 1)

	descWs := wsList[0].(map[string]any)
	descPropsRaw, hasDescProps := descWs["WorkspaceProperties"]
	assert.True(t, hasDescProps, "DescribeWorkspaces must reflect creation-time WorkspaceProperties")

	descProps := descPropsRaw.(map[string]any)
	assert.Equal(t, "AUTO_STOP", descProps["RunningMode"])
}

// ---------------------------------------------------------------------------
// 6. SubnetId propagation
// ---------------------------------------------------------------------------

func TestParity3_CreateWorkspaces_SubnetId_Propagated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": []map[string]any{
			{
				"UserName":    "alice",
				"DirectoryId": "d-abc",
				"BundleId":    "wsb-bh8rsxt14",
				"SubnetId":    "subnet-12345678",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	pending := createResp["PendingRequests"].([]any)
	require.Len(t, pending, 1)
	ws := pending[0].(map[string]any)
	wsID := ws["WorkspaceId"].(string)

	// Confirm SubnetId in DescribeWorkspaces.
	rec2 := doTargetRequest(t, h, "DescribeWorkspaces", map[string]any{
		"WorkspaceIds": []string{wsID},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &descResp))
	wsList := descResp["Workspaces"].([]any)
	require.Len(t, wsList, 1)
	assert.Equal(t, "subnet-12345678", wsList[0].(map[string]any)["SubnetId"])
}

// ---------------------------------------------------------------------------
// 7 & 8. ModifyWorkspaceProperties validation
// ---------------------------------------------------------------------------

func TestParity3_ModifyWorkspaceProperties_InvalidComputeType_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)
	rec := doTargetRequest(t, h, "ModifyWorkspaceProperties", map[string]any{
		"WorkspaceId": wsID,
		"WorkspaceProperties": map[string]any{
			"ComputeTypeName": "GIGACORP_TURBO",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "unknown compute type must return 400")
}

func TestParity3_ModifyWorkspaceProperties_InvalidRunningMode_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)
	rec := doTargetRequest(t, h, "ModifyWorkspaceProperties", map[string]any{
		"WorkspaceId": wsID,
		"WorkspaceProperties": map[string]any{
			"RunningMode": "TURBO_MODE",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "unknown running mode must return 400")
}

func TestParity3_ModifyWorkspaceProperties_ValidComputeTypes_Accept(t *testing.T) {
	t.Parallel()

	validTypes := []string{
		"VALUE", "STANDARD", "PERFORMANCE", "POWER",
		"GRAPHICS", "GRAPHICSPRO", "POWERPRO",
		"GRAPHICS_G4DN", "GRAPHICSPRO_G4DN",
	}

	for _, ct := range validTypes {
		ct := ct

		t.Run(ct, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			wsID := createWorkspace(t, h)
			rec := doTargetRequest(t, h, "ModifyWorkspaceProperties", map[string]any{
				"WorkspaceId": wsID,
				"WorkspaceProperties": map[string]any{
					"ComputeTypeName": ct,
				},
			})
			assert.Equal(t, http.StatusOK, rec.Code, "ComputeTypeName %q must be accepted", ct)
		})
	}
}

func TestParity3_ModifyWorkspaceProperties_ValidRunningModes_Accept(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"ALWAYS_ON", "AUTO_STOP"} {
		mode := mode

		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			wsID := createWorkspace(t, h)
			rec := doTargetRequest(t, h, "ModifyWorkspaceProperties", map[string]any{
				"WorkspaceId": wsID,
				"WorkspaceProperties": map[string]any{
					"RunningMode": mode,
				},
			})
			assert.Equal(t, http.StatusOK, rec.Code, "RunningMode %q must be accepted", mode)
		})
	}
}

// ---------------------------------------------------------------------------
// 9. RunningModeAutoStopTimeoutInMinutes validation
// ---------------------------------------------------------------------------

func TestParity3_ModifyWorkspaceProperties_AutoStopTimeout_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		timeout     int
		wantCode    int
	}{
		{name: "60_accepted", timeout: 60, wantCode: http.StatusOK},
		{name: "120_accepted", timeout: 120, wantCode: http.StatusOK},
		{name: "600_accepted", timeout: 600, wantCode: http.StatusOK},
		{name: "30_rejected", timeout: 30, wantCode: http.StatusBadRequest},
		{name: "601_rejected", timeout: 601, wantCode: http.StatusBadRequest},
		{name: "90_not_multiple_of_60_rejected", timeout: 90, wantCode: http.StatusBadRequest},
		{name: "0_accepted_no_op", timeout: 0, wantCode: http.StatusOK},
	}

	for _, tc := range tests {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			wsID := createWorkspace(t, h)
			rec := doTargetRequest(t, h, "ModifyWorkspaceProperties", map[string]any{
				"WorkspaceId": wsID,
				"WorkspaceProperties": map[string]any{
					"RunningModeAutoStopTimeoutInMinutes": tc.timeout,
				},
			})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// 10. Bundle response fidelity
// ---------------------------------------------------------------------------

func TestParity3_DescribeWorkspaceBundles_ComputeTypeAndStorage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTargetRequest(t, h, "DescribeWorkspaceBundles", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	bundles := resp["Bundles"].([]any)
	require.NotEmpty(t, bundles)

	for _, b := range bundles {
		bun := b.(map[string]any)
		bundleID := bun["BundleId"].(string)

		ctRaw, hasComputeType := bun["ComputeType"]
		assert.True(t, hasComputeType, "bundle %s must have ComputeType", bundleID)

		if hasComputeType {
			ct := ctRaw.(map[string]any)
			name, _ := ct["Name"].(string)
			assert.NotEmpty(t, name, "bundle %s ComputeType.Name must not be empty", bundleID)
		}

		usRaw, hasUserStorage := bun["UserStorage"]
		assert.True(t, hasUserStorage, "bundle %s must have UserStorage", bundleID)

		if hasUserStorage {
			us := usRaw.(map[string]any)
			cap, _ := us["Capacity"].(float64)
			assert.Greater(t, cap, float64(0), "bundle %s UserStorage.Capacity must be > 0", bundleID)
		}

		rsRaw, hasRootStorage := bun["RootStorage"]
		assert.True(t, hasRootStorage, "bundle %s must have RootStorage", bundleID)

		if hasRootStorage {
			rs := rsRaw.(map[string]any)
			cap, _ := rs["Capacity"].(float64)
			assert.Greater(t, cap, float64(0), "bundle %s RootStorage.Capacity must be > 0", bundleID)
		}
	}
}

func TestParity3_DescribeWorkspaceBundles_ByOwnerAmazon(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a custom bundle.
	rec := doTargetRequest(t, h, "CreateWorkspaceBundle", map[string]any{
		"BundleName":  "MyBundle",
		"ComputeType": map[string]any{"Name": "STANDARD"},
		"UserStorage": map[string]any{"Capacity": 50},
		"RootStorage": map[string]any{"Capacity": 80},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Filter by owner=Amazon: should NOT include custom bundle.
	rec2 := doTargetRequest(t, h, "DescribeWorkspaceBundles", map[string]any{
		"Owner": "Amazon",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	bundles := resp["Bundles"].([]any)

	for _, b := range bundles {
		bun := b.(map[string]any)
		assert.Equal(t, "Amazon", bun["Owner"], "owner=Amazon filter must exclude custom bundles")
	}
}

// ---------------------------------------------------------------------------
// 11. Custom bundles in DescribeWorkspaceBundles
// ---------------------------------------------------------------------------

func TestParity3_DescribeWorkspaceBundles_IncludesCustomBundle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doTargetRequest(t, h, "CreateWorkspaceBundle", map[string]any{
		"BundleName":        "MyCustomBundle",
		"BundleDescription": "A test bundle",
		"ComputeType":       map[string]any{"Name": "STANDARD"},
		"UserStorage":       map[string]any{"Capacity": 50},
		"RootStorage":       map[string]any{"Capacity": 80},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	customBundleID := createResp["WorkspaceBundle"].(map[string]any)["BundleId"].(string)

	// Without owner filter: should include both Amazon and custom bundles.
	rec2 := doTargetRequest(t, h, "DescribeWorkspaceBundles", map[string]any{})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	bundles := resp["Bundles"].([]any)

	found := false
	for _, b := range bundles {
		if b.(map[string]any)["BundleId"] == customBundleID {
			found = true
		}
	}

	assert.True(t, found, "custom bundle must appear in unfiltered DescribeWorkspaceBundles")
}

func TestParity3_DescribeWorkspaceBundles_FilterByID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTargetRequest(t, h, "DescribeWorkspaceBundles", map[string]any{
		"BundleIds": []string{"wsb-bh8rsxt14"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	bundles := resp["Bundles"].([]any)
	require.Len(t, bundles, 1)
	assert.Equal(t, "wsb-bh8rsxt14", bundles[0].(map[string]any)["BundleId"])
}

// ---------------------------------------------------------------------------
// 12. DescribeWorkspaceDirectories
// ---------------------------------------------------------------------------

func TestParity3_DescribeWorkspaceDirectories_AfterRegister(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Before registration: no directories returned.
	rec := doTargetRequest(t, h, "DescribeWorkspaceDirectories", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	dirs := resp["Directories"].([]any)
	assert.Empty(t, dirs, "DescribeWorkspaceDirectories must be empty before registration")

	// Register a directory.
	rec2 := doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{
		"DirectoryId": "d-abc123456",
		"SubnetIds":   []string{"subnet-aaa", "subnet-bbb"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	// After registration: directory must appear.
	rec3 := doTargetRequest(t, h, "DescribeWorkspaceDirectories", map[string]any{})
	require.Equal(t, http.StatusOK, rec3.Code)

	var resp3 map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &resp3))
	dirs3 := resp3["Directories"].([]any)
	require.Len(t, dirs3, 1)

	dir := dirs3[0].(map[string]any)
	assert.Equal(t, "d-abc123456", dir["DirectoryId"])
	assert.Equal(t, "REGISTERED", dir["State"])

	subnetIDs, ok := dir["SubnetIds"].([]any)
	require.True(t, ok, "SubnetIds must be present after registration with subnets")
	assert.Len(t, subnetIDs, 2)
}

func TestParity3_DescribeWorkspaceDirectories_FilterByID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{
		"DirectoryId": "d-aaaa",
	})
	doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{
		"DirectoryId": "d-bbbb",
	})

	rec := doTargetRequest(t, h, "DescribeWorkspaceDirectories", map[string]any{
		"DirectoryIds": []string{"d-aaaa"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	dirs := resp["Directories"].([]any)
	require.Len(t, dirs, 1)
	assert.Equal(t, "d-aaaa", dirs[0].(map[string]any)["DirectoryId"])
}

func TestParity3_DescribeWorkspaceDirectories_AfterDeregister(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{
		"DirectoryId": "d-xyz",
	})

	doTargetRequest(t, h, "DeregisterWorkspaceDirectory", map[string]any{
		"DirectoryId": "d-xyz",
	})

	rec := doTargetRequest(t, h, "DescribeWorkspaceDirectories", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	dirs := resp["Directories"].([]any)
	assert.Empty(t, dirs, "deregistered directory must not appear")
}

// ---------------------------------------------------------------------------
// 13. GetWorkspacesConnectionStatus
// ---------------------------------------------------------------------------

func TestParity3_ConnectionStatus_Available_IsDisconnected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	rec := doTargetRequest(t, h, "DescribeWorkspacesConnectionStatus", map[string]any{
		"WorkspaceIds": []string{wsID},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	statuses := resp["WorkspacesConnectionStatus"].([]any)
	require.Len(t, statuses, 1)
	assert.Equal(t, "DISCONNECTED", statuses[0].(map[string]any)["ConnectionState"],
		"AVAILABLE workspace must have DISCONNECTED connection state")
}

func TestParity3_ConnectionStatus_Stopped_IsNotConnected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	doTargetRequest(t, h, "StopWorkspaces", map[string]any{
		"StopWorkspaceRequests": []map[string]any{{"WorkspaceId": wsID}},
	})

	rec := doTargetRequest(t, h, "DescribeWorkspacesConnectionStatus", map[string]any{
		"WorkspaceIds": []string{wsID},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	statuses := resp["WorkspacesConnectionStatus"].([]any)
	require.Len(t, statuses, 1)
	assert.Equal(t, "NOT_CONNECTED", statuses[0].(map[string]any)["ConnectionState"],
		"STOPPED workspace must have NOT_CONNECTED connection state")
}

func TestParity3_ConnectionStatus_AllWorkspaces_NoFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	id1 := createWorkspaceWithSpec(t, h, "u1", "d-aaa", "wsb-bh8rsxt14")
	id2 := createWorkspaceWithSpec(t, h, "u2", "d-aaa", "wsb-bh8rsxt14")

	// No WorkspaceIds filter → return all.
	rec := doTargetRequest(t, h, "DescribeWorkspacesConnectionStatus", map[string]any{
		"WorkspaceIds": []string{},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	statuses := resp["WorkspacesConnectionStatus"].([]any)
	assert.Len(t, statuses, 2, "empty WorkspaceIds must return status for all workspaces")

	ids := map[string]struct{}{id1: {}, id2: {}}
	for _, s := range statuses {
		id := s.(map[string]any)["WorkspaceId"].(string)
		_, ok := ids[id]
		assert.True(t, ok, "unexpected workspace ID in connection status: %q", id)
	}
}

func TestParity3_ConnectionStatus_UnknownID_Excluded(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doTargetRequest(t, h, "DescribeWorkspacesConnectionStatus", map[string]any{
		"WorkspaceIds": []string{"ws-doesnotexist"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	statuses := resp["WorkspacesConnectionStatus"].([]any)
	assert.Empty(t, statuses, "unknown workspace ID must be silently excluded from status results")
}

// ---------------------------------------------------------------------------
// 16. Reboot/Rebuild do not change state
// ---------------------------------------------------------------------------

func TestParity3_RebootWorkspaces_DoesNotChangeState(t *testing.T) {
	t.Parallel()

	backend := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
	h := workspaces.NewHandler(backend)
	wsID := createWorkspace(t, h)

	rec := doTargetRequest(t, h, "RebootWorkspaces", map[string]any{
		"RebootWorkspaceRequests": []map[string]any{{"WorkspaceId": wsID}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, "AVAILABLE", workspaces.WorkspaceState(backend, wsID),
		"RebootWorkspaces must not change workspace state")
}

func TestParity3_RebuildWorkspaces_DoesNotChangeState(t *testing.T) {
	t.Parallel()

	backend := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
	h := workspaces.NewHandler(backend)
	wsID := createWorkspace(t, h)

	// Stop first to verify the state is NOT reset on rebuild.
	doTargetRequest(t, h, "StopWorkspaces", map[string]any{
		"StopWorkspaceRequests": []map[string]any{{"WorkspaceId": wsID}},
	})
	require.Equal(t, "STOPPED", workspaces.WorkspaceState(backend, wsID))

	rec := doTargetRequest(t, h, "RebuildWorkspaces", map[string]any{
		"RebuildWorkspaceRequests": []map[string]any{{"WorkspaceId": wsID}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, "STOPPED", workspaces.WorkspaceState(backend, wsID),
		"RebuildWorkspaces must not change workspace state")
}

// ---------------------------------------------------------------------------
// 17. MigrateWorkspace
// ---------------------------------------------------------------------------

func TestParity3_MigrateWorkspace_SourceRemovedTargetCreated(t *testing.T) {
	t.Parallel()

	backend := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
	h := workspaces.NewHandler(backend)
	srcID := createWorkspace(t, h)

	rec := doTargetRequest(t, h, "MigrateWorkspace", map[string]any{
		"SourceWorkspaceId": srcID,
		"BundleId":          "wsb-gm4d5tx2v",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	// Original workspace must no longer exist.
	assert.Equal(t, "", workspaces.WorkspaceState(backend, srcID),
		"source workspace must be removed after migration")

	// A new workspace must exist.
	targetID, _ := resp["TargetWorkspaceId"].(string)
	require.NotEmpty(t, targetID)
	assert.NotEqual(t, srcID, targetID)
	assert.Equal(t, "AVAILABLE", workspaces.WorkspaceState(backend, targetID))
}

func TestParity3_MigrateWorkspace_UnknownSource_Returns404(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTargetRequest(t, h, "MigrateWorkspace", map[string]any{
		"SourceWorkspaceId": "ws-doesnotexist",
		"BundleId":          "wsb-gm4d5tx2v",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---------------------------------------------------------------------------
// Additional edge cases
// ---------------------------------------------------------------------------

func TestParity3_DescribeWorkspaces_EmptyList_WhenNoWorkspaces(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTargetRequest(t, h, "DescribeWorkspaces", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wsList := resp["Workspaces"].([]any)
	assert.Empty(t, wsList)

	_, hasToken := resp["NextToken"]
	assert.False(t, hasToken, "NextToken must be absent when there are no results")
}

func TestParity3_DescribeWorkspaces_MultipleIDs_AllReturned(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id1 := createWorkspaceWithSpec(t, h, "u1", "d-aaa", "wsb-bh8rsxt14")
	id2 := createWorkspaceWithSpec(t, h, "u2", "d-aaa", "wsb-bh8rsxt14")

	rec := doTargetRequest(t, h, "DescribeWorkspaces", map[string]any{
		"WorkspaceIds": []string{id1, id2},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wsList := resp["Workspaces"].([]any)
	assert.Len(t, wsList, 2)
}

func TestParity3_DescribeWorkspaces_FilterByUserName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createWorkspaceWithSpec(t, h, "alice", "d-aaa", "wsb-bh8rsxt14")
	createWorkspaceWithSpec(t, h, "bob", "d-aaa", "wsb-bh8rsxt14")
	createWorkspaceWithSpec(t, h, "alice", "d-aaa", "wsb-bh8rsxt14")

	rec := doTargetRequest(t, h, "DescribeWorkspaces", map[string]any{
		"UserName":    "alice",
		"DirectoryId": "d-aaa",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wsList := resp["Workspaces"].([]any)
	assert.Len(t, wsList, 2, "UserName filter must return only Alice's workspaces")

	for _, w := range wsList {
		assert.Equal(t, "alice", w.(map[string]any)["UserName"])
	}
}

func TestParity3_CreateWorkspaces_VolumeEncryption_Propagated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": []map[string]any{
			{
				"UserName":                    "alice",
				"DirectoryId":                 "d-abc",
				"BundleId":                    "wsb-bh8rsxt14",
				"VolumeEncryptionKey":         "arn:aws:kms:us-east-1:123456789012:key/abc123",
				"UserVolumeEncryptionEnabled": true,
				"RootVolumeEncryptionEnabled": true,
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	pending := createResp["PendingRequests"].([]any)
	require.Len(t, pending, 1)
	ws := pending[0].(map[string]any)
	wsID := ws["WorkspaceId"].(string)

	// VolumeEncryptionKey must be propagated in DescribeWorkspaces.
	rec2 := doTargetRequest(t, h, "DescribeWorkspaces", map[string]any{
		"WorkspaceIds": []string{wsID},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &descResp))
	wsList := descResp["Workspaces"].([]any)
	require.Len(t, wsList, 1)
	descWs := wsList[0].(map[string]any)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/abc123", descWs["VolumeEncryptionKey"])
}
