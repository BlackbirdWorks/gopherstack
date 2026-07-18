package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// ---- Folder CRUD round-trip, not-found, and duplicate errors ----

func TestQuickSight_FolderCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *quicksight.Handler)
		check    func(t *testing.T, body map[string]any)
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "CreateFolder returns 200 with Arn and FolderId",
			method:   http.MethodPost,
			path:     accountPath("/folders/fld1"),
			body:     map[string]any{"Name": "My Folder"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "fld1", body["FolderId"])
				assert.Contains(t, body["Arn"], "arn:aws:quicksight:us-east-1:000000000000:folder/fld1")
			},
		},
		{
			name:   "CreateFolder duplicate returns 409 ResourceExistsException",
			method: http.MethodPost,
			path:   accountPath("/folders/dup"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/folders/dup"), map[string]any{"Name": "x"})
			},
			body:     map[string]any{"Name": "x"},
			wantCode: http.StatusConflict,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "ResourceExistsException", body["Code"])
			},
		},
		{
			name:     "CreateFolder missing Name returns 400",
			method:   http.MethodPost,
			path:     accountPath("/folders/noname"),
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "CreateFolder invalid FolderType returns 400",
			method:   http.MethodPost,
			path:     accountPath("/folders/badtype"),
			body:     map[string]any{"Name": "x", "FolderType": "BOGUS"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DescribeFolder returns folder",
			method: http.MethodGet,
			path:   accountPath("/folders/fld2"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/folders/fld2"), map[string]any{
					"Name": "F2", "FolderType": "RESTRICTED",
				})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				f, ok := body["Folder"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "F2", f["Name"])
				assert.Equal(t, "RESTRICTED", f["FolderType"])
				assert.Equal(t, "fld2", f["FolderId"])
			},
		},
		{
			name:     "DescribeFolder missing returns 404",
			method:   http.MethodGet,
			path:     accountPath("/folders/notexist"),
			wantCode: http.StatusNotFound,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "ResourceNotFoundException", body["Code"])
			},
		},
		{
			name:   "UpdateFolder renames folder",
			method: http.MethodPut,
			path:   accountPath("/folders/fld3"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/folders/fld3"), map[string]any{"Name": "Old"})
			},
			body:     map[string]any{"Name": "New"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "fld3", body["FolderId"])
			},
		},
		{
			name:     "UpdateFolder missing returns 404",
			method:   http.MethodPut,
			path:     accountPath("/folders/notexist"),
			body:     map[string]any{"Name": "New"},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DeleteFolder removes folder",
			method: http.MethodDelete,
			path:   accountPath("/folders/fld4"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/folders/fld4"), map[string]any{"Name": "x"})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "fld4", body["FolderId"])
			},
		},
		{
			name:     "DeleteFolder missing returns 404",
			method:   http.MethodDelete,
			path:     accountPath("/folders/notexist"),
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, parseBody(t, rec))
			}
		})
	}
}

// DeleteFolder must cascade-delete its memberships.
func TestQuickSight_DeleteFolder_CascadesMemberships(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, accountPath("/folders/casc"), map[string]any{"Name": "x"})
	doRequest(t, h, http.MethodPut, accountPath("/folders/casc/members/DATASET/ds1"), nil)

	doRequest(t, h, http.MethodDelete, accountPath("/folders/casc"), nil)

	// Recreate the folder; membership must not have survived the parent's deletion.
	doRequest(t, h, http.MethodPost, accountPath("/folders/casc"), map[string]any{"Name": "x"})
	rec := doRequest(t, h, http.MethodGet, accountPath("/folders/casc/members"), nil)
	body := parseBody(t, rec)
	items, ok := body["FolderMemberList"].([]any)
	require.True(t, ok)
	assert.Empty(t, items)
}

// ---- ListFolders pagination ----

func TestQuickSight_ListFolders_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		doRequest(t, h, http.MethodPost, accountPath("/folders/"+id), map[string]any{"Name": id})
	}

	rec := doRequest(t, h, http.MethodGet, accountPath("/folders?max-results=2"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	items, ok := body["FolderSummaryList"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
	next, ok := body["NextToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, next)

	seen := map[string]bool{}
	for _, it := range items {
		m1, isMap := it.(map[string]any)
		require.True(t, isMap)
		seen[m1["FolderId"].(string)] = true
	}

	rec2 := doRequest(t, h, http.MethodGet, accountPath(fmt.Sprintf("/folders?max-results=2&next-token=%s", next)), nil)
	require.Equal(t, http.StatusOK, rec2.Code)
	body2 := parseBody(t, rec2)
	items2, ok := body2["FolderSummaryList"].([]any)
	require.True(t, ok)
	assert.Len(t, items2, 2)
	for _, it := range items2 {
		m2, isMap := it.(map[string]any)
		require.True(t, isMap)
		assert.False(t, seen[m2["FolderId"].(string)], "page 2 must not repeat page 1 items")
	}
}

// ---- SearchFolders ----

func TestQuickSight_SearchFolders(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, accountPath("/folders/parent"), map[string]any{"Name": "Parent"})
	parentArn := fmt.Sprintf("arn:aws:quicksight:%s:%s:folder/parent", testRegion, testAccountID)
	doRequest(t, h, http.MethodPost, accountPath("/folders/child1"), map[string]any{
		"Name": "ChildOne", "ParentFolderArn": parentArn,
	})
	doRequest(t, h, http.MethodPost, accountPath("/folders/child2"), map[string]any{"Name": "ChildTwo"})

	rec := doRequest(t, h, http.MethodPost, accountPath("/search/folders"), map[string]any{
		"Filters": []any{
			map[string]any{"Operator": "StringEquals", "Name": "PARENT_FOLDER_ARN", "Value": parentArn},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	items, ok := body["FolderSummaryList"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)
	m, ok := items[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "child1", m["FolderId"])

	recByName := doRequest(t, h, http.MethodPost, accountPath("/search/folders"), map[string]any{
		"Filters": []any{
			map[string]any{"Operator": "StringEquals", "Name": "FOLDER_NAME", "Value": "ChildTwo"},
		},
	})
	require.Equal(t, http.StatusOK, recByName.Code)
	bodyByName := parseBody(t, recByName)
	itemsByName, ok := bodyByName["FolderSummaryList"].([]any)
	require.True(t, ok)
	require.Len(t, itemsByName, 1)
}

// ---- Folder memberships ----

func TestQuickSight_FolderMemberships(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, accountPath("/folders/mfld"), map[string]any{"Name": "x"})

	rec := doRequest(t, h, http.MethodPut, accountPath("/folders/mfld/members/DASHBOARD/dash1"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	member, ok := body["FolderMember"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "dash1", member["MemberId"])

	doRequest(t, h, http.MethodPut, accountPath("/folders/mfld/members/DATASET/ds1"), nil)

	listRec := doRequest(t, h, http.MethodGet, accountPath("/folders/mfld/members"), nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	listBody := parseBody(t, listRec)
	items, ok := listBody["FolderMemberList"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)

	delRec := doRequest(t, h, http.MethodDelete, accountPath("/folders/mfld/members/DASHBOARD/dash1"), nil)
	assert.Equal(t, http.StatusOK, delRec.Code)

	afterDelRec := doRequest(t, h, http.MethodGet, accountPath("/folders/mfld/members"), nil)
	afterDelBody := parseBody(t, afterDelRec)
	afterItems, ok := afterDelBody["FolderMemberList"].([]any)
	require.True(t, ok)
	assert.Len(t, afterItems, 1)

	missingDelRec := doRequest(t, h, http.MethodDelete, accountPath("/folders/mfld/members/DASHBOARD/dash1"), nil)
	assert.Equal(t, http.StatusNotFound, missingDelRec.Code)

	badCreateRec := doRequest(t, h, http.MethodPut, accountPath("/folders/mfld/members/BOGUS/x"), nil)
	assert.Equal(t, http.StatusBadRequest, badCreateRec.Code)

	noFolderRec := doRequest(t, h, http.MethodPut, accountPath("/folders/notexist/members/DASHBOARD/d1"), nil)
	assert.Equal(t, http.StatusNotFound, noFolderRec.Code)
}

// ---- Folder permissions: grant, revoke, describe, resolved (with parent inheritance) ----

func TestQuickSight_FolderPermissions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, accountPath("/folders/pfld"), map[string]any{"Name": "x"})

	describeEmpty := doRequest(t, h, http.MethodGet, accountPath("/folders/pfld/permissions"), nil)
	require.Equal(t, http.StatusOK, describeEmpty.Code)
	emptyBody := parseBody(t, describeEmpty)
	assert.Equal(t, "pfld", emptyBody["FolderId"])
	emptyPerms, ok := emptyBody["Permissions"].([]any)
	require.True(t, ok)
	assert.Empty(t, emptyPerms)

	grantRec := doRequest(t, h, http.MethodPut, accountPath("/folders/pfld/permissions"), map[string]any{
		"GrantPermissions": []any{
			map[string]any{
				"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/alice",
				"Actions":   []any{"quicksight:DescribeFolder", "quicksight:CreateFolder"},
			},
		},
	})
	require.Equal(t, http.StatusOK, grantRec.Code)
	grantBody := parseBody(t, grantRec)
	perms, ok := grantBody["Permissions"].([]any)
	require.True(t, ok)
	require.Len(t, perms, 1)
	p0, ok := perms[0].(map[string]any)
	require.True(t, ok)
	actions, ok := p0["Actions"].([]any)
	require.True(t, ok)
	assert.Len(t, actions, 2)

	revokeRec := doRequest(t, h, http.MethodPut, accountPath("/folders/pfld/permissions"), map[string]any{
		"RevokePermissions": []any{
			map[string]any{
				"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/alice",
				"Actions":   []any{"quicksight:CreateFolder"},
			},
		},
	})
	require.Equal(t, http.StatusOK, revokeRec.Code)
	revokeBody := parseBody(t, revokeRec)
	revokedPerms, ok := revokeBody["Permissions"].([]any)
	require.True(t, ok)
	require.Len(t, revokedPerms, 1)
	rp0, ok := revokedPerms[0].(map[string]any)
	require.True(t, ok)
	rActions, ok := rp0["Actions"].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"quicksight:DescribeFolder"}, rActions)

	permsMissing := doRequest(t, h, http.MethodGet, accountPath("/folders/notexist/permissions"), nil)
	assert.Equal(t, http.StatusNotFound, permsMissing.Code)
}

func TestQuickSight_FolderResolvedPermissions_InheritsFromParent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, accountPath("/folders/root"), map[string]any{"Name": "Root"})
	doRequest(t, h, http.MethodPut, accountPath("/folders/root/permissions"), map[string]any{
		"GrantPermissions": []any{
			map[string]any{
				"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/bob",
				"Actions":   []any{"quicksight:DescribeFolder"},
			},
		},
	})

	parentArn := fmt.Sprintf("arn:aws:quicksight:%s:%s:folder/root", testRegion, testAccountID)
	doRequest(t, h, http.MethodPost, accountPath("/folders/child"), map[string]any{
		"Name": "Child", "ParentFolderArn": parentArn,
	})
	doRequest(t, h, http.MethodPut, accountPath("/folders/child/permissions"), map[string]any{
		"GrantPermissions": []any{
			map[string]any{
				"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/carol",
				"Actions":   []any{"quicksight:UpdateFolder"},
			},
		},
	})

	rec := doRequest(t, h, http.MethodGet, accountPath("/folders/child/resolved-permissions"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	perms, ok := body["Permissions"].([]any)
	require.True(t, ok)
	require.Len(t, perms, 2)

	principals := map[string]bool{}
	for _, item := range perms {
		m, isMap := item.(map[string]any)
		require.True(t, isMap)
		principals[m["Principal"].(string)] = true
	}
	assert.True(t, principals["arn:aws:quicksight:us-east-1:000000000000:user/default/bob"])
	assert.True(t, principals["arn:aws:quicksight:us-east-1:000000000000:user/default/carol"])
}

// ---- Folder tests ---- //nolint:godot // existing issue.
func TestQuickSight_Folders(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		body       any
		setup      func(h any)
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "create folder",
			method:     http.MethodPost,
			path:       accountPath("/folders/f1"),
			body:       map[string]any{"Name": "MyFolder"},
			wantStatus: http.StatusOK,
			wantKey:    "FolderId",
		},
		{
			name:       "describe folder",
			method:     http.MethodGet,
			path:       accountPath("/folders/f1"),
			wantStatus: http.StatusOK,
			wantKey:    "Folder",
		},
		{
			name:       "update folder",
			method:     http.MethodPut,
			path:       accountPath("/folders/f1"),
			body:       map[string]any{"Name": "Renamed"},
			wantStatus: http.StatusOK,
			wantKey:    "FolderId",
		},
		{
			name:       "list folders",
			method:     http.MethodGet,
			path:       accountPath("/folders"),
			wantStatus: http.StatusOK,
			wantKey:    "FolderSummaryList",
		},
		{
			name:       "describe folder permissions",
			method:     http.MethodGet,
			path:       accountPath("/folders/f1/permissions"),
			wantStatus: http.StatusOK,
			wantKey:    "FolderId",
		},
		{
			name:       "update folder permissions",
			method:     http.MethodPut,
			path:       accountPath("/folders/f1/permissions"),
			body:       map[string]any{"GrantPermissions": []any{}, "RevokePermissions": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "FolderId",
		},
		{
			name:       "describe folder resolved permissions",
			method:     http.MethodGet,
			path:       accountPath("/folders/f1/resolved-permissions"),
			wantStatus: http.StatusOK,
			wantKey:    "FolderId",
		},
		{
			name:       "create folder membership",
			method:     http.MethodPut,
			path:       accountPath("/folders/f1/members/DATASET/ds1"),
			wantStatus: http.StatusOK,
			wantKey:    "FolderMember",
		},
		{
			name:       "list folder members",
			method:     http.MethodGet,
			path:       accountPath("/folders/f1/members"),
			wantStatus: http.StatusOK,
			wantKey:    "FolderMemberList",
		},
		{
			name:       "list folders for resource",
			method:     http.MethodGet,
			path:       accountPath("/resource/ds1resarn/folders"),
			wantStatus: http.StatusOK,
			wantKey:    "Folders",
		},
		{
			name:       "delete folder membership",
			method:     http.MethodDelete,
			path:       accountPath("/folders/f1/members/DATASET/ds1"),
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete folder",
			method:     http.MethodDelete,
			path:       accountPath("/folders/f1"),
			wantStatus: http.StatusOK,
			wantKey:    "FolderId",
		},
	}

	h := newTestHandler(t)

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		})
	}
}

func TestQuickSight_FolderRoundTrip(t *testing.T) { //nolint:paralleltest // shared handler state.
	b := newTestBackend(t)
	h := quicksight.NewHandler(b)

	steps := []roundTripStep{
		{
			name:         "describe missing folder returns not found",
			method:       http.MethodGet,
			path:         accountPath("/folders/rtf1"),
			wantStatus:   http.StatusNotFound,
			wantContains: "Message",
		},
		{
			name:         "create folder",
			method:       http.MethodPost,
			path:         accountPath("/folders/rtf1"),
			body:         map[string]any{"Name": "RT Folder"},
			wantStatus:   http.StatusOK,
			wantContains: "FolderId",
		},
		{
			name:         "describe reflects created folder",
			method:       http.MethodGet,
			path:         accountPath("/folders/rtf1"),
			wantStatus:   http.StatusOK,
			wantContains: "Folder",
		},
		{
			name:         "update folder",
			method:       http.MethodPut,
			path:         accountPath("/folders/rtf1"),
			body:         map[string]any{"Name": "RT Renamed"},
			wantStatus:   http.StatusOK,
			wantContains: "FolderId",
		},
		{
			name:         "list folders includes folder",
			method:       http.MethodGet,
			path:         accountPath("/folders"),
			wantStatus:   http.StatusOK,
			wantContains: "FolderSummaryList",
		},
		{
			name:         "delete folder",
			method:       http.MethodDelete,
			path:         accountPath("/folders/rtf1"),
			wantStatus:   http.StatusOK,
			wantContains: "FolderId",
		},
		{
			name:         "describe after delete returns not found",
			method:       http.MethodGet,
			path:         accountPath("/folders/rtf1"),
			wantStatus:   http.StatusNotFound,
			wantContains: "Message",
		},
	}

	runRoundTrip(t, h, steps)

	// describe reflects renamed value before delete is verified separately below.
	assert.Equal(t, 0, quicksight.FolderCount(b), "folder removed after delete")
}

func TestQuickSight_FolderDescribeReflectsUpdate(t *testing.T) { //nolint:paralleltest // shared handler state.
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, accountPath("/folders/upd1"), map[string]any{"Name": "Original"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodPut, accountPath("/folders/upd1"), map[string]any{"Name": "Updated"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, accountPath("/folders/upd1"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	folder, ok := body["Folder"].(map[string]any)
	require.True(t, ok, "Folder object present")
	assert.Equal(t, "Updated", folder["Name"], "describe reflects updated name")
}
