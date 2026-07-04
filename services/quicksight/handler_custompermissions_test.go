package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- CustomPermissions CRUD round-trip and errors ----

func TestQuickSight_CustomPermissionsCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, accountPath("/custom-permissions"), map[string]any{
		"CustomPermissionsName": "cp1",
		"Capabilities":          map[string]any{"ExportToCsv": "DENY"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	arn, ok := parseBody(t, createRec)["Arn"].(string)
	require.True(t, ok)
	assert.Contains(t, arn, "custom-permissions/cp1")

	dupRec := doRequest(t, h, http.MethodPost, accountPath("/custom-permissions"), map[string]any{
		"CustomPermissionsName": "cp1",
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)

	describeRec := doRequest(t, h, http.MethodGet, accountPath("/custom-permissions/cp1"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	cp := parseBody(t, describeRec)["CustomPermissions"].(map[string]any)
	caps := cp["Capabilities"].(map[string]any)
	assert.Equal(t, "DENY", caps["ExportToCsv"])

	missingRec := doRequest(t, h, http.MethodGet, accountPath("/custom-permissions/notexist"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)

	updateRec := doRequest(t, h, http.MethodPut, accountPath("/custom-permissions/cp1"), map[string]any{
		"Capabilities": map[string]any{"ExportToCsv": "ALLOW"},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	describeAfterUpdate := doRequest(t, h, http.MethodGet, accountPath("/custom-permissions/cp1"), nil)
	afterCaps := parseBody(t, describeAfterUpdate)["CustomPermissions"].(map[string]any)["Capabilities"].(map[string]any)
	assert.Equal(t, "ALLOW", afterCaps["ExportToCsv"])

	// Assign to a role, then deleting should conflict.
	assignRec := doRequest(t, h, http.MethodPut, nsPath("/roles/AUTHOR/custom-permission"), map[string]any{
		"CustomPermissionsName": "cp1",
	})
	require.Equal(t, http.StatusOK, assignRec.Code)

	inUseRec := doRequest(t, h, http.MethodDelete, accountPath("/custom-permissions/cp1"), nil)
	assert.Equal(t, http.StatusConflict, inUseRec.Code)

	unassignRec := doRequest(t, h, http.MethodDelete, nsPath("/roles/AUTHOR/custom-permission"), nil)
	require.Equal(t, http.StatusOK, unassignRec.Code)

	deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/custom-permissions/cp1"), nil)
	require.Equal(t, http.StatusOK, deleteRec.Code)

	deleteMissingRec := doRequest(t, h, http.MethodDelete, accountPath("/custom-permissions/cp1"), nil)
	assert.Equal(t, http.StatusNotFound, deleteMissingRec.Code)
}

// ---- ListCustomPermissions pagination ----

func TestQuickSight_ListCustomPermissions_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		doRequest(t, h, http.MethodPost, accountPath("/custom-permissions"), map[string]any{
			"CustomPermissionsName": name,
		})
	}

	rec := doRequest(t, h, http.MethodGet, accountPath("/custom-permissions?max-results=2"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	items, ok := body["CustomPermissionsList"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
	next := body["NextToken"].(string)
	require.NotEmpty(t, next)

	page2 := doRequest(
		t, h, http.MethodGet,
		accountPath(fmt.Sprintf("/custom-permissions?max-results=2&next-token=%s", next)), nil,
	)
	require.Equal(t, http.StatusOK, page2.Code)
	assert.Len(t, parseBody(t, page2)["CustomPermissionsList"].([]any), 2)
}

// ---- Role custom permission errors ----

func TestQuickSight_RoleCustomPermission_Errors(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Describe before anything is assigned -> 404.
	describeRec := doRequest(t, h, http.MethodGet, nsPath("/roles/READER/custom-permission"), nil)
	assert.Equal(t, http.StatusNotFound, describeRec.Code)

	// Assigning a CustomPermissionsName that doesn't exist -> 404.
	updateRec := doRequest(t, h, http.MethodPut, nsPath("/roles/READER/custom-permission"), map[string]any{
		"CustomPermissionsName": "doesnotexist",
	})
	assert.Equal(t, http.StatusNotFound, updateRec.Code)

	// Deleting a role custom permission that was never set -> 404.
	deleteRec := doRequest(t, h, http.MethodDelete, nsPath("/roles/READER/custom-permission"), nil)
	assert.Equal(t, http.StatusNotFound, deleteRec.Code)
}

// ---- Role membership CRUD round-trip and errors ----

func TestQuickSight_RoleMembershipCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, nsPath("/roles/AUTHOR/members/group1"), nil)
	require.Equal(t, http.StatusOK, createRec.Code)

	dupRec := doRequest(t, h, http.MethodPost, nsPath("/roles/AUTHOR/members/group1"), nil)
	assert.Equal(t, http.StatusConflict, dupRec.Code)

	listRec := doRequest(t, h, http.MethodGet, nsPath("/roles/AUTHOR/members"), nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	members := parseBody(t, listRec)["MembersList"].([]any)
	assert.Contains(t, members, "group1")

	deleteRec := doRequest(t, h, http.MethodDelete, nsPath("/roles/AUTHOR/members/group1"), nil)
	require.Equal(t, http.StatusOK, deleteRec.Code)

	deleteMissingRec := doRequest(t, h, http.MethodDelete, nsPath("/roles/AUTHOR/members/group1"), nil)
	assert.Equal(t, http.StatusNotFound, deleteMissingRec.Code)
}

// ---- User custom permission errors ----

func TestQuickSight_UserCustomPermission_Errors(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, accountPath("/custom-permissions"), map[string]any{
		"CustomPermissionsName": "cp1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// User doesn't exist yet -> 404.
	updateRec := doRequest(t, h, http.MethodPut, nsPath("/users/nosuchuser/custom-permission"), map[string]any{
		"CustomPermissionsName": "cp1",
	})
	assert.Equal(t, http.StatusNotFound, updateRec.Code)

	registerRec := doRequest(t, h, http.MethodPost, nsPath("/users"), map[string]any{
		"UserName": "u1", "Email": "u1@example.com", "IdentityType": "QUICKSIGHT", "UserRole": "READER",
	})
	require.Equal(t, http.StatusOK, registerRec.Code)

	// CustomPermissionsName doesn't exist -> 404.
	badPermRec := doRequest(t, h, http.MethodPut, nsPath("/users/u1/custom-permission"), map[string]any{
		"CustomPermissionsName": "doesnotexist",
	})
	assert.Equal(t, http.StatusNotFound, badPermRec.Code)

	okRec := doRequest(t, h, http.MethodPut, nsPath("/users/u1/custom-permission"), map[string]any{
		"CustomPermissionsName": "cp1",
	})
	require.Equal(t, http.StatusOK, okRec.Code)

	deleteRec := doRequest(t, h, http.MethodDelete, nsPath("/users/u1/custom-permission"), nil)
	require.Equal(t, http.StatusOK, deleteRec.Code)

	deleteMissingRec := doRequest(t, h, http.MethodDelete, nsPath("/users/u1/custom-permission"), nil)
	assert.Equal(t, http.StatusNotFound, deleteMissingRec.Code)
}
