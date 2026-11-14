package azuregraph_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const userAdminTemplate = "fe930be7-5e62-47db-91af-98c3a49a38b1"

func TestDirectoryRoleTemplates(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	r := e.req(t, http.MethodGet, "/v1.0/directoryRoleTemplates", nil)
	require.Equal(t, http.StatusOK, r.status, r.raw)

	items, _ := r.body["value"].([]any)
	assert.NotEmpty(t, items)

	one := e.req(t, http.MethodGet, "/v1.0/directoryRoleTemplates/"+userAdminTemplate, nil)
	require.Equal(t, http.StatusOK, one.status, one.raw)
	assert.Equal(t, "User Administrator", str(one.body, "displayName"))

	assertErr(t, e.req(t, http.MethodGet, "/v1.0/directoryRoleTemplates/"+missingRoleID, nil),
		http.StatusNotFound, "Request_ResourceNotFound")
}

const missingRoleID = "00000000-0000-4000-8000-0000000000aa"

func TestDirectoryRoles(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	alice := newUser(t, e, "alice"+upnDomain)
	bob := newUser(t, e, "bob"+upnDomain)

	assertErr(t, e.req(t, http.MethodPost, "/v1.0/directoryRoles", map[string]any{}), http.StatusBadRequest,
		"Request_BadRequest")
	assertErr(t, e.req(t, http.MethodPost, "/v1.0/directoryRoles", map[string]any{"roleTemplateId": missingRoleID}),
		http.StatusBadRequest, "Request_BadRequest")

	created := e.req(t, http.MethodPost, "/v1.0/directoryRoles", map[string]any{"roleTemplateId": userAdminTemplate})
	require.Equal(t, http.StatusCreated, created.status, created.raw)

	role := str(created.body, "id")
	assert.Equal(t, "User Administrator", str(created.body, "displayName"))
	assert.Equal(t, userAdminTemplate, str(created.body, "roleTemplateId"))

	assertErr(t, e.req(t, http.MethodPost, "/v1.0/directoryRoles", map[string]any{"roleTemplateId": userAdminTemplate}),
		http.StatusBadRequest, "Request_BadRequest")

	t.Run("get and list", func(t *testing.T) {
		t.Parallel()

		for _, p := range []string{
			"/v1.0/directoryRoles/" + role,
			"/v1.0/directoryRoles(roleTemplateId='" + userAdminTemplate + "')",
		} {
			r := e.req(t, http.MethodGet, p, nil)
			require.Equal(t, http.StatusOK, r.status, r.raw)
			assert.Equal(t, role, str(r.body, "id"))
		}

		list := e.req(t, http.MethodGet, "/v1.0/directoryRoles", nil)
		require.Equal(t, http.StatusOK, list.status, list.raw)
		assert.Contains(t, list.raw, role)

		assertErr(t, e.req(t, http.MethodGet, "/v1.0/directoryRoles/"+missingRoleID, nil),
			http.StatusNotFound, "Request_ResourceNotFound")
	})

	t.Run("members", func(t *testing.T) {
		t.Parallel()

		base := "/v1.0/directoryRoles/" + role + "/members"
		ref := map[string]any{"@odata.id": "https://graph.microsoft.com/v1.0/directoryObjects/" + str(alice, "id")}

		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodPost, base+"/$ref", ref).status)
		assertErr(t, e.req(t, http.MethodPost, base+"/$ref", ref), http.StatusBadRequest, "Request_BadRequest")

		list := e.req(t, http.MethodGet, base, nil)
		require.Equal(t, http.StatusOK, list.status, list.raw)
		assert.Contains(t, list.raw, str(alice, "id"))
		assert.NotContains(t, list.raw, str(bob, "id"))

		ra := e.req(t, http.MethodGet, "/v1.0/roleManagement/directory/roleAssignments", nil)
		assert.Contains(t, ra.raw, str(alice, "id"))

		byTemplate := e.req(
			t,
			http.MethodGet,
			"/v1.0/directoryRoles(roleTemplateId='"+userAdminTemplate+"')/members",
			nil,
		)
		assert.Contains(t, byTemplate.raw, str(alice, "id"))

		require.Equal(
			t,
			http.StatusNoContent,
			e.req(t, http.MethodDelete, base+"/"+str(alice, "id")+"/$ref", nil).status,
		)
		assertErr(t, e.req(t, http.MethodDelete, base+"/"+str(alice, "id")+"/$ref", nil),
			http.StatusNotFound, "Request_ResourceNotFound")
		assert.NotContains(t, e.req(t, http.MethodGet, base, nil).raw, str(alice, "id"))
	})
}

func TestDirectoryRoleActivatedByAssignment(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	u := newUser(t, e, "carol"+upnDomain)

	assert.NotContains(t, e.req(t, http.MethodGet, "/v1.0/directoryRoles", nil).raw, userAdminTemplate)

	r := e.req(t, http.MethodPost, "/v1.0/roleManagement/directory/roleAssignments", map[string]any{
		"principalId": str(u, "id"), "roleDefinitionId": userAdminTemplate, "directoryScopeId": "/",
	})
	require.Equal(t, http.StatusCreated, r.status, r.raw)

	list := e.req(t, http.MethodGet, "/v1.0/directoryRoles(roleTemplateId='"+userAdminTemplate+"')/members", nil)
	require.Equal(t, http.StatusOK, list.status, list.raw)
	assert.Contains(t, list.raw, str(u, "id"))
}
