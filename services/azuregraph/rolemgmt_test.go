package azuregraph_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	globalAdminRole = "62e90394-69f5-4237-9190-012177145e10"
	readersRole     = "88d8e3e3-8f55-4a1e-953a-9b9898b8876b"
	rolesBase       = "/v1.0/roleManagement/directory/"
)

func TestRoleDefinitions(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	list := e.req(t, http.MethodGet, rolesBase+"roleDefinitions", nil)
	require.Equal(t, http.StatusOK, list.status, list.raw)
	assert.Contains(t, values2(list, "id"), globalAdminRole)

	one := e.req(t, http.MethodGet, rolesBase+"roleDefinitions/"+globalAdminRole, nil)
	require.Equal(t, http.StatusOK, one.status, one.raw)
	assert.Equal(t, "Global Administrator", str(one.body, "displayName"))
	assert.Equal(t, true, one.body["isBuiltIn"])

	filtered := e.req(
		t,
		http.MethodGet,
		rolesBase+"roleDefinitions?$filter=displayName%20eq%20'Directory%20Readers'",
		nil,
	)
	assert.Equal(t, []string{readersRole}, values2(filtered, "id"))

	assertErr(t, e.req(t, http.MethodGet, rolesBase+"roleDefinitions/"+newID0(), nil),
		http.StatusNotFound, "Request_ResourceNotFound")
	assertErr(t, e.req(t, http.MethodPost, rolesBase+"roleDefinitions", map[string]any{}),
		http.StatusMethodNotAllowed, "Request_BadRequest")
}

func TestRoleAssignments(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	u := newUser(t, e, "admin"+upnDomain)
	app := newApp(t, e, "Scoped")
	sp := e.mustCreate(t, "/v1.0/servicePrincipals", map[string]any{"appId": str(app, "appId")})
	plain := newGroup(t, e, "Plain")
	assignable := e.mustCreate(t, "/v1.0/groups", map[string]any{
		"displayName": "RoleGroup", "mailEnabled": false, "mailNickname": "rg", "securityEnabled": true,
		"isAssignableToRole": true,
	})

	assign := func(principal, role, scope string) resp {
		return e.req(t, http.MethodPost, rolesBase+"roleAssignments", map[string]any{
			"principalId": principal, "roleDefinitionId": role, "directoryScopeId": scope,
		})
	}

	created := assign(str(u, "id"), globalAdminRole, "/")
	require.Equal(t, http.StatusCreated, created.status, created.raw)
	assert.Equal(t, "/", str(created.body, "directoryScopeId"))

	require.Equal(t, http.StatusCreated, assign(str(assignable, "id"), readersRole, "/").status)
	require.Equal(t, http.StatusCreated, assign(str(sp, "id"), readersRole, "/"+str(app, "id")).status)

	assertErr(
		t,
		assign(str(u, "id"), globalAdminRole, "/"),
		http.StatusConflict,
		"Request_MultipleObjectsWithSameKeyValue",
	)
	assertErr(t, assign(str(plain, "id"), readersRole, "/"), http.StatusBadRequest, "Request_BadRequest")
	assertErr(t, assign(newID0(), readersRole, "/"), http.StatusBadRequest, "Request_BadRequest")
	assertErr(t, assign(str(u, "id"), newID0(), "/"), http.StatusBadRequest, "Request_BadRequest")
	assertErr(t, assign(str(u, "id"), readersRole, "/"+newID0()), http.StatusBadRequest, "Request_BadRequest")
	assertErr(t, e.req(t, http.MethodPost, rolesBase+"roleAssignments", map[string]any{"principalId": str(u, "id")}),
		http.StatusBadRequest, "Request_BadRequest")

	byPrincipal := e.req(
		t,
		http.MethodGet,
		rolesBase+"roleAssignments?$filter=principalId%20eq%20'"+str(u, "id")+"'",
		nil,
	)
	require.Equal(t, http.StatusOK, byPrincipal.status, byPrincipal.raw)
	assert.Equal(t, []string{str(created.body, "id")}, values2(byPrincipal, "id"))

	got := e.req(t, http.MethodGet, rolesBase+"roleAssignments/"+str(created.body, "id"), nil)
	require.Equal(t, http.StatusOK, got.status, got.raw)
	assert.Equal(t, globalAdminRole, str(got.body, "roleDefinitionId"))

	require.Equal(t, http.StatusNoContent,
		e.req(t, http.MethodDelete, rolesBase+"roleAssignments/"+str(created.body, "id"), nil).status)
	assertErr(t, e.req(t, http.MethodGet, rolesBase+"roleAssignments/"+str(created.body, "id"), nil),
		http.StatusNotFound, "Request_ResourceNotFound")

	t.Run("deleting the principal removes its assignments", func(t *testing.T) {
		t.Parallel()

		require.Equal(
			t,
			http.StatusNoContent,
			e.req(t, http.MethodDelete, "/v1.0/applications/"+str(app, "id"), nil).status,
		)

		left := e.req(
			t,
			http.MethodGet,
			rolesBase+"roleAssignments?$filter=principalId%20eq%20'"+str(sp, "id")+"'",
			nil,
		)
		assert.Empty(t, values2(left, "id"))
	})
}
