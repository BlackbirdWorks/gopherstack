package azuregraph_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdministrativeUnitCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	base := "/v1.0/directory/administrativeUnits"

	created := e.req(t, http.MethodPost, base, map[string]any{
		"displayName": "Seattle", "description": "Seattle district", "visibility": "HiddenMembership",
	})
	require.Equal(t, http.StatusCreated, created.status, created.raw)

	id := str(created.body, "id")
	assert.Equal(t, "Seattle", str(created.body, "displayName"))
	assert.Equal(t, "HiddenMembership", str(created.body, "visibility"))
	assert.Contains(t, created.body, "membershipType")

	got := e.req(t, http.MethodGet, base+"/"+id, nil)
	require.Equal(t, http.StatusOK, got.status, got.raw)
	assert.Equal(t, "Seattle district", str(got.body, "description"))

	list := e.req(t, http.MethodGet, base, nil)
	require.Equal(t, http.StatusOK, list.status, list.raw)

	items, _ := list.body["value"].([]any)
	assert.Len(t, items, 1)

	patch := e.req(t, http.MethodPatch, base+"/"+id, map[string]any{"description": "changed", "visibility": nil})
	require.Equal(t, http.StatusNoContent, patch.status, patch.raw)

	got = e.req(t, http.MethodGet, base+"/"+id, nil)
	assert.Equal(t, "changed", str(got.body, "description"))
	assert.Nil(t, got.body["visibility"])

	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, base+"/"+id, nil).status)
	assertErr(t, e.req(t, http.MethodGet, base+"/"+id, nil), http.StatusNotFound, "Request_ResourceNotFound")
}

func TestAdministrativeUnitValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	base := "/v1.0/directory/administrativeUnits"

	tests := []struct {
		body map[string]any
		name string
	}{
		{map[string]any{}, "missing display name"},
		{map[string]any{"displayName": "x", "visibility": "Public"}, "bad visibility"},
		{map[string]any{"displayName": "x", "membershipType": "static"}, "bad membership type"},
		{map[string]any{"displayName": "x", "membershipType": "dynamic"}, "dynamic without rule"},
		{map[string]any{"displayName": "x", "id": "1"}, "read only id"},
		{map[string]any{"displayName": "x", "bogus": 1}, "unknown property"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assertErr(t, e.req(t, http.MethodPost, base, tt.body), http.StatusBadRequest, "Request_BadRequest")
		})
	}

	assertErr(t, e.req(t, http.MethodGet, base+"/not-a-guid", nil), http.StatusBadRequest, "Request_BadRequest")
}

func TestAdministrativeUnitMembers(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	base := "/v1.0/directory/administrativeUnits"
	au := str(e.mustCreate(t, base, map[string]any{"displayName": "AU"}), "id")
	alice := str(newUser(t, e, "alice"+upnDomain), "id")
	grp := str(newGroup(t, e, "Team"), "id")
	app := str(newApp(t, e, "app"), "id")

	ref := func(id string) map[string]any {
		return map[string]any{"@odata.id": "https://graph.microsoft.com/v1.0/directoryObjects/" + id}
	}

	for _, id := range []string{alice, grp} {
		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodPost, base+"/"+au+"/members/$ref", ref(id)).status)
	}

	assertErr(t, e.req(t, http.MethodPost, base+"/"+au+"/members/$ref", ref(alice)), http.StatusBadRequest,
		"Request_BadRequest")
	assertErr(t, e.req(t, http.MethodPost, base+"/"+au+"/members/$ref", ref(app)), http.StatusBadRequest,
		"Request_BadRequest")
	assertErr(t, e.req(t, http.MethodPost, base+"/"+au+"/members/$ref", ref(missingRoleID)), http.StatusNotFound,
		"Request_ResourceNotFound")

	list := e.req(t, http.MethodGet, base+"/"+au+"/members", nil)
	require.Equal(t, http.StatusOK, list.status, list.raw)

	members, _ := list.body["value"].([]any)
	assert.Len(t, members, 2)

	require.Equal(
		t,
		http.StatusNoContent,
		e.req(t, http.MethodDelete, base+"/"+au+"/members/"+alice+"/$ref", nil).status,
	)
	assertErr(t, e.req(t, http.MethodDelete, base+"/"+au+"/members/"+alice+"/$ref", nil), http.StatusNotFound,
		"Request_ResourceNotFound")

	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, "/v1.0/groups/"+grp, nil).status)

	list = e.req(t, http.MethodGet, base+"/"+au+"/members", nil)
	members, _ = list.body["value"].([]any)
	assert.Empty(t, members, "a deleted principal leaves the administrative unit")
}

func TestAdministrativeUnitScopedRoleMembers(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	base := "/v1.0/directory/administrativeUnits"
	au := str(e.mustCreate(t, base, map[string]any{"displayName": "AU"}), "id")
	alice := newUser(t, e, "alice"+upnDomain)

	role := e.req(t, http.MethodPost, "/v1.0/directoryRoles", map[string]any{"roleTemplateId": userAdminTemplate})
	require.Equal(t, http.StatusCreated, role.status, role.raw)

	roleID := str(role.body, "id")
	path := base + "/" + au + "/scopedRoleMembers"
	body := map[string]any{"roleId": roleID, "roleMemberInfo": map[string]any{"id": str(alice, "id")}}

	created := e.req(t, http.MethodPost, path, body)
	require.Equal(t, http.StatusCreated, created.status, created.raw)
	assert.Equal(t, au, str(created.body, "administrativeUnitId"))
	assert.Equal(t, roleID, str(created.body, "roleId"))

	info, _ := created.body["roleMemberInfo"].(map[string]any)
	assert.Equal(t, str(alice, "userPrincipalName"), str(info, "userPrincipalName"))

	assertErr(t, e.req(t, http.MethodPost, path, body), http.StatusBadRequest, "Request_BadRequest")
	assertErr(t, e.req(t, http.MethodPost, path, map[string]any{
		"roleId": missingRoleID, "roleMemberInfo": map[string]any{"id": str(alice, "id")},
	}), http.StatusNotFound, "Request_ResourceNotFound")
	assertErr(t, e.req(t, http.MethodPost, path, map[string]any{"roleId": roleID}), http.StatusBadRequest,
		"Request_BadRequest")

	sid := str(created.body, "id")
	require.Equal(t, http.StatusOK, e.req(t, http.MethodGet, path+"/"+sid, nil).status)

	list := e.req(t, http.MethodGet, path, nil)
	items, _ := list.body["value"].([]any)
	assert.Len(t, items, 1)

	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, path+"/"+sid, nil).status)
	assertErr(t, e.req(t, http.MethodGet, path+"/"+sid, nil), http.StatusNotFound, "Request_ResourceNotFound")
}

func TestAdministrativeUnitPersistence(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	base := "/v1.0/directory/administrativeUnits"
	au := str(e.mustCreate(t, base, map[string]any{"displayName": "AU"}), "id")
	alice := str(newUser(t, e, "alice"+upnDomain), "id")

	ref := map[string]any{"@odata.id": "https://graph.microsoft.com/v1.0/directoryObjects/" + alice}
	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodPost, base+"/"+au+"/members/$ref", ref).status)

	snap := e.h.Backend.Snapshot(t.Context())
	e.h.Backend.Reset()
	assertErr(t, e.req(t, http.MethodGet, base+"/"+au, nil), http.StatusNotFound, "Request_ResourceNotFound")
	require.NoError(t, e.h.Backend.Restore(t.Context(), snap))

	got := e.req(t, http.MethodGet, base+"/"+au+"/members", nil)
	members, _ := got.body["value"].([]any)
	assert.Len(t, members, 1)
}
