package azuregraph_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuregraph"
)

const (
	graphSP    = "7f0b5d3e-3c1f-4e6a-9d2b-5a8c1e4f6b20"
	roleID     = "0b6c5d3a-9b2c-4d8e-8f10-2a3b4c5d6e7f"
	appReadAll = "9a5d68dd-52b0-4cc2-bd40-abcf44ac3a30"
)

func TestServicePrincipals(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	app := e.mustCreate(t, "/v1.0/applications", map[string]any{
		"displayName": "Svc", "identifierUris": []string{"api://svc"},
		"appRoles": []map[string]any{{
			"id": roleID, "value": "Reader", "displayName": "Reader", "allowedMemberTypes": []string{"Application", "User"}, "isEnabled": true,
		}},
	})
	appID := str(app, "appId")

	assertErr(t, e.req(t, http.MethodPost, "/v1.0/servicePrincipals", map[string]any{"appId": newID0()}), http.StatusBadRequest, "Request_BadRequest")
	assertErr(t, e.req(t, http.MethodPost, "/v1.0/servicePrincipals", map[string]any{}), http.StatusBadRequest, "Request_BadRequest")

	sp := e.mustCreate(t, "/v1.0/servicePrincipals", map[string]any{"appId": appID})
	spID := str(sp, "id")

	assert.Equal(t, "Svc", str(sp, "displayName"))
	assert.Equal(t, "Application", str(sp, "servicePrincipalType"))
	assert.Equal(t, true, sp["accountEnabled"])
	assert.Equal(t, azuregraph.DefaultTenantID, str(sp, "appOwnerOrganizationId"))
	assert.ElementsMatch(t, []any{appID, "api://svc"}, sp["servicePrincipalNames"])
	require.Len(t, sp["appRoles"], 1)

	assertErr(t, e.req(t, http.MethodPost, "/v1.0/servicePrincipals", map[string]any{"appId": appID}), http.StatusBadRequest, "Request_BadRequest")

	t.Run("alternate key and patch", func(t *testing.T) {
		t.Parallel()

		r := e.req(t, http.MethodGet, "/v1.0/servicePrincipals(appId='"+appID+"')", nil)
		require.Equal(t, http.StatusOK, r.status)
		assert.Equal(t, spID, str(r.body, "id"))

		assert.Equal(t, http.StatusNoContent, e.req(t, http.MethodPatch, "/v1.0/servicePrincipals/"+spID,
			map[string]any{"appRoleAssignmentRequired": true, "tags": []string{"a"}}).status)
		assertErr(t, e.req(t, http.MethodPatch, "/v1.0/servicePrincipals/"+spID, map[string]any{"appId": "x"}),
			http.StatusBadRequest, "Request_BadRequest")
	})

	t.Run("application changes sync to the service principal", func(t *testing.T) {
		t.Parallel()

		a2 := e.mustCreate(t, "/v1.0/applications", map[string]any{"displayName": "Sync"})
		s2 := e.mustCreate(t, "/v1.0/servicePrincipals", map[string]any{"appId": str(a2, "appId")})
		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodPatch, "/v1.0/applications/"+str(a2, "id"), map[string]any{
			"identifierUris": []string{"api://sync"},
			"appRoles": []map[string]any{{
				"id": "1d6c5d3a-9b2c-4d8e-8f10-2a3b4c5d6e7f", "value": "R", "allowedMemberTypes": []string{"Application"}, "isEnabled": true,
			}},
		}).status)

		got := e.req(t, http.MethodGet, "/v1.0/servicePrincipals/"+str(s2, "id"), nil).body
		assert.Len(t, got["appRoles"], 1)
		assert.Contains(t, got["servicePrincipalNames"], "api://sync")

		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, "/v1.0/applications/"+str(a2, "id"), nil).status)
		assertErr(t, e.req(t, http.MethodGet, "/v1.0/servicePrincipals/"+str(s2, "id"), nil), http.StatusNotFound, "Request_ResourceNotFound")
	})

	t.Run("built-in Microsoft Graph service principal", func(t *testing.T) {
		t.Parallel()

		r := e.req(t, http.MethodGet, "/v1.0/servicePrincipals(appId='"+azuregraph.MicrosoftGraphAppID+"')", nil)
		require.Equal(t, http.StatusOK, r.status)
		assert.Equal(t, "Microsoft Graph", str(r.body, "displayName"))
		assert.NotEmpty(t, r.body["appRoles"])
		assert.NotEmpty(t, r.body["oauth2PermissionScopes"])

		assertErr(t, e.req(t, http.MethodPatch, "/v1.0/servicePrincipals/"+graphSP, map[string]any{"notes": "x"}),
			http.StatusForbidden, "Authorization_RequestDenied")
		assertErr(t, e.req(t, http.MethodDelete, "/v1.0/servicePrincipals/"+graphSP, nil), http.StatusForbidden, "Authorization_RequestDenied")
		assertErr(t, e.req(t, http.MethodPost, "/v1.0/servicePrincipals/"+graphSP+"/addPassword", map[string]any{}),
			http.StatusForbidden, "Authorization_RequestDenied")
	})

	t.Run("app role assignments", func(t *testing.T) {
		t.Parallel()

		client := e.mustCreate(t, "/v1.0/servicePrincipals", map[string]any{"appId": str(newApp(t, e, "Client"), "appId")})
		cid := str(client, "id")
		user := newUser(t, e, "assign@gopherstack.onmicrosoft.com")

		tests := []struct {
			name   string
			path   string
			body   map[string]any
			status int
			code   string
		}{
			{"sp gets graph role", "/v1.0/servicePrincipals/" + cid + "/appRoleAssignments",
				map[string]any{"principalId": cid, "resourceId": graphSP, "appRoleId": appReadAll}, http.StatusCreated, ""},
			{"duplicate", "/v1.0/servicePrincipals/" + cid + "/appRoleAssignments",
				map[string]any{"principalId": cid, "resourceId": graphSP, "appRoleId": appReadAll}, http.StatusBadRequest, "Request_BadRequest"},
			{"unknown role", "/v1.0/servicePrincipals/" + cid + "/appRoleAssignments",
				map[string]any{"principalId": cid, "resourceId": graphSP, "appRoleId": newID0()}, http.StatusBadRequest, "Request_BadRequest"},
			{"path mismatch", "/v1.0/servicePrincipals/" + cid + "/appRoleAssignments",
				map[string]any{"principalId": newID0(), "resourceId": graphSP, "appRoleId": appReadAll}, http.StatusBadRequest, "Request_BadRequest"},
			{"role not for users", "/v1.0/servicePrincipals/" + graphSP + "/appRoleAssignedTo",
				map[string]any{"principalId": str(user, "id"), "resourceId": graphSP, "appRoleId": appReadAll}, http.StatusBadRequest, "Request_BadRequest"},
			{"user gets custom role", "/v1.0/servicePrincipals/" + spID + "/appRoleAssignedTo",
				map[string]any{"principalId": str(user, "id"), "resourceId": spID, "appRoleId": roleID}, http.StatusCreated, ""},
			{"default access", "/v1.0/servicePrincipals/" + spID + "/appRoleAssignedTo",
				map[string]any{"principalId": str(newGroup(t, e, "Assignees"), "id"), "resourceId": spID,
					"appRoleId": "00000000-0000-0000-0000-000000000000"}, http.StatusCreated, ""},
			{"missing property", "/v1.0/servicePrincipals/" + cid + "/appRoleAssignments", map[string]any{}, http.StatusBadRequest, "Request_BadRequest"},
		}

		var first map[string]any

		for _, tt := range tests {
			r := e.req(t, http.MethodPost, tt.path, tt.body)
			if tt.code != "" {
				assertErr(t, r, tt.status, tt.code)

				continue
			}

			require.Equal(t, tt.status, r.status, tt.name+r.raw)

			if first == nil {
				first = r.body
			}
		}

		assert.Equal(t, "Microsoft Graph", str(first, "resourceDisplayName"))
		assert.Equal(t, "ServicePrincipal", str(first, "principalType"))

		list := e.req(t, http.MethodGet, "/v1.0/servicePrincipals/"+cid+"/appRoleAssignments", nil)
		require.Len(t, list.body["value"], 1)

		to := e.req(t, http.MethodGet, "/v1.0/servicePrincipals/"+graphSP+"/appRoleAssignedTo", nil)
		assert.Len(t, to.body["value"], 1)

		require.Equal(t, http.StatusNoContent,
			e.req(t, http.MethodDelete, "/v1.0/servicePrincipals/"+cid+"/appRoleAssignments/"+str(first, "id"), nil).status)
		assertErr(t, e.req(t, http.MethodDelete, "/v1.0/servicePrincipals/"+cid+"/appRoleAssignments/"+str(first, "id"), nil),
			http.StatusNotFound, "Request_ResourceNotFound")
	})

	t.Run("oauth2 permission grants", func(t *testing.T) {
		t.Parallel()

		client := e.mustCreate(t, "/v1.0/servicePrincipals", map[string]any{"appId": str(newApp(t, e, "Granter"), "appId")})
		cid := str(client, "id")

		grant := e.mustCreate(t, "/v1.0/oauth2PermissionGrants", map[string]any{
			"clientId": cid, "consentType": "AllPrincipals", "resourceId": graphSP, "scope": "User.Read",
		})
		gid := str(grant, "id")

		assertErr(t, e.req(t, http.MethodPost, "/v1.0/oauth2PermissionGrants", map[string]any{
			"clientId": cid, "consentType": "Principal", "resourceId": graphSP, "scope": "x"}), http.StatusBadRequest, "Request_BadRequest")
		assertErr(t, e.req(t, http.MethodPost, "/v1.0/oauth2PermissionGrants", map[string]any{
			"clientId": newID0(), "consentType": "AllPrincipals", "resourceId": graphSP, "scope": "x"}), http.StatusNotFound, "Request_ResourceNotFound")

		assert.Equal(t, http.StatusNoContent, e.req(t, http.MethodPatch, "/v1.0/oauth2PermissionGrants/"+gid, map[string]any{"scope": "User.Read openid"}).status)
		assertErr(t, e.req(t, http.MethodPatch, "/v1.0/oauth2PermissionGrants/"+gid, map[string]any{"clientId": cid}), http.StatusBadRequest, "Request_BadRequest")

		got := e.req(t, http.MethodGet, "/v1.0/oauth2PermissionGrants/"+gid, nil)
		assert.Equal(t, "User.Read openid", str(got.body, "scope"))

		byClient := e.req(t, http.MethodGet, "/v1.0/servicePrincipals/"+cid+"/oauth2PermissionGrants", nil)
		assert.Len(t, byClient.body["value"], 1)

		filtered := e.req(t, http.MethodGet, "/v1.0/oauth2PermissionGrants?$filter=clientId%20eq%20'"+cid+"'", nil)
		assert.Len(t, filtered.body["value"], 1)

		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, "/v1.0/oauth2PermissionGrants/"+gid, nil).status)
		assertErr(t, e.req(t, http.MethodGet, "/v1.0/oauth2PermissionGrants/"+gid, nil), http.StatusNotFound, "Request_ResourceNotFound")
	})
}
