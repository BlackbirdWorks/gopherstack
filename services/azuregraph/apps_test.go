package azuregraph_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplications(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	app := e.mustCreate(t, "/v1.0/applications", map[string]any{
		"displayName": "My App", "uniqueName": "my-app", "identifierUris": []string{"api://my-app"},
		"web": map[string]any{
			"redirectUris":          []string{"https://a.example/cb"},
			"implicitGrantSettings": map[string]any{"enableIdTokenIssuance": true},
		},
		"api": map[string]any{"oauth2PermissionScopes": []map[string]any{{
			"id": "5c5a3e2e-7f1a-4b2a-a1b4-6d0e3f1c9a10", "value": "Files.Read", "type": "User", "isEnabled": true,
		}}},
		"appRoles": []map[string]any{{
			"id": "0b6c5d3a-9b2c-4d8e-8f10-2a3b4c5d6e7f", "value": "Reader", "displayName": "Reader",
			"allowedMemberTypes": []string{"Application"}, "isEnabled": true,
		}},
		"requiredResourceAccess": []map[string]any{{
			"resourceAppId":  "00000003-0000-0000-c000-000000000000",
			"resourceAccess": []map[string]any{{"id": "e1fe6dd8-ba31-4d61-89e7-88639da4683d", "type": "Scope"}},
		}},
		"spa": map[string]any{"redirectUris": []string{"https://spa.example/"}},
	})

	id, appID := str(app, "id"), str(app, "appId")
	assert.Len(t, id, 36)
	assert.Len(t, appID, 36)
	assert.NotEqual(t, id, appID)
	assert.Equal(t, "AzureADMyOrg", str(app, "signInAudience"))
	assert.Equal(t, "gopherstack.onmicrosoft.com", str(app, "publisherDomain"))
	assert.Equal(t, "2026-01-02T03:04:05Z", str(app, "createdDateTime"))
	assert.Contains(t, str(app, "@odata.context"), "/v1.0/$metadata#applications/$entity")

	t.Run("alternate keys", func(t *testing.T) {
		t.Parallel()

		for _, path := range []string{
			"/v1.0/applications/" + id,
			"/v1.0/applications(appId='" + appID + "')",
			"/v1.0/applications(uniqueName='my-app')",
		} {
			r := e.req(t, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, r.status, path+r.raw)
			assert.Equal(t, id, str(r.body, "id"), path)
		}

		assertErr(t, e.req(t, http.MethodGet, "/v1.0/applications(appId='"+newID0()+"')", nil), http.StatusNotFound, "Request_ResourceNotFound")
		assertErr(t, e.req(t, http.MethodGet, "/v1.0/applications/not-a-guid", nil), http.StatusBadRequest, "Request_BadRequest")
	})

	t.Run("patch merges nested facets", func(t *testing.T) {
		t.Parallel()

		other := newApp(t, e, "Patched")
		oid := str(other, "id")
		r := e.req(t, http.MethodPatch, "/v1.0/applications/"+oid, map[string]any{
			"web": map[string]any{"redirectUris": []string{"https://x.example/cb"}}, "description": "d",
		})
		require.Equal(t, http.StatusNoContent, r.status, r.raw)

		r = e.req(t, http.MethodPatch, "/v1.0/applications/"+oid, map[string]any{"web": map[string]any{"homePageUrl": "https://h.example"}})
		require.Equal(t, http.StatusNoContent, r.status)

		got := e.req(t, http.MethodGet, "/v1.0/applications/"+oid, nil).body
		web, _ := got["web"].(map[string]any)
		assert.Equal(t, []any{"https://x.example/cb"}, web["redirectUris"])
		assert.Equal(t, "https://h.example", str(web, "homePageUrl"))
		assert.Equal(t, "d", str(got, "description"))

		e.req(t, http.MethodPatch, "/v1.0/applications/"+oid, map[string]any{"description": nil})
		got = e.req(t, http.MethodGet, "/v1.0/applications/"+oid, nil).body
		_, has := got["description"]
		assert.False(t, has)
	})

	t.Run("body validation", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name   string
			method string
			path   string
			body   any
		}{
			{"missing displayName", http.MethodPost, "/v1.0/applications", map[string]any{}},
			{"unknown property", http.MethodPost, "/v1.0/applications", map[string]any{"displayName": "x", "nope": 1}},
			{"read only appId", http.MethodPatch, "/v1.0/applications/" + id, map[string]any{"appId": "x"}},
			{"password via patch", http.MethodPatch, "/v1.0/applications/" + id, map[string]any{"passwordCredentials": []any{}}},
			{"bad audience", http.MethodPost, "/v1.0/applications", map[string]any{"displayName": "x", "signInAudience": "Nope"}},
			{"dup identifierUri", http.MethodPost, "/v1.0/applications", map[string]any{"displayName": "x", "identifierUris": []string{"api://my-app"}}},
			{"dup uniqueName", http.MethodPost, "/v1.0/applications", map[string]any{"displayName": "x", "uniqueName": "my-app"}},
			{"bad identifierUri", http.MethodPost, "/v1.0/applications", map[string]any{"displayName": "x", "identifierUris": []string{"nope"}}},
			{"bad role id", http.MethodPost, "/v1.0/applications", map[string]any{"displayName": "x", "appRoles": []map[string]any{{"id": "x"}}}},
			{"malformed json", http.MethodPost, "/v1.0/applications", "{"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assertErr(t, e.req(t, tt.method, tt.path, tt.body), http.StatusBadRequest, "Request_BadRequest")
			})
		}
	})

	t.Run("delete", func(t *testing.T) {
		t.Parallel()

		other := newApp(t, e, "Doomed")
		path := "/v1.0/applications/" + str(other, "id")
		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, path, nil).status)
		assertErr(t, e.req(t, http.MethodGet, path, nil), http.StatusNotFound, "Request_ResourceNotFound")
		assertErr(t, e.req(t, http.MethodDelete, path, nil), http.StatusNotFound, "Request_ResourceNotFound")
	})

	t.Run("odata key escaping", func(t *testing.T) {
		t.Parallel()

		assertErr(t, e.req(t, http.MethodGet, "/v1.0/applications(appId='"+url.PathEscape("a''b")+"')", nil),
			http.StatusNotFound, "Request_ResourceNotFound")
	})
}

func newID0() string { return "99999999-9999-9999-9999-999999999999" }

func TestPasswordCredentials(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	app := newApp(t, e, "Secrets")
	base := "/v1.0/applications/" + str(app, "id")

	add := e.req(t, http.MethodPost, base+"/addPassword", map[string]any{
		"passwordCredential": map[string]any{"displayName": "ci"},
	})
	require.Equal(t, http.StatusOK, add.status, add.raw)

	secret := str(add.body, "secretText")
	keyID := str(add.body, "keyId")

	assert.Len(t, secret, 40)
	assert.Equal(t, "8Q~", secret[3:6])
	assert.Equal(t, secret[:3], str(add.body, "hint"))
	assert.Equal(t, "ci", str(add.body, "displayName"))
	assert.Equal(t, "2028-01-02T03:04:05Z", str(add.body, "endDateTime"), "default lifetime is two years")

	t.Run("secret is never returned again", func(t *testing.T) {
		t.Parallel()

		for _, path := range []string{base, "/v1.0/applications", "/v1.0/applications(appId='" + str(app, "appId") + "')"} {
			r := e.req(t, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, r.status)
			assert.NotContains(t, r.raw, secret, path)
		}

		got := e.req(t, http.MethodGet, base, nil).body
		creds, _ := got["passwordCredentials"].([]any)
		require.Len(t, creds, 1)

		c, _ := creds[0].(map[string]any)
		assert.Nil(t, c["secretText"])
		assert.Equal(t, keyID, str(c, "keyId"))
		assert.Equal(t, secret[:3], str(c, "hint"))
	})

	t.Run("validation", func(t *testing.T) {
		t.Parallel()

		past := e.req(t, http.MethodPost, base+"/addPassword", map[string]any{
			"passwordCredential": map[string]any{"endDateTime": "2020-01-01T00:00:00Z"},
		})
		assertErr(t, past, http.StatusBadRequest, "Request_BadRequest")
		assertErr(t, e.req(t, http.MethodPost, base+"/removePassword", map[string]any{"keyId": newID0()}), http.StatusBadRequest, "Request_BadRequest")
		assertErr(t, e.req(t, http.MethodPost, base+"/removePassword", map[string]any{}), http.StatusBadRequest, "Request_BadRequest")
		assertErr(t, e.req(t, http.MethodPost, "/v1.0/applications/"+newID0()+"/addPassword", map[string]any{}), http.StatusNotFound, "Request_ResourceNotFound")
	})

	t.Run("remove", func(t *testing.T) {
		t.Parallel()

		other := newApp(t, e, "Remove")
		p := "/v1.0/applications/" + str(other, "id")
		added := e.req(t, http.MethodPost, p+"/addPassword", map[string]any{})
		require.Equal(t, http.StatusOK, added.status)
		require.Equal(t, http.StatusNoContent,
			e.req(t, http.MethodPost, p+"/removePassword", map[string]any{"keyId": str(added.body, "keyId")}).status)

		got := e.req(t, http.MethodGet, p, nil).body
		assert.Empty(t, got["passwordCredentials"])
	})
}

func TestKeyCredentials(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	app := newApp(t, e, "Keys")
	base := "/v1.0/applications/" + str(app, "id")

	good := map[string]any{"keyCredential": map[string]any{"type": "AsymmetricX509Cert", "usage": "Verify", "key": "AAEC"}}
	add := e.req(t, http.MethodPost, base+"/addKey", good)
	require.Equal(t, http.StatusOK, add.status, add.raw)
	assert.Nil(t, add.body["key"], "key material is not returned")

	for name, body := range map[string]any{
		"bad type":  map[string]any{"keyCredential": map[string]any{"type": "x", "usage": "Verify", "key": "AAEC"}},
		"bad usage": map[string]any{"keyCredential": map[string]any{"type": "AsymmetricX509Cert", "usage": "x", "key": "AAEC"}},
		"bad key":   map[string]any{"keyCredential": map[string]any{"type": "AsymmetricX509Cert", "usage": "Verify", "key": "%%"}},
		"no key":    map[string]any{"keyCredential": map[string]any{"type": "AsymmetricX509Cert", "usage": "Verify"}},
	} {
		assertErr(t, e.req(t, http.MethodPost, base+"/addKey", body), http.StatusBadRequest, "Request_BadRequest")
		_ = name
	}

	require.Equal(t, http.StatusNoContent,
		e.req(t, http.MethodPost, base+"/removeKey", map[string]any{"keyId": str(add.body, "keyId")}).status)
	assertErr(t, e.req(t, http.MethodPost, base+"/removeKey", map[string]any{"keyId": str(add.body, "keyId")}),
		http.StatusBadRequest, "Request_BadRequest")
}
