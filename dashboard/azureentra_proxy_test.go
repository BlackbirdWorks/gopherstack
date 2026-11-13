package dashboard_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azuregraph"
)

const (
	entraGraph = "/dashboard/api/v1/azure/graph"
	entraAuthz = "/dashboard/api/v1/azure/authz"
	entraARM   = "/dashboard/api/v1/azure/arm"
	entraSub   = "/subscriptions/" + armSub
	entraRG    = entraSub + "/resourceGroups/entra"
	entraAPI   = "?api-version=2022-04-01"
	entraAuth  = "/providers/Microsoft.Authorization/"
	entraGUID1 = "11111111-1111-1111-1111-111111111111"
	entraGUID2 = "22222222-2222-2222-2222-222222222222"
	entraRole  = "/subscriptions/" + armSub +
		"/providers/Microsoft.Authorization/roleDefinitions/acdd72a7-3385-48ef-bd42-f606fba81ae7"
)

type entraEnv struct {
	client *http.Client
	url    string
}

func newEntraEnv(t *testing.T) *entraEnv {
	t.Helper()

	armSvc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	armH, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)
	t.Cleanup(func() { armH.Shutdown(context.WithoutCancel(t.Context())) })

	gs := azuregraph.DefaultSettings()
	graphH := azuregraph.NewHandler(azuregraph.NewBackend(gs), gs)

	e := echo.New()
	e.Any("/*", dashboard.NewHandler(dashboard.Config{AzureARMOps: armH, AzureGraphOps: graphH}).Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	env := &entraEnv{url: srv.URL, client: testclient.New(t)}

	code, _ := env.do(t, http.MethodPut, entraARM+entraRG+"?api-version=2021-04-01", `{"location":"eastus"}`)
	require.Less(t, code, http.StatusBadRequest)

	return env
}

func (e *entraEnv) do(t *testing.T, method, path, body string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, e.url+path, strings.NewReader(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(b)
}

func (e *entraEnv) create(t *testing.T, path, body string) map[string]any {
	t.Helper()

	code, out := e.do(t, http.MethodPost, path, body)
	require.Less(t, code, http.StatusBadRequest, out)

	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &m))

	return m
}

func TestAzureGraphProxyAllowList(t *testing.T) {
	t.Parallel()

	env := newEntraEnv(t)
	app := entraGraph + "/applications/" + entraGUID1

	tests := []struct {
		name, method, path, body string
	}{
		{"delete user", http.MethodDelete, entraGraph + "/users/" + entraGUID1, ""},
		{"delete group", http.MethodDelete, entraGraph + "/groups/" + entraGUID1, ""},
		{"delete sp", http.MethodDelete, entraGraph + "/servicePrincipals/" + entraGUID1, ""},
		{"patch app", http.MethodPatch, app, `{}`},
		{"put app", http.MethodPut, app, `{}`},
		{"add key", http.MethodPost, app + "/addKey", `{}`},
		{"non guid id", http.MethodGet, entraGraph + "/applications/not-a-guid", ""},
		{"me", http.MethodGet, entraGraph + "/me", ""},
		{"grants", http.MethodGet, entraGraph + "/oauth2PermissionGrants", ""},
		{"organization", http.MethodGet, entraGraph + "/organization", ""},
		{"traversal", http.MethodGet, entraGraph + "/applications/%2e%2e/users", ""},
		{"slash", http.MethodGet, entraGraph + "/applications/" + entraGUID1 + "%2Fowners", ""},
		{"sp password", http.MethodPost, entraGraph + "/servicePrincipals/" + entraGUID1 + "/addPassword", `{}`},
		{"member ref get", http.MethodGet, entraGraph + "/groups/" + entraGUID1 + "/members/$ref", ""},
		{"remove member bad id", http.MethodDelete, entraGraph + "/groups/" + entraGUID1 + "/members/x/$ref", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, body := env.do(t, tt.method, tt.path, tt.body)
			assert.Contains(t, []int{http.StatusBadRequest, http.StatusMethodNotAllowed, http.StatusNotFound}, code)
			assert.NotContains(t, body, "Request_")
		})
	}
}

func TestAzureGraphProxyBadBodies(t *testing.T) {
	t.Parallel()

	env := newEntraEnv(t)

	tests := []struct{ name, path, body string }{
		{"app no name", "/applications", `{}`},
		{"app not json", "/applications", `nope`},
		{"sp bad appid", "/servicePrincipals", `{"appId":"x"}`},
		{"user no password", "/users", `{"displayName":"A","mailNickname":"a",` +
			`"userPrincipalName":"a@gopherstack.onmicrosoft.com"}`},
		{"user bad upn", "/users", `{"displayName":"A","mailNickname":"a","userPrincipalName":"a b",` +
			`"password":"Xx1!xxxxxx"}`},
		{"group bad nick", "/groups", `{"displayName":"G","mailNickname":"a b"}`},
		{"remove password bad key", "/applications/" + entraGUID1 + "/removePassword", `{"keyId":"x"}`},
		{"member bad id", "/groups/" + entraGUID1 + "/members/$ref", `{"id":"http://evil/x"}`},
		{"getbyids empty", "/directoryObjects/getByIds", `{"ids":[]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, body := env.do(t, http.MethodPost, entraGraph+tt.path, tt.body)
			assert.Equal(t, http.StatusBadRequest, code)
			assert.Contains(t, body, "InvalidRequestContent")
		})
	}
}

func TestAzureGraphProxySecretOnce(t *testing.T) {
	t.Parallel()

	env := newEntraEnv(t)

	app := env.create(t, entraGraph+"/applications",
		`{"displayName":"web","passwordCredentials":[{"secretText":"smuggled"}],`+
			`"signInAudience":"AzureADMultipleOrgs"}`)
	id, _ := app["id"].(string)
	require.NotEmpty(t, id)
	assert.Equal(t, "AzureADMyOrg", app["signInAudience"])

	pw := env.create(t, entraGraph+"/applications/"+id+"/addPassword", `{"displayName":"ci","extra":"ignored"}`)
	secret, _ := pw["secretText"].(string)
	require.Len(t, secret, 40)
	assert.NotEmpty(t, pw["hint"])

	paths := []string{"/applications", "/applications/" + id, "/applications?$select=id,passwordCredentials"}

	for _, path := range paths {
		code, body := env.do(t, http.MethodGet, entraGraph+path, "")
		require.Equal(t, http.StatusOK, code, body)
		assert.NotContains(t, body, secret, path)
		assert.NotContains(t, body, "secretText", path)
		assert.Contains(t, body, `"hint"`, path)
	}

	keyID, _ := pw["keyId"].(string)
	code, body := env.do(t, http.MethodPost, entraGraph+"/applications/"+id+"/removePassword", `{"keyId":"`+keyID+`"}`)
	require.Equal(t, http.StatusNoContent, code, body)

	_, body = env.do(t, http.MethodGet, entraGraph+"/applications/"+id, "")
	assert.NotContains(t, body, keyID)

	code, _ = env.do(t, http.MethodDelete, entraGraph+"/applications/"+id, "")
	assert.Equal(t, http.StatusNoContent, code)
}

func TestAzureGraphProxyDirectory(t *testing.T) {
	t.Parallel()

	env := newEntraEnv(t)
	const pass = "Sup3r-Secret-Pw!"

	app := env.create(t, entraGraph+"/applications", `{"displayName":"api"}`)
	appID, _ := app["appId"].(string)
	sp := env.create(t, entraGraph+"/servicePrincipals", `{"appId":"`+appID+`","accountEnabled":false}`)
	spID, _ := sp["id"].(string)
	assert.Equal(t, true, sp["accountEnabled"])

	user := env.create(t, entraGraph+"/users", `{"displayName":"Ann","mailNickname":"ann",`+
		`"userPrincipalName":"ann@gopherstack.onmicrosoft.com","password":"`+pass+`","accountEnabled":false}`)
	userID, _ := user["id"].(string)
	assert.Equal(t, true, user["accountEnabled"])

	group := env.create(t, entraGraph+"/groups", `{"displayName":"ops","mailNickname":"ops","mailEnabled":true}`)
	groupID, _ := group["id"].(string)
	assert.Equal(t, false, group["mailEnabled"])

	code, body := env.do(t, http.MethodPost, entraGraph+"/groups/"+groupID+"/members/$ref", `{"id":"`+userID+`"}`)
	require.Equal(t, http.StatusNoContent, code, body)

	_, body = env.do(t, http.MethodGet, entraGraph+"/groups/"+groupID+"/members", "")
	assert.Contains(t, body, "ann@gopherstack.onmicrosoft.com")

	code, body = env.do(t, http.MethodPost, entraGraph+"/directoryObjects/getByIds",
		`{"ids":["`+userID+`","`+spID+`"],"types":["application"]}`)
	require.Equal(t, http.StatusOK, code, body)
	assert.Contains(t, body, "Ann")

	code, body = env.do(t, http.MethodDelete, entraGraph+"/groups/"+groupID+"/members/"+userID+"/$ref", "")
	require.Equal(t, http.StatusNoContent, code, body)

	code, body = env.do(t, http.MethodGet, entraGraph+"/servicePrincipals/"+spID+"/appRoleAssignments", "")
	require.Equal(t, http.StatusOK, code, body)

	for _, path := range []string{"/users", "/users/" + userID, "/groups", "/servicePrincipals"} {
		_, out := env.do(t, http.MethodGet, entraGraph+path, "")
		assert.NotContains(t, out, pass, path)
		assert.NotContains(t, strings.ToLower(out), "passwordprofile", path)
	}

	_, out := env.do(t, http.MethodPost, entraGraph+"/users", `{"displayName":"Bob","mailNickname":"bob",`+
		`"userPrincipalName":"bob@gopherstack.onmicrosoft.com","password":"`+pass+`"}`)
	assert.NotContains(t, out, pass)

	code, out = env.do(t, http.MethodPost, entraGraph+"/users", `{"displayName":"Weak","mailNickname":"weak",`+
		`"userPrincipalName":"weak@gopherstack.onmicrosoft.com","password":"short"}`)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.NotContains(t, out, "short\"")
}

func TestAzureAuthzProxy(t *testing.T) {
	t.Parallel()

	env := newEntraEnv(t)
	assignPath := entraAuthz + entraRG + "/providers/Microsoft.Authorization/roleAssignments/" + entraGUID2
	assignBody := `{"properties":{"roleDefinitionId":"` + entraRole + `","principalId":"` + entraGUID1 +
		`","principalType":"User","condition":"true","description":"smuggled","scope":"/"}}`

	code, body := env.do(t, http.MethodPut, assignPath+entraAPI, assignBody)
	require.Less(t, code, http.StatusBadRequest, body)
	assert.NotContains(t, body, "smuggled")
	assert.Contains(t, body, entraRG)

	code, body = env.do(t, http.MethodGet, entraAuthz+entraRG+entraAuth+"roleAssignments"+entraAPI, "")
	require.Equal(t, http.StatusOK, code, body)
	assert.Contains(t, body, entraGUID2)

	code, body = env.do(t, http.MethodGet, entraAuthz+entraSub+entraAuth+"roleDefinitions"+entraAPI, "")
	require.Equal(t, http.StatusOK, code, body)
	assert.Contains(t, body, "Contributor")

	code, body = env.do(t, http.MethodGet, entraAuthz+entraSub+entraAuth+"policyDefinitions?api-version=2021-06-01", "")
	require.Equal(t, http.StatusOK, code, body)

	lock := entraAuthz + entraRG + "/providers/Microsoft.Authorization/locks/keep?api-version=2020-05-01"
	lockBody := `{"properties":{"level":"CanNotDelete","notes":"n","owners":[{"applicationId":"x"}]}}`
	code, body = env.do(t, http.MethodPut, lock, lockBody)
	require.Less(t, code, http.StatusBadRequest, body)
	assert.NotContains(t, body, "applicationId")

	code, body = env.do(t, http.MethodDelete, entraARM+entraRG+"?api-version=2021-04-01", "")
	assert.Equal(t, http.StatusConflict, code)
	assert.Contains(t, body, "ScopeLocked")

	code, _ = env.do(t, http.MethodDelete, lock, "")
	assert.Less(t, code, http.StatusMultipleChoices)

	code, _ = env.do(t, http.MethodDelete, assignPath+entraAPI, "")
	assert.Less(t, code, http.StatusMultipleChoices)
}

func TestAzureAuthzProxyRejects(t *testing.T) {
	t.Parallel()

	env := newEntraEnv(t)
	loc := entraRG + "/providers/Microsoft.Authorization/"

	tests := []struct {
		name, method, path, body string
	}{
		{"put role definition", http.MethodPut, entraSub + entraAuth + "roleDefinitions/" + entraGUID1, `{}`},
		{"put policy", http.MethodPut, loc + "policyAssignments/x", `{}`},
		{"delete policy", http.MethodDelete, loc + "policyAssignments/x", ""},
		{"other namespace", http.MethodGet, entraRG + "/providers/Microsoft.Storage/storageAccounts", ""},
		{
			"resource scope",
			http.MethodGet,
			entraRG + "/providers/Microsoft.Compute/virtualMachines/a" + entraAuth + "locks",
			"",
		},
		{"assignment bad name", http.MethodPut, loc + "roleAssignments/not-a-guid", `{}`},
		{
			"assignment bad role", http.MethodPut, loc + "roleAssignments/" + entraGUID2,
			`{"properties":{"roleDefinitionId":"/providers/x","principalId":"` + entraGUID1 + `"}}`,
		},
		{
			"assignment bad principal", http.MethodPut, loc + "roleAssignments/" + entraGUID2,
			`{"properties":{"roleDefinitionId":"` + entraRole + `","principalId":"nope"}}`,
		},
		{"lock bad level", http.MethodPut, loc + "locks/a", `{"properties":{"level":"Nope"}}`},
		{"lock bad name", http.MethodPut, loc + "locks/a%20b", `{"properties":{"level":"ReadOnly"}}`},
		{"post", http.MethodPost, loc + "locks/a", `{}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, _ := env.do(t, tt.method, entraAuthz+tt.path+entraAPI, tt.body)
			assert.Contains(t, []int{http.StatusBadRequest, http.StatusMethodNotAllowed}, code)
		})
	}
}

func TestEntraProxiesUnavailable(t *testing.T) {
	t.Parallel()

	e := echo.New()
	e.Any("/*", dashboard.NewHandler(dashboard.Config{}).Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	env := &entraEnv{url: srv.URL, client: testclient.New(t)}

	for _, path := range []string{
		entraGraph + "/applications",
		entraAuthz + entraSub + "/providers/Microsoft.Authorization/roleDefinitions" + entraAPI,
	} {
		code, body := env.do(t, http.MethodGet, path, "")
		assert.Equal(t, http.StatusServiceUnavailable, code, path)
		assert.Contains(t, body, "ServiceUnavailable")
	}
}
