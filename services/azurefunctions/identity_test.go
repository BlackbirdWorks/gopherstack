package azurefunctions_test

import (
	"encoding/json/v2"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

func TestManagedIdentity_TokenService(t *testing.T) {
	t.Parallel()

	const userRes = "/subscriptions/s/resourceGroups/g/providers/Microsoft.ManagedIdentity/userAssignedIdentities/u"

	tests := []struct {
		name       string
		query      string
		header     string
		wantClient string
		wantCode   int
	}{
		{"system", "resource=https://vault.azure.net&api-version=2019-08-01", "ok", "sys-principal", http.StatusOK},
		{
			"user by client id", "resource=r&api-version=2019-08-01&client_id=user-client", "ok",
			"user-client", http.StatusOK,
		},
		{
			"user by resource id", "resource=r&api-version=2019-08-01&mi_res_id=" + userRes, "ok",
			"user-client", http.StatusOK,
		},
		{"unknown user", "resource=r&api-version=2019-08-01&client_id=nope", "ok", "", http.StatusBadRequest},
		{"two selectors", "resource=r&api-version=2019-08-01&client_id=a&object_id=b", "ok", "", http.StatusBadRequest},
		{"no resource", "api-version=2019-08-01", "ok", "", http.StatusBadRequest},
		{"old api version", "resource=r&api-version=2018-02-01", "ok", "", http.StatusBadRequest},
		{"wrong header", "resource=r&api-version=2019-08-01", "bad", "", http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			e.backend.SetIdentity(
				func(app string) (azurefunctions.Identity, bool) {
					return azurefunctions.Identity{
						TenantID:          "tenant",
						SystemPrincipalID: "sys-principal",
						User: []azurefunctions.UserIdentity{
							{ResourceID: userRes, ClientID: "user-client", PrincipalID: "user-principal"},
						},
					}, app == "app"
				},
				func(tenant, clientID, objectID, resource string) (string, time.Time, error) {
					return "tok:" + tenant + ":" + clientID + ":" + objectID + ":" + resource, time.Unix(
						1700000000,
						0,
					), nil
				},
			)
			e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})
			_, err := e.backend.Deploy("app", helloZip(t), "t", "m")
			require.NoError(t, err)

			resp, _ := e.do(t, http.MethodGet, "app."+testDomain, "/api/greet", nil, nil)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			env := envOf(e.rt.specs()[0])
			assert.Contains(t, env["IDENTITY_ENDPOINT"], "/msi/token")
			require.NotEmpty(t, env["IDENTITY_HEADER"])

			hdr := env["IDENTITY_HEADER"]
			if tt.header == "bad" {
				hdr = "wrong"
			}

			resp, body := e.do(t, http.MethodGet, "172.17.0.1:"+strconv.Itoa(azurefunctions.DefaultPort),
				"/MSI/token?"+tt.query, nil, map[string]string{"X-Identity-Header": hdr})
			require.Equal(t, tt.wantCode, resp.StatusCode, body)

			if tt.wantCode != http.StatusOK {
				return
			}

			var got map[string]string
			require.NoError(t, json.Unmarshal([]byte(body), &got, jsonx.V1()))
			assert.Equal(t, "Bearer", got["token_type"])
			assert.Equal(t, "1700000000", got["expires_on"])
			assert.Equal(t, tt.wantClient, got["client_id"])
			assert.Contains(t, got["access_token"], "tok:tenant:"+tt.wantClient)
		})
	}
}

func TestManagedIdentity_NoEnvWithoutIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		settings map[string]string
		name     string
		has      bool
		wantEnv  bool
	}{
		{nil, "identity", true, true},
		{nil, "none", false, false},
		{map[string]string{"WEBSITE_DISABLE_MSI": "true"}, "disabled", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			e.backend.SetIdentity(
				func(string) (azurefunctions.Identity, bool) {
					return azurefunctions.Identity{SystemPrincipalID: "p"}, tt.has
				},
				nil,
			)
			e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})
			require.NoError(t, e.backend.SetSettings("app", tt.settings))
			_, err := e.backend.Deploy("app", helloZip(t), "t", "m")
			require.NoError(t, err)

			e.do(t, http.MethodGet, "app."+testDomain, "/api/greet", nil, nil)

			env := envOf(e.rt.specs()[0])
			_, has := env["IDENTITY_ENDPOINT"]
			assert.Equal(t, tt.wantEnv, has)
		})
	}
}
