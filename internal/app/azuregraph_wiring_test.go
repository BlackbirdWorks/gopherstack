package app

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azuregraph"
)

type graphARMConfig struct{}

func (graphARMConfig) GetAzureARMSettings() azurearm.Settings {
	s := azurearm.DefaultSettings()
	s.ValidatePrincipals = true

	return s
}

type graphStack struct {
	arm   *httptest.Server
	graph *httptest.Server
}

func newGraphStack(t *testing.T) *graphStack {
	t.Helper()

	armSvc, err := (&azurearm.Provider{}).Init(&service.AppContext{Config: graphARMConfig{}})
	require.NoError(t, err)

	armH, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	settings := azuregraph.DefaultSettings()
	settings.ValidateTokens = true
	graphH := azuregraph.NewHandler(azuregraph.NewBackend(settings), settings)

	wireAzureGraph(map[string]service.Registerable{"AzureARM": armH, "AzureGraph": graphH})

	ae, ge := echo.New(), echo.New()
	ae.Any("/*", armH.Handler())
	ge.Any("/*", graphH.Handler())

	s := &graphStack{arm: httptest.NewTLSServer(ae), graph: httptest.NewTLSServer(ge)}
	t.Cleanup(s.arm.Close)
	t.Cleanup(s.graph.Close)

	return s
}

func do(t *testing.T, srv *httptest.Server, method, path, contentType, body string, headers ...string) (int, map[string]any) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, srv.URL+path, strings.NewReader(body))
	require.NoError(t, err)

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}

	resp, err := srv.Client().Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var out map[string]any
	_ = json.Unmarshal(raw, &out)

	return resp.StatusCode, out
}

func (s *graphStack) token(t *testing.T, clientID, secret string) (int, map[string]any) {
	t.Helper()

	form := url.Values{
		"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {secret},
		"scope": {"https://graph.microsoft.com/.default"},
	}

	return do(t, s.arm, http.MethodPost, "/"+azuregraph.DefaultTenantID+"/oauth2/v2.0/token",
		"application/x-www-form-urlencoded", form.Encode())
}

func (s *graphStack) call(t *testing.T, bearer, method, path, body string) (int, map[string]any) {
	t.Helper()

	return do(t, s.graph, method, path, "application/json", body, "Authorization", "Bearer "+bearer)
}

func TestAzureGraphClientCredentialsAndPrincipals(t *testing.T) {
	t.Parallel()

	s := newGraphStack(t)

	status, boot := s.token(t, "", "")
	require.Equal(t, http.StatusOK, status)

	admin, _ := boot["access_token"].(string)

	status, app := s.call(t, admin, http.MethodPost, "/v1.0/applications", `{"displayName":"daemon"}`)
	require.Equal(t, http.StatusCreated, status)

	appID, _ := app["appId"].(string)

	status, sp := s.call(t, admin, http.MethodPost, "/v1.0/servicePrincipals", `{"appId":"`+appID+`"}`)
	require.Equal(t, http.StatusCreated, status)

	spID, _ := sp["id"].(string)

	status, pw := s.call(t, admin, http.MethodPost, "/v1.0/applications/"+app["id"].(string)+"/addPassword", `{}`) //nolint:forcetypeassert,errcheck // created above
	require.Equal(t, http.StatusOK, status)

	secret, _ := pw["secretText"].(string)

	t.Run("token endpoint verifies registered client secrets", func(t *testing.T) {
		t.Parallel()

		status, body := s.token(t, appID, "wrong")
		assert.Equal(t, http.StatusUnauthorized, status)
		assert.Equal(t, "invalid_client", body["error"])
		assert.Equal(t, []any{float64(7000215)}, body["error_codes"])

		status, body = s.token(t, appID, secret)
		require.Equal(t, http.StatusOK, status)

		tok, _ := body["access_token"].(string)
		status, got := s.call(t, tok, http.MethodGet, "/v1.0/servicePrincipals(appId='"+appID+"')", "")
		require.Equal(t, http.StatusOK, status)
		assert.Equal(t, spID, got["id"])

		status, got = s.call(t, tok, http.MethodGet, "/v1.0/me", "")
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "BadRequest", got["error"].(map[string]any)["code"]) //nolint:forcetypeassert,errcheck // error envelope
	})

	t.Run("graph rejects tokens for another audience", func(t *testing.T) {
		t.Parallel()

		form := url.Values{"client_id": {appID}, "client_secret": {secret}, "scope": {"https://management.azure.com/.default"}}
		_, body := do(t, s.arm, http.MethodPost, "/"+azuregraph.DefaultTenantID+"/oauth2/v2.0/token",
			"application/x-www-form-urlencoded", form.Encode())

		arm, _ := body["access_token"].(string)
		status, _ := s.call(t, arm, http.MethodGet, "/v1.0/applications", "")
		assert.Equal(t, http.StatusUnauthorized, status)
	})

	t.Run("role assignments resolve principals in the directory", func(t *testing.T) {
		t.Parallel()

		path := "/subscriptions/" + azurearm.DefaultSubscriptionID + "/providers/Microsoft.Authorization/roleAssignments/"
		role := "/subscriptions/" + azurearm.DefaultSubscriptionID +
			"/providers/Microsoft.Authorization/roleDefinitions/acdd72a7-3385-48ef-bd42-f606fba81ae7"

		put := func(name, principal string) (int, map[string]any) {
			return do(t, s.arm, http.MethodPut, path+name+"?api-version=2022-04-01", "application/json",
				`{"properties":{"roleDefinitionId":"`+role+`","principalId":"`+principal+`"}}`)
		}

		status, body := put("66666666-6666-6666-6666-666666666666", spID)
		require.Equal(t, http.StatusCreated, status, body)
		assert.Equal(t, "ServicePrincipal", body["properties"].(map[string]any)["principalType"]) //nolint:forcetypeassert,errcheck // ARM envelope

		status, body = put("77777777-7777-7777-7777-777777777777", "99999999-9999-9999-9999-999999999999")
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "PrincipalNotFound", body["error"].(map[string]any)["code"]) //nolint:forcetypeassert,errcheck // ARM envelope
	})

	t.Run("environment metadata points at the graph listener", func(t *testing.T) {
		t.Parallel()

		status, meta := do(t, s.arm, http.MethodGet, "/metadata/endpoints?api-version=2022-09-01", "", "")
		require.Equal(t, http.StatusOK, status)

		res, _ := meta["microsoftGraphResourceId"].(string)
		assert.True(t, strings.HasSuffix(res, ":10014"), res)
	})
}
