package azurearm_test

import (
	"context"
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
)

type fakeClients struct{}

func (fakeClients) AuthenticateClient(clientID, secret string) (string, bool, error) {
	if clientID != "registered" {
		return "", false, nil
	}

	if secret == "right" {
		return "oid-registered", true, nil
	}

	return "", true, &azurearm.ClientAuthError{
		Name:        "invalid_client",
		Status:      http.StatusUnauthorized,
		Code:        7000215,
		Description: "AADSTS7000215: bad secret",
	}
}

func TestTokenEndpointClientAuthentication(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // test-only struct; layout not performance-critical
		name      string
		form      url.Values
		basicUser string
		basicPass string
		status    int
		wantOID   string
		wantCode  int
	}{
		{
			"registered ok",
			url.Values{"client_id": {"registered"}, "client_secret": {"right"}},
			"",
			"",
			200,
			"oid-registered",
			0,
		},
		{"registered basic auth", url.Values{}, "registered", "right", 200, "oid-registered", 0},
		{
			"registered wrong secret",
			url.Values{"client_id": {"registered"}, "client_secret": {"wrong"}},
			"",
			"",
			401,
			"",
			7000215,
		},
		{"registered no secret", url.Values{"client_id": {"registered"}}, "", "", 401, "", 7000215},
		{
			"unregistered stays permissive",
			url.Values{"client_id": {"someone"}, "client_secret": {"x"}},
			"",
			"",
			200,
			"someone",
			0,
		},
		{"no client id uses dev client", url.Values{}, "", "", 200, azurearm.DefaultClientID, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			h.SetClientAuthenticator(fakeClients{})

			tt.form.Set("grant_type", "client_credentials")
			tt.form.Set("scope", "https://graph.microsoft.com/.default")

			req := httptest.NewRequest(
				http.MethodPost,
				"/tenant/oauth2/v2.0/token",
				strings.NewReader(tt.form.Encode()),
			)
			req.Host = "host:10006"
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			if tt.basicUser != "" {
				req.SetBasicAuth(tt.basicUser, tt.basicPass)
			}

			rec := httptest.NewRecorder()
			newEchoServer(h).ServeHTTP(rec, req)
			require.Equal(t, tt.status, rec.Code, rec.Body.String())

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

			if tt.wantCode != 0 {
				assert.Equal(t, "invalid_client", body["error"])
				assert.Equal(t, []any{float64(tt.wantCode)}, body["error_codes"])
				assert.Contains(t, body["error_description"], "Trace ID:")
				assert.NotContains(t, rec.Body.String(), "wrong")

				return
			}

			claims, err := h.Issuer.Parse(
				body["access_token"].(string),
			) //nolint:forcetypeassert // asserted by the status check
			require.NoError(t, err)
			assert.Equal(t, tt.wantOID, claims["oid"])
			assert.Equal(t, "https://graph.microsoft.com/", claims["aud"])
		})
	}
}

type fakeDirectory map[string]string

func (d fakeDirectory) PrincipalType(id string) (string, bool) {
	t, ok := d[strings.ToLower(id)]

	return t, ok
}

type validatingConfig struct{ validate bool }

func (c validatingConfig) GetAzureARMSettings() azurearm.Settings {
	s := azurearm.DefaultSettings()
	s.ValidatePrincipals = c.validate

	return s
}

func TestRoleAssignmentPrincipalDirectory(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // test-only struct; layout not performance-critical
		name      string
		validate  bool
		principal string
		status    int
		wantType  string
	}{
		{"known group typed from directory", true, guidA, http.StatusCreated, "Group"},
		{"known user", true, guidB, http.StatusCreated, "User"},
		{"unknown rejected when validating", true, guidC, http.StatusBadRequest, ""},
		{"unknown accepted by default", false, guidC, http.StatusCreated, "User"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, err := (&azurearm.Provider{}).Init(&service.AppContext{Config: validatingConfig{tt.validate}})
			require.NoError(t, err)

			h, ok := svc.(*azurearm.Handler)
			require.True(t, ok)

			t.Cleanup(func() { h.Shutdown(context.Background()) })
			h.SetPrincipalDirectory(fakeDirectory{guidA: "Group", guidB: "User"})

			e := echo.New()
			e.Any("/*", h.Handler())

			srv := httptest.NewTLSServer(e)
			t.Cleanup(srv.Close)

			body := `{"properties":{"roleDefinitionId":"` + authSubScope +
				`/providers/Microsoft.Authorization/roleDefinitions/` + roleReader + `","principalId":"` + tt.principal + `"}}`
			req, err := http.NewRequestWithContext(
				t.Context(),
				http.MethodPut,
				srv.URL+authSubScope+"/providers/Microsoft.Authorization/roleAssignments/"+
					"55555555-5555-5555-5555-555555555555?api-version=2022-04-01",
				strings.NewReader(body),
			)
			require.NoError(t, err)

			req.Header.Set("Content-Type", "application/json")

			resp, err := srv.Client().Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			raw, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, tt.status, resp.StatusCode, string(raw))

			if tt.status != http.StatusCreated {
				assert.Contains(t, string(raw), "PrincipalNotFound")

				return
			}

			assert.Contains(t, string(raw), `"principalType":"`+tt.wantType+`"`)
		})
	}
}
