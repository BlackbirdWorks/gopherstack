package azurearm_test

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

type fakePasswordClients struct{ fakeClients }

func (fakePasswordClients) AuthenticateUser(username, password string) (string, string, error) {
	switch {
	case username == "alice@contoso.test" && password == "Right#Pass1":
		return "oid-alice", "alice@contoso.test", nil
	case username == "alice@contoso.test":
		return "", "", &azurearm.ClientAuthError{
			Name: "invalid_grant", Status: http.StatusBadRequest, Code: 50126,
			Description: "AADSTS50126: Error validating credentials due to invalid username or password.",
		}
	}

	return "", "", &azurearm.ClientAuthError{
		Name: "invalid_grant", Status: http.StatusBadRequest, Code: 50034, Description: "AADSTS50034: no such user",
	}
}

func TestTokenEndpointPasswordGrant(t *testing.T) {
	t.Parallel()

	const tenantID = "11111111-2222-3333-4444-555555555555"

	tests := []struct {
		form      url.Values
		name      string
		tenant    string
		wantOID   string
		wantErr   string
		wantScope string
		wantAud   string
		status    int
		wantCode  int
	}{
		{
			name:    "valid",
			tenant:  "common",
			form:    url.Values{"username": {"alice@contoso.test"}, "password": {"Right#Pass1"}},
			status:  http.StatusOK,
			wantOID: "oid-alice",
			wantAud: "https://graph.microsoft.com/",
		},
		{name: "valid scoped", tenant: tenantID, form: url.Values{
			"username": {"alice@contoso.test"}, "password": {"Right#Pass1"},
			"scope": {"User.Read openid"}},
			status: http.StatusOK, wantOID: "oid-alice", wantScope: "User.Read",
			wantAud: "https://graph.microsoft.com/"},
		{name: "domain tenant", tenant: "contoso.test",
			form:   url.Values{"username": {"alice@contoso.test"}, "password": {"Right#Pass1"}},
			status: http.StatusOK, wantOID: "oid-alice", wantAud: "https://graph.microsoft.com/"},
		{name: "foreign tenant", tenant: "99999999-2222-3333-4444-555555555555",
			form:   url.Values{"username": {"alice@contoso.test"}, "password": {"Right#Pass1"}},
			status: http.StatusBadRequest, wantCode: 90002, wantErr: "invalid_request"},
		{name: "missing client", tenant: "common", form: url.Values{
			"username": {"alice@contoso.test"}, "password": {"Right#Pass1"}, "client_id": {""}},
			status: http.StatusBadRequest, wantCode: 900144, wantErr: "invalid_request"},
		{
			name:     "wrong password",
			tenant:   "common",
			form:     url.Values{"username": {"alice@contoso.test"}, "password": {"nope"}},
			status:   http.StatusBadRequest,
			wantCode: 50126,
			wantErr:  "invalid_grant",
		},
		{name: "unknown user", tenant: "common", form: url.Values{"username": {"bob@contoso.test"}, "password": {"x"}},
			status: http.StatusBadRequest, wantCode: 50034, wantErr: "invalid_grant"},
		{name: "missing username", tenant: "common", form: url.Values{"password": {"x"}},
			status: http.StatusBadRequest, wantCode: 900144, wantErr: "invalid_request"},
		{name: "missing password", tenant: "common", form: url.Values{"username": {"alice@contoso.test"}},
			status: http.StatusBadRequest, wantCode: 900144, wantErr: "invalid_request"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			h.Settings.TenantID = tenantID
			h.SetClientAuthenticator(fakePasswordClients{})

			tt.form.Set("grant_type", "password")

			if _, set := tt.form["client_id"]; !set {
				tt.form.Set("client_id", "04b07795-8ddb-461a-bbee-02f9e1bf7b46")
			}

			if tt.form.Get("scope") == "" {
				tt.form.Set("scope", "https://graph.microsoft.com/.default")
			}

			req := httptest.NewRequest(
				http.MethodPost,
				"/"+tt.tenant+"/oauth2/v2.0/token",
				strings.NewReader(tt.form.Encode()),
			)
			req.Host = "host:10006"
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			rec := httptest.NewRecorder()
			newEchoServer(h).ServeHTTP(rec, req)
			require.Equal(t, tt.status, rec.Code, rec.Body.String())

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body, jsonx.V1()))

			if tt.wantErr != "" {
				assert.Equal(t, tt.wantErr, body["error"])
				assert.Equal(t, []any{float64(tt.wantCode)}, body["error_codes"])
				assert.NotContains(t, rec.Body.String(), "Right#Pass1")
				assert.NotContains(t, body, "access_token")

				return
			}

			claims, err := h.Issuer.Parse(body["access_token"].(string)) //nolint:forcetypeassert // status checked
			require.NoError(t, err)
			assert.Equal(t, tt.wantOID, claims["oid"])
			assert.Equal(t, tenantID, claims["tid"])
			assert.Equal(t, "alice@contoso.test", claims["upn"])
			assert.Equal(t, "04b07795-8ddb-461a-bbee-02f9e1bf7b46", claims["appid"])

			if tt.wantScope == "" {
				assert.NotContains(t, claims, "scp")
			} else {
				assert.Equal(t, tt.wantScope, claims["scp"])
			}

			assert.Equal(t, tt.wantAud, claims["aud"])
		})
	}
}

func TestTokenEndpointClientCredentialsUnchanged(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.SetClientAuthenticator(fakePasswordClients{})

	form := url.Values{
		"grant_type": {
			"client_credentials",
		}, "client_id": {"app-1"}, "scope": {"https://management.azure.com/.default"},
	}
	req := httptest.NewRequest(http.MethodPost, "/tenant/oauth2/v2.0/token", strings.NewReader(form.Encode()))
	req.Host = "host:10006"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()
	newEchoServer(h).ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body, jsonx.V1()))

	claims, err := h.Issuer.Parse(body["access_token"].(string)) //nolint:forcetypeassert // status checked
	require.NoError(t, err)
	assert.Equal(t, "tenant", claims["tid"])
	assert.Equal(t, "https://management.azure.com/", claims["aud"])
	assert.NotContains(t, claims, "upn")
	assert.NotContains(t, claims, "scp")
}
