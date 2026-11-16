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

func (fakePasswordClients) AuthenticateUser(username, password string) (string, error) {
	switch {
	case username == "alice@contoso.test" && password == "Right#Pass1":
		return "oid-alice", nil
	case username == "alice@contoso.test":
		return "", &azurearm.ClientAuthError{
			Name: "invalid_grant", Status: http.StatusBadRequest, Code: 50126,
			Description: "AADSTS50126: Error validating credentials due to invalid username or password.",
		}
	}

	return "", &azurearm.ClientAuthError{
		Name: "invalid_grant", Status: http.StatusBadRequest, Code: 50034, Description: "AADSTS50034: no such user",
	}
}

func TestTokenEndpointPasswordGrant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		form     url.Values
		name     string
		wantOID  string
		wantErr  string
		status   int
		wantCode int
	}{
		{name: "valid", form: url.Values{"username": {"alice@contoso.test"}, "password": {"Right#Pass1"}},
			status: http.StatusOK, wantOID: "oid-alice"},
		{name: "wrong password", form: url.Values{"username": {"alice@contoso.test"}, "password": {"nope"}},
			status: http.StatusBadRequest, wantCode: 50126, wantErr: "invalid_grant"},
		{name: "unknown user", form: url.Values{"username": {"bob@contoso.test"}, "password": {"x"}},
			status: http.StatusBadRequest, wantCode: 50034, wantErr: "invalid_grant"},
		{name: "missing username", form: url.Values{"password": {"x"}},
			status: http.StatusBadRequest, wantCode: 900144, wantErr: "invalid_request"},
		{name: "missing password", form: url.Values{"username": {"alice@contoso.test"}},
			status: http.StatusBadRequest, wantCode: 900144, wantErr: "invalid_request"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			h.SetClientAuthenticator(fakePasswordClients{})

			tt.form.Set("grant_type", "password")
			tt.form.Set("scope", "https://graph.microsoft.com/.default")

			req := httptest.NewRequest(
				http.MethodPost,
				"/tenant/oauth2/v2.0/token",
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

				return
			}

			claims, err := h.Issuer.Parse(body["access_token"].(string)) //nolint:forcetypeassert // status checked
			require.NoError(t, err)
			assert.Equal(t, tt.wantOID, claims["oid"])
		})
	}
}
