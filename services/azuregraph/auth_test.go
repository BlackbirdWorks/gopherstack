package azuregraph_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/aadauth"
	"github.com/blackbirdworks/gopherstack/services/azuregraph"
)

func TestAuthentication(t *testing.T) {
	t.Parallel()

	other, err := aadauth.NewIssuer()
	require.NoError(t, err)

	foreign, err := other.IssueClientCredentialsToken(aadauth.ClientCredentialsClaims{
		Audience: graphAudience, TenantID: azuregraph.DefaultTenantID, ClientID: newID0(),
	}, 0)
	require.NoError(t, err)

	tests := []struct {
		name     string
		validate bool
		token    func(*env) string
		status   int
	}{
		{"no token", false, func(*env) string { return "" }, http.StatusUnauthorized},
		{"garbage token", false, func(*env) string { return "abc" }, http.StatusUnauthorized},
		{"any jwt accepted by default", false, func(e *env) string { return foreign }, http.StatusOK},
		{"validated ok", true, func(e *env) string { return e.tokenFor(newID0(), graphAudience) }, http.StatusOK},
		{"validated trailing slash audience", true, func(e *env) string { return e.tokenFor(newID0(), graphAudience+"/") }, http.StatusOK},
		{"validated app id audience", true, func(e *env) string { return e.tokenFor(newID0(), "00000003-0000-0000-c000-000000000000") }, http.StatusOK},
		{"validated wrong audience", true, func(e *env) string { return e.tokenFor(newID0(), "https://management.azure.com/") }, http.StatusUnauthorized},
		{"validated foreign signature", true, func(*env) string { return foreign }, http.StatusUnauthorized},
		{"validated garbage", true, func(*env) string { return "abc" }, http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, func(s *azuregraph.Settings) { s.ValidateTokens = tt.validate })
			e.token = tt.token(e)

			r := e.req(t, http.MethodGet, "/v1.0/applications", nil)
			assert.Equal(t, tt.status, r.status, r.raw)

			if tt.status == http.StatusUnauthorized {
				assertErr(t, r, http.StatusUnauthorized, "InvalidAuthenticationToken")
				assert.NotEmpty(t, r.header.Get("WWW-Authenticate"))
			}
		})
	}
}

func TestMe(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	user := newUser(t, e, "me"+upnDomain)
	app := newApp(t, e, "MeApp")
	sp := e.mustCreate(t, "/v1.0/servicePrincipals", map[string]any{"appId": str(app, "appId")})

	tests := []struct {
		name   string
		oid    string
		path   string
		status int
		code   string
	}{
		{"user", str(user, "id"), "/v1.0/me", http.StatusOK, ""},
		{"user memberOf", str(user, "id"), "/v1.0/me/memberOf", http.StatusOK, ""},
		{"app-only token", str(sp, "id"), "/v1.0/me", http.StatusBadRequest, "BadRequest"},
		{"unknown oid", newID0(), "/v1.0/me", http.StatusNotFound, "Request_ResourceNotFound"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := *e
			c.token = e.tokenFor(tt.oid, graphAudience)

			r := c.req(t, http.MethodGet, tt.path, nil)
			if tt.code != "" {
				assertErr(t, r, tt.status, tt.code)

				return
			}

			require.Equal(t, tt.status, r.status, r.raw)

			if tt.path == "/v1.0/me" {
				assert.Equal(t, "me"+upnDomain, str(r.body, "userPrincipalName"))
			}
		})
	}
}

func TestAuthenticateClient(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	app := newApp(t, e, "Daemon")
	appID := str(app, "appId")
	pw := e.req(t, http.MethodPost, "/v1.0/applications/"+str(app, "id")+"/addPassword", map[string]any{})
	secret := str(pw.body, "secretText")

	noSP := newApp(t, e, "NoSP")

	b := e.h.Backend

	t.Run("app without service principal", func(t *testing.T) {
		_, known, err := b.AuthenticateClient(str(noSP, "appId"), "x")
		assert.True(t, known)

		var ce *azuregraph.ClientAuthError

		require.ErrorAs(t, err, &ce)
		assert.Equal(t, 7000229, ce.Code)
	})

	sp := e.mustCreate(t, "/v1.0/servicePrincipals", map[string]any{"appId": appID})

	tests := []struct {
		name     string
		clientID string
		secret   string
		advance  int
		known    bool
		code     int
	}{
		{"valid", appID, secret, 0, true, 0},
		{"valid mixed case", "" + upper(appID), secret, 0, true, 0},
		{"wrong secret", appID, "nope", 0, true, 7000215},
		{"empty secret", appID, "", 0, true, 7000215},
		{"unknown client", newID0(), "x", 0, false, 0},
		{"expired", appID, secret, 3 * 365 * 24, true, 7000215},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// not parallel: the expired case advances the shared clock
			e.clock.advance(time.Duration(tt.advance) * time.Hour)

			oid, known, err := b.AuthenticateClient(tt.clientID, tt.secret)
			assert.Equal(t, tt.known, known)

			if tt.code == 0 {
				require.NoError(t, err)

				if known {
					assert.Equal(t, str(sp, "id"), oid)
				}

				return
			}

			var ce *azuregraph.ClientAuthError

			require.ErrorAs(t, err, &ce)
			assert.Equal(t, tt.code, ce.Code)
			assert.NotContains(t, ce.Description, secret)
		})
	}

	_ = e.req(t, http.MethodPatch, "/v1.0/servicePrincipals/"+str(sp, "id"), map[string]any{"accountEnabled": false})

	e.clock.advance(0)
	_, _, err := b.AuthenticateClient(appID, secret)

	var ce *azuregraph.ClientAuthError

	require.ErrorAs(t, err, &ce)
	assert.Equal(t, 7000112, ce.Code)
}

func upper(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		}
	}

	return string(b)
}

func TestDevClientAndCreatorOwnership(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	oid, known, err := e.h.Backend.AuthenticateClient(azuregraph.DefaultTenantID, "any secret at all")
	require.NoError(t, err)
	assert.True(t, known)

	dev := e.req(t, http.MethodGet, "/v1.0/servicePrincipals/"+oid, nil)
	require.Equal(t, http.StatusOK, dev.status)
	assert.Equal(t, azuregraph.DefaultTenantID, str(dev.body, "appId"))
	assertErr(t, e.req(t, http.MethodDelete, "/v1.0/servicePrincipals/"+oid, nil), http.StatusForbidden, "Authorization_RequestDenied")

	user := newUser(t, e, "creator"+upnDomain)

	tests := []struct {
		name      string
		oid       string
		path      string
		body      map[string]any
		wantOwner bool
	}{
		{"service principal creates application", oid, "/v1.0/applications", map[string]any{"displayName": "A"}, true},
		{"user creates group", str(user, "id"), "/v1.0/groups",
			map[string]any{"displayName": "G", "mailEnabled": false, "mailNickname": "g", "securityEnabled": true}, true},
		{"non-directory caller", newID0(), "/v1.0/applications", map[string]any{"displayName": "B"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := *e
			c.token = e.tokenFor(tt.oid, graphAudience)

			created := c.mustCreate(t, tt.path, tt.body)
			owners := c.req(t, http.MethodGet, tt.path+"/"+str(created, "id")+"/owners", nil)
			require.Equal(t, http.StatusOK, owners.status, owners.raw)

			if !tt.wantOwner {
				assert.Empty(t, owners.body["value"])

				return
			}

			list, _ := owners.body["value"].([]any)
			require.Len(t, list, 1)
			assert.Equal(t, tt.oid, str(list[0].(map[string]any), "id")) //nolint:forcetypeassert,errcheck // owners are objects
		})
	}
}
