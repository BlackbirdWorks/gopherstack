package app

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuregraph"
)

func (s *graphStack) passwordToken(t *testing.T, tenant, username, password string) (int, map[string]any) {
	t.Helper()

	form := url.Values{
		"grant_type": {"password"}, "client_id": {"04b07795-8ddb-461a-bbee-02f9e1bf7b46"},
		"username": {username}, "password": {password}, "scope": {"https://graph.microsoft.com/User.Read"},
	}

	return do(t, s.arm, http.MethodPost, "/"+tenant+"/oauth2/v2.0/token",
		"application/x-www-form-urlencoded", form.Encode())
}

func TestAzureGraphPasswordSignInEndToEnd(t *testing.T) {
	t.Parallel()

	const password = "C0mplex!Passw0rd"

	s := newGraphStack(t)
	admin := s.bootstrapToken(t)
	upn := "ropc@" + azuregraph.DefaultDomain

	status, created := s.call(t, admin, http.MethodPost, "/v1.0/users",
		`{"accountEnabled":true,"displayName":"Ropc","mailNickname":"ropc","userPrincipalName":"`+upn+
			`","passwordProfile":{"password":"`+password+`","forceChangePasswordNextSignIn":false}}`)
	require.Equal(t, http.StatusCreated, status, created)

	id, _ := created["id"].(string)

	t.Run("token reaches me", func(t *testing.T) {
		t.Parallel()

		code, body := s.passwordToken(t, azuregraph.DefaultTenantID, upn, password)
		require.Equal(t, http.StatusOK, code, body)

		tok, _ := body["access_token"].(string)
		claims := tokenClaims(t, tok)
		assert.Equal(t, id, claims["oid"])
		assert.Equal(t, azuregraph.DefaultTenantID, claims["tid"])
		assert.Equal(t, upn, claims["upn"])
		assert.Equal(t, "User.Read", claims["scp"])
		assert.Equal(t, "https://graph.microsoft.com/", claims["aud"])

		code, me := s.call(t, tok, http.MethodGet, "/v1.0/me", "")
		require.Equal(t, http.StatusOK, code, me)
		assert.Equal(t, id, me["id"])
		assert.Equal(t, upn, me["userPrincipalName"])
	})

	t.Run("wrong password issues no token", func(t *testing.T) {
		t.Parallel()

		code, body := s.passwordToken(t, azuregraph.DefaultTenantID, upn, "nope")
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Equal(t, []any{float64(50126)}, body["error_codes"])
		assert.NotContains(t, body, "access_token")
	})

	t.Run("foreign tenant issues no token", func(t *testing.T) {
		t.Parallel()

		code, body := s.passwordToken(t, "99999999-0000-0000-0000-000000000000", upn, password)
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Equal(t, []any{float64(90002)}, body["error_codes"])
		assert.NotContains(t, body, "access_token")
	})
}
