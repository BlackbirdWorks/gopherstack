package azuregraph_test

import (
	"maps"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuregraph"
)

const (
	goodPassword = "C0mplex!Passw0rd"
	newPassword  = "An0ther#Passw0rd"
)

func newPasswordUser(t *testing.T, e *env, name string, extra map[string]any) map[string]any {
	t.Helper()

	body := map[string]any{
		"accountEnabled": true, "displayName": name, "mailNickname": name,
		"userPrincipalName": name + "@" + azuregraph.DefaultDomain,
		"passwordProfile":   map[string]any{"password": goodPassword, "forceChangePasswordNextSignIn": false},
	}
	maps.Copy(body, extra)

	return e.mustCreate(t, "/v1.0/users", body)
}

func authCode(t *testing.T, err error) int {
	t.Helper()

	var ce *azuregraph.ClientAuthError

	require.ErrorAs(t, err, &ce)
	assert.Equal(t, "invalid_grant", ce.Name)
	assert.Equal(t, http.StatusBadRequest, ce.Status)

	return ce.Code
}

func TestAuthenticateUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		extra    map[string]any
		name     string
		username string
		password string
		wantCode int
	}{
		{name: "valid", username: "ok", password: goodPassword},
		{name: "upn case insensitive", username: "OK", password: goodPassword},
		{name: "wrong password", username: "ok", password: "wrong", wantCode: 50126},
		{name: "empty password", username: "ok", password: "", wantCode: 50126},
		{name: "oversize password", username: "ok", password: strings.Repeat("a", 300), wantCode: 50126},
		{name: "unknown user", username: "nobody", password: goodPassword, wantCode: 50034},
		{name: "disabled", username: "ok", password: goodPassword, extra: map[string]any{"accountEnabled": false},
			wantCode: 50057},
		{name: "forced change", username: "ok", password: goodPassword, wantCode: 50055,
			extra: map[string]any{"passwordProfile": map[string]any{
				"password": goodPassword, "forceChangePasswordNextSignIn": true,
			}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			u := newPasswordUser(t, e, "ok", tt.extra)

			upn := tt.username
			if !strings.Contains(upn, "@") {
				upn += "@" + azuregraph.DefaultDomain
			}

			oid, err := e.h.Backend.AuthenticateUser(upn, tt.password)
			if tt.wantCode != 0 {
				assert.Equal(t, tt.wantCode, authCode(t, err))
				assert.Empty(t, oid)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, str(u, "id"), oid)
		})
	}
}

func TestPasswordLockoutUsesBackendClock(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	newPasswordUser(t, e, "lock", nil)

	upn := "lock@" + azuregraph.DefaultDomain

	for range 10 {
		_, err := e.h.Backend.AuthenticateUser(upn, "wrong")
		require.Equal(t, 50126, authCode(t, err))
	}

	_, err := e.h.Backend.AuthenticateUser(upn, goodPassword)
	assert.Equal(t, 50053, authCode(t, err), "locked even for the right password")

	e.clock.advance(59 * time.Second)

	_, err = e.h.Backend.AuthenticateUser(upn, goodPassword)
	assert.Equal(t, 50053, authCode(t, err))

	e.clock.advance(2 * time.Second)

	_, err = e.h.Backend.AuthenticateUser(upn, goodPassword)
	require.NoError(t, err)
}

func TestPasswordChangeAndPersistence(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	u := newPasswordUser(t, e, "chg", nil)
	id, upn := str(u, "id"), "chg@"+azuregraph.DefaultDomain

	r := e.req(t, http.MethodPatch, "/v1.0/users/"+id,
		map[string]any{"passwordProfile": map[string]any{"password": newPassword}})
	require.Equal(t, http.StatusNoContent, r.status, r.raw)

	_, err := e.h.Backend.AuthenticateUser(upn, goodPassword)
	assert.Equal(t, 50126, authCode(t, err), "old password no longer works")

	_, err = e.h.Backend.AuthenticateUser(upn, newPassword)
	require.NoError(t, err)

	snap := e.h.Backend.Snapshot(t.Context())
	assert.NotContains(t, string(snap), newPassword, "plaintext never persisted")
	assert.NotContains(t, string(snap), goodPassword)

	fresh := newEnv(t)
	require.NoError(t, fresh.h.Backend.Restore(t.Context(), snap))

	oid, err := fresh.h.Backend.AuthenticateUser(upn, newPassword)
	require.NoError(t, err)
	assert.Equal(t, id, oid)

	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, "/v1.0/users/"+id, nil).status)
	_, err = e.h.Backend.AuthenticateUser(upn, newPassword)
	assert.Equal(t, 50034, authCode(t, err))

	require.Equal(t, http.StatusOK,
		e.req(t, http.MethodPost, "/v1.0/directory/deletedItems/"+id+"/restore", nil).status)

	_, err = e.h.Backend.AuthenticateUser(upn, newPassword)
	require.NoError(t, err, "a restored user keeps its password")
}

func TestPasswordRestoreWithoutStoredHashFailsClosed(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	newPasswordUser(t, e, "legacy", nil)

	snap := e.h.Backend.Snapshot(t.Context())
	stripped := strings.ReplaceAll(string(snap), `"user:`, `"gone:`)

	fresh := newEnv(t)
	require.NoError(t, fresh.h.Backend.Restore(t.Context(), []byte(stripped)))

	_, err := fresh.h.Backend.AuthenticateUser("legacy@"+azuregraph.DefaultDomain, goodPassword)
	require.Equal(t, 50126, authCode(t, err))

	var ce *azuregraph.ClientAuthError

	require.ErrorAs(t, err, &ce)
	assert.Contains(t, ce.Description, "no stored password")
}
