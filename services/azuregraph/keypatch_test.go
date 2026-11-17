package azuregraph_test

import (
	"maps"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const patchAudience = "https://login.test/t/oauth2/v2.0/token"

func keyList(t *testing.T, e *env, path string) []any {
	t.Helper()

	r := e.req(t, http.MethodGet, path+"?$select=keyCredentials", nil)
	require.Equal(t, http.StatusOK, r.status, r.raw)

	keys, _ := r.body["keyCredentials"].([]any)

	return keys
}

func (c testCert) authenticates(t *testing.T, e *env, appID string) bool {
	t.Helper()

	assertion := c.sign(t, certAssertionClaims(e.clock.now(), appID), nil)
	_, _, err := e.h.Backend.AuthenticateAssertion(appID, assertion, []string{patchAudience})

	return err == nil
}

func TestPatchKeyCredentials(t *testing.T) {
	t.Parallel()

	for _, holder := range []string{"applications", "servicePrincipals"} {
		t.Run(holder, func(t *testing.T) {
			t.Parallel()

			e, app, sp := assertionEnv(t)
			obj := app
			if holder == "servicePrincipals" {
				obj = sp
			}

			path := "/v1.0/" + holder + "/" + str(obj, "id")
			one, two := clockCert(t), clockCert(t)

			patch := func(keys any, extra ...map[string]any) resp {
				body := map[string]any{"keyCredentials": keys}
				for _, x := range extra {
					maps.Copy(body, x)
				}

				return e.req(t, http.MethodPatch, path, body)
			}

			require.Equal(t, http.StatusNoContent, patch([]any{one.keyBody()}).status)

			keys := keyList(t, e, path)
			require.Len(t, keys, 1)

			first, _ := keys[0].(map[string]any)
			assert.Nil(t, first["key"], "key material is never returned")
			assert.Equal(t, "2027-01-01T00:00:00Z", str(first, "endDateTime"))
			assert.True(t, one.authenticates(t, e, str(app, "appId")), "the PATCHed certificate authenticates")

			keep := map[string]any{"keyId": str(first, "keyId"), "displayName": "renamed"}
			require.Equal(t, http.StatusNoContent, patch([]any{keep, two.keyBody()}).status)

			keys = keyList(t, e, path)
			require.Len(t, keys, 2)
			assert.True(t, one.authenticates(t, e, str(app, "appId")), "a retained entry keeps its material")
			assert.True(t, two.authenticates(t, e, str(app, "appId")))

			require.Equal(t, http.StatusNoContent, patch([]any{two.keyBody()}).status)
			assert.False(t, one.authenticates(t, e, str(app, "appId")), "a dropped entry loses its material")
			assert.NotContains(t, string(e.h.Snapshot(t.Context())), one.b64, "dropped key material is not retained")

			fresh := newEnv(t)
			require.NoError(t, fresh.h.Restore(t.Context(), e.h.Snapshot(t.Context())))
			fresh.clock.advance(e.clock.now().Sub(fresh.clock.now()))
			assert.True(t, two.authenticates(t, fresh, str(app, "appId")), "key material survives a snapshot")
			assert.Len(t, keyList(t, fresh, path), 1)

			require.Equal(t, http.StatusNoContent, patch([]any{}).status)
			assert.Empty(t, keyList(t, e, path))
			assert.False(t, two.authenticates(t, e, str(app, "appId")))
		})
	}
}

func TestPatchKeyCredentialsRejects(t *testing.T) {
	t.Parallel()

	cert := clockCert(t)
	good := cert.keyBody()

	with := func(k string, v any) map[string]any {
		b := cert.keyBody()
		if v == nil {
			delete(b, k)
		} else {
			b[k] = v
		}

		return b
	}

	tests := []struct {
		keys any
		name string
	}{
		{nil, "null"},
		{"x", "not a list"},
		{[]any{with("type", "x")}, "bad type"},
		{[]any{with("usage", "x")}, "bad usage"},
		{[]any{with("key", "%%")}, "bad base64"},
		{[]any{with("key", "AAEC")}, "not a certificate"},
		{[]any{with("key", nil)}, "key missing for unknown entry"},
		{[]any{map[string]any{"keyId": newID0()}}, "unknown keyId without key"},
		{[]any{with("keyId", "nope")}, "malformed keyId"},
		{[]any{with("keyId", newID0()), with("keyId", newID0())}, "duplicate keyId"},
		{func() any {
			many := make([]any, 101)
			for i := range many {
				many[i] = good
			}

			return many
		}(), "over the cap"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, app, _ := assertionEnv(t)
			path := "/v1.0/applications/" + str(app, "id")
			require.Equal(t, http.StatusNoContent,
				e.req(t, http.MethodPatch, path, map[string]any{"keyCredentials": []any{good}}).status)

			r := e.req(t, http.MethodPatch, path, map[string]any{"keyCredentials": tt.keys, "displayName": "changed"})
			assertErr(t, r, http.StatusBadRequest, "Request_BadRequest")

			assert.Len(t, keyList(t, e, path), 1, "a rejected PATCH leaves the credentials alone")
			assert.True(t, cert.authenticates(t, e, str(app, "appId")))
			assert.Equal(t, "Assert", str(e.req(t, http.MethodGet, path, nil).body, "displayName"))
		})
	}
}

func TestPatchKeyCredentialsConcurrent(t *testing.T) {
	t.Parallel()

	e, app, _ := assertionEnv(t)
	path := "/v1.0/applications/" + str(app, "id")
	certs := []testCert{clockCert(t), clockCert(t), clockCert(t), clockCert(t)}

	var wg sync.WaitGroup

	for _, c := range certs {
		wg.Go(func() {
			e.req(t, http.MethodPatch, path, map[string]any{"keyCredentials": []any{c.keyBody()}})
		})
	}

	wg.Wait()
	require.Len(t, keyList(t, e, path), 1)

	live := 0

	for _, c := range certs {
		if c.authenticates(t, e, str(app, "appId")) {
			live++
		}
	}

	assert.Equal(t, 1, live, "exactly the winning PATCH's certificate stays usable")
}

func TestPatchKeyCredentialsCannotTakeAnotherHoldersKeyID(t *testing.T) {
	t.Parallel()

	e, victim, _ := assertionEnv(t)
	attacker := newApp(t, e, "Attacker")
	good, evil := clockCert(t), clockCert(t)

	victimPath := "/v1.0/applications/" + str(victim, "id")
	attackerPath := "/v1.0/applications/" + str(attacker, "id")
	require.Equal(t, http.StatusNoContent,
		e.req(t, http.MethodPatch, victimPath, map[string]any{"keyCredentials": []any{good.keyBody()}}).status)

	keyID := str(keyList(t, e, victimPath)[0].(map[string]any), "keyId") //nolint:forcetypeassert // test data

	stolen := evil.keyBody()
	stolen["keyId"] = keyID

	r := e.req(t, http.MethodPatch, attackerPath, map[string]any{"keyCredentials": []any{stolen}})
	assertErr(t, r, http.StatusBadRequest, "Request_BadRequest")

	assert.True(t, good.authenticates(t, e, str(victim, "appId")), "the victim's certificate is untouched")
	assert.False(t, evil.authenticates(t, e, str(victim, "appId")), "the attacker's certificate does not authenticate")
	assert.Empty(t, keyList(t, e, attackerPath))
}
