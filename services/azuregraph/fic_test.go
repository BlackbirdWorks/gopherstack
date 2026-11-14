package azuregraph_test

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuregraph"
)

const (
	ficIssuer   = "https://token.actions.githubusercontent.com"
	ficSubject  = "repo:org/repo:ref:refs/heads/main"
	ficAudience = "api://AzureADTokenExchange"
)

func ficBody(name string) map[string]any {
	return map[string]any{
		"name": name, "issuer": ficIssuer, "subject": ficSubject + name, "audiences": []string{ficAudience},
		"description": "ci",
	}
}

func TestFederatedIdentityCredentialsCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	app := newApp(t, e, "Fed")
	base := "/v1.0/applications/" + str(app, "id") + "/federatedIdentityCredentials"

	created := e.mustCreate(t, base, ficBody("main"))
	assert.Equal(t, "main", str(created, "name"))
	assert.NotEmpty(t, str(created, "id"))
	assert.Contains(t, str(created, "@odata.context"), "federatedIdentityCredentials")

	list := e.req(t, http.MethodGet, base, nil)
	require.Equal(t, http.StatusOK, list.status, list.raw)
	assert.Equal(t, []string{"main"}, values2(list, "name"))

	for _, key := range []string{str(created, "id"), "main"} {
		got := e.req(t, http.MethodGet, base+"/"+key, nil)
		require.Equal(t, http.StatusOK, got.status, got.raw)
		assert.Equal(t, ficIssuer, str(got.body, "issuer"))
	}

	alt := e.req(
		t,
		http.MethodGet,
		"/v1.0/applications(appId='"+str(app, "appId")+"')/federatedIdentityCredentials",
		nil,
	)
	require.Equal(t, http.StatusOK, alt.status, alt.raw)

	require.Equal(
		t,
		http.StatusNoContent,
		e.req(
			t,
			http.MethodPatch,
			base+"/main",
			map[string]any{"subject": "repo:org/other", "description": "x"},
		).status,
	)
	assert.Equal(t, "repo:org/other", str(e.req(t, http.MethodGet, base+"/main", nil).body, "subject"))

	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, base+"/main", nil).status)
	assertErr(t, e.req(t, http.MethodGet, base+"/main", nil), http.StatusNotFound, "Request_ResourceNotFound")

	assertErr(t, e.req(t, http.MethodGet, "/v1.0/servicePrincipals/"+str(
		e.mustCreate(t, "/v1.0/servicePrincipals", map[string]any{"appId": str(app, "appId")}), "id")+
		"/federatedIdentityCredentials", nil), http.StatusBadRequest, "BadRequest")
}

func TestFederatedIdentityCredentialValidation(t *testing.T) {
	t.Parallel()

	with := func(k string, v any) map[string]any {
		m := ficBody("base")
		if v == nil {
			delete(m, k)
		} else {
			m[k] = v
		}

		return m
	}

	tests := []struct {
		body   map[string]any
		name   string
		code   string
		status int
	}{
		{ficBody("ok"), "valid", "", http.StatusCreated},
		{with("name", nil), "missing name", "Request_BadRequest", http.StatusBadRequest},
		{with("name", "-bad"), "bad name start", "Request_BadRequest", http.StatusBadRequest},
		{with("name", strings.Repeat("a", 121)), "name too long", "Request_BadRequest", http.StatusBadRequest},
		{with("issuer", nil), "missing issuer", "Request_BadRequest", http.StatusBadRequest},
		{with("subject", strings.Repeat("s", 601)), "subject too long", "Request_BadRequest", http.StatusBadRequest},
		{with("audiences", []string{"a", "b"}), "two audiences", "Request_BadRequest", http.StatusBadRequest},
		{with("audiences", nil), "no audiences", "Request_BadRequest", http.StatusBadRequest},
		{with("bogus", "x"), "unknown property", "Request_BadRequest", http.StatusBadRequest},
		{with("id", newID0()), "id is read-only", "Request_BadRequest", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			app := newApp(t, e, "Val")
			r := e.req(
				t,
				http.MethodPost,
				"/v1.0/applications/"+str(app, "id")+"/federatedIdentityCredentials",
				tt.body,
			)
			require.Equal(t, tt.status, r.status, r.raw)

			if tt.code != "" {
				assertErr(t, r, tt.status, tt.code)
			}
		})
	}
}

func TestFederatedIdentityCredentialLimitsAndUniqueness(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	app := newApp(t, e, "Limits")
	base := "/v1.0/applications/" + str(app, "id") + "/federatedIdentityCredentials"

	first := e.mustCreate(t, base, ficBody("first"))

	dupName := ficBody("first")
	dupName["subject"] = "different"
	assertErr(
		t,
		e.req(t, http.MethodPost, base, dupName),
		http.StatusConflict,
		"Request_MultipleObjectsWithSameKeyValue",
	)

	dupPair := ficBody("second")
	dupPair["subject"] = str(first, "subject")
	assertErr(
		t,
		e.req(t, http.MethodPost, base, dupPair),
		http.StatusConflict,
		"Request_MultipleObjectsWithSameKeyValue",
	)

	assertErr(t, e.req(t, http.MethodPatch, base+"/first", map[string]any{"name": "renamed"}),
		http.StatusBadRequest, "Request_BadRequest")

	for i := 1; i < 20; i++ {
		e.mustCreate(t, base, ficBody(fmt.Sprintf("c%02d", i)))
	}

	assertErr(t, e.req(t, http.MethodPost, base, ficBody("overflow")), http.StatusBadRequest, "Request_BadRequest")

	other := newApp(t, e, "Other")
	e.mustCreate(t, "/v1.0/applications/"+str(other, "id")+"/federatedIdentityCredentials", ficBody("first"))
}

func federatedAssertion(t *testing.T, key *rsa.PrivateKey, kid string, mutate func(jwt.MapClaims)) string {
	t.Helper()

	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	claims := jwt.MapClaims{
		"iss": ficIssuer, "sub": ficSubject + "main", "aud": ficAudience,
		"nbf": now.Unix(), "exp": now.Add(10 * time.Minute).Unix(),
	}

	if mutate != nil {
		mutate(claims)
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = kid

	s, err := tok.SignedString(key)
	require.NoError(t, err)

	return s
}

func TestAuthenticateAssertionFederated(t *testing.T) {
	t.Parallel()

	trusted, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	impostor, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tests := []struct {
		key     *rsa.PrivateKey
		mutate  func(jwt.MapClaims)
		name    string
		kid     string
		advance time.Duration
		code    int
	}{
		{trusted, nil, "matching claims signed by a known issuer key", "emu", 0, 0},
		{impostor, nil, "known issuer kid with a foreign signature", "emu", 0, 700027},
		{impostor, nil, "external issuer is matched by claims only", "ext", 0, 0},
		{trusted, func(c jwt.MapClaims) { c["iss"] = "https://evil.test" }, "issuer mismatch", "emu", 0, 70021},
		{trusted, func(c jwt.MapClaims) { c["sub"] = "repo:org/other" }, "subject mismatch", "emu", 0, 70021},
		{trusted, func(c jwt.MapClaims) { c["aud"] = "sts.amazonaws.com" }, "audience mismatch", "emu", 0, 70021},
		{trusted, func(c jwt.MapClaims) { c["aud"] = []string{"x", ficAudience} }, "audience list", "emu", 0, 0},
		{trusted, nil, "expired", "emu", time.Hour, 700024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, app, sp := assertionEnv(t)
			e.h.Backend.SetIssuerKeys(func(kid string) (crypto.PublicKey, bool) {
				return &trusted.PublicKey, kid == "emu"
			})
			require.Equal(t, http.StatusCreated, e.req(t, http.MethodPost,
				"/v1.0/applications/"+str(app, "id")+"/federatedIdentityCredentials", ficBody("main")).status)

			e.clock.advance(tt.advance)

			oid, known, aerr := e.h.Backend.AuthenticateAssertion(str(app, "appId"),
				federatedAssertion(t, tt.key, tt.kid, tt.mutate), nil)
			assert.True(t, known)

			if tt.code == 0 {
				require.NoError(t, aerr)
				assert.Equal(t, str(sp, "id"), oid)

				return
			}

			var ce *azuregraph.ClientAuthError

			require.ErrorAs(t, aerr, &ce)
			assert.Equal(t, tt.code, ce.Code)
		})
	}

	t.Run("deleted credential no longer matches", func(t *testing.T) {
		t.Parallel()

		e, app, _ := assertionEnv(t)
		base := "/v1.0/applications/" + str(app, "id") + "/federatedIdentityCredentials"
		e.mustCreate(t, base, ficBody("main"))
		require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, base+"/main", nil).status)

		_, _, authErr := e.h.Backend.AuthenticateAssertion(
			str(app, "appId"),
			federatedAssertion(t, trusted, "ext", nil),
			nil,
		)

		var ce *azuregraph.ClientAuthError

		require.ErrorAs(t, authErr, &ce)
		assert.Equal(t, 70021, ce.Code)
	})

	t.Run("credentials survive soft delete and restore", func(t *testing.T) {
		t.Parallel()

		e, app, sp := assertionEnv(t)
		e.mustCreate(t, "/v1.0/applications/"+str(app, "id")+"/federatedIdentityCredentials", ficBody("main"))
		require.Equal(
			t,
			http.StatusNoContent,
			e.req(t, http.MethodDelete, "/v1.0/applications/"+str(app, "id"), nil).status,
		)

		for _, id := range []string{str(app, "id"), str(sp, "id")} {
			require.Equal(
				t,
				http.StatusOK,
				e.req(t, http.MethodPost, "/v1.0/directory/deletedItems/"+id+"/restore", nil).status,
			)
		}

		oid, _, authErr := e.h.Backend.AuthenticateAssertion(str(app, "appId"),
			federatedAssertion(t, trusted, "ext", nil), nil)
		require.NoError(t, authErr)
		assert.Equal(t, str(sp, "id"), oid)
	})
}
