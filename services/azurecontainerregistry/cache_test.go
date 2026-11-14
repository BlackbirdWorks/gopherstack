package azurecontainerregistry_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	acr "github.com/blackbirdworks/gopherstack/services/azurecontainerregistry"
)

const upstreamHost = "upstreamreg.azurecr.localhost:10011"

func newCacheEnv(t *testing.T, rule acr.CacheRule) (*env, acr.Credentials) {
	t.Helper()

	e := newEnv(t, "")
	up := e.registry(t, "upstreamreg", true)
	cache := e.registry(t, "cachereg", true)

	upClient := e.client(t, "upstreamreg", basic(up.Username, up.Password1))
	upClient.PushImage("ext/app", "v1")
	upClient.PushImage("ext/tools/cli", "v2")

	e.h.Backend.SetSecretResolver(func(uri string) (string, error) {
		switch uri {
		case "https://kv.vault.azure.net/secrets/user":
			return up.Username, nil
		case "https://kv.vault.azure.net/secrets/pass":
			return up.Password1, nil
		}

		return "", errors.New("no such secret")
	})

	rule.Name = "rule1"
	require.NoError(t, e.h.Backend.SetCacheRule("cachereg", rule))

	return e, cache
}

func goodCreds() *acr.CacheCredentials {
	return &acr.CacheCredentials{
		LoginServer:    upstreamHost,
		UsernameSecret: "https://kv.vault.azure.net/secrets/user",
		PasswordSecret: "https://kv.vault.azure.net/secrets/pass",
	}
}

func TestCacheRulePullThrough(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rule acr.CacheRule
		repo string
		ref  string
		want int
	}{
		{name: "exact rule", rule: acr.CacheRule{Source: upstreamHost + "/ext/app", Target: "cached/app",
			Credentials: goodCreds()}, repo: "cached/app", ref: "v1", want: http.StatusOK},
		{name: "wildcard rule", rule: acr.CacheRule{Source: upstreamHost + "/ext/*", Target: "mirror/*",
			Credentials: goodCreds()}, repo: "mirror/tools/cli", ref: "v2", want: http.StatusOK},
		{name: "missing tag upstream", rule: acr.CacheRule{Source: upstreamHost + "/ext/app", Target: "cached/app",
			Credentials: goodCreds()}, repo: "cached/app", ref: "nope", want: http.StatusNotFound},
		{name: "uncovered repository", rule: acr.CacheRule{Source: upstreamHost + "/ext/app", Target: "cached/app",
			Credentials: goodCreds()}, repo: "other/app", ref: "v1", want: http.StatusNotFound},
		{name: "no credentials upstream refuses", rule: acr.CacheRule{Source: upstreamHost + "/ext/app",
			Target: "cached/app"}, repo: "cached/app", ref: "v1", want: http.StatusNotFound},
		{name: "unreadable secret", rule: acr.CacheRule{Source: upstreamHost + "/ext/app", Target: "cached/app",
			Credentials: &acr.CacheCredentials{
				LoginServer: upstreamHost, UsernameSecret: "https://kv.vault.azure.net/secrets/missing",
				PasswordSecret: "https://kv.vault.azure.net/secrets/pass",
			}}, repo: "cached/app", ref: "v1", want: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, cache := newCacheEnv(t, tt.rule)
			assert.Equal(t, tt.want, pullStatus(t, e, cache, tt.repo, tt.ref))
		})
	}
}

func TestCacheRuleKeepsPulledImage(t *testing.T) {
	t.Parallel()

	e, cache := newCacheEnv(t, acr.CacheRule{
		Source: upstreamHost + "/ext/app", Target: "cached/app", Credentials: goodCreds(),
	})

	require.Equal(t, http.StatusOK, pullStatus(t, e, cache, "cached/app", "v1"))

	require.NoError(t, e.h.Backend.DeleteRegistry("upstreamreg"))
	assert.Equal(t, http.StatusOK, pullStatus(t, e, cache, "cached/app", "v1"))

	tags := e.client(t, "cachereg", basic(cache.Username, cache.Password1)).
		Do(http.MethodGet, "/acr/v1/cached/app/_tags", nil, nil)
	assert.Contains(t, string(tags.Body), `"name":"v1"`)

	require.NoError(t, e.h.Backend.DeleteCacheRule("cachereg", "rule1"))
	require.ErrorIs(t, e.h.Backend.DeleteCacheRule("cachereg", "rule1"), acr.ErrCacheRuleNotFound)
}

func TestCacheRulePullByDigest(t *testing.T) {
	t.Parallel()

	e, cache := newCacheEnv(t, acr.CacheRule{
		Source: upstreamHost + "/ext/app", Target: "cached/app", Credentials: goodCreds(),
	})

	up, err := e.h.Backend.Credentials("upstreamreg")
	require.NoError(t, err)

	digest := e.client(t, "upstreamreg", basic(up.Username, up.Password1)).
		Do(http.MethodHead, "/v2/ext/app/manifests/v1", nil, nil).Header.Get("Docker-Content-Digest")
	require.NotEmpty(t, digest)

	assert.Equal(t, http.StatusOK, pullStatus(t, e, cache, "cached/app", digest))
}
