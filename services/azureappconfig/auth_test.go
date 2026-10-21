package azureappconfig_test

import (
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azappconfig/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ac "github.com/blackbirdworks/gopherstack/services/azureappconfig"
)

func TestHMAC_SDKCredentials(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{validate: true, realTime: true})
	forged := ac.AccessKey{ID: e.keys[0].ID, Secret: base64.StdEncoding.EncodeToString([]byte("not-the-secret"))}
	unknown := ac.AccessKey{ID: "zzzz-l0-s0:unknown", Secret: e.keys[0].Secret}

	tests := []struct {
		name   string
		key    ac.AccessKey
		status int
	}{
		{name: "primary", key: e.keys[0], status: 0},
		{name: "secondary", key: e.keys[1], status: 0},
		{name: "wrong secret", key: forged, status: http.StatusUnauthorized},
		{name: "unknown credential", key: unknown, status: http.StatusUnauthorized},
		{name: "read-only key cannot write", key: e.keys[2], status: http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := e.clientWith(t, tt.key)
			_, err := c.SetSetting(t.Context(), "auth/"+strings.ReplaceAll(tt.name, " ", "-"), new("v"), nil)
			assert.Equal(t, tt.status, statusOf(err))
		})
	}
}

func TestHMAC_ReadOnlyKeyReads(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{validate: true, realTime: true})

	_, err := e.client(t).SetSetting(t.Context(), "auth/shared", new("v"), nil)
	require.NoError(t, err)

	got, err := e.clientWith(t, e.keys[3]).GetSetting(t.Context(), "auth/shared", nil)
	require.NoError(t, err)
	assert.Equal(t, "v", *got.Value)
}

func TestHMAC_RegeneratedKeyReplacesOld(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{validate: true, realTime: true})
	old := e.keys[1]

	fresh, err := e.backend.RegenerateKey(storeName, old.ID)
	require.NoError(t, err)
	assert.NotEqual(t, old.ID, fresh.ID)

	_, err = e.clientWith(t, old).GetSetting(t.Context(), "auth/shared", nil)
	assert.Equal(t, http.StatusUnauthorized, statusOf(err))

	_, err = e.clientWith(t, fresh).GetSetting(t.Context(), "auth/shared", nil)
	assert.NotEqual(t, http.StatusUnauthorized, statusOf(err))
}

func signedGet(t *testing.T, e *env, mutate func(*http.Request)) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, e.srv.URL+"/kv?"+av, nil)
	require.NoError(t, err)

	sum := sha256.Sum256(nil)
	req.Header.Set("X-Ms-Date", time.Now().UTC().Format(http.TimeFormat))
	req.Header.Set("X-Ms-Content-Sha256", base64.StdEncoding.EncodeToString(sum[:]))

	server := req.Clone(t.Context())
	server.RequestURI, server.Host = req.URL.RequestURI(), req.URL.Host
	signed := []string{"x-ms-date", "host", "x-ms-content-sha256"}
	sig, err := ac.SignRequest(server, signed, e.keys[0].Secret)
	require.NoError(t, err)

	req.Header.Set("Authorization", "HMAC-SHA256 Credential="+e.keys[0].ID+
		"&SignedHeaders="+strings.Join(signed, ";")+"&Signature="+sig)

	if mutate != nil {
		mutate(req)
	}

	resp, err := e.srv.Client().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	return resp
}

func TestHMAC_RawRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*http.Request)
		name   string
		status int
	}{
		{name: "documented ampersand form", status: http.StatusOK},
		{name: "tampered signature", status: http.StatusUnauthorized, mutate: func(r *http.Request) {
			r.Header.Set("Authorization", strings.TrimRight(r.Header.Get("Authorization"), "=")+"A")
		}},
		{name: "wrong content hash", status: http.StatusUnauthorized, mutate: func(r *http.Request) {
			r.Header.Set("X-Ms-Content-Sha256", "AAAA")
		}},
		{name: "stale date", status: http.StatusUnauthorized, mutate: func(r *http.Request) {
			r.Header.Set("X-Ms-Date", time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat))
		}},
		{name: "no authorization", status: http.StatusUnauthorized, mutate: func(r *http.Request) {
			r.Header.Del("Authorization")
		}},
		{name: "jwt shaped bearer", status: http.StatusOK, mutate: func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer aaa.bbb.ccc")
		}},
		{name: "opaque bearer", status: http.StatusUnauthorized, mutate: func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer opaque")
		}},
	}

	e := newEnv(t, envOpts{validate: true, realTime: true})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := signedGet(t, e, tt.mutate)
			assert.Equal(t, tt.status, resp.StatusCode)

			if tt.status == http.StatusUnauthorized {
				assert.Contains(t, resp.Header.Get("WWW-Authenticate"), "HMAC-SHA256")
			}
		})
	}
}

func TestHMAC_ValidationOffAcceptsAnything(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	forged := ac.AccessKey{ID: "nobody", Secret: e.keys[0].Secret}

	_, err := e.clientWith(t, forged).SetSetting(t.Context(), "free", new("v"), nil)
	require.NoError(t, err)

	got, err := e.clientWith(t, forged).GetSetting(t.Context(), "free", &azappconfig.GetSettingOptions{})
	require.NoError(t, err)
	assert.Equal(t, "v", *got.Value, "an unknown credential falls back to the default store")

	_, err = e.client(t).GetSetting(t.Context(), "free", nil)
	assert.Equal(t, http.StatusNotFound, statusOf(err), "the named store is separate from the default store")
}
