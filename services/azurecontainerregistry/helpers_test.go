package azurecontainerregistry_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
	acr "github.com/blackbirdworks/gopherstack/services/azurecontainerregistry"
)

type env struct {
	h   *acr.Handler
	srv *httptest.Server
}

func newEnv(t *testing.T, dir string) *env {
	t.Helper()

	b := acr.NewInMemoryBackend()
	b.SetDataDir(dir)
	b.SetContext(t.Context())

	h := acr.NewHandler(b, acr.DefaultSettings())
	t.Cleanup(func() { h.Shutdown(t.Context()) })

	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	return &env{h: h, srv: srv}
}

func (e *env) registry(t *testing.T, name string, admin bool) acr.Credentials {
	t.Helper()

	_, err := e.h.Backend.PutRegistry(name, admin)
	require.NoError(t, err)

	if !admin {
		return acr.Credentials{Username: name}
	}

	creds, err := e.h.Backend.Credentials(name)
	require.NoError(t, err)

	return creds
}

func host(name string) string { return name + ".azurecr.localhost" }

func basic(user, pass string) func(*http.Request) {
	value := "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))

	return func(r *http.Request) { r.Header.Set("Authorization", value) }
}

func bearer(tok string) func(*http.Request) {
	return func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+tok) }
}

func (e *env) client(t *testing.T, reg string, prep func(*http.Request)) ocitest.Client {
	t.Helper()

	return ocitest.Client{T: t, Base: e.srv.URL, Host: host(reg), Prepare: prep}
}

func decode(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))

	return out
}
