package dashboard_test

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
	"github.com/blackbirdworks/gopherstack/services/azurecontainerregistry"
)

const acrBase = "/dashboard/api/v1/azure/acr/registries"

type acrEnv struct {
	h     *dashboard.DashboardHandler
	creds azurecontainerregistry.Credentials
}

func newACREnv(t *testing.T) *acrEnv {
	t.Helper()

	b := azurecontainerregistry.NewInMemoryBackend()
	b.SetDataDir(t.TempDir())
	b.SetContext(t.Context())

	acr := azurecontainerregistry.NewHandler(b, azurecontainerregistry.DefaultSettings())
	t.Cleanup(func() { acr.Shutdown(t.Context()) })

	srv := httptest.NewServer(acr)
	t.Cleanup(srv.Close)

	_, err := b.PutRegistry("devregistry", true)
	require.NoError(t, err)
	_, err = b.PutRegistry("plainregistry", false)
	require.NoError(t, err)

	creds, err := b.Credentials("devregistry")
	require.NoError(t, err)

	auth := base64.StdEncoding.EncodeToString([]byte(creds.Username + ":" + creds.Password1))
	c := ocitest.Client{
		T: t, Base: srv.URL, Host: "devregistry.azurecr.localhost",
		Prepare: func(r *http.Request) { r.Header.Set("Authorization", "Basic "+auth) },
	}
	c.PushImage("team/app", "v1")
	c.PushImage("team/app", "v2")

	return &acrEnv{
		h:     dashboard.NewHandler(dashboard.Config{AzureACROps: acr}),
		creds: creds,
	}
}

func TestAzureACRProxy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		wantBody string
		wantCode int
	}{
		{name: "registries", method: http.MethodGet, path: acrBase, wantCode: 200, wantBody: `"devregistry"`},
		{
			name: "repositories", method: http.MethodGet, path: acrBase + "/devregistry/repositories",
			wantCode: 200, wantBody: `"team/app"`,
		},
		{
			name: "tags", method: http.MethodGet, path: acrBase + "/devregistry/tags?repo=team%2Fapp",
			wantCode: 200, wantBody: `"name":"v1"`,
		},
		{
			name: "manifests", method: http.MethodGet, path: acrBase + "/devregistry/manifests?repo=team%2Fapp",
			wantCode: 200, wantBody: `"digest":"sha256:`,
		},
		{
			name: "unknown registry", method: http.MethodGet, path: acrBase + "/nope/repositories",
			wantCode: 404, wantBody: "NOT_FOUND",
		},
		{
			name: "unknown repository", method: http.MethodGet, path: acrBase + "/devregistry/tags?repo=ghost",
			wantCode: 404, wantBody: "NOT_FOUND",
		},
		{
			name: "traversal repo", method: http.MethodGet, path: acrBase + "/devregistry/tags?repo=..%2Fx",
			wantCode: 400, wantBody: "invalid repository name",
		},
		{
			name: "uppercase repo", method: http.MethodGet, path: acrBase + "/devregistry/tags?repo=Team",
			wantCode: 400, wantBody: "invalid repository name",
		},
		{
			name:     "bad tag",
			method:   http.MethodDelete,
			path:     acrBase + "/devregistry/tags?repo=team%2Fapp&tag=sha256:abc",
			wantCode: 400,
			wantBody: "invalid tag name",
		},
		{
			name:     "delete missing tag",
			method:   http.MethodDelete,
			path:     acrBase + "/devregistry/tags?repo=team%2Fapp&tag=zzz",
			wantCode: 404,
			wantBody: "NOT_FOUND",
		},
		{
			name: "no write to repositories", method: http.MethodPost, path: acrBase + "/devregistry/repositories",
			wantCode: 404, wantBody: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newACREnv(t)
			rec := doProxy(t, env.h, tt.method, tt.path, "")

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestAzureACRTagDelete(t *testing.T) {
	t.Parallel()

	env := newACREnv(t)

	rec := doProxy(t, env.h, http.MethodDelete, acrBase+"/devregistry/tags?repo=team%2Fapp&tag=v1", "")
	require.Equal(t, http.StatusNoContent, rec.Code)

	rec = doProxy(t, env.h, http.MethodGet, acrBase+"/devregistry/tags?repo=team%2Fapp", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), `"name":"v1"`)
	assert.Contains(t, rec.Body.String(), `"name":"v2"`)
}

func TestAzureACRCredentialsMasked(t *testing.T) {
	t.Parallel()

	env := newACREnv(t)

	rec := doProxy(t, env.h, http.MethodGet, acrBase+"/devregistry/credentials", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), env.creds.Password1)
	assert.NotContains(t, rec.Body.String(), env.creds.Password2)
	assert.Contains(t, rec.Body.String(), `"adminEnabled":true`)

	for _, path := range []string{"/registries", "/devregistry/repositories", "/devregistry/tags?repo=team%2Fapp"} {
		rec = doProxy(t, env.h, http.MethodGet, "/dashboard/api/v1/azure/acr"+path, "")
		assert.NotContains(t, rec.Body.String(), env.creds.Password1, path)
	}
}

func TestAzureACRReveal(t *testing.T) {
	t.Parallel()

	env := newACREnv(t)

	rec := doProxy(t, env.h, http.MethodPost, acrBase+"/devregistry/credentials/reveal", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	assert.Contains(t, rec.Body.String(), env.creds.Password1)

	rec = doProxy(t, env.h, http.MethodGet, acrBase+"/devregistry/credentials/reveal", "")
	assert.NotContains(t, rec.Body.String(), env.creds.Password1)

	rec = doProxy(t, env.h, http.MethodPost, acrBase+"/plainregistry/credentials/reveal", "")
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestAzureACRAbsent(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})
	rec := doProxy(t, h, http.MethodGet, acrBase, "")

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "UNAVAILABLE")
}
