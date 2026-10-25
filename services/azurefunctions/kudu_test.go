package azurefunctions_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

func TestKudu_Auth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hdr      func(valid map[string]string) map[string]string
		name     string
		wantCode int
	}{
		{func(map[string]string) map[string]string { return nil }, "no credentials", http.StatusUnauthorized},
		{
			func(map[string]string) map[string]string { return map[string]string{"basic": "$app:nope"} },
			"wrong password",
			http.StatusUnauthorized,
		},
		{
			func(map[string]string) map[string]string { return map[string]string{"basic": "$other:x"} },
			"wrong user",
			http.StatusUnauthorized,
		},
		{func(v map[string]string) map[string]string { return v }, "publishing credentials", http.StatusOK},
		{
			func(map[string]string) map[string]string { return map[string]string{"Authorization": "Bearer abc"} },
			"bearer token",
			http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			e.backend.PutApp("app", azurefunctions.AppConfig{})

			resp, _ := e.do(
				t,
				http.MethodGet,
				"app.scm."+testDomain,
				"/api/deployments",
				nil,
				tt.hdr(e.kuduAuth(t, "app")),
			)
			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if tt.wantCode == http.StatusUnauthorized {
				assert.Contains(t, resp.Header.Get("WWW-Authenticate"), "Basic")
			}
		})
	}
}

func TestKudu_ZipDeploy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     func(t *testing.T) []byte
		name     string
		method   string
		path     string
		wantCode int
	}{
		{helloZip, "zipdeploy", http.MethodPost, "/api/zipdeploy", http.StatusOK},
		{helloZip, "zipdeploy put", http.MethodPut, "/api/zipdeploy", http.StatusOK},
		{helloZip, "zipdeploy async", http.MethodPost, "/api/zipdeploy?isAsync=true", http.StatusAccepted},
		{helloZip, "onedeploy zip", http.MethodPost, "/api/publish?type=zip", http.StatusOK},
		{helloZip, "onedeploy other type", http.MethodPost, "/api/publish?type=static", http.StatusBadRequest},
		{helloZip, "onedeploy no type", http.MethodPost, "/api/publish", http.StatusBadRequest},
		{
			func(*testing.T) []byte { return []byte("plain text") },
			"not a zip",
			http.MethodPost,
			"/api/zipdeploy",
			http.StatusBadRequest,
		},
		{
			func(*testing.T) []byte { return nil },
			"get is rejected",
			http.MethodGet,
			"/api/zipdeploy",
			http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			e.backend.PutApp("app", azurefunctions.AppConfig{})

			resp, _ := e.do(t, tt.method, "app.scm."+testDomain, tt.path, tt.body(t), e.kuduAuth(t, "app"))
			require.Equal(t, tt.wantCode, resp.StatusCode)

			deployed := tt.wantCode == http.StatusOK || tt.wantCode == http.StatusAccepted
			assert.Equal(t, deployed, e.backend.Package("app") != nil)

			if deployed {
				assert.Contains(t, resp.Header.Get("Location"), "/api/deployments/latest")
			}
		})
	}
}

func TestKudu_DeploymentsAndDownload(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	e.backend.PutApp("app", azurefunctions.AppConfig{})

	auth := e.kuduAuth(t, "app")
	scm := "app.scm." + testDomain

	_, body := e.do(t, http.MethodGet, scm, "/api/deployments", nil, auth)
	assert.JSONEq(t, "[]", body)

	resp, _ := e.do(t, http.MethodGet, scm, "/api/deployments/latest", nil, auth)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	pkg := helloZip(t)
	_, body = e.do(t, http.MethodPost, scm, "/api/zipdeploy", pkg, auth)

	var first map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &first))
	assert.InDelta(t, 4, first["status"], 0)
	assert.Equal(t, true, first["complete"])
	assert.Equal(t, "ZipDeploy", first["deployer"])

	e.do(t, http.MethodPost, scm, "/api/publish?type=zip", pkg, auth)

	_, body = e.do(t, http.MethodGet, scm, "/api/deployments", nil, auth)

	var list []map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &list))
	require.Len(t, list, 2)
	assert.Equal(t, "OneDeploy", list[0]["deployer"], "newest first")
	assert.Equal(t, true, list[0]["active"])
	assert.Equal(t, false, list[1]["active"])

	_, body = e.do(t, http.MethodGet, scm, "/api/deployments/latest", nil, auth)
	assert.Contains(t, body, list[0]["id"])

	_, body = e.do(t, http.MethodGet, scm, "/api/deployments/"+first["id"].(string), nil, auth)
	assert.Contains(t, body, first["id"])

	resp, body = e.do(t, http.MethodGet, scm, "/api/zip/site/wwwroot/", nil, auth)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/zip", resp.Header.Get("Content-Type"))
	assert.Equal(t, string(pkg), body)
}

func TestKudu_ReservedPathsOnPlainHost(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})

	// A bare host is not the app's own site, so Kudu paths reach Kudu (and need credentials).
	resp, _ := e.do(t, http.MethodPost, "127.0.0.1", "/api/zipdeploy", helloZip(t), nil)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	resp, _ = e.do(t, http.MethodPost, "127.0.0.1", "/api/zipdeploy", helloZip(t), e.kuduAuth(t, "app"))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// On the app's own hostname the same path is just a function route.
	_, _ = e.backend.Deploy("app", helloZip(t), "t", "m")
	resp, body := e.do(t, http.MethodGet, "app."+testDomain, "/api/zipdeploy", nil, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "echo /api/zipdeploy", body)
}

func TestKudu_DeployUnknownApp(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	_, err := e.backend.Deploy("ghost", helloZip(t), "t", "m")
	require.ErrorIs(t, err, azurefunctions.ErrAppNotFound)

	_, err = e.backend.Deploy("ghost", []byte("nope"), "t", "m")
	require.ErrorIs(t, err, azurefunctions.ErrInvalidPackage)
}
