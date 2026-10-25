package azurefunctions_test

import (
	"maps"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

func TestInvoke_HostRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		host     string
		path     string
		wantBody string
		wantCode int
	}{
		{"app label", "alpha." + testDomain + ":10012", "/api/hello", "echo /api/hello", http.StatusOK},
		{"other app", "beta." + testDomain, "/api/x", "echo /api/x", http.StatusOK},
		{"unknown app on domain", "gamma." + testDomain, "/api/x", "no function app", http.StatusNotFound},
		{"first label match", "alpha.example.test", "/api/y", "echo /api/y", http.StatusOK},
		{"ip host with two apps", "127.0.0.1", "/api/x", "no function app", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)

			for _, app := range []string{"alpha", "beta"} {
				e.backend.PutApp(app, azurefunctions.AppConfig{Kind: "functionapp,linux", LinuxFx: "NODE|20"})
				_, err := e.backend.Deploy(app, helloZip(t), "test", "m")
				require.NoError(t, err)
			}

			resp, body := e.do(t, http.MethodGet, tt.host, tt.path, nil, nil)
			assert.Equal(t, tt.wantCode, resp.StatusCode)
			assert.Contains(t, body, tt.wantBody)
		})
	}
}

func TestInvoke_SingleAppFallback(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.backend.PutApp("solo", azurefunctions.AppConfig{LinuxFx: "NODE|20"})
	_, err := e.backend.Deploy("solo", helloZip(t), "test", "m")
	require.NoError(t, err)

	resp, body := e.do(t, http.MethodGet, "127.0.0.1:10012", "/api/greet", nil, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "echo /api/greet", body)
}

func TestInvoke_ContainerSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		linuxFx   string
		settings  map[string]string
		wantImage string
		wantWork  string
	}{
		{"node default", "NODE|20", nil, "mcr.microsoft.com/azure-functions/node:4-node20", "node"},
		{"node 22", "Node|22", nil, "mcr.microsoft.com/azure-functions/node:4-node22", "node"},
		{"python", "PYTHON|3.12", nil, "mcr.microsoft.com/azure-functions/python:4-python3.12", "python"},
		{
			"dotnet isolated",
			"DOTNET-ISOLATED|8.0",
			nil,
			"mcr.microsoft.com/azure-functions/dotnet-isolated:4-dotnet-isolated8.0",
			"dotnet-isolated",
		},
		{"java", "JAVA|21", nil, "mcr.microsoft.com/azure-functions/java:4-java21", "java"},
		{
			"powershell",
			"POWERSHELL|7.4",
			nil,
			"mcr.microsoft.com/azure-functions/powershell:4-powershell7.4",
			"powershell",
		},
		{
			"custom handler",
			"",
			map[string]string{"FUNCTIONS_WORKER_RUNTIME": "custom"},
			"mcr.microsoft.com/azure-functions/base:4",
			"custom",
		},
		{
			"worker from setting",
			"",
			map[string]string{"FUNCTIONS_WORKER_RUNTIME": "node", "WEBSITE_NODE_DEFAULT_VERSION": "~18"},
			"mcr.microsoft.com/azure-functions/node:4-node18",
			"node",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			e.backend.PutApp("spec", azurefunctions.AppConfig{LinuxFx: tt.linuxFx})
			require.NoError(t, e.backend.SetSettings("spec", map[string]string{"MY_SETTING": "v1"}))

			if tt.settings != nil {
				s := map[string]string{"MY_SETTING": "v1"}
				maps.Copy(s, tt.settings)

				require.NoError(t, e.backend.SetSettings("spec", s))
			}

			_, err := e.backend.Deploy("spec", helloZip(t), "test", "m")
			require.NoError(t, err)

			resp, _ := e.do(t, http.MethodGet, "spec."+testDomain, "/api/greet", nil, nil)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			specs := e.rt.specs()
			require.Len(t, specs, 1)

			env := envOf(specs[0])
			assert.Equal(t, tt.wantImage, specs[0].Image)
			assert.Equal(t, tt.wantWork, env["FUNCTIONS_WORKER_RUNTIME"])
			assert.Equal(t, "v1", env["MY_SETTING"])
			assert.Equal(t, "files", env["AzureWebJobsSecretStorageType"])
			assert.Equal(t, "/home/site/wwwroot", env["AzureWebJobsScriptRoot"])
			assert.Equal(t, "spec", env["WEBSITE_SITE_NAME"])
			assert.True(t, strings.HasPrefix(env["WEBSITE_HOSTNAME"], "spec."+testDomain))
			assert.NotEmpty(t, mountSource(specs[0], "/home/site/wwwroot"))
			assert.NotEmpty(t, mountSource(specs[0], "/azure-functions-host/Secrets"))
		})
	}
}

func TestInvoke_Unservable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, e *env)
		name     string
		path     string
		wantBody string
		wantCode int
	}{
		{
			name:     "undeployed root shows welcome page",
			setup:    func(*testing.T, *env) {},
			path:     "/",
			wantCode: http.StatusOK,
			wantBody: "up and running",
		},
		{
			name:     "undeployed api is not found",
			setup:    func(*testing.T, *env) {},
			path:     "/api/hello",
			wantCode: http.StatusNotFound,
		},
		{
			name: "stopped app is forbidden",
			setup: func(t *testing.T, e *env) {
				t.Helper()
				_, err := e.backend.Deploy("app", helloZip(t), "test", "m")
				require.NoError(t, err)
				require.NoError(t, e.backend.SetStopped("app", true))
			},
			path:     "/api/greet",
			wantCode: http.StatusForbidden,
			wantBody: "stopped",
		},
		{
			name: "unsupported runtime",
			setup: func(t *testing.T, e *env) {
				t.Helper()
				require.NoError(t, e.backend.SetSettings("app", map[string]string{"FUNCTIONS_WORKER_RUNTIME": "cobol"}))
				_, err := e.backend.Deploy("app", helloZip(t), "test", "m")
				require.NoError(t, err)
			},
			path:     "/api/greet",
			wantCode: http.StatusServiceUnavailable,
			wantBody: "FUNCTIONS_WORKER_RUNTIME",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})
			tt.setup(t, e)

			resp, body := e.do(t, http.MethodGet, "app."+testDomain, tt.path, nil, nil)
			assert.Equal(t, tt.wantCode, resp.StatusCode)
			assert.Contains(t, body, tt.wantBody)
			assert.Zero(t, e.rt.running())
		})
	}
}

func TestInvoke_NoContainerRuntime(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})
	_, err := e.backend.Deploy("app", helloZip(t), "test", "m")
	require.NoError(t, err)

	resp, body := e.do(t, http.MethodGet, "app."+testDomain, "/api/greet", nil, nil)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Contains(t, body, "container runtime is unavailable")
}

func TestInvoke_RedeployAndSettingsRestartHost(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})
	_, err := e.backend.Deploy("app", helloZip(t), "test", "m")
	require.NoError(t, err)

	for range 2 {
		resp, _ := e.do(t, http.MethodGet, "app."+testDomain, "/api/greet", nil, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	assert.Len(t, e.rt.specs(), 1, "host is reused across requests")

	_, err = e.backend.Deploy("app", helloZip(t), "test", "again")
	require.NoError(t, err)
	assert.Zero(t, e.rt.running(), "redeploy discards the running host")

	resp, _ := e.do(t, http.MethodGet, "app."+testDomain, "/api/greet", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Len(t, e.rt.specs(), 2)

	require.NoError(t, e.backend.SetSettings("app", map[string]string{"A": "b"}))
	assert.Zero(t, e.rt.running(), "settings change discards the running host")

	resp, _ = e.do(t, http.MethodGet, "app."+testDomain, "/api/greet", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "b", envOf(e.rt.specs()[2])["A"])

	e.backend.DeleteApp("app")
	assert.Zero(t, e.rt.running())
}

func TestInvoke_ForwardsRequestToHost(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})
	_, err := e.backend.Deploy("app", helloZip(t), "test", "m")
	require.NoError(t, err)

	resp, _ := e.do(t, http.MethodPost, "app."+testDomain, "/api/greet?code=abc&name=x", []byte("payload"),
		map[string]string{"x-functions-key": "k1"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	reqs := e.rt.requests()
	require.Len(t, reqs, 1)
	assert.Equal(t, "code=abc&name=x", reqs[0].Query)
	assert.Equal(t, "payload", reqs[0].Body)
	assert.Equal(t, "k1", reqs[0].Headers.Get("X-Functions-Key"))
	assert.True(t, strings.HasPrefix(reqs[0].Host, "app."+testDomain))
}
