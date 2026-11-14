package azurefunctions_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

const webKind = "app,linux"

func containerPortOf(t *testing.T, spec container.Spec) string {
	t.Helper()

	_, _, cport, err := container.ParsePortSpec(spec.Ports[0])
	require.NoError(t, err)

	return cport
}

func TestWebApp_ContainerSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		files     map[string]string
		settings  map[string]string
		name      string
		linuxFx   string
		cmdLine   string
		wantImage string
		wantCmd   string
		wantPort  string
		wantMount string
	}{
		{
			name:      "node npm start",
			linuxFx:   "NODE|20-lts",
			files:     map[string]string{"package.json": `{"scripts":{"start":"x"}}`},
			wantImage: "node:20-slim",
			wantCmd:   "npm start",
			wantPort:  "8080",
			wantMount: "/home/site/wwwroot",
		},
		{
			name: "node entry file", linuxFx: "NODE|22-lts", files: map[string]string{"server.js": "1"},
			wantImage: "node:22-slim", wantCmd: "node server.js", wantPort: "8080", wantMount: "/home/site/wwwroot",
		},
		{
			name:      "node command line",
			linuxFx:   "NODE|20-lts",
			cmdLine:   "node custom.js --flag",
			files:     map[string]string{"custom.js": "1"},
			wantImage: "node:20-slim",
			wantCmd:   "sh -c node custom.js --flag",
			wantPort:  "8080",
			wantMount: "/home/site/wwwroot",
		},
		{
			name: "python main", linuxFx: "PYTHON|3.12", files: map[string]string{"main.py": "1"},
			wantImage: "python:3.12-slim", wantCmd: "python main.py", wantPort: "8080", wantMount: "/home/site/wwwroot",
		},
		{
			name: "python flask", linuxFx: "PYTHON|3.12", files: map[string]string{"app.py": "1"},
			wantImage: "python:3.12-slim", wantCmd: pyGunicornCmd("app:app"), wantPort: "8080",
			wantMount: "/home/site/wwwroot",
		},
		{
			name: "python flask application", linuxFx: "PYTHON|3.12", files: map[string]string{"application.py": "1"},
			wantImage: "python:3.12-slim", wantCmd: pyGunicornCmd("application:app"), wantPort: "8080",
			wantMount: "/home/site/wwwroot",
		},
		{
			name: "python django", linuxFx: "PYTHON|3.12",
			files:     map[string]string{"app.py": "1", "mysite/wsgi.py": "1"},
			wantImage: "python:3.12-slim", wantCmd: pyGunicornCmd("mysite.wsgi"), wantPort: "8080",
			wantMount: "/home/site/wwwroot",
		},
		{
			name:      "dotnet",
			linuxFx:   "DOTNETCORE|8.0",
			files:     map[string]string{"web.runtimeconfig.json": "{}", "web.dll": "1"},
			wantImage: "mcr.microsoft.com/dotnet/aspnet:8.0",
			wantCmd:   "dotnet web.dll",
			wantPort:  "8080",
			wantMount: "/home/site/wwwroot",
		},
		{
			name: "java", linuxFx: "JAVA|17-java17", files: map[string]string{"app.jar": "1"},
			wantImage: "eclipse-temurin:17-jre", wantCmd: "java -jar /home/site/wwwroot/app.jar", wantPort: "8080",
			wantMount: "/home/site/wwwroot",
		},
		{
			name: "php apache", linuxFx: "PHP|8.3", files: map[string]string{"index.php": "1"},
			wantImage: "php:8.3-apache", wantCmd: "", wantPort: "80", wantMount: "/var/www/html",
		},
		{
			name: "docker default port", linuxFx: "DOCKER|nginx:alpine",
			wantImage: "nginx:alpine", wantCmd: "", wantPort: "80",
		},
		{
			name:      "docker websites port and command",
			linuxFx:   "DOCKER|reg.example/app:1",
			cmdLine:   `serve "a b" -p 9000`,
			settings:  map[string]string{"WEBSITES_PORT": "9000"},
			wantImage: "reg.example/app:1",
			wantCmd:   "serve a b -p 9000",
			wantPort:  "9000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			e.backend.PutApp(
				"webapp",
				azurefunctions.AppConfig{Kind: webKind, LinuxFx: tt.linuxFx, AppCommandLine: tt.cmdLine},
			)
			require.NoError(t, e.backend.SetSettings("webapp", tt.settings))

			if tt.files != nil {
				_, err := e.backend.Deploy("webapp", zipOf(t, tt.files), "test", "m")
				require.NoError(t, err)
			}

			resp, body := e.do(t, http.MethodGet, "webapp."+testDomain, "/page", nil, nil)
			require.Equal(t, http.StatusOK, resp.StatusCode, body)
			assert.Equal(t, "echo /page", body)

			specs := e.rt.specs()
			require.Len(t, specs, 1)
			assert.Equal(t, tt.wantImage, specs[0].Image)
			assert.Equal(t, tt.wantCmd, strings.Join(specs[0].Cmd, " "))
			assert.Equal(t, tt.wantPort, containerPortOf(t, specs[0]))

			env := envOf(specs[0])
			assert.Equal(t, tt.wantPort, env["PORT"])
			assert.Equal(t, "webapp", env["WEBSITE_SITE_NAME"])
			assert.NotContains(t, env, "FUNCTIONS_WORKER_RUNTIME")

			if tt.wantMount != "" {
				assert.NotEmpty(t, mountSource(specs[0], tt.wantMount))
			}
		})
	}
}

func TestWebApp_AppSettingsAreEnv(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.backend.PutApp("envapp", azurefunctions.AppConfig{Kind: webKind, LinuxFx: "DOCKER|busybox"})
	require.NoError(t, e.backend.SetSettings("envapp", map[string]string{"GREETING": "hi", "PORT": "7000"}))

	resp, _ := e.do(t, http.MethodGet, "envapp."+testDomain, "/", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	spec := e.rt.specs()[0]
	assert.Equal(t, "hi", envOf(spec)["GREETING"])
	assert.Equal(t, "7000", containerPortOf(t, spec), "PORT selects the container port like WEBSITES_PORT")
}

func TestWebApp_NotServable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		linuxFx  string
		files    map[string]string
		wantBody string
		wantCode int
	}{
		{"no content shows the hosting start page", "NODE|20-lts", nil, "waiting for your content", http.StatusOK},
		{
			"no startup command", "NODE|20-lts",
			map[string]string{"readme.txt": "x"},
			"no startup command",
			http.StatusBadGateway,
		},
		{
			"dotnet without runtimeconfig",
			"DOTNETCORE|8.0",
			map[string]string{"a.txt": "x"},
			"appCommandLine",
			http.StatusBadGateway,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			e.backend.PutApp("w", azurefunctions.AppConfig{Kind: webKind, LinuxFx: tt.linuxFx})

			if tt.files != nil {
				_, err := e.backend.Deploy("w", zipOf(t, tt.files), "test", "m")
				require.NoError(t, err)
			}

			resp, body := e.do(t, http.MethodGet, "w."+testDomain, "/", nil, nil)
			assert.Equal(t, tt.wantCode, resp.StatusCode)
			assert.Contains(t, body, tt.wantBody)
			assert.Zero(t, e.rt.running())

			info, ok := e.backend.App("w")
			require.True(t, ok)

			if tt.wantCode == http.StatusBadGateway {
				assert.Contains(t, info.Failure, tt.wantBody)
			}
		})
	}
}

func TestWebApp_EngineNone(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	info := e.backend.PutApp("w", azurefunctions.AppConfig{Kind: webKind, LinuxFx: "DOCKER|busybox"})
	assert.Contains(t, info.Failure, "container runtime is unavailable")

	resp, body := e.do(t, http.MethodGet, "w."+testDomain, "/", nil, nil)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Contains(t, body, "web app is not serving")
}

func TestWebApp_StopStartRestart(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.backend.PutApp("w", azurefunctions.AppConfig{Kind: webKind, LinuxFx: "DOCKER|busybox"})

	host := "w." + testDomain
	resp, _ := e.do(t, http.MethodGet, host, "/", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 1, e.rt.running())

	require.NoError(t, e.backend.SetStopped("w", true))
	assert.Zero(t, e.rt.running())

	resp, body := e.do(t, http.MethodGet, host, "/", nil, nil)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Contains(t, body, "stopped")

	require.NoError(t, e.backend.SetStopped("w", false))
	resp, _ = e.do(t, http.MethodGet, host, "/", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.NoError(t, e.backend.Restart("w"))
	resp, _ = e.do(t, http.MethodGet, host, "/", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Len(t, e.rt.specs(), 3, "each restart starts a fresh container")
	assert.Equal(t, 1, e.rt.running())
}

func TestWebApp_HealthCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		wantBody  string
		unhealthy int64
		timeout   time.Duration
		wantCode  int
	}{
		{"healthy after retries", "/healthz", "echo /", 2, 30 * time.Second, http.StatusOK},
		{"never healthy", "/healthz", "health check /healthz", 1 << 20, 600 * time.Millisecond, http.StatusBadGateway},
		{"no path means no probe", "", "echo /", 1 << 20, 30 * time.Second, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			e.rt.unhealthy.Store(tt.unhealthy)
			e.backend.Configure(azurefunctions.Settings{StartupTimeout: tt.timeout})
			e.backend.PutApp(
				"w",
				azurefunctions.AppConfig{Kind: webKind, LinuxFx: "DOCKER|busybox", HealthCheckPath: tt.path},
			)

			resp, body := e.do(t, http.MethodGet, "w."+testDomain, "/", nil, nil)
			assert.Equal(t, tt.wantCode, resp.StatusCode)
			assert.Contains(t, body, tt.wantBody)

			if tt.wantCode == http.StatusBadGateway {
				assert.Zero(t, e.rt.running(), "an unhealthy container is removed")
			}
		})
	}
}

func TestWebApp_ValidateConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr string
		cfg     azurefunctions.AppConfig
	}{
		{"node", "", azurefunctions.AppConfig{Kind: webKind, LinuxFx: "NODE|20-lts"}},
		{"docker", "", azurefunctions.AppConfig{Kind: "app,linux,container", LinuxFx: "DOCKER|nginx"}},
		{"function app is not checked here", "", azurefunctions.AppConfig{Kind: "functionapp,linux", LinuxFx: "x"}},
		{"empty stack", "linuxFxVersion", azurefunctions.AppConfig{Kind: webKind}},
		{"unknown stack", "RUBY", azurefunctions.AppConfig{Kind: webKind, LinuxFx: "RUBY|3.2"}},
		{"tomcat", "TOMCAT", azurefunctions.AppConfig{Kind: webKind, LinuxFx: "TOMCAT|10.0-java17"}},
		{"old node", "supported: 18", azurefunctions.AppConfig{Kind: webKind, LinuxFx: "NODE|10-lts"}},
		{
			"health path",
			"healthCheckPath",
			azurefunctions.AppConfig{Kind: webKind, LinuxFx: "NODE|20-lts", HealthCheckPath: "up"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := azurefunctions.ValidateConfig(tt.cfg)
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestWebApp_KuduAndFunctionsIsolation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.backend.PutApp("w", azurefunctions.AppConfig{Kind: webKind, LinuxFx: "NODE|20-lts"})

	resp, _ := e.do(t, http.MethodPost, "w.scm."+testDomain, "/api/zipdeploy",
		zipOf(t, map[string]string{"index.js": "1"}), e.kuduAuth(t, "w"))
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, _ = e.do(t, http.MethodGet, "w."+testDomain, "/", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, []string{"node", "index.js"}, e.rt.specs()[0].Cmd)

	fns, err := e.backend.Functions(t.Context(), "w")
	require.NoError(t, err)
	assert.Empty(t, fns, "web apps have no functions")
}

func TestWebApp_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	src := newEnv(t, false)
	src.backend.PutApp("w", azurefunctions.AppConfig{
		Kind: webKind, LinuxFx: "NODE|20-lts", AppCommandLine: "node a.js", HealthCheckPath: "/up",
	})

	dst := newEnv(t, true)
	require.NoError(t, dst.handler.Restore(t.Context(), src.handler.Snapshot(t.Context())))
	_, err := dst.backend.Deploy("w", zipOf(t, map[string]string{"a.js": "1"}), "test", "m")
	require.NoError(t, err)

	resp, _ := dst.do(t, http.MethodGet, "w."+testDomain, "/", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	spec := dst.rt.specs()[0]
	assert.Equal(t, "node:20-slim", spec.Image)
	assert.Equal(t, []string{"sh", "-c", "node a.js"}, spec.Cmd)
}

func TestWebApp_UnhealthyInstanceIsReplaced(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		unhealthy int64
		replaced  bool
	}{
		{"healthy stays", 0, false},
		{"unhealthy for too long", 1 << 40, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			e.backend.SetHealthTiming(20*time.Millisecond, 100*time.Millisecond)
			e.backend.PutApp("w", azurefunctions.AppConfig{
				Kind: webKind, LinuxFx: "DOCKER|busybox", HealthCheckPath: "/up",
			})
			_, err := e.backend.Deploy("w", helloZip(t), "t", "m")
			require.NoError(t, err)

			resp, _ := e.do(t, http.MethodGet, "w."+testDomain, "/", nil, nil)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, 1, e.rt.running())

			e.rt.unhealthy.Store(tt.unhealthy)

			if tt.replaced {
				require.Eventually(t, func() bool { return e.rt.running() == 0 }, 10*time.Second, 10*time.Millisecond,
					"the instance is replaced after continuous failures")

				e.rt.unhealthy.Store(0)

				resp, _ = e.do(t, http.MethodGet, "w."+testDomain, "/", nil, nil)
				assert.Equal(t, http.StatusOK, resp.StatusCode)
				assert.Len(t, e.rt.specs(), 2, "the next request starts a new instance")

				return
			}

			pings := func() int {
				n := 0

				for _, r := range e.rt.requests() {
					if r.Headers.Get("User-Agent") == "HealthCheck/1.0" {
						n++
					}
				}

				return n
			}

			require.Eventually(t, func() bool { return pings() >= 5 }, 10*time.Second, 10*time.Millisecond)
			assert.Equal(t, 1, e.rt.running())
		})
	}
}

func TestWebApp_PrivateRegistryCredentials(t *testing.T) {
	t.Parallel()

	creds := map[string]string{
		"DOCKER_REGISTRY_SERVER_URL":      "https://reg.azurecr.io",
		"DOCKER_REGISTRY_SERVER_USERNAME": "u",
		"DOCKER_REGISTRY_SERVER_PASSWORD": "p",
	}

	tests := []struct {
		settings map[string]string
		name     string
		image    string
		want     []string
	}{
		{creds, "matching registry", "reg.azurecr.io/team/app:1", []string{"reg.azurecr.io/team/app:1|u|p"}},
		{creds, "other registry", "other.example.com/app:1", nil},
		{creds, "docker hub", "busybox", nil},
		{map[string]string{"DOCKER_REGISTRY_SERVER_URL": "reg.azurecr.io"}, "no login", "reg.azurecr.io/a", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			e.backend.PutApp("w", azurefunctions.AppConfig{Kind: "app,linux,container", LinuxFx: "DOCKER|" + tt.image})
			require.NoError(t, e.backend.SetSettings("w", tt.settings))

			resp, _ := e.do(t, http.MethodGet, "w."+testDomain, "/", nil, nil)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, tt.want, e.rt.pulled())

			for _, spec := range e.rt.specs() {
				env := envOf(spec)
				assert.NotContains(t, env, "DOCKER_REGISTRY_SERVER_PASSWORD", "reserved names are not exposed")
				assert.NotContains(t, env, "DOCKER_REGISTRY_SERVER_USERNAME")
			}
		})
	}
}

func TestFunctionApp_PrivateRegistryCredentials(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.backend.PutApp("fn", azurefunctions.AppConfig{LinuxFx: "DOCKER|reg.azurecr.io/fn:1"})
	require.NoError(t, e.backend.SetSettings("fn", map[string]string{
		"DOCKER_REGISTRY_SERVER_URL": "reg.azurecr.io", "DOCKER_REGISTRY_SERVER_USERNAME": "u",
		"DOCKER_REGISTRY_SERVER_PASSWORD": "p", "FUNCTIONS_WORKER_RUNTIME": "node",
	}))

	resp, _ := e.do(t, http.MethodGet, "fn."+testDomain, "/api/x", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, []string{"reg.azurecr.io/fn:1|u|p"}, e.rt.pulled())
}

func pyGunicornCmd(target string) string {
	return `sh -c python -c "import gunicorn" 2>/dev/null || ` +
		`pip install --quiet --disable-pip-version-check --target /tmp/gunicorn gunicorn; ` +
		`PYTHONPATH="/tmp/gunicorn:$PYTHONPATH" exec python -m gunicorn --bind=0.0.0.0:"$PORT" --timeout 600 ` + target
}
