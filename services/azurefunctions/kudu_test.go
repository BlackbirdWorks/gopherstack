package azurefunctions_test

import (
	"archive/zip"
	"bytes"
	"encoding/json/v2"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
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
	require.NoError(t, json.Unmarshal([]byte(body), &first, jsonx.V1()))
	assert.InDelta(t, 4, first["status"], 0)
	assert.Equal(t, true, first["complete"])
	assert.Equal(t, "ZipDeploy", first["deployer"])

	e.do(t, http.MethodPost, scm, "/api/publish?type=zip", pkg, auth)

	_, body = e.do(t, http.MethodGet, scm, "/api/deployments", nil, auth)

	var list []map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &list, jsonx.V1()))
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

func TestKudu_PathsOnlyOnScmHost(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})

	resp, _ := e.do(t, http.MethodPost, "127.0.0.1", "/api/zipdeploy", helloZip(t), e.kuduAuth(t, "app"))
	assert.Equal(t, http.StatusNotFound, resp.StatusCode, "unknown host")

	// On the app's own hostname the path is just a function route.
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

func TestKudu_RemoteBuild(t *testing.T) {
	t.Parallel()

	nodeZip := func(t *testing.T) []byte {
		t.Helper()

		return zipOf(t, map[string]string{
			"host.json":    `{"version":"2.0"}`,
			"package.json": `{"dependencies":{"left-pad":"1.3.0"}}`,
		})
	}

	tests := []struct {
		settings   map[string]string
		linuxFx    string
		path       string
		name       string
		execCode   int
		wantCode   int
		wantBuilt  bool
		wantStored bool
	}{
		{nil, "NODE|20", "/api/publish?type=zip&RemoteBuild=true", "query flag", 0, http.StatusOK, true, true},
		{
			map[string]string{"SCM_DO_BUILD_DURING_DEPLOYMENT": "true"}, "NODE|20", "/api/zipdeploy",
			"build setting", 0, http.StatusOK, true, true,
		},
		{nil, "NODE|20", "/api/zipdeploy", "no flag no build", 0, http.StatusOK, false, true},
		{
			nil, "NODE|20", "/api/publish?type=zip&RemoteBuild=true", "failing build",
			1, http.StatusInternalServerError, false, false,
		},
		{
			nil, "DOTNET-ISOLATED|8.0", "/api/publish?type=zip&RemoteBuild=true", "unsupported worker",
			0, http.StatusBadRequest, false, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			e.rt.onExec = func(spec container.Spec, cmd []string) container.ExecResult {
				dir := mountSource(spec, "/build")
				if strings.Contains(cmd[2], "npm install") && tt.execCode == 0 {
					_ = os.MkdirAll(filepath.Join(dir, "node_modules", "left-pad"), 0o755)
					_ = os.WriteFile(filepath.Join(dir, "node_modules", "left-pad", "index.js"), []byte("ok"), 0o644)
				}

				return container.ExecResult{Stdout: "added 1 package", ExitCode: tt.execCode}
			}
			e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: tt.linuxFx})
			require.NoError(t, e.backend.SetSettings("app", tt.settings))

			resp, _ := e.do(t, http.MethodPost, "app.scm."+testDomain, tt.path, nodeZip(t), e.kuduAuth(t, "app"))
			require.Equal(t, tt.wantCode, resp.StatusCode)

			pkg := e.backend.Package("app")
			assert.Equal(t, tt.wantStored, pkg != nil)

			if pkg != nil {
				zr, err := zip.NewReader(bytes.NewReader(pkg), int64(len(pkg)))
				require.NoError(t, err)

				names := make([]string, 0, len(zr.File))
				for _, f := range zr.File {
					names = append(names, f.Name)
				}

				assert.Equal(t, tt.wantBuilt, slices.Contains(names, "node_modules/left-pad/index.js"))
			}

			if tt.wantCode == http.StatusInternalServerError {
				ds, err := e.backend.Deployments("app")
				require.NoError(t, err)
				require.Len(t, ds, 1)
				assert.False(t, ds[0].Succeeded)
				assert.Contains(t, ds[0].Log, "exit code")
			}

			if tt.wantBuilt {
				_, body := e.do(
					t,
					http.MethodGet,
					"app.scm."+testDomain,
					"/api/deployments/latest/log",
					nil,
					e.kuduAuth(t, "app"),
				)
				assert.Contains(t, body, "added 1 package")
			}
		})
	}
}

func TestKudu_RemoteBuildWebApp(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.rt.onExec = func(spec container.Spec, cmd []string) container.ExecResult {
		if strings.Contains(cmd[2], "pip install") {
			_ = os.MkdirAll(filepath.Join(mountSource(spec, "/build"), ".python_packages"), 0o755)
		}

		return container.ExecResult{Stdout: "installed"}
	}
	e.backend.PutApp("web", azurefunctions.AppConfig{Kind: "app,linux", LinuxFx: "PYTHON|3.12"})

	pkg := zipOf(t, map[string]string{"app.py": "print(1)", "requirements.txt": "flask"})
	resp, _ := e.do(t, http.MethodPost, "web.scm."+testDomain, "/api/zipdeploy?x=1", pkg, e.kuduAuth(t, "web"))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, e.rt.specs(), "no build without the build setting")

	require.NoError(t, e.backend.SetSettings("web", map[string]string{"SCM_DO_BUILD_DURING_DEPLOYMENT": "true"}))

	resp, _ = e.do(t, http.MethodPost, "web.scm."+testDomain, "/api/zipdeploy", pkg, e.kuduAuth(t, "web"))
	require.Equal(t, http.StatusOK, resp.StatusCode)

	specs := e.rt.specs()
	require.Len(t, specs, 1)
	assert.Equal(t, "python:3.12-slim", specs[0].Image)

	built := e.backend.Package("web")
	zr, err := zip.NewReader(bytes.NewReader(built), int64(len(built)))
	require.NoError(t, err)

	names := make([]string, 0, len(zr.File))
	for _, f := range zr.File {
		names = append(names, f.Name)
	}

	assert.Contains(t, names, ".python_packages/")
}

func TestKudu_RemoteBuildScript(t *testing.T) {
	t.Parallel()

	tests := []struct {
		settings map[string]string
		name     string
		linuxFx  string
		files    map[string]string
		want     []string
		absent   []string
	}{
		{
			name:    "python hooks in order",
			linuxFx: "PYTHON|3.12",
			settings: map[string]string{
				"PRE_BUILD_COMMAND": "echo pre-hook", "POST_BUILD_COMMAND": "echo post-hook",
			},
			files:  map[string]string{"requirements.txt": "x", "manage.py": "1"},
			want:   []string{"echo pre-hook", "-r requirements.txt", "collectstatic", "echo post-hook"},
			absent: nil,
		},
		{
			name:     "collectstatic disabled",
			linuxFx:  "PYTHON|3.12",
			settings: map[string]string{"DISABLE_COLLECTSTATIC": "true"},
			files:    map[string]string{"manage.py": "1", "setup.py": "1"},
			want:     []string{"pip install --disable-pip-version-check --target .python_packages/lib/site-packages ."},
			absent:   []string{"collectstatic"},
		},
		{
			name:     "node post hook",
			linuxFx:  "NODE|20",
			settings: map[string]string{"POST_BUILD_COMMAND": "echo after"},
			files:    map[string]string{"package.json": "{}"},
			want:     []string{"npm install", "echo after"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)

			var script string

			e.rt.onExec = func(_ container.Spec, cmd []string) container.ExecResult {
				if strings.Contains(cmd[2], "cd /build") {
					script = cmd[2]
				}

				return container.ExecResult{}
			}
			e.backend.PutApp("w", azurefunctions.AppConfig{Kind: "app,linux", LinuxFx: tt.linuxFx})

			tt.settings["SCM_DO_BUILD_DURING_DEPLOYMENT"] = "1"
			require.NoError(t, e.backend.SetSettings("w", tt.settings))

			resp, _ := e.do(
				t,
				http.MethodPost,
				"w.scm."+testDomain,
				"/api/zipdeploy",
				zipOf(t, tt.files),
				e.kuduAuth(t, "w"),
			)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			last := -1

			for _, w := range tt.want {
				i := strings.Index(script, w)
				require.GreaterOrEqual(t, i, 0, w)
				assert.Greater(t, i, last, "%q comes after the previous step", w)

				last = i
			}

			for _, a := range tt.absent {
				assert.NotContains(t, script, a)
			}
		})
	}
}
