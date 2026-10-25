package gcpartifactregistry_test

import (
	"archive/zip"
	"bytes"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNPMClientPublishAndInstall(t *testing.T) {
	t.Parallel()

	npm, err := exec.LookPath("npm")
	if err != nil {
		t.Skip("npm is not installed")
	}

	e := newEnv(t, "")
	e.formatRepo(t, "web", artifactregistrypb.Repository_NPM)

	registry := "http://127.0.0.1:" + e.port + "/npm/" + project + "/web/"
	auth := "--//127.0.0.1:" + e.port + "/npm/" + project + "/web/:_authToken=token"

	pub := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(pub, "package.json"),
		[]byte(`{"name":"@acme/real","version":"1.2.3","description":"d","main":"index.js"}`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(pub, "index.js"), []byte("module.exports = 42\n"), 0o600))

	run := func(dir string, args ...string) string {
		cmd := exec.CommandContext(t.Context(), npm, append(args, "--registry", registry, auth, "--no-audit", "--no-fund")...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "HOME="+t.TempDir(), "npm_config_update_notifier=false")
		out, runErr := cmd.CombinedOutput()
		require.NoError(t, runErr, string(out))

		return string(out)
	}

	run(pub, "publish")
	assert.Equal(t, []string{"@acme%2Freal"}, e.packageIDs(t, "web"))
	assert.Equal(t, []string{"1.2.3"}, e.versionIDs(t, "web", "@acme%2Freal"))
	assert.Contains(t, run(pub, "view", "@acme/real", "dist-tags"), "latest: '1.2.3'")

	use := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(use, "package.json"), []byte(`{"name":"use","version":"1.0.0"}`), 0o600))
	run(use, "install", "@acme/real")

	got, err := os.ReadFile(filepath.Join(use, "node_modules", "@acme", "real", "index.js"))
	require.NoError(t, err)
	assert.Equal(t, "module.exports = 42\n", string(got))

	e.denyDownloads(t, "web")
	assert.Equal(t, http.StatusForbidden,
		e.call(t, http.MethodGet, "/npm/"+project+"/web/@acme/real/-/real-1.2.3.tgz", "", nil).code)
}

func demoWheel(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	files := map[string]string{
		"demo_pkg/__init__.py":                  "VALUE = 7\n",
		"demo_pkg-1.0.0.dist-info/METADATA":     "Metadata-Version: 2.1\nName: demo-pkg\nVersion: 1.0.0\n",
		"demo_pkg-1.0.0.dist-info/WHEEL":        "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
		"demo_pkg-1.0.0.dist-info/RECORD":       "",
	}

	for name, body := range files {
		w, err := zw.Create(name)
		require.NoError(t, err)
		_, err = w.Write([]byte(body))
		require.NoError(t, err)
	}

	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func TestPipInstallFromPythonRepo(t *testing.T) {
	t.Parallel()

	pip, err := exec.LookPath("pip")
	if err != nil {
		t.Skip("pip is not installed")
	}

	e := newEnv(t, "")
	e.formatRepo(t, "py", artifactregistrypb.Repository_PYTHON)

	ctype, body := twineBody(t, map[string]string{
		":action": "file_upload", "name": "demo_pkg", "version": "1.0.0", "requires_python": ">=3.8",
	}, "demo_pkg-1.0.0-py3-none-any.whl", demoWheel(t))
	require.Equal(t, http.StatusOK, e.call(t, http.MethodPost, "/python/"+project+"/py/", ctype, body).code)

	target := t.TempDir()
	cmd := exec.CommandContext(t.Context(), pip, "install", "--no-deps", "--no-cache-dir", "--target", target,
		"--index-url", "http://127.0.0.1:"+e.port+"/python/"+project+"/py/simple/", "Demo.Pkg")
	cmd.Env = append(os.Environ(), "HOME="+t.TempDir(), "PIP_DISABLE_PIP_VERSION_CHECK=1")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	got, err := os.ReadFile(filepath.Join(target, "demo_pkg", "__init__.py"))
	require.NoError(t, err)
	assert.Equal(t, "VALUE = 7\n", string(got))
}
