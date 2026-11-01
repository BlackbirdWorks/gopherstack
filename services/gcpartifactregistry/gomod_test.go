package gcpartifactregistry_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/mod/module"
	modzip "golang.org/x/mod/zip"
	arapi "google.golang.org/api/artifactregistry/v1"
)

func makeModuleZip(t *testing.T, path, version string) []byte {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module "+path+"\n\ngo 1.21\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "lib.go"), []byte("package lib\n\nconst Version = \""+version+"\"\n"), 0o600))

	var buf bytes.Buffer
	require.NoError(t, modzip.CreateFromDir(&buf, module.Version{Path: path, Version: version}, dir))

	return buf.Bytes()
}

func uploadGoModule(t *testing.T, svc *arapi.Service, repo string, zipData []byte) error {
	t.Helper()

	_, err := svc.Projects.Locations.Repositories.GoModules.Upload(repoName(repo), &arapi.UploadGoModuleRequest{}).
		Media(bytes.NewReader(zipData)).Context(t.Context()).Do()

	return err
}

func TestGoModuleProxyProtocol(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "gomods", &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_GO})

	svc := restService(t, e)
	mod := "example.com/My/Lib"

	for _, v := range []string{"v1.0.0", "v1.2.3", "v1.10.0"} {
		require.NoError(t, uploadGoModule(t, svc, "gomods", makeModuleZip(t, mod, v)))
	}

	require.Error(t, uploadGoModule(t, svc, "gomods", makeModuleZip(t, mod, "v1.2.3")), "versions are immutable")
	require.Error(t, uploadGoModule(t, svc, "gomods", []byte("not a zip")))

	base := "http://127.0.0.1:" + e.port + "/go/" + project + "/gomods/example.com/!my/!lib/@v/"

	tests := []struct {
		name string
		path string
		want int
		body string
	}{
		{"list", "list", http.StatusOK, "v1.0.0\nv1.2.3\nv1.10.0\n"},
		{"info", "v1.2.3.info", http.StatusOK, `"Version":"v1.2.3"`},
		{"mod", "v1.2.3.mod", http.StatusOK, "module example.com/My/Lib"},
		{"missing version", "v9.9.9.info", http.StatusNotFound, ""},
		{"unknown extension", "v1.2.3.txt", http.StatusNotFound, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, body := getBody(t, base+tt.path)
			assert.Equal(t, tt.want, code)
			assert.Contains(t, string(body), tt.body)
		})
	}

	code, body := getBody(t, "http://127.0.0.1:"+e.port+"/go/"+project+"/gomods/example.com/!my/!lib/@latest")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, string(body), `"Version":"v1.10.0"`, "latest is the highest semver, not the newest upload")

	code, zipBody := getBody(t, base+"v1.0.0.zip")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, makeModuleZip(t, mod, "v1.0.0"), zipBody)
}

func TestGoModuleDownloadWithGoCommand(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}

	e := newEnv(t, "")
	e.createRepo(t, "gomods", &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_GO})

	svc := restService(t, e)
	require.NoError(t, uploadGoModule(t, svc, "gomods", makeModuleZip(t, "example.com/lib", "v1.2.3")))

	app := t.TempDir()
	gomod := "module example.com/app\n\ngo 1.21\n\nrequire example.com/lib v1.2.3\n"
	require.NoError(t, os.WriteFile(filepath.Join(app, "go.mod"), []byte(gomod), 0o600))

	modCache := t.TempDir()
	t.Cleanup(func() {
		_ = filepath.WalkDir(modCache, func(p string, _ os.DirEntry, _ error) error { return os.Chmod(p, 0o700) })
	})

	cmd := exec.CommandContext(t.Context(), "go", "mod", "download", "-json", "example.com/lib")
	cmd.Dir = app
	cmd.Env = append(os.Environ(),
		"GOPROXY=http://127.0.0.1:"+e.port+"/go/"+project+"/gomods",
		"GOFLAGS=-mod=mod", "GOSUMDB=off", "GONOSUMDB=*", "GONOSUMCHECK=1", "GOTOOLCHAIN=local",
		"GOMODCACHE="+modCache, "GOCACHE="+t.TempDir(), "GOPATH="+t.TempDir(), "GOWORK=off",
	)

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
	assert.Contains(t, string(out), `"Version": "v1.2.3"`)

	var dl struct{ Dir string }
	require.NoError(t, json.Unmarshal(out, &dl))

	extracted, rerr := os.ReadFile(filepath.Join(dl.Dir, "lib.go"))
	require.NoError(t, rerr)
	assert.True(t, strings.Contains(string(extracted), `Version = "v1.2.3"`))
}
