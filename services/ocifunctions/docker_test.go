package ocifunctions_test

import (
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/functions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	oci "github.com/blackbirdworks/gopherstack/services/ocifunctions"
)

const dockerOptIn = "GOPHERSTACK_OCIFN_DOCKER_TEST"

// fdkServer speaks the Fn FDK http-stream contract the way the official FDKs do: it listens on a phony
// socket in the FN_LISTENER directory and symlinks FN_LISTENER to it.
const fdkServer = `package main

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	target := strings.TrimPrefix(os.Getenv("FN_LISTENER"), "unix:")
	phony := filepath.Join(filepath.Dir(target), "phony.sock")

	ln, err := net.Listen("unix", phony)
	if err != nil {
		panic(err)
	}
	if err = os.Chmod(phony, 0o666); err != nil {
		panic(err)
	}
	if err = os.Symlink(filepath.Base(phony), target); err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/call", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch string(body) {
		case "hang":
			<-r.Context().Done()
			return
		case "fail":
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Fn-Http-Status", "200")
		w.Header().Set("Fn-Http-H-X-Fdk", "tiny")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"body": string(body), "greeting": os.Getenv("GREETING"), "app": os.Getenv("FN_APP_NAME"),
			"fn": os.Getenv("FN_FN_NAME"), "format": os.Getenv("FN_FORMAT"), "memory": os.Getenv("FN_MEMORY"),
			"method": r.Header.Get("Fn-Http-Method"), "callId": r.Header.Get("Fn-Call-Id"),
			"deadline": r.Header.Get("Fn-Deadline"), "ctype": r.Header.Get("Content-Type"),
			"hdr": r.Header.Get("Fn-Http-H-X-Custom"),
		})
	})
	_ = http.Serve(ln, mux)
}
`

// buildFDKImage compiles fdkServer into a FROM scratch image and removes it when the test ends.
func buildFDKImage(t *testing.T) string {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable")
	}

	src, ctxDir := t.TempDir(), t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(src, "main.go"), []byte(fdkServer), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(src, "go.mod"), []byte("module fdk\n\ngo 1.22\n"), 0o600))

	build := exec.CommandContext(t.Context(), "go", "build", "-o", filepath.Join(ctxDir, "fdk"), ".")
	build.Dir = src
	build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOFLAGS=")

	out, err := build.CombinedOutput()
	require.NoError(t, err, string(out))

	dockerfile := "FROM scratch\nCOPY fdk /fdk\nENTRYPOINT [\"/fdk\"]\n"
	require.NoError(t, os.WriteFile(filepath.Join(ctxDir, "Dockerfile"), []byte(dockerfile), 0o600))

	tag := "gopherstack-ocifn-it:" + awsid.NewN("", 8)

	if out, err = exec.CommandContext(t.Context(), "docker", "build", "-q", "-t", tag, ctxDir).CombinedOutput(); err != nil {
		t.Skipf("docker build failed: %v: %s", err, out)
	}

	t.Cleanup(func() { _ = exec.CommandContext(t.Context(), "docker", "rmi", "-f", tag).Run() })

	return tag
}

// TestDocker_FDKImage runs a real container speaking the FDK contract. It needs a container runtime and
// the Go toolchain, so it is opt-in via GOPHERSTACK_OCIFN_DOCKER_TEST=1.
func TestDocker_FDKImage(t *testing.T) {
	t.Parallel()

	if os.Getenv(dockerOptIn) == "" {
		t.Skipf("set %s=1 to run against a real container runtime", dockerOptIn)
	}

	lr := gcpserve.NewLazyRuntime(nil, "ocifunctions.test.runtime")
	t.Cleanup(func() { _ = lr.Close() })

	if _, err := lr.Get(); err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	image := buildFDKImage(t)

	e := newEnv(t, envOpts{})
	e.backend.SetRuntime(func() (oci.Runner, error) { return lr.Get() })
	e.backend.Configure(oci.Settings{Engine: oci.EngineAuto, StartupTimeout: time.Minute})
	e.backend.SetTimeoutUnit(200 * time.Millisecond)

	app := e.mkApp(t, "it-app", func(d *functions.CreateApplicationDetails) {
		d.Config = map[string]string{"GREETING": "hello"}
	})
	fn := e.mkFn(t, app, "it-fn", func(d *functions.CreateFunctionDetails) {
		d.SourceDetails = functions.CreateContainerImageFunctionSourceDetails{Image: new(image)}
		d.MemoryInMBs = new(int64(256))
		d.TimeoutInSeconds = new(10)
	})

	call := func(body string, mod func(*functions.InvokeFunctionRequest)) (*http.Response, map[string]string, error) {
		ie := &invokeEnv{env: e, fn: fn}
		resp, out, ierr := ie.invoke(t, body, mod)

		got := map[string]string{}
		if ierr == nil && out != "" && resp.RawResponse.StatusCode == http.StatusOK {
			require.NoError(t, json.Unmarshal([]byte(out), &got), out)
		}

		return resp.RawResponse, got, ierr
	}

	raw, got, err := call("ping", nil)
	require.NoError(t, err)
	assert.Equal(t, "ping", got["body"])
	assert.Equal(t, "hello", got["greeting"])
	assert.Equal(t, "it-app", got["app"])
	assert.Equal(t, "it-fn", got["fn"])
	assert.Equal(t, "http-stream", got["format"])
	assert.Equal(t, "256", got["memory"])
	assert.Equal(t, http.MethodPost, got["method"])
	assert.NotEmpty(t, got["callId"])
	assert.Equal(t, "tiny", raw.Header.Get("Fn-Http-H-X-Fdk"))
	assert.Equal(t, 1, e.backend.LiveContainers())

	_, again, err := call("again", nil)
	require.NoError(t, err)
	assert.Equal(t, "again", again["body"])
	assert.Equal(t, 1, e.backend.LiveContainers(), "second call must reuse the warm container")

	_, _, err = call("fail", nil)
	assert.Equal(t, http.StatusBadGateway, svcErr(t, err).GetHTTPStatusCode())

	_, _, err = call("hang", nil)
	assert.Equal(t, http.StatusGatewayTimeout, svcErr(t, err).GetHTTPStatusCode())
	assert.Equal(t, 0, e.backend.LiveContainers(), "a timed out container is removed")

	assert.Empty(t, containersOf(t, image), "the timed out container is gone from the runtime")

	_, _, err = call("revive", nil)
	require.NoError(t, err)
	assert.Len(t, containersOf(t, image), 1)

	e.handler.Shutdown(t.Context())

	assert.Empty(t, containersOf(t, image), "shutdown removes every function container")
}

func containersOf(t *testing.T, image string) []string {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", "ps", "-aq", "--filter", "ancestor="+image).Output()
	require.NoError(t, err)

	return strings.Fields(string(out))
}
