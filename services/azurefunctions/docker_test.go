package azurefunctions_test

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

const dockerOptIn = "GOPHERSTACK_AZFN_DOCKER_TEST"

func nodeAuthZip(t *testing.T) []byte {
	t.Helper()

	fn := func(name, level, body string) map[string]string {
		return map[string]string{
			name + "/function.json": `{"bindings":[{"authLevel":"` + level + `","type":"httpTrigger","direction":"in","name":"req",` +
				`"methods":["get","post"]},{"type":"http","direction":"out","name":"res"}]}`,
			name + "/index.js": "module.exports = async function (context, req) { context.res = { body: " + body + " }; };",
		}
	}

	files := map[string]string{"host.json": `{"version":"2.0"}`}

	for name, level := range map[string]string{"open": "anonymous", "locked": "function", "superuser": "admin"} {
		for k, v := range fn(name, level, `"hi from `+name+` " + (process.env.GREETING || "") + " " + (req.query.name || "")`) {
			files[k] = v
		}
	}

	return zipOf(t, files)
}

// TestDocker_NodeAuthLevels runs a real Azure Functions host container. It needs a container runtime and the
// official node image (about 1GB), so it is opt-in via GOPHERSTACK_AZFN_DOCKER_TEST=1.
func TestDocker_NodeAuthLevels(t *testing.T) {
	t.Parallel()

	if os.Getenv(dockerOptIn) == "" {
		t.Skipf("set %s=1 to run against a real container runtime", dockerOptIn)
	}

	lr := gcpserve.NewLazyRuntime(nil, "azurefunctions.test.runtime")
	t.Cleanup(func() { _ = lr.Close() })

	if _, err := lr.Get(); err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	e := newEnv(t, false)
	e.backend.SetRuntime(func() (azurefunctions.Runner, error) { return lr.Get() })
	e.backend.Configure(azurefunctions.Settings{StartupTimeout: 8 * time.Minute})

	e.backend.PutApp("real", azurefunctions.AppConfig{Kind: "functionapp,linux", LinuxFx: "NODE|20"})
	require.NoError(t, e.backend.SetSettings("real", map[string]string{"GREETING": "hello"}))

	resp, _ := e.do(t, http.MethodPost, "real.scm."+testDomain, "/api/zipdeploy", nodeAuthZip(t), e.kuduAuth(t, "real"))
	require.Equal(t, http.StatusOK, resp.StatusCode)

	keys, err := e.backend.HostKeys("real")
	require.NoError(t, err)

	host := "real." + testDomain
	hostKey := keys.FunctionKeys["default"]

	tests := []struct {
		name     string
		path     string
		hdr      map[string]string
		wantCode int
		wantBody string
	}{
		{"anonymous needs no key", "/api/open?name=x", nil, http.StatusOK, "hi from open hello x"},
		{"function level without key", "/api/locked", nil, http.StatusUnauthorized, ""},
		{"function level wrong key", "/api/locked?code=nope", nil, http.StatusUnauthorized, ""},
		{"function level host key in query", "/api/locked?code=" + url.QueryEscape(hostKey), nil, http.StatusOK, "hi from locked hello"},
		{"function level host key in header", "/api/locked", map[string]string{"x-functions-key": hostKey}, http.StatusOK, "hi from locked"},
		{"admin level rejects function key", "/api/superuser", map[string]string{"x-functions-key": hostKey}, http.StatusUnauthorized, ""},
		{"admin level master key", "/api/superuser", map[string]string{"x-functions-key": keys.MasterKey}, http.StatusOK, "hi from superuser"},
		{"unknown function", "/api/missing", nil, http.StatusNotFound, ""},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 9*time.Minute)
	defer cancel()

	// First request pulls the image and starts the host; give it its own deadline.
	first, _ := e.do(t, http.MethodGet, host, "/api/open", nil, nil)
	require.Equal(t, http.StatusOK, first.StatusCode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, body := e.do(t, http.MethodGet, host, tt.path, nil, tt.hdr)
			assert.Equal(t, tt.wantCode, resp.StatusCode)
			assert.Contains(t, body, tt.wantBody)
		})
	}

	t.Run("function key set through the backend works without restart", func(t *testing.T) {
		val, setErr := e.backend.SetFunctionKey("real", "locked", "mine", "FnKey1234567890abcdefghijklmnopqrstuvwxyz0123==")
		require.NoError(t, setErr)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, _ := e.do(t, http.MethodGet, host, "/api/locked", nil, map[string]string{"x-functions-key": val})
			assert.Equal(c, http.StatusOK, resp.StatusCode)
		}, time.Minute, time.Second)
	})

	t.Run("function listing comes from the host", func(t *testing.T) {
		fns, listErr := e.backend.Functions(ctx, "real")
		require.NoError(t, listErr)

		names := make([]string, 0, len(fns))
		for _, f := range fns {
			names = append(names, f.Name)
			assert.Equal(t, "node", f.Language)
		}

		assert.ElementsMatch(t, []string{"open", "locked", "superuser"}, names)
	})
}
