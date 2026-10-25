package azurefunctions_test

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

const testDomain = "azurewebsites.localhost"

type env struct {
	handler *azurefunctions.Handler
	backend *azurefunctions.InMemoryBackend
	rt      *fakeRuntime
	srv     *httptest.Server
	client  *http.Client
}

func newEnv(t *testing.T, withRuntime bool) *env {
	t.Helper()

	b := azurefunctions.NewInMemoryBackend()
	b.Configure(azurefunctions.Settings{Domain: testDomain, Port: azurefunctions.DefaultPort, StartupTimeout: 20 * time.Second})

	e := &env{backend: b, client: &http.Client{Timeout: 30 * time.Second}}

	if withRuntime {
		e.rt = newFakeRuntime()
		b.SetRuntime(func() (azurefunctions.Runner, error) { return e.rt, nil })
	}

	e.handler = azurefunctions.NewHandler(b, azurefunctions.DefaultSettings())
	e.srv = httptest.NewServer(e.handler)

	t.Cleanup(func() {
		e.srv.Close()
		b.Close()
		e.client.CloseIdleConnections()
	})

	return e
}

func zipOf(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	for name, content := range files {
		w, err := zw.Create(name)
		require.NoError(t, err)

		_, err = w.Write([]byte(content))
		require.NoError(t, err)
	}

	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func helloZip(t *testing.T) []byte {
	t.Helper()

	return zipOf(t, map[string]string{
		"host.json": `{"version":"2.0"}`,
		"hello/function.json": `{"bindings":[{"authLevel":"anonymous","type":"httpTrigger","direction":"in","name":"req",` +
			`"methods":["get","post"],"route":"greet"}]}`,
		"hello/index.js": "module.exports = async (ctx) => { ctx.res = { body: 'hi' } }",
		"tick/function.json": `{"bindings":[{"type":"timerTrigger","direction":"in","name":"t","schedule":"0 */5 * * * *"}],` +
			`"disabled":true}`,
	})
}

// do sends a request to the listener with the given Host.
func (e *env) do(t *testing.T, method, host, path string, body []byte, hdr map[string]string) (*http.Response, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), method, e.srv.URL+path, bytes.NewReader(body))
	require.NoError(t, err)

	req.Host = host

	for k, v := range hdr {
		if k == "basic" {
			u, p, _ := strings.Cut(v, ":")
			req.SetBasicAuth(u, p)

			continue
		}

		req.Header.Set(k, v)
	}

	resp, err := e.client.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp, string(raw)
}

func (e *env) kuduAuth(t *testing.T, app string) map[string]string {
	t.Helper()

	user, pass, _, err := e.backend.PublishingCredentials(app)
	require.NoError(t, err)

	return map[string]string{"basic": user + ":" + pass}
}
