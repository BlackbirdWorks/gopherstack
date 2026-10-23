package azureappconfig_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azappconfig/v2"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	ac "github.com/blackbirdworks/gopherstack/services/azureappconfig"
)

const storeName = "teststore"

type fakeClock struct {
	t  time.Time
	mu sync.Mutex
}

func newClock() *fakeClock { return &fakeClock{t: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(d)
}

type env struct {
	backend *ac.InMemoryBackend
	clock   *fakeClock
	srv     *httptest.Server
	keys    []ac.AccessKey
}

type envOpts struct {
	validate bool
	realTime bool
}

func newEnv(t *testing.T, o envOpts) *env {
	t.Helper()

	b := ac.NewInMemoryBackend()
	clk := newClock()

	if !o.realTime {
		ac.SetNow(b, clk.Now)
	}

	_, err := b.PutStore(storeName)
	require.NoError(t, err)

	keys, err := b.AccessKeys(storeName)
	require.NoError(t, err)

	h := ac.NewHandler(b, ac.Settings{Port: ac.DefaultPort, ValidateHMAC: o.validate, AutoCreate: true})
	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	return &env{backend: b, clock: clk, srv: srv, keys: keys}
}

func (e *env) connString(k ac.AccessKey) string {
	return "Endpoint=" + e.srv.URL + ";Id=" + k.ID + ";Secret=" + k.Secret
}

func (e *env) client(t *testing.T) *azappconfig.Client {
	t.Helper()

	return e.clientWith(t, e.keys[0])
}

func (e *env) clientWith(t *testing.T, k ac.AccessKey) *azappconfig.Client {
	t.Helper()

	c, err := azappconfig.NewClientFromConnectionString(e.connString(k), &azappconfig.ClientOptions{
		Transport: e.srv.Client(), Retry: policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	return c
}

func (e *env) raw(t *testing.T, method, path, body string, hdr map[string]string) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, e.srv.URL+path, strings.NewReader(body))
	require.NoError(t, err)

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	resp, err := e.srv.Client().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	return resp
}
