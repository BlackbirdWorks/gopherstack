package gcpidentitytoolkit_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/gcpidentitytoolkit"
)

const (
	adminAuth    = "Bearer owner"
	testPassword = "secret1"
)

type fakeClock struct {
	t  time.Time
	mu sync.Mutex
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *fakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(d)
}

// skewedNow keeps token timestamps in the past (within the SDK's 5 minute tolerance) so revocation, which has
// second resolution, is deterministic.
func skewedNow() time.Time { return time.Now().Add(-2 * time.Minute) }

type env struct {
	backend *gcpidentitytoolkit.InMemoryBackend
	clock   *fakeClock
	hc      *http.Client
	url     string
	project string
}

func newEnv(t *testing.T) *env {
	t.Helper()

	backend := gcpidentitytoolkit.NewInMemoryBackend()
	clock := &fakeClock{t: skewedNow()}
	backend.SetClock(clock.Now)

	e := echo.New()
	e.Any("/*", gcpidentitytoolkit.NewHandler(backend).Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return &env{
		backend: backend, clock: clock, hc: testclient.New(t), url: srv.URL,
		project: "proj-" + strings.ToLower(strings.NewReplacer("/", "-", "_", "-").Replace(t.Name())),
	}
}

// result is a decoded REST response.
type result struct {
	body   map[string]any
	status int
}

func (r result) str(key string) string {
	s, _ := r.body[key].(string)

	return s
}

func (r result) errMsg() string {
	e, _ := r.body["error"].(map[string]any)
	m, _ := e["message"].(string)

	return m
}

func (e *env) do(t *testing.T, method, path, auth string, body any) result {
	t.Helper()

	var rdr *bytes.Reader

	if body != nil {
		data, err := json.Marshal(body)
		require.NoError(t, err)

		rdr = bytes.NewReader(data)
	} else {
		rdr = bytes.NewReader(nil)
	}

	req, err := http.NewRequestWithContext(context.Background(), method, e.url+path, rdr)
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	if auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := e.hc.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out := result{status: resp.StatusCode}
	_ = json.NewDecoder(resp.Body).Decode(&out.body)

	return out
}

// client posts a key-authenticated client call under the test's project.
func (e *env) client(t *testing.T, verb string, body any) result {
	t.Helper()

	return e.do(t, http.MethodPost, "/v1/projects/"+e.project+"/accounts:"+verb+"?key=k", "", body)
}

// admin posts an OAuth-authenticated call under the test's project.
func (e *env) admin(t *testing.T, verb string, body any) result {
	t.Helper()

	return e.do(t, http.MethodPost, "/v1/projects/"+e.project+"/accounts:"+verb, adminAuth, body)
}

func (e *env) signUp(t *testing.T, email string) result {
	t.Helper()

	r := e.client(t, "signUp", map[string]any{"email": email, "password": testPassword, "returnSecureToken": true})
	require.Equal(t, http.StatusOK, r.status, r.body)

	return r
}

func (e *env) setConfig(t *testing.T, patch map[string]any) {
	t.Helper()

	r := e.do(t, http.MethodPatch, "/v2/projects/"+e.project+"/config", adminAuth, patch)
	require.Equal(t, http.StatusOK, r.status, r.body)
}

func (e *env) assertErr(t *testing.T, r result, status int, msg string) {
	t.Helper()

	assert.Equal(t, status, r.status, r.body)
	assert.Equal(t, msg, r.errMsg())
}

func testClient(t *testing.T) *http.Client {
	t.Helper()

	return testclient.New(t)
}
