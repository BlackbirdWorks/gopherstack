package azuregraph_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/aadauth"
	"github.com/blackbirdworks/gopherstack/services/azuregraph"
)

const graphAudience = "https://graph.microsoft.com"

type fakeClock struct{ ns atomic.Int64 }

func newClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).UnixNano())

	return c
}

func (c *fakeClock) now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *fakeClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

type env struct {
	h      *azuregraph.Handler
	issuer *aadauth.Issuer
	clock  *fakeClock
	srv    *httptest.Server
	token  string
}

func newEnv(t *testing.T, mutate ...func(*azuregraph.Settings)) *env {
	t.Helper()

	settings := azuregraph.DefaultSettings()
	for _, m := range mutate {
		m(&settings)
	}

	clock := newClock()
	h := azuregraph.NewHandler(azuregraph.NewBackend(settings, azuregraph.WithClock(clock.now)), settings)

	issuer, err := aadauth.NewIssuer()
	require.NoError(t, err)

	h.Issuer = issuer

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	env := &env{h: h, issuer: issuer, clock: clock, srv: srv}
	env.token = env.tokenFor("11111111-1111-1111-1111-111111111111", graphAudience)

	return env
}

func (e *env) tokenFor(oid, aud string) string {
	tok, err := e.issuer.IssueClientCredentialsToken(aadauth.ClientCredentialsClaims{
		Issuer: "https://login.test/t/v2.0", Audience: aud, TenantID: azuregraph.DefaultTenantID,
		ClientID: oid, ObjectID: oid,
	}, 0)
	if err != nil {
		panic(err)
	}

	return tok
}

type resp struct {
	body   map[string]any
	header http.Header
	raw    string
	status int
}

func (e *env) req(t *testing.T, method, path string, body any, hdr ...string) resp {
	t.Helper()

	var rdr io.Reader

	switch b := body.(type) {
	case nil:
	case string:
		rdr = strings.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		require.NoError(t, err)

		rdr = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(t.Context(), method, e.srv.URL+path, rdr)
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	if e.token != "" {
		req.Header.Set("Authorization", "Bearer "+e.token)
	}

	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}

	res, err := e.srv.Client().Do(req)
	require.NoError(t, err)

	defer res.Body.Close()

	raw, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	out := resp{status: res.StatusCode, header: res.Header, raw: string(raw)}
	_ = json.Unmarshal(raw, &out.body)

	return out
}

func (e *env) mustCreate(t *testing.T, path string, body any) map[string]any {
	t.Helper()

	r := e.req(t, http.MethodPost, path, body)
	require.Equal(t, http.StatusCreated, r.status, r.raw)

	return r.body
}

func str(m map[string]any, k string) string {
	s, _ := m[k].(string)

	return s
}

func assertErr(t *testing.T, r resp, status int, code string) {
	t.Helper()

	assert.Equal(t, status, r.status, r.raw)

	errObj, _ := r.body["error"].(map[string]any)
	assert.Equal(t, code, str(errObj, "code"), r.raw)
	assert.NotEmpty(t, str(errObj, "message"))

	inner, _ := errObj["innerError"].(map[string]any)
	assert.NotEmpty(t, str(inner, "date"))
	assert.NotEmpty(t, str(inner, "request-id"))
	assert.NotEmpty(t, str(inner, "client-request-id"))
}

func newUser(t *testing.T, e *env, upn string) map[string]any {
	t.Helper()

	return e.mustCreate(t, "/v1.0/users", map[string]any{
		"accountEnabled":    true,
		"displayName":       "User " + upn,
		"mailNickname":      strings.Split(upn, "@")[0],
		"userPrincipalName": upn,
		"passwordProfile":   map[string]any{"password": "Sup3r-secret!", "forceChangePasswordNextSignIn": false},
	})
}

func newApp(t *testing.T, e *env, name string) map[string]any {
	t.Helper()

	return e.mustCreate(t, "/v1.0/applications", map[string]any{"displayName": name})
}

func newGroup(t *testing.T, e *env, name string) map[string]any {
	t.Helper()

	return e.mustCreate(t, "/v1.0/groups", map[string]any{
		"displayName":     name,
		"mailEnabled":     false,
		"mailNickname":    strings.ReplaceAll(name, " ", ""),
		"securityEnabled": true,
	})
}

type azuregraphSettings = azuregraph.Settings
