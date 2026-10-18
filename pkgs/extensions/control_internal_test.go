package extensions

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func newBare(t *testing.T, specs ...Spec) *Manager {
	t.Helper()

	cfg, err := Config{Extensions: specs}.normalize()
	require.NoError(t, err)

	m := New(cfg, Options{Logger: logger.NewTestLogger()})
	t.Cleanup(m.mu.Close)

	for i, e := range m.exts {
		e.token = "tok-" + e.spec.Name
		e.port = 9000 + i
		e.registered = make(chan struct{})
	}

	return m
}

func doRegister(m *Manager, remote, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/_gopherstack/extensions/register", strings.NewReader(body))
	req.RemoteAddr = remote

	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	rec := httptest.NewRecorder()
	m.ServeRegister(rec, req)

	return rec
}

func TestRegister(t *testing.T) {
	t.Parallel()

	proc := Spec{Name: "a", Command: []string{"x"}}
	over := Spec{Name: "o", Command: []string{"x"}, AllowOverride: true}
	img := Spec{Name: "c", Image: "img"}

	tests := []struct {
		name   string
		remote string
		token  string
		body   string
		spec   []Spec
		want   int
	}{
		{name: "ok", remote: "127.0.0.1:5", token: "tok-a", body: `{"routes":{"path_prefixes":["/hello"]}}`, want: 200},
		{name: "no token", remote: "127.0.0.1:5", body: `{}`, want: 401},
		{name: "wrong token", remote: "127.0.0.1:5", token: "nope", body: `{}`, want: 401},
		{name: "non-loopback process", remote: "10.0.0.5:5", token: "tok-a", body: `{}`, want: 403},
		{name: "non-loopback container ok", remote: "172.17.0.2:5", token: "tok-c", body: `{}`, want: 200},
		{name: "name mismatch", remote: "127.0.0.1:5", token: "tok-a", body: `{"name":"zzz"}`, want: 400},
		{name: "bad json", remote: "127.0.0.1:5", token: "tok-a", body: `{`, want: 400},
		{
			name:   "reserved prefix",
			remote: "127.0.0.1:5",
			token:  "tok-a",
			body:   `{"routes":{"path_prefixes":["/_gopherstack/x"]}}`,
			want:   400,
		},
		{
			name:   "root prefix without override",
			remote: "127.0.0.1:5",
			token:  "tok-a",
			body:   `{"routes":{"path_prefixes":["/"]}}`,
			want:   400,
		},
		{
			name:   "no leading slash",
			remote: "127.0.0.1:5",
			token:  "tok-a",
			body:   `{"routes":{"path_prefixes":["hello"]}}`,
			want:   400,
		},
		{
			name:   "remote endpoint",
			remote: "127.0.0.1:5",
			token:  "tok-a",
			body:   `{"endpoint":"http://example.com:80"}`,
			want:   400,
		},
		{name: "bad hook path", remote: "127.0.0.1:5", token: "tok-a", body: `{"hooks":{"on_startup":"x"}}`, want: 400},
		{
			name:   "request hook without services",
			remote: "127.0.0.1:5",
			token:  "tok-a",
			body:   `{"hooks":{"request":[{"pre":"/p"}]}}`,
			want:   400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := newBare(t, proc, over, img)
			rec := doRegister(m, tt.remote, tt.token, tt.body)
			assert.Equal(t, tt.want, rec.Code, rec.Body.String())
		})
	}

	t.Run("claim conflict", func(t *testing.T) {
		t.Parallel()

		m := newBare(t, proc, over)
		body := `{"routes":{"path_prefixes":["/same"]}}`
		require.Equal(t, 200, doRegister(m, "127.0.0.1:5", "tok-a", body).Code)
		assert.Equal(t, 409, doRegister(m, "127.0.0.1:5", "tok-o", body).Code)
	})

	t.Run("host and target claims", func(t *testing.T) {
		t.Parallel()

		m := newBare(t, proc)
		require.Equal(t, 200, doRegister(m, "127.0.0.1:5", "tok-a",
			`{"routes":{"hosts":["ext.test"],"target_prefixes":["MyService."]}}`).Code)

		byHost := httptest.NewRequest(http.MethodGet, "http://ext.test:8000/x", nil)
		byTarget := httptest.NewRequest(http.MethodPost, "/", nil)
		byTarget.Header.Set("X-Amz-Target", "MyService.Do")
		miss := httptest.NewRequest(http.MethodPost, "/", nil)
		miss.Header.Set("X-Amz-Target", "Other.Do")

		assert.NotNil(t, m.Route(byHost, false))
		assert.NotNil(t, m.Route(byTarget, false))
		assert.Nil(t, m.Route(miss, false))
	})

	t.Run("unready extension answers 503", func(t *testing.T) {
		t.Parallel()

		m := newBare(t, proc)
		require.Equal(t, 200, doRegister(m, "127.0.0.1:5", "tok-a", `{"routes":{"path_prefixes":["/hello"]}}`).Code)
		m.setStatus(m.exts[0], StatusUnhealthy, "x")

		h := m.Route(httptest.NewRequest(http.MethodGet, "/hello", nil), false)
		require.NotNil(t, h)

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/hello", nil))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})
}
