package extensions

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func newHelperManager(t *testing.T, spec Spec, reg Registration, crash bool) (*Manager, string) {
	t.Helper()

	logFile := filepath.Join(t.TempDir(), "events.log")
	spec.Command = []string{os.Args[0], "-test.run=NONE"}
	spec.Env = map[string]string{
		helperEnv:     "1",
		"HELPER_LOG":  logFile,
		"HELPER_REG":  mustJSON(reg),
		"HELPER_NAME": spec.Name,
	}

	if crash {
		spec.Env["HELPER_CRASH"] = "1"
	}

	cfg, err := Config{Extensions: []Spec{spec}}.normalize()
	require.NoError(t, err)

	mux := http.NewServeMux()
	m := New(cfg, Options{Logger: logger.NewTestLogger()})
	mux.HandleFunc("POST /_gopherstack/extensions/register", m.ServeRegister)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	m.opts.ControlURL = srv.URL

	return m, logFile
}

func statusOf(m *Manager) Status { return m.List()[0].Status }

func TestExtensionProcessLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		shadowed bool
		override bool
		claimed  bool
	}{
		{name: "unshadowed", claimed: true},
		{name: "shadowed without override", shadowed: true},
		{name: "shadowed with override", shadowed: true, override: true, claimed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reg := Registration{
				Version: "1.2.3",
				Routes:  Routes{PathPrefixes: []string{"/hello"}},
				Hooks:   Hooks{OnStartup: "/hooks/startup", OnReady: "/hooks/ready", OnShutdown: "/hooks/shutdown"},
			}
			m, logFile := newHelperManager(
				t,
				Spec{Name: "hello", AllowOverride: tt.override, HealthPath: "/health"},
				reg,
				false,
			)

			m.Start(t.Context())
			require.Eventually(
				t,
				func() bool { return statusOf(m) == StatusReady },
				20*time.Second,
				20*time.Millisecond,
			)

			h := m.Route(httptest.NewRequest(http.MethodGet, "/hello?x=1", nil), tt.shadowed)
			if !tt.claimed {
				assert.Nil(t, h)
			} else {
				require.NotNil(t, h)
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/hello?x=1", nil))
				assert.Equal(t, "hello from hello x=1", rec.Body.String())
			}

			assert.Nil(t, m.Route(httptest.NewRequest(http.MethodGet, "/other", nil), false))

			m.MarkReady(t.Context())
			m.Stop(context.WithoutCancel(t.Context()))

			assert.Equal(t, StatusStopped, statusOf(m))

			ev, err := os.ReadFile(logFile)
			require.NoError(t, err)
			assert.Equal(t, "/hooks/startup\n/hooks/ready\n/hooks/shutdown\nsigterm\n", string(ev))
		})
	}
}

func TestExtensionListHidesToken(t *testing.T) {
	t.Parallel()

	m, _ := newHelperManager(
		t,
		Spec{Name: "hello"},
		Registration{Routes: Routes{PathPrefixes: []string{"/hello"}}},
		false,
	)
	m.Start(t.Context())
	t.Cleanup(func() { m.Stop(context.WithoutCancel(t.Context())) })

	require.Eventually(t, func() bool { return statusOf(m) == StatusReady }, 20*time.Second, 20*time.Millisecond)

	m.mu.RLock("test")
	tok := m.exts[0].token
	m.mu.RUnlock()

	rec := httptest.NewRecorder()
	m.ServeList(rec, httptest.NewRequest(http.MethodGet, "/_gopherstack/extensions", nil))

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, tok)
	assert.NotContains(t, rec.Body.String(), tok)
	assert.Contains(t, rec.Body.String(), `"status":"ready"`)
}

func TestExtensionRestartPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		restart      string
		wantStatus   Status
		wantRestarts int
	}{
		{name: "on-failure exhausts", restart: RestartOnFailure, wantStatus: StatusFailed, wantRestarts: 2},
		{name: "never", restart: RestartNever, wantStatus: StatusFailed, wantRestarts: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spec := Spec{Name: "crashy", Restart: tt.restart, MaxRestarts: 2, RestartBackoff: 5 * time.Millisecond}
			m, _ := newHelperManager(t, spec, Registration{Routes: Routes{PathPrefixes: []string{"/c"}}}, true)

			m.Start(t.Context())
			t.Cleanup(func() { m.Stop(context.WithoutCancel(t.Context())) })

			require.Eventually(t, func() bool {
				return statusOf(m) == tt.wantStatus && m.List()[0].Restarts == tt.wantRestarts
			}, 30*time.Second, 20*time.Millisecond)

			assert.Nil(t, m.Route(httptest.NewRequest(http.MethodGet, "/c", nil), false))
			assert.Contains(t, m.List()[0].LastError, "exited")
		})
	}
}

func TestExtensionStartTimeout(t *testing.T) {
	t.Parallel()

	cfg, err := Config{Extensions: []Spec{{
		Name: "silent", Command: []string{os.Args[0], "-test.run=NONE"}, Restart: RestartNever,
		StartTimeout: 50 * time.Millisecond, Env: map[string]string{helperEnv: "1", "HELPER_REG": "{}"},
	}}}.normalize()
	require.NoError(t, err)

	// no control URL: registration cannot succeed, so the helper exits and the start fails
	m := New(cfg, Options{Logger: logger.NewTestLogger(), ControlURL: "http://127.0.0.1:1"})
	m.Start(t.Context())
	t.Cleanup(func() { m.Stop(context.WithoutCancel(t.Context())) })

	require.Eventually(t, func() bool { return statusOf(m) == StatusFailed }, 20*time.Second, 20*time.Millisecond)
}
