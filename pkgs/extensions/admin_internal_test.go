package extensions

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startedHelper(t *testing.T) (*Manager, string) {
	t.Helper()

	reg := Registration{
		Routes: Routes{PathPrefixes: []string{"/hello"}},
		Hooks:  Hooks{OnStartup: "/hooks/startup"},
	}
	m, logFile := newHelperManager(t, Spec{Name: "hello", HealthInterval: time.Hour}, reg, false)

	m.Start(t.Context())
	t.Cleanup(func() { m.Stop(context.WithoutCancel(t.Context())) })
	require.Eventually(t, func() bool { return statusOf(m) == StatusReady }, 20*time.Second, 20*time.Millisecond)

	return m, logFile
}

func TestManualRestart(t *testing.T) {
	t.Parallel()

	m, logFile := startedHelper(t)

	require.NoError(t, m.Restart("hello"))
	require.Eventually(t, func() bool {
		b, err := os.ReadFile(logFile)

		return err == nil && strings.Count(string(b), "/hooks/startup\n") == 2 && statusOf(m) == StatusReady
	}, 20*time.Second, 20*time.Millisecond)

	assert.Equal(t, 1, m.List()[0].Restarts)
	assert.ErrorIs(t, m.Restart("nope"), ErrUnknownExtension)
}

func TestRestartRevivesFailedExtension(t *testing.T) {
	t.Parallel()

	m, _ := newHelperManager(t, Spec{Name: "hello", Restart: RestartNever}, Registration{}, true)

	m.Start(t.Context())
	t.Cleanup(func() { m.Stop(context.WithoutCancel(t.Context())) })
	require.Eventually(t, func() bool { return statusOf(m) == StatusFailed }, 20*time.Second, 20*time.Millisecond)

	require.NoError(t, m.Restart("hello"))
	require.Eventually(t, func() bool {
		lines, _ := m.Output("hello")

		return len(lines) == 2
	}, 20*time.Second, 20*time.Millisecond)
}

func TestOutputRedactsToken(t *testing.T) {
	t.Parallel()

	m, _ := startedHelper(t)

	m.mu.RLock("test")
	tok := m.exts[0].token
	m.mu.RUnlock()

	var lines []string

	require.Eventually(t, func() bool {
		lines, _ = m.Output("hello")

		return len(lines) > 0
	}, 20*time.Second, 20*time.Millisecond)

	assert.Equal(t, []string{"stdout: token is [redacted]"}, lines)
	assert.NotContains(t, strings.Join(lines, "\n"), tok)

	_, err := m.Output("nope")
	require.ErrorIs(t, err, ErrUnknownExtension)
}

func TestAdminHandlers(t *testing.T) {
	t.Parallel()

	m, _ := startedHelper(t)

	tests := []struct {
		call   func(w http.ResponseWriter, r *http.Request, name string)
		name   string
		remote string
		ext    string
		want   int
	}{
		{name: "restart ok", call: m.ServeRestart, remote: "127.0.0.1:1", ext: "hello", want: http.StatusAccepted},
		{name: "restart remote", call: m.ServeRestart, remote: "10.1.2.3:1", ext: "hello", want: http.StatusForbidden},
		{name: "restart unknown", call: m.ServeRestart, remote: "127.0.0.1:1", ext: "x", want: http.StatusNotFound},
		{name: "output ok", call: m.ServeOutput, remote: "[::1]:1", ext: "hello", want: http.StatusOK},
		{name: "output remote", call: m.ServeOutput, remote: "10.1.2.3:1", ext: "hello", want: http.StatusForbidden},
		{name: "output unknown", call: m.ServeOutput, remote: "127.0.0.1:1", ext: "x", want: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = tt.remote
			rec := httptest.NewRecorder()

			tt.call(rec, req, tt.ext)
			assert.Equal(t, tt.want, rec.Code)
		})
	}
}
