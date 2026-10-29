package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/extensions"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

type extTestService struct {
	name     string
	path     string
	priority int
}

func (s *extTestService) Name() string { return s.name }

func (s *extTestService) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error { return c.String(http.StatusOK, "builtin-"+s.name) }
}

func (s *extTestService) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		p := c.Request().URL.Path

		return !strings.HasPrefix(p, "/_gopherstack/") && strings.HasPrefix(p, s.path)
	}
}

func (s *extTestService) GetSupportedOperations() []string      { return []string{"Op"} }
func (s *extTestService) ExtractOperation(*echo.Context) string { return "Op" }
func (s *extTestService) ExtractResource(*echo.Context) string  { return "r" }
func (s *extTestService) MatchPriority() int                    { return s.priority }

func TestExtensionRoutingAndHooks(t *testing.T) {
	t.Parallel()

	var preCalls atomic.Int32

	ext := http.NewServeMux()
	ext.HandleFunc("/ext/ping", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "pong") })
	ext.HandleFunc(
		"/dash/claimed",
		func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "from-ext") },
	)
	ext.HandleFunc("/hook/pre", func(http.ResponseWriter, *http.Request) { preCalls.Add(1) })
	ext.HandleFunc("/hook/post", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"body":"cG9zdGVk","set_headers":{"X-Post":"1"}}`)
	})

	extSrv := httptest.NewServer(ext)
	t.Cleanup(extSrv.Close)

	dir := t.TempDir()
	regFile := filepath.Join(dir, "reg.json")
	reg, err := json.Marshal(map[string]any{
		"endpoint": extSrv.URL,
		"routes":   map[string]any{"path_prefixes": []string{"/ext/", "/dash/claimed"}},
		"hooks": map[string]any{"request": []map[string]any{
			{"services": []string{"Dash"}, "pre": "/hook/pre", "post": "/hook/post"},
		}},
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(regFile, reg, 0o600))

	// Never exits: CI runs internal/app for 12+ minutes and the parallel request
	// subtests below can be queued that long after the readiness check.
	script := fmt.Sprintf(`curl -sf -X POST -H "Authorization: Bearer $GOPHERSTACK_EXTENSION_TOKEN" -d @%s `+
		`"$GOPHERSTACK_CONTROL_URL/_gopherstack/extensions/register" && exec tail -f /dev/null`, regFile)
	cfgFile := filepath.Join(dir, "ext.json")
	cfg, err := json.Marshal(map[string]any{"extensions": []map[string]any{
		{"name": "t", "command": []string{"sh", "-c", script}, "health_interval": "1h"},
	}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(cfgFile, cfg, 0o600))

	e := echo.New()
	srv := httptest.NewUnstartedServer(e)
	cli := &CLI{
		Port:       ":" + strings.Split(srv.Listener.Addr().String(), ":")[1],
		Extensions: extensions.Settings{Config: cfgFile},
	}
	log := logger.NewTestLogger()
	require.NoError(t, cli.initExtensions(log, e))

	reg2 := service.NewRegistry()
	require.NoError(t, reg2.Register(&extTestService{name: "Dash", path: "/dash", priority: service.PriorityPathUI}))
	require.NoError(t, reg2.Register(&extTestService{name: "S3", priority: service.PriorityCatchAll}))
	e.Use(service.NewServiceRouter(reg2).WithInterceptor(cli.extensionInterceptor()).RouteHandler())

	srv.Start()
	t.Cleanup(srv.Close)

	cli.startExtensions(t.Context())
	t.Cleanup(func() { cli.stopExtensions(context.WithoutCancel(t.Context())) })

	get := func(path string) (int, string, http.Header) {
		resp, gerr := testclient.New(t).Get(srv.URL + path)
		require.NoError(t, gerr)

		defer resp.Body.Close()

		b, _ := io.ReadAll(resp.Body)

		return resp.StatusCode, string(b), resp.Header
	}

	require.Eventually(t, func() bool {
		code, body, _ := get("/_gopherstack/extensions")

		return code == 200 && strings.Contains(body, `"status":"ready"`)
	}, 20*time.Second, 20*time.Millisecond)

	_, body, _ := get("/_gopherstack/extensions")
	assert.NotContains(t, body, "GOPHERSTACK_EXTENSION_TOKEN")

	tests := []struct {
		name     string
		path     string
		wantBody string
		wantHdr  string
	}{
		{name: "claimed beats catch-all", path: "/ext/ping", wantBody: "pong"},
		{name: "builtin wins unless override", path: "/dash/claimed", wantBody: "posted", wantHdr: "1"},
		{name: "unclaimed falls to catch-all", path: "/bucket/key", wantBody: "builtin-S3"},
	}

	t.Run("requests", func(t *testing.T) {
		t.Parallel()

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				code, got, hdr := get(tt.path)
				assert.Equal(t, http.StatusOK, code)
				assert.Equal(t, tt.wantBody, got)
				assert.Equal(t, tt.wantHdr, hdr.Get("X-Post"))
			})
		}
	})

	t.Cleanup(func() { assert.Equal(t, int32(1), preCalls.Load()) })
}

func TestExtensionInterceptorDisabled(t *testing.T) {
	t.Parallel()

	cli := &CLI{}
	assert.Nil(t, cli.extensionInterceptor())
	cli.startExtensions(t.Context())
	cli.stopExtensions(t.Context())
}
