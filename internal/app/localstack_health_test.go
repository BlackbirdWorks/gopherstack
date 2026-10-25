package app

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type resettableStub struct {
	service.Registerable
	resets *atomic.Int32
}

func (r resettableStub) Reset() { r.resets.Add(1) }

func TestLocalstackHealthPost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		remote     string
		allowKill  bool
		wantStatus int
		wantResets int32
		wantKills  int32
	}{
		{name: "restart", body: `{"action":"restart"}`, remote: "10.0.0.5:1234", wantStatus: 200, wantResets: 1},
		{name: "restart_case", body: `{"action":"RESTART"}`, remote: "127.0.0.1:1", wantStatus: 200, wantResets: 1},
		{name: "kill_disabled", body: `{"action":"kill"}`, remote: "127.0.0.1:1", wantStatus: 403},
		{name: "kill_remote", body: `{"action":"kill"}`, remote: "10.0.0.5:1", allowKill: true, wantStatus: 403},
		{name: "kill_loopback", body: `{"action":"kill"}`, remote: "127.0.0.1:1", allowKill: true, wantStatus: 200, wantKills: 1},
		{name: "kill_loopback_v6", body: `{"action":"kill"}`, remote: "[::1]:1", allowKill: true, wantStatus: 200, wantKills: 1},
		{name: "unknown", body: `{"action":"nope"}`, remote: "127.0.0.1:1", allowKill: true, wantStatus: 400},
		{name: "bad_json", body: `{`, remote: "127.0.0.1:1", wantStatus: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var resets, kills atomic.Int32

			killed := make(chan struct{}, 1)
			h := buildLocalstackHealthPost(
				[]service.Registerable{resettableStub{resets: &resets}},
				tt.allowKill,
				func() { kills.Add(1); killed <- struct{}{} },
			)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/_localstack/health", strings.NewReader(tt.body))
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			req.RemoteAddr = tt.remote
			rec := httptest.NewRecorder()

			require.NoError(t, h(e.NewContext(req, rec)))
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantResets, resets.Load())

			if tt.wantKills > 0 {
				<-killed
			}

			assert.Equal(t, tt.wantKills, kills.Load())
		})
	}
}

func TestLocalstackSwaggerEndpoints(t *testing.T) {
	t.Parallel()

	srv, _ := devEndpointsServer(t)

	status, raw := devDo(t, http.MethodGet, srv.URL+"/_localstack/swagger", "")
	require.Equal(t, http.StatusOK, status)
	assert.Contains(t, string(raw), "/_localstack/swagger/openapi")

	status, raw = devDo(t, http.MethodGet, srv.URL+"/_localstack/swagger/openapi", "")
	require.Equal(t, http.StatusOK, status)
	assert.Contains(t, string(raw), `"/_localstack/health"`)
}

func TestTransparentInjectionDisabled(t *testing.T) {
	t.Parallel()

	cli := &CLI{}
	cli.Transparent.Enabled = true
	cli.Transparent.DisableInjection = true

	tr, srv, err := startDNSAndTransparent(t.Context(), cli)
	require.NoError(t, err)
	assert.Nil(t, tr)
	assert.Nil(t, srv)
}
