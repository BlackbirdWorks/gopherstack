package azurearm_test

import (
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type wsBackend struct {
	srv    *httptest.Server
	seen   chan http.Header
	paths  chan string
	closed chan struct{}
	kick   chan struct{}
	once   sync.Once
}

func (b *wsBackend) kickNow() { b.once.Do(func() { close(b.kick) }) }

func newWSBackend(t *testing.T) *wsBackend {
	t.Helper()

	b := &wsBackend{
		seen: make(chan http.Header, 8), paths: make(chan string, 8),
		closed: make(chan struct{}, 8), kick: make(chan struct{}),
	}

	up := websocket.Upgrader{}
	b.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.seen <- r.Header.Clone()
		b.paths <- r.URL.Path

		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		defer c.Close()

		go func() {
			<-b.kick
			_ = c.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseGoingAway, "bye"),
				time.Now().Add(time.Second),
			)
			_ = c.Close()
		}()

		for {
			mt, msg, rerr := c.ReadMessage()
			if rerr != nil {
				b.closed <- struct{}{}

				return
			}

			_ = c.WriteMessage(mt, append([]byte("echo:"), msg...))
		}
	}))
	t.Cleanup(func() {
		b.kickNow()
		b.srv.Close()
	})

	return b
}

func (e *gwEnv) wsAPI(t *testing.T, id, protocols, serviceURL string) {
	t.Helper()

	status, body, _ := e.raw(t, http.MethodPut, e.armPath("/apis/"+id),
		`{"properties":{"displayName":"`+id+`","path":"`+id+`","protocols":[`+protocols+`],"serviceUrl":"`+serviceURL+
			`","type":"websocket"}}`, nil)
	require.Less(t, status, http.StatusBadRequest, body)

	e.putOp(t, id, "onhandshake", "get", "/")
}

func (e *gwEnv) wsDial(t *testing.T, path string, hdr map[string]string) (*websocket.Conn, *http.Response, error) {
	t.Helper()

	tr, ok := e.hc.Transport.(*http.Transport)
	require.True(t, ok)

	h := http.Header{"Ocp-Apim-Subscription-Key": {e.key}}
	for k, v := range hdr {
		h.Set(k, v)
	}

	d := websocket.Dialer{TLSClientConfig: tr.TLSClientConfig.Clone()}
	d.TLSClientConfig.MinVersion = tls.VersionTLS12

	return d.DialContext(
		t.Context(),
		"wss://"+strings.TrimPrefix(e.url, "https://")+"/_apim/"+apimSvc+path,
		h,
	)
}

func newWSEnv(t *testing.T) (*gwEnv, *wsBackend) {
	t.Helper()

	e := newGWEnv(t)
	be := newWSBackend(t)
	e.wsAPI(t, "ws", `"https","wss"`, "ws"+strings.TrimPrefix(be.srv.URL, "http")+"/chat")

	e.policy(t, "/apis/ws", policyDoc(headerPolicy("X-Policy", "applied")+
		checkHeader("X-Token", "401", "token required", "false", "secret"), "", ""), http.StatusCreated)

	return e, be
}

func TestAPIMGateway_WebSocket(t *testing.T) {
	t.Parallel()

	secret := map[string]string{"X-Token": "secret"}

	t.Run("handshake rejected by the inbound policy", func(t *testing.T) {
		t.Parallel()

		e, be := newWSEnv(t)

		_, resp, err := e.wsDial(t, "/ws", nil)
		require.ErrorIs(t, err, websocket.ErrBadHandshake)
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		assert.Empty(t, be.seen, "the backend never saw the handshake")
	})

	t.Run("frames pass both ways", func(t *testing.T) {
		t.Parallel()

		e, be := newWSEnv(t)

		c, resp, err := e.wsDial(t, "/ws", secret)
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)

		require.NoError(t, c.WriteMessage(websocket.TextMessage, []byte("hello")))

		_, msg, err := c.ReadMessage()
		require.NoError(t, err)
		assert.Equal(t, "echo:hello", string(msg))

		h := <-be.seen
		assert.Equal(t, "applied", h.Get("X-Policy"), "the handshake policies ran")
		assert.Equal(t, "/chat/", <-be.paths)

		require.NoError(t, c.Close())
		<-be.closed
	})

	t.Run("backend close ends the client", func(t *testing.T) {
		t.Parallel()

		e, be := newWSEnv(t)

		c, resp, err := e.wsDial(t, "/ws", secret)
		require.NoError(t, err)

		defer resp.Body.Close()
		defer c.Close()

		<-be.seen
		<-be.paths

		be.kickNow()

		_, _, err = c.ReadMessage()
		require.Error(t, err)
		assert.True(t, websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure), "%v", err)
	})

	t.Run("plain request", func(t *testing.T) {
		t.Parallel()

		e, _ := newWSEnv(t)

		status, body, _ := e.call(t, "GET", "/ws", "", secret)
		assert.Equal(t, http.StatusInternalServerError, status)
		assert.Contains(t, body, "not a WebSocket upgrade")
	})
}

func TestAPIMGateway_WebSocketProtocols(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	be := newWSBackend(t)
	e.wsAPI(t, "wsonly", `"ws"`, "ws"+strings.TrimPrefix(be.srv.URL, "http"))

	_, resp, err := e.wsDial(t, "/wsonly", nil)
	require.Error(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode, "the API does not offer wss")
}

func newHalfCloseWSBackend(t *testing.T) *httptest.Server {
	t.Helper()

	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		defer c.Close()

		if tc, ok := c.UnderlyingConn().(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}

		for {
			if _, _, rerr := c.ReadMessage(); rerr != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)

	return srv
}

func TestAPIMGateway_WebSocketTeardown(t *testing.T) {
	t.Parallel()

	secret := map[string]string{"X-Token": "secret"}

	tests := []struct {
		stop     func(*gwEnv)
		name     string
		halfOpen bool
	}{
		{name: "close", stop: func(e *gwEnv) { e.prov.Close() }},
		{name: "reset", stop: func(e *gwEnv) { e.prov.Reset() }},
		{name: "close with half-closed backend", stop: func(e *gwEnv) { e.prov.Close() }, halfOpen: true},
		{name: "reset with half-closed backend", stop: func(e *gwEnv) { e.prov.Reset() }, halfOpen: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var e *gwEnv

			if tt.halfOpen {
				e = newGWEnv(t)
				srv := newHalfCloseWSBackend(t)
				e.wsAPI(t, "ws", `"https","wss"`, "ws"+strings.TrimPrefix(srv.URL, "http"))
			} else {
				e, _ = newWSEnv(t)
			}

			c, resp, err := e.wsDial(t, "/ws", secret)
			require.NoError(t, err)

			defer resp.Body.Close()
			defer c.Close()

			if !tt.halfOpen {
				require.NoError(t, c.WriteMessage(websocket.TextMessage, []byte("hello")))

				_, _, err = c.ReadMessage()
				require.NoError(t, err)
			}

			start := time.Now()
			tt.stop(e)
			assert.Less(t, time.Since(start), 5*time.Second, "teardown ended the session, not its wait bound")

			require.NoError(t, c.SetReadDeadline(time.Now().Add(5*time.Second)))

			_, _, err = c.ReadMessage()
			require.Error(t, err)

			var ne net.Error
			assert.False(t, errors.As(err, &ne) && ne.Timeout(), "the gateway must end the client side: %v", err)
		})
	}
}
