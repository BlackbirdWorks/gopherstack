package azurearm_test

import (
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

func TestAPIMGateway_WebSocketHopByHopHeaders(t *testing.T) {
	t.Parallel()

	e, be := newWSEnv(t)

	c, resp, err := e.wsDial(t, "/ws", map[string]string{
		"X-Token": "secret", "Proxy-Authorization": "Basic cHJveHk6cHc=", "Keep-Alive": "timeout=5",
		"Proxy-Connection": "keep-alive", "Te": "trailers",
	})
	require.NoError(t, err)

	defer resp.Body.Close()
	defer c.Close()

	h := <-be.seen
	for _, name := range []string{"Proxy-Authorization", "Keep-Alive", "Proxy-Connection", "Te"} {
		assert.Empty(t, h.Get(name), name)
	}

	assert.Equal(t, "secret", h.Get("X-Token"), "end-to-end headers are kept")
}

func TestAPIMGateway_WebSocketSlotsReleased(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)

	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		defer c.Close()

		for {
			if _, _, rerr := c.ReadMessage(); rerr != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)

	e.wsAPI(t, "ws", `"https","wss"`, "ws"+strings.TrimPrefix(srv.URL, "http"))

	for i := range 300 {
		c, resp, err := e.wsDial(t, "/ws", nil)
		require.NoError(t, err, "session %d", i)
		require.NoError(t, c.Close())
		require.NoError(t, resp.Body.Close())
	}
}

func apimProviderOf(t *testing.T, e *gwEnv) *azurearm.APIMProvider {
	t.Helper()

	pr, ok := e.handler.Registry.ProviderNamed("Microsoft.ApiManagement")
	require.True(t, ok)

	prov, ok := pr.(*azurearm.APIMProvider)
	require.True(t, ok)

	return prov
}

func goroutinesIn(marker string) int {
	buf := make([]byte, 1<<22)
	buf = buf[:runtime.Stack(buf, true)]

	n := 0

	for g := range strings.SplitSeq(string(buf), "\n\n") {
		if strings.Contains(g, marker) {
			n++
		}
	}

	return n
}

//nolint:paralleltest // inspects the process's goroutines, so it must not overlap other APIM tests
func TestAPIMGateway_CloseEndsWebSocketSessions(t *testing.T) {
	e, be := newWSEnv(t)
	secret := map[string]string{"X-Token": "secret"}

	clients := make([]*websocket.Conn, 0, 2)

	for range 2 {
		c, resp, err := e.wsDial(t, "/ws", secret)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		clients = append(clients, c)
	}

	require.Equal(t, 2, goroutinesIn("(*gwState).proxyWebSocket"))

	apimProviderOf(t, e).Close()

	for _, c := range clients {
		_, _, err := c.ReadMessage()
		require.Error(t, err, "the client leg is closed")
		_ = c.Close()
	}

	<-be.closed
	<-be.closed

	assert.Zero(t, goroutinesIn("(*gwState).proxyWebSocket"), "no session goroutine remains")
	assert.Zero(t, goroutinesIn("handleUpgradeResponse"))

	_, resp, err := e.wsDial(t, "/ws", secret)
	require.Error(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, "a closed gateway refuses new sessions")
}

//nolint:paralleltest // inspects the process's goroutines, so it must not overlap other APIM tests
func TestAPIMGateway_CloseCancelsOneWayRequests(t *testing.T) {
	e := newGWEnv(t)

	started, ended := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(ended)
	}))
	t.Cleanup(srv.Close)

	e.policy(t, "/apis/echo", policyDoc(`<send-one-way-request mode="new"><set-url>`+srv.URL+
		`/audit</set-url><set-method>PUT</set-method></send-one-way-request>`, "", ""), http.StatusCreated)

	status, body, _ := e.call(t, "GET", "/echo/items/1", "", nil)
	require.Equal(t, http.StatusOK, status, body)

	<-started

	apimProviderOf(t, e).Close()

	<-ended

	assert.Zero(t, goroutinesIn("(*gwState).sendOneWay"))
}

type wsSeen struct {
	header http.Header
	cn     string
}

func newSeenWSBackend(t *testing.T, tlsCfg *tls.Config) (*httptest.Server, chan wsSeen) {
	t.Helper()

	seen := make(chan wsSeen, 4)
	up := websocket.Upgrader{}

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := wsSeen{header: r.Header.Clone()}
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			s.cn = r.TLS.PeerCertificates[0].Subject.CommonName
		}

		seen <- s

		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		defer c.Close()

		for {
			if _, _, rerr := c.ReadMessage(); rerr != nil {
				return
			}
		}
	}))

	if tlsCfg != nil {
		srv.TLS = tlsCfg
		srv.StartTLS()
	} else {
		srv.Start()
	}

	t.Cleanup(srv.Close)

	return srv, seen
}

func TestAPIMGateway_WebSocketBackendAuthentication(t *testing.T) {
	t.Parallel()

	t.Run("authentication-basic", func(t *testing.T) {
		t.Parallel()

		e := newGWEnv(t)
		srv, seen := newSeenWSBackend(t, nil)
		e.wsAPI(t, "ws", `"https","wss"`, "ws"+strings.TrimPrefix(srv.URL, "http"))
		e.policy(t, "/apis/ws", policyDoc(`<authentication-basic username="svc" password="pw" />`, "", ""),
			http.StatusCreated)

		c, resp, err := e.wsDial(t, "/ws", map[string]string{"Authorization": "Bearer client-token"})
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.NoError(t, c.Close())

		got := (<-seen).header
		assert.Equal(t, "Basic c3ZjOnB3", got.Get("Authorization"), "the policy credentials replace the client's")
	})

	t.Run("backend credentials", func(t *testing.T) {
		t.Parallel()

		e := newGWEnv(t)
		srv, seen := newSeenWSBackend(t, nil)
		e.wsAPI(t, "ws", `"https","wss"`, "ws"+strings.TrimPrefix(srv.URL, "http"))

		status, body, _ := e.raw(t, http.MethodPut, e.armPath("/backends/bk"),
			`{"properties":{"url":"`+srv.URL+`","protocol":"http","credentials":{"header":{"X-Cred":["v1"]},`+
				`"authorization":{"scheme":"Bearer","parameter":"tok"}}}}`, nil)
		require.Less(t, status, http.StatusBadRequest, body)

		e.policy(t, "/apis/ws", policyDoc(`<set-backend-service backend-id="bk" />`, "", ""), http.StatusCreated)

		c, resp, err := e.wsDial(t, "/ws", nil)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.NoError(t, c.Close())

		got := (<-seen).header
		assert.Equal(t, "v1", got.Get("X-Cred"))
		assert.Equal(t, "Bearer tok", got.Get("Authorization"))
	})

	t.Run("authentication-certificate", func(t *testing.T) {
		t.Parallel()

		e := newGWEnv(t)
		_, der, pfx := rsaCertPFX(t, "ws-client.example")
		sum := sha1.Sum(der)
		thumb := strings.ToUpper(hex.EncodeToString(sum[:]))

		srv, seen := newSeenWSBackend(
			t,
			&tls.Config{ClientAuth: tls.RequireAnyClientCert, MinVersion: tls.VersionTLS12},
		)

		pool := x509.NewCertPool()
		pool.AddCert(srv.Certificate())
		e.prov.SetBackendRootCAs(pool)

		status, body, _ := e.raw(t, http.MethodPut, e.armPath("/certificates/cl"),
			`{"properties":{"data":"`+base64.StdEncoding.EncodeToString(pfx)+`","password":"pw"}}`, nil)
		require.Less(t, status, http.StatusBadRequest, body)

		e.wsAPI(t, "ws", `"https","wss"`, "wss"+strings.TrimPrefix(srv.URL, "https"))
		e.policy(t, "/apis/ws", policyDoc(`<authentication-certificate thumbprint="`+thumb+`" />`, "", ""),
			http.StatusCreated)

		c, resp, err := e.wsDial(t, "/ws", nil)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.NoError(t, c.Close())

		assert.Equal(t, "ws-client.example", (<-seen).cn)
	})
}
