package azurearm

import (
	"errors"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

const (
	apimWSMaxSessions = 256
	apimWSMaxLifetime = time.Hour
	apimSchemeWS      = "ws"
	apimSchemeWSS     = "wss"
)

var errAPIMNotUpgrade = errors.New("the API is a WebSocket API and the request is not a WebSocket upgrade")

func apimIsWebSocket(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

func apimWSScheme(tls bool) string {
	if tls {
		return apimSchemeWSS
	}

	return apimSchemeWS
}

// apimHTTPScheme maps the ws and wss schemes of a WebSocket serviceUrl to the scheme used to dial the backend.
func apimHTTPScheme(s string) string {
	switch s {
	case apimSchemeWS:
		return acaScaleHTTP
	case apimSchemeWSS:
		return acaSchemeHTTPS
	}

	return s
}

// wsTransport closes an upgraded connection after its maximum lifetime.
type wsTransport struct {
	base http.RoundTripper
	max  time.Duration
}

func (t wsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil || resp.StatusCode != http.StatusSwitchingProtocols {
		return resp, err
	}

	if rwc, ok := resp.Body.(io.ReadWriteCloser); ok {
		resp.Body = &wsConn{ReadWriteCloser: rwc, timer: time.AfterFunc(t.max, func() { _ = rwc.Close() })}
	}

	return resp, nil
}

type wsConn struct {
	io.ReadWriteCloser

	timer *time.Timer
}

func (c *wsConn) Close() error {
	c.timer.Stop()

	return c.ReadWriteCloser.Close()
}

// proxyWebSocket passes a WebSocket handshake, with the policies already applied to it, to the backend and then
// copies frames both ways until either side closes.
func (g *gwState) proxyWebSocket(u *url.URL) {
	select {
	case g.p.wsSlots <- struct{}{}:
		defer func() { <-g.p.wsSlots }()
	default:
		g.failR(http.StatusServiceUnavailable, "BackendConnectionFailure", "Too many WebSocket connections are open.")

		return
	}

	rp := &httputil.ReverseProxy{
		Transport: wsTransport{base: g.httpClient().Transport, max: apimWSMaxLifetime},
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL = u
			pr.Out.Host = u.Host
			pr.Out.Header = http.Header{}
			copyForwardHeaders(pr.Out.Header, g.header)
			pr.Out.Header.Set(apimHeaderXForward, hostOnly(g.r.RemoteAddr))
			pr.Out.Header.Set("Upgrade", "websocket")
			pr.Out.Header.Set("Connection", "Upgrade")
		},
		ErrorHandler: func(_ http.ResponseWriter, _ *http.Request, err error) { g.upstreamError(err) },
	}

	rp.ServeHTTP(g.w, g.r)

	if !g.errored {
		g.wrote = true
	}
}
