package elbv2

import (
	"bufio"
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	readHeaderTimeout = 30 * time.Second
	tlsVersionHeader  = "X-Amzn-Tls-Version"
	tlsCipherHeader   = "X-Amzn-Tls-Cipher-Suite"
	xffModeAttr       = "routing.http.xff_header_processing.mode"
	xffClientPortAttr = "routing.http.xff_client_port.enabled"
)

// serveHTTPListener starts an HTTP(S) server for an ALB listener. Callers must hold b.mu.
func (b *InMemoryBackend) serveHTTPListener(bl *boundListener, h *Handler, arn string, secure bool) {
	srv := &http.Server{
		ReadHeaderTimeout: readHeaderTimeout,
		ErrorLog:          slog.NewLogLogger(logger.Load(bl.ctx).Handler(), slog.LevelDebug),
		BaseContext:       func(net.Listener) context.Context { return bl.ctx },
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			l, lb, ok := b.listenerView(arn)
			if !ok {
				writeDPError(w, http.StatusServiceUnavailable, msgNoTargets)

				return
			}

			h.serveListenerHTTP(w, r, b, lb, l)
		}),
	}

	ln := bl.tcp
	if secure {
		ln = tls.NewListener(ln, &tls.Config{GetConfigForClient: b.clientConfig(arn, false)})
	}

	bl.srv = srv

	bl.wg.Go(func() {
		_ = srv.Serve(ln)
	})
}

func (h *Handler) serveListenerHTTP(
	w http.ResponseWriter,
	r *http.Request,
	be StorageBackend,
	lb *LoadBalancer,
	l *Listener,
) {
	if r.TLS != nil {
		r.Header.Set(tlsVersionHeader, tlsVersionName(r.TLS.Version))
		r.Header.Set(tlsCipherHeader, opensslCipherName(r.TLS.CipherSuite))
	}

	raw := r.URL.EscapedPath()
	if raw == "" {
		raw = "/"
	}

	ip, port := clientAddrPort(r)
	h.route(w, &dpRequest{
		r: r, be: be, lb: lb, listener: l, path: r.URL.Path, rawPath: raw,
		host: hostWithoutPort(r.Host), client: ip, clientPort: port,
	})
}

func clientAddrPort(r *http.Request) (netip.Addr, uint16) {
	ap, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		return clientAddr(r), 0
	}

	return ap.Addr().Unmap(), ap.Port()
}

// xffValue renders the client entry appended to X-Forwarded-For per routing.http.xff_client_port.enabled.
func xffValue(req *dpRequest) string {
	ip := req.client.String()

	if req.lb == nil || req.lb.Attributes[xffClientPortAttr] != attrValueTrue || req.clientPort == 0 {
		return ip
	}

	if req.client.Is6() {
		ip = "[" + ip + "]"
	}

	return ip + ":" + strconv.Itoa(int(req.clientPort))
}

func applyXFF(h http.Header, req *dpRequest) {
	switch req.lb.Attributes[xffModeAttr] {
	case "preserve":
		return
	case "remove":
		h.Del("X-Forwarded-For")

		return
	}

	v := xffValue(req)
	if prior := h.Get("X-Forwarded-For"); prior != "" {
		v = prior + ", " + v
	}

	h.Set("X-Forwarded-For", v)
}

func isUpgradeRequest(r *http.Request) bool {
	if r.Header.Get("Upgrade") == "" {
		return false
	}

	for _, v := range r.Header.Values("Connection") {
		for tok := range strings.SplitSeq(v, ",") {
			if strings.EqualFold(strings.TrimSpace(tok), "upgrade") {
				return true
			}
		}
	}

	return false
}

// proxyUpgrade tunnels a connection upgrade (WebSocket) to the target: the request goes through
// unmodified apart from the forwarding headers, and a 101 turns both sockets into a raw pipe.
func (h *Handler) proxyUpgrade(
	w http.ResponseWriter,
	req *dpRequest,
	ap netip.AddrPort,
	secure bool,
	sticky string,
	tg *TargetGroup,
) {
	ctx := req.r.Context()

	d := net.Dialer{Timeout: dialTimeout}

	tconn, err := d.DialContext(ctx, "tcp", ap.String())
	if err != nil {
		writeDPError(w, http.StatusBadGateway, msgBadGateway)

		return
	}

	defer tconn.Close()

	if secure {
		tc := tls.Client(tconn, targetTLSConfig())
		if err = tc.HandshakeContext(ctx); err != nil {
			writeDPError(w, http.StatusBadGateway, msgBadGateway)

			return
		}

		tconn = tc
	}

	target := req.rawPath
	if q := req.r.URL.RawQuery; q != "" {
		target += "?" + q
	}

	out, err := http.NewRequestWithContext(ctx, req.r.Method, "http://"+ap.String()+target, nil)
	if err != nil {
		writeDPError(w, http.StatusBadGateway, msgBadGateway)

		return
	}

	for k, vs := range req.r.Header {
		out.Header[k] = append([]string(nil), vs...)
	}

	setForwardHeaders(out.Header, req)
	out.Host = req.r.Host

	if err = out.Write(tconn); err != nil {
		writeDPError(w, http.StatusBadGateway, msgBadGateway)

		return
	}

	tr := bufio.NewReader(tconn)

	resp, err := http.ReadResponse(tr, out)
	if err != nil {
		writeDPError(w, http.StatusBadGateway, msgBadGateway)

		return
	}

	if resp.StatusCode != http.StatusSwitchingProtocols {
		defer resp.Body.Close()

		copyHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)

		return
	}

	if sticky != "" {
		resp.Header.Add("Set-Cookie", stickyCookie(tg, sticky))
	}

	cconn, crw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return
	}

	defer cconn.Close()

	if err = resp.Write(cconn); err != nil {
		return
	}

	pipeConns(ctx, cconn, crw.Reader, tconn, tr)
}
