package elbv2

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"slices"
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

// proxyUpgrade forwards a connection upgrade (WebSocket) to the target via httputil.ReverseProxy,
// which switches to a raw pipe on a 101.
func (h *Handler) proxyUpgrade(
	w http.ResponseWriter,
	req *dpRequest,
	ap netip.AddrPort,
	scheme string,
	sticky string,
	tg *TargetGroup,
) {
	rp := &httputil.ReverseProxy{
		Transport: h.dp.client.Transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = scheme
			pr.Out.URL.Host = ap.String()
			pr.Out.Host = pr.In.Host
			pr.Out.Header["X-Forwarded-For"] = slices.Clone(pr.In.Header.Values("X-Forwarded-For"))
			setForwardHeaders(pr.Out.Header, req)
		},
		ModifyResponse: func(resp *http.Response) error {
			if sticky != "" && resp.StatusCode == http.StatusSwitchingProtocols {
				resp.Header.Add("Set-Cookie", stickyCookie(tg, sticky))
			}

			return nil
		},
		ErrorHandler: func(ew http.ResponseWriter, _ *http.Request, _ error) {
			writeDPError(ew, http.StatusBadGateway, msgBadGateway)
		},
	}

	rp.ServeHTTP(w, req.r)
}
