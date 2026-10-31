package ociloadbalancer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	defaultRequestIDHeader = "X-Request-Id"
	routeCookie            = "X-Oracle-BMC-LBS-Route"
	dialTimeout            = 10 * time.Second
	pipeDirections         = 2
)

func hostOnly(h string) string {
	if host, _, err := net.SplitHostPort(h); err == nil {
		return host
	}

	return strings.Trim(h, "[]")
}

func hostMatches(pattern, host string) bool {
	p, h := strs.Fold(pattern), strs.Fold(host)

	switch {
	case strings.HasPrefix(p, "*."):
		return strings.HasSuffix(h, p[1:]) && len(h) > len(p)-1
	case strings.HasSuffix(p, ".*"):
		return strings.HasPrefix(h, p[:len(p)-1]) && len(h) > len(p)-1
	default:
		return p == h
	}
}

// listenersOn returns the listeners bound to a port, ordered by name.
func listenersOn(lb *LoadBalancer, port int) []*Listener {
	var out []*Listener

	for _, l := range lb.Listeners {
		if l.Port == port {
			out = append(out, l)
		}
	}

	slices.SortFunc(out, func(a, b *Listener) int { return strings.Compare(a.Name, b.Name) })

	return out
}

// pickListener chooses the listener on a port for a virtual host: an exact hostname match, then a wildcard
// match, then a listener without hostnames.
func pickListener(lb *LoadBalancer, port int, host string) *Listener {
	group := listenersOn(lb, port)

	var wild, open *Listener

	for _, l := range group {
		if len(l.Hostnames) == 0 {
			open = l

			continue
		}

		for _, hn := range l.Hostnames {
			pat := lb.Hostnames[hn].Hostname
			if strs.Equal(pat, host) {
				return l
			}

			if wild == nil && hostMatches(pat, host) {
				wild = l
			}
		}
	}

	if wild != nil {
		return wild
	}

	return open
}

func (d *dataPlane) selectSet(lb *LoadBalancer, l *Listener, r *http.Request) string {
	if l.Policy != "" {
		cc := newCondCtx(r)

		for _, rule := range lb.RoutingPolicies[l.Policy].Rules {
			if n, err := parseCondition(rule.Condition); err == nil && n.eval(cc) {
				return rule.Actions[0].BackendSetName
			}
		}

		return l.DefaultSet
	}

	if l.PathRoutes != "" {
		if name, ok := routePath(lb.PathRouteSets[l.PathRoutes], r.URL.Path); ok {
			return name
		}
	}

	return l.DefaultSet
}

func routeValue(be Backend) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(be.Name))

	return strconv.FormatUint(h.Sum64(), 16)
}

func ruleSetsOf(lb *LoadBalancer, l *Listener) []*RuleSet {
	out := make([]*RuleSet, 0, len(l.RuleSets))
	for _, n := range l.RuleSets {
		out = append(out, lb.RuleSets[n])
	}

	return out
}

func (d *dataPlane) serveHTTP(w http.ResponseWriter, r *http.Request, s portSpec) {
	lb := d.b.activeLB(s.lbID)
	if lb == nil {
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)

		return
	}

	l := pickListener(lb, s.port, hostOnly(r.Host))
	if l == nil {
		http.Error(w, "Not Found", http.StatusNotFound)

		return
	}

	sc := reqScope{scheme: schemeHTTP, port: s.port, ip: clientAddr(r)}
	if s.tls {
		sc.scheme = schemeHTTPS
	}

	if lb.RequestIDOn && r.Header.Get(lb.RequestIDHeader) == "" {
		r.Header.Set(lb.RequestIDHeader, uuid.NewString())
	}

	reply, respRules := applyRequestRules(ruleSetsOf(lb, l), r, sc)
	if reply != nil {
		if reply.location != "" {
			w.Header().Set("Location", reply.location)
		}

		w.WriteHeader(reply.status)

		return
	}

	setName := d.selectSet(lb, l, r)
	d.runDueChecks(lb.ID, setName)
	bs := lb.BackendSets[setName]

	ch, status := d.choose(lb, bs, sc.ip, r)
	if ch == nil {
		http.Error(w, http.StatusText(status), status)

		return
	}

	defer ch.release()

	d.proxy(w, r, lb, l, ch, respRules)
}

func (d *dataPlane) proxy(
	w http.ResponseWriter,
	r *http.Request,
	lb *LoadBalancer,
	l *Listener,
	ch *chosen,
	respRules []Rule,
) {
	scheme := schemeHTTP
	if ch.set.SSL != nil {
		scheme = schemeHTTPS
	}

	target := scheme + "://" + net.JoinHostPort(ch.be.IPAddress, itoa(ch.be.Port))
	rt := d.transportForRequest(lb, ch.set)

	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme, pr.Out.URL.Host = scheme, strings.TrimPrefix(target, scheme+"://")
			pr.Out.Host = pr.In.Host
			pr.SetXForwarded()
		},
		Transport: idleTransport{rt: rt, idle: idleFor(l)},
		ModifyResponse: func(resp *http.Response) error {
			applyResponseRules(resp.Header, respRules)
			stickCookie(resp, ch)

			return nil
		},
		ErrorHandler: func(rw http.ResponseWriter, _ *http.Request, err error) {
			status := http.StatusBadGateway
			if errors.Is(err, context.DeadlineExceeded) || isTimeout(err) {
				status = http.StatusGatewayTimeout
			}

			http.Error(rw, http.StatusText(status), status)
		},
	}

	rp.ServeHTTP(w, r)
}

func (d *dataPlane) transportForRequest(lb *LoadBalancer, bs *BackendSet) *http.Transport {
	d.b.mu.RLock("transportForRequest")
	defer d.b.mu.RUnlock()

	return d.transportFor(lb, bs)
}

func idleFor(l *Listener) time.Duration {
	if l.Connection != nil {
		return time.Duration(l.Connection.IdleTimeout) * time.Second
	}

	return defaultHTTPIdle * time.Second
}

func isTimeout(err error) bool {
	var ne net.Error

	return errors.As(err, &ne) && ne.Timeout()
}

// idleTransport fails a request whose backend sends no response headers within idle.
type idleTransport struct {
	rt   http.RoundTripper
	idle time.Duration
}

func (t idleTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancel(req.Context())

	var fired atomic.Bool

	timer := time.AfterFunc(t.idle, func() {
		fired.Store(true)
		cancel()
	})

	resp, err := t.rt.RoundTrip(req.WithContext(ctx))
	timer.Stop()

	if err != nil {
		cancel()

		if fired.Load() {
			return nil, context.DeadlineExceeded
		}

		return nil, err
	}

	resp.Body = &cancelBody{ReadCloser: resp.Body, cancel: cancel}

	return resp, nil
}

type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelBody) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()

	return err
}

// chosen is the backend a request or connection was assigned to.
type chosen struct {
	set     *BackendSet
	release func()
	be      Backend
	setCook bool
}

func (d *dataPlane) available(lb *LoadBalancer, bs *BackendSet, be Backend) bool {
	if be.Offline {
		return false
	}

	if st, _, _ := d.statusOf(lb.ID, bs.Name, be); st == hCritical {
		return false
	}

	limit := be.MaxConnections
	if limit == nil {
		limit = bs.BackendMaxConnections
	}

	return limit == nil || d.counter(healthKey(lb.ID, bs.Name, be.Name)).Load() < int64(*limit)
}

// choose picks a backend: the session-persistent one if usable, otherwise per the set policy among healthy
// backends, falling back to backups only when no regular backend is usable. The status is the failure code.
func (d *dataPlane) choose(lb *LoadBalancer, bs *BackendSet, ip netip.Addr, r *http.Request) (*chosen, int) {
	var regular, backup, drained []Backend

	for _, be := range bs.Backends {
		if !d.available(lb, bs, be) {
			continue
		}

		switch {
		case be.Drain:
			drained = append(drained, be)
		case be.Backup:
			backup = append(backup, be)
		default:
			regular = append(regular, be)
		}
	}

	var pick *Backend

	if r != nil {
		pick = stickyBackend(bs, r, slices.Concat(regular, backup, drained))
		if pick == nil && stickyCookie(bs, r) != "" && bs.LBCookiePersistence != nil &&
			boolOr(bs.LBCookiePersistence.DisableFallback) {
			return nil, http.StatusBadGateway
		}
	}

	if pick == nil {
		pool := regular
		if len(pool) == 0 {
			pool = backup
		}

		if len(pool) == 0 {
			return nil, http.StatusServiceUnavailable
		}

		pick = d.byPolicy(lb, bs, pool, ip)
	}

	cnt := d.counter(healthKey(lb.ID, bs.Name, pick.Name))
	cnt.Add(1)

	return &chosen{be: *pick, set: bs, release: func() { cnt.Add(-1) }, setCook: bs.LBCookiePersistence != nil}, 0
}

func stickyCookie(bs *BackendSet, r *http.Request) string {
	name := routeCookie
	if bs.LBCookiePersistence != nil && bs.LBCookiePersistence.CookieName != "" {
		name = bs.LBCookiePersistence.CookieName
	}

	if bs.LBCookiePersistence == nil && bs.SessionPersistence == nil {
		return ""
	}

	if c, err := r.Cookie(name); err == nil {
		return c.Value
	}

	return ""
}

func stickyBackend(bs *BackendSet, r *http.Request, usable []Backend) *Backend {
	v := stickyCookie(bs, r)
	if v == "" {
		return nil
	}

	for i := range usable {
		if routeValue(usable[i]) == v {
			return &usable[i]
		}
	}

	return nil
}

func (d *dataPlane) byPolicy(lb *LoadBalancer, bs *BackendSet, pool []Backend, ip netip.Addr) *Backend {
	switch bs.Policy {
	case policyIPHash:
		h := fnv.New32a()
		_, _ = h.Write([]byte(ip.String()))

		return &pool[int(h.Sum32())%len(pool)]
	case policyLeastConns:
		best, bestLoad := 0, 0.0

		for i, be := range pool {
			load := float64(d.counter(healthKey(lb.ID, bs.Name, be.Name)).Load()) / float64(be.Weight)
			if i == 0 || load < bestLoad {
				best, bestLoad = i, load
			}
		}

		return &pool[best]
	default:
		return d.weightedRoundRobin(lb, bs, pool)
	}
}

// weightedRoundRobin is smooth weighted round robin: each backend accrues its weight per pick and the leader
// pays the total.
func (d *dataPlane) weightedRoundRobin(lb *LoadBalancer, bs *BackendSet, pool []Backend) *Backend {
	d.mu.Lock("weightedRoundRobin")
	defer d.mu.Unlock()

	total, best := 0, 0

	for i, be := range pool {
		k := healthKey(lb.ID, bs.Name, be.Name)
		d.cursors[k] += be.Weight
		total += be.Weight

		if d.cursors[k] > d.cursors[healthKey(lb.ID, bs.Name, pool[best].Name)] {
			best = i
		}
	}

	d.cursors[healthKey(lb.ID, bs.Name, pool[best].Name)] -= total

	return &pool[best]
}

// stickCookie adds the load balancer's route cookie to a response for session persistence.
func stickCookie(resp *http.Response, ch *chosen) {
	switch {
	case ch.set.LBCookiePersistence != nil:
		lc := ch.set.LBCookiePersistence
		name := routeCookie

		if lc.CookieName != "" {
			name = lc.CookieName
		}

		ck := &http.Cookie{ //nolint:gosec // attributes come from the listener configuration
			Name: name, Value: routeValue(ch.be), Path: "/", Domain: lc.Domain, Secure: boolOr(lc.IsSecure),
			HttpOnly: boolOr(lc.IsHTTPOnly), SameSite: http.SameSiteLaxMode,
		}
		if lc.Path != "" {
			ck.Path = lc.Path
		}

		if lc.MaxAgeInSeconds != nil {
			ck.MaxAge = *lc.MaxAgeInSeconds
		}

		resp.Header.Add("Set-Cookie", ck.String())
	case ch.set.SessionPersistence != nil:
		for _, ck := range resp.Cookies() {
			if ck.Name == ch.set.SessionPersistence.CookieName {
				resp.Header.Add(
					"Set-Cookie",
					(&http.Cookie{ //nolint:gosec // route cookie is opaque and path-scoped
						Name: routeCookie, Value: routeValue(ch.be), Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode,
					}).String(),
				)

				return
			}
		}
	}
}

// idleConn refreshes a connection deadline before every read and write.
type idleConn struct {
	net.Conn
	idle time.Duration
}

func (c idleConn) Read(p []byte) (int, error) {
	_ = c.SetReadDeadline(time.Now().Add(c.idle))

	return c.Conn.Read(p)
}

func (c idleConn) Write(p []byte) (int, error) {
	_ = c.SetWriteDeadline(time.Now().Add(c.idle))

	return c.Conn.Write(p)
}

func pipe(a, b net.Conn, idle time.Duration) {
	done := make(chan struct{}, pipeDirections)
	ia, ib := idleConn{Conn: a, idle: idle}, idleConn{Conn: b, idle: idle}

	go func() { _, _ = io.Copy(ib, ia); done <- struct{}{} }()
	go func() { _, _ = io.Copy(ia, ib); done <- struct{}{} }()

	<-done
	_ = a.Close()
	_ = b.Close()
	<-done
}

func (d *dataPlane) acceptTCP(ctx context.Context, p *portListener, ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}

			continue
		}

		d.track(p, c)
		p.wg.Go(func() {
			defer d.untrack(p, c)
			defer c.Close()

			d.handleTCP(ctx, p.spec, c)
		})
	}
}

func (d *dataPlane) handleTCP(ctx context.Context, s portSpec, c net.Conn) {
	lb := d.b.activeLB(s.lbID)
	if lb == nil {
		return
	}

	l := pickListener(lb, s.port, "")
	if l == nil {
		return
	}

	bs := lb.BackendSets[l.DefaultSet]
	d.runDueChecks(lb.ID, bs.Name)

	ip := netip.Addr{}
	if ta, ok := c.RemoteAddr().(*net.TCPAddr); ok {
		ip = ta.AddrPort().Addr().Unmap()
	}

	ch, _ := d.choose(lb, bs, ip, nil)
	if ch == nil {
		return
	}

	defer ch.release()

	up, err := d.dialBackend(ctx, lb, bs, ch.be)
	if err != nil {
		return
	}

	idle := defaultTCPIdle
	if l.Connection != nil {
		idle = int(l.Connection.IdleTimeout)
	}

	pipe(c, up, time.Duration(idle)*time.Second)
}

func (d *dataPlane) dialBackend(ctx context.Context, lb *LoadBalancer, bs *BackendSet, be Backend) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	conn, err := d.dialer.DialContext(ctx, "tcp", net.JoinHostPort(be.IPAddress, itoa(be.Port)))
	if err != nil {
		return nil, fmt.Errorf("dial backend: %w", err)
	}

	if bs.SSL == nil {
		return conn, nil
	}

	d.b.mu.RLock("dialBackend")
	cfg := d.b.backendTLS(lb, bs.SSL)
	d.b.mu.RUnlock()

	cfg.ServerName = be.IPAddress
	tc := tls.Client(conn, cfg)

	if err = tc.HandshakeContext(ctx); err != nil {
		_ = conn.Close()

		return nil, fmt.Errorf("backend handshake: %w", err)
	}

	return tc, nil
}
