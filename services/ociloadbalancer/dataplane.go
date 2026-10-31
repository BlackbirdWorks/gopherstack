package ociloadbalancer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	loopbackHost    = "127.0.0.1"
	defaultHTTPIdle = 60
	defaultTCPIdle  = 300
	kindHTTP        = "http"
	kindTCP         = "tcp"
)

// portSpec is what a data-plane port must look like for the current load balancer configuration.
type portSpec struct {
	lbID string
	kind string
	port int
	tls  bool
}

func (s portSpec) key() string { return s.lbID + "|" + strconv.Itoa(s.port) }

type portListener struct {
	ln    net.Listener
	srv   *http.Server
	conns map[net.Conn]struct{}
	stop  context.CancelFunc
	addr  string
	spec  portSpec
	wg    sync.WaitGroup
}

type transportEntry struct {
	rt          *http.Transport
	fingerprint string
}

// dataPlane owns the listening sockets and the runtime state (health, counters, balancing cursors).
type dataPlane struct {
	b          *InMemoryBackend
	mu         *lockmetrics.RWMutex
	syncMu     *lockmetrics.RWMutex
	ports      map[string]*portListener
	health     map[string]*hstate
	conns      map[string]*atomic.Int64
	cursors    map[string]int
	transports map[string]*transportEntry
	dialer     *net.Dialer
	baseCtx    context.Context //nolint:containedctx // process-lifetime context for accepted connections
}

func newDataPlane(b *InMemoryBackend) *dataPlane {
	d := &dataPlane{
		b: b, mu: lockmetrics.New("ociloadbalancer.dataplane"), syncMu: lockmetrics.New("ociloadbalancer.sync"),
		baseCtx: context.Background(),
	}
	d.dialer = httpclient.LoopbackDialer(b.externalAllowed, ErrDestinationNotPermitted)
	d.initMaps()

	return d
}

func (d *dataPlane) initMaps() {
	d.ports, d.health = map[string]*portListener{}, map[string]*hstate{}
	d.conns, d.cursors = map[string]*atomic.Int64{}, map[string]int{}
	d.transports = map[string]*transportEntry{}
}

// reset closes every listener and clears runtime state.
func (d *dataPlane) reset() {
	d.syncMu.Lock("reset")
	defer d.syncMu.Unlock()

	d.mu.Lock("reset")
	open := d.ports
	trs := d.transports
	d.initMaps()
	d.mu.Unlock()

	for _, p := range open {
		d.closePort(p)
	}

	for _, t := range trs {
		t.rt.CloseIdleConnections()
	}
}

// address returns the host:port a load balancer listener port is reachable on, or "" while it is not serving.
func (d *dataPlane) address(lbID string, port int) string {
	d.mu.RLock("address")
	defer d.mu.RUnlock()

	if p := d.ports[portSpec{lbID: lbID, port: port}.key()]; p != nil {
		return p.addr
	}

	return ""
}

// Endpoint is one serving data-plane listener.
type Endpoint struct {
	LoadBalancerID string `json:"loadBalancerId"`
	Listener       string `json:"listener"`
	Protocol       string `json:"protocol"`
	Address        string `json:"address"`
	Port           int    `json:"port"`
}

// Endpoints lists the loopback addresses the active listeners are reachable on.
func (b *InMemoryBackend) Endpoints() []Endpoint {
	b.mu.RLock("Endpoints")
	defer b.mu.RUnlock()

	out := []Endpoint{}

	for _, lb := range b.lbs {
		for _, l := range lb.Listeners {
			if addr := b.dp.address(lb.ID, l.Port); addr != "" {
				out = append(out, Endpoint{
					LoadBalancerID: lb.ID, Listener: l.Name, Protocol: l.Protocol, Address: addr, Port: l.Port,
				})
			}
		}
	}

	return out
}

func (b *InMemoryBackend) desiredPorts() map[string]portSpec {
	b.mu.RLock("desiredPorts")
	defer b.mu.RUnlock()

	want := map[string]portSpec{}

	for _, lb := range b.lbs {
		if lb.State != stateActive {
			continue
		}

		for _, l := range lb.Listeners {
			s := portSpec{lbID: lb.ID, port: l.Port, tls: l.SSL != nil, kind: kindHTTP}
			if l.Protocol == protoTCP {
				s.kind = kindTCP
			}

			want[s.key()] = s
		}
	}

	return want
}

// sync opens and closes listening sockets so they match the active load balancers.
func (d *dataPlane) sync() {
	want := d.b.desiredPorts()

	d.syncMu.Lock("sync")
	defer d.syncMu.Unlock()

	d.mu.Lock("sync")

	var stale []*portListener

	for k, p := range d.ports {
		if w, ok := want[k]; !ok || w != p.spec {
			stale = append(stale, p)
			delete(d.ports, k)
		}
	}

	var open []portSpec

	for k, w := range want {
		if d.ports[k] == nil {
			open = append(open, w)
		}
	}

	d.mu.Unlock()

	for _, p := range stale {
		d.closePort(p)
	}

	for _, w := range open {
		p, err := d.open(w)
		if err != nil {
			logger.Load(d.baseCtx).
				ErrorContext(d.baseCtx, "ociloadbalancer: open listener", "port", w.port, "error", err)

			continue
		}

		d.mu.Lock("sync")
		d.ports[w.key()] = p
		d.mu.Unlock()
	}

	d.prune()
}

// prune forgets runtime state of load balancers that are no longer active.
func (d *dataPlane) prune() {
	live := d.b.liveIDs()

	d.mu.Lock("prune")
	defer d.mu.Unlock()

	for k := range d.health {
		if !live[lbOfKey(k)] {
			delete(d.health, k)
		}
	}

	for k := range d.conns {
		if !live[lbOfKey(k)] {
			delete(d.conns, k)
		}
	}

	for k := range d.cursors {
		if !live[lbOfKey(k)] {
			delete(d.cursors, k)
		}
	}

	for k, t := range d.transports {
		if !live[lbOfKey(k)] {
			t.rt.CloseIdleConnections()
			delete(d.transports, k)
		}
	}
}

func lbOfKey(k string) string {
	if before, _, ok := strings.Cut(k, "|"); ok {
		return before
	}

	return k
}

func (b *InMemoryBackend) liveIDs() map[string]bool {
	b.mu.RLock("liveIDs")
	defer b.mu.RUnlock()

	out := make(map[string]bool, len(b.lbs))

	for id, lb := range b.lbs {
		if lb.State == stateActive {
			out[id] = true
		}
	}

	return out
}

func (d *dataPlane) open(s portSpec) (*portListener, error) {
	var lc net.ListenConfig

	ln, err := lc.Listen(d.baseCtx, "tcp", net.JoinHostPort(loopbackHost, "0"))
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}

	ctx, cancel := context.WithCancel(d.baseCtx)
	p := &portListener{ln: ln, addr: ln.Addr().String(), spec: s, stop: cancel, conns: map[net.Conn]struct{}{}}

	if s.kind == kindHTTP {
		d.serveHTTPPort(p)

		return p, nil
	}

	var accept = ln
	if s.tls {
		accept = tls.NewListener(
			ln,
			&tls.Config{GetConfigForClient: d.frontTLS(s.lbID, s.port), MinVersion: tls.VersionTLS12},
		)
	}

	p.wg.Go(func() { d.acceptTCP(ctx, p, accept) })

	return p, nil
}

func (d *dataPlane) serveHTTPPort(p *portListener) {
	s := p.spec
	srv := &http.Server{
		Handler:           http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { d.serveHTTP(w, r, s) }),
		ReadHeaderTimeout: readHeaderTimeout,
		ErrorLog:          nil,
	}
	p.srv = srv

	p.wg.Go(func() {
		var err error
		if s.tls {
			srv.TLSConfig = &tls.Config{GetConfigForClient: d.frontTLS(s.lbID, s.port), MinVersion: tls.VersionTLS12}
			err = srv.ServeTLS(p.ln, "", "")
		} else {
			err = srv.Serve(p.ln)
		}

		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			logger.Load(d.baseCtx).ErrorContext(d.baseCtx, "ociloadbalancer: listener stopped", "error", err)
		}
	})
}

func (d *dataPlane) closePort(p *portListener) {
	p.stop()

	if p.srv != nil {
		_ = p.srv.Close()
	}

	_ = p.ln.Close()
	d.closeConns(p)
	p.wg.Wait()
}

// closeConns closes every accepted TCP connection of a port.
func (d *dataPlane) closeConns(p *portListener) {
	d.mu.Lock("closeConns")
	conns := make([]net.Conn, 0, len(p.conns))

	for c := range p.conns {
		conns = append(conns, c)
	}

	d.mu.Unlock()

	for _, c := range conns {
		_ = c.Close()
	}
}

func (d *dataPlane) track(p *portListener, c net.Conn) {
	d.mu.Lock("track")
	p.conns[c] = struct{}{}
	d.mu.Unlock()
}

func (d *dataPlane) untrack(p *portListener, c net.Conn) {
	d.mu.Lock("untrack")
	delete(p.conns, c)
	d.mu.Unlock()
}

// counter returns the live connection counter for a backend.
func (d *dataPlane) counter(key string) *atomic.Int64 {
	d.mu.RLock("counter")
	c := d.conns[key]
	d.mu.RUnlock()

	if c != nil {
		return c
	}

	d.mu.Lock("counter")
	defer d.mu.Unlock()

	if c = d.conns[key]; c == nil {
		c = &atomic.Int64{}
		d.conns[key] = c
	}

	return c
}

// transportFor returns the pooled transport for a backend set, rebuilt when its TLS configuration changes.
// Callers hold the backend read lock.
func (d *dataPlane) transportFor(lb *LoadBalancer, bs *BackendSet) *http.Transport {
	key := lb.ID + "|" + bs.Name
	fp := transportFingerprint(lb, bs)

	d.mu.RLock("transportFor")
	cur := d.transports[key]
	d.mu.RUnlock()

	if cur != nil && cur.fingerprint == fp {
		return cur.rt
	}

	tr := httpclient.NewTransport()
	tr.Proxy, tr.DialContext, tr.ForceAttemptHTTP2 = nil, d.dialer.DialContext, false
	tr.TLSClientConfig = d.b.backendTLS(lb, bs.SSL)

	d.mu.Lock("transportFor")
	old := d.transports[key]
	d.transports[key] = &transportEntry{rt: tr, fingerprint: fp}
	d.mu.Unlock()

	if old != nil {
		old.rt.CloseIdleConnections()
	}

	return tr
}

func transportFingerprint(lb *LoadBalancer, bs *BackendSet) string {
	if bs.SSL == nil {
		return "plain"
	}

	fp := fmt.Sprintf("%v|%v|%s|%v|%v", boolOr(bs.SSL.VerifyPeerCertificate), bs.SSL.Protocols, bs.SSL.CipherSuiteName,
		bs.SSL.CertificateName, len(lb.Certificates))
	if c := lb.Certificates[bs.SSL.CertificateName]; c != nil {
		fp += "|" + c.CA + "|" + c.Public
	}

	if cs := lb.SSLCipherSuites[bs.SSL.CipherSuiteName]; cs != nil {
		fp += fmt.Sprint(cs.Ciphers)
	}

	return fp
}

var errBackendTLS = errors.New("backend TLS configuration")

// backendTLS builds the TLS client configuration for connections to a backend set. The peer chain and its IP
// address are always verified (never InsecureSkipVerify): against the bundle named by certificateName when one
// is given, otherwise against the system roots.
func (b *InMemoryBackend) backendTLS(lb *LoadBalancer, s *SSLConfig) *tls.Config {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if s == nil {
		return cfg
	}

	applyProtocols(cfg, s.Protocols)
	applyCiphers(cfg, lb, s.CipherSuiteName)

	c := lb.Certificates[s.CertificateName]
	if c == nil {
		return cfg
	}

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM([]byte(c.CA))
	pool.AppendCertsFromPEM([]byte(c.Public))
	cfg.RootCAs = pool

	if key := b.keys[lb.ID+"/"+s.CertificateName]; key != nil {
		cfg.Certificates = []tls.Certificate{*key}
	}

	return cfg
}

func applyProtocols(cfg *tls.Config, protocols []string) {
	if len(protocols) == 0 {
		return
	}

	cfg.MinVersion, cfg.MaxVersion = tls.VersionTLS13, tls.VersionTLS12

	for _, p := range protocols {
		if p == "TLSv1.2" && cfg.MinVersion > tls.VersionTLS12 {
			cfg.MinVersion = tls.VersionTLS12
		}

		if p == "TLSv1.3" {
			cfg.MaxVersion = tls.VersionTLS13
		}
	}

	if cfg.MaxVersion < cfg.MinVersion {
		cfg.MaxVersion = cfg.MinVersion
	}
}

func applyCiphers(cfg *tls.Config, lb *LoadBalancer, suite string) {
	cs := lb.SSLCipherSuites[suite]
	if cs == nil {
		return
	}

	for _, c := range cs.Ciphers {
		cfg.CipherSuites = append(cfg.CipherSuites, cipherIDs[c])
	}
}

// frontTLS returns the per-handshake TLS configuration of a listener port: the listener is chosen by SNI and
// serves its certificate bundle, protocols, cipher suite and ALPN.
func (d *dataPlane) frontTLS(lbID string, port int) func(*tls.ClientHelloInfo) (*tls.Config, error) {
	return func(hi *tls.ClientHelloInfo) (*tls.Config, error) {
		lb := d.b.activeLB(lbID)
		if lb == nil {
			return nil, errBackendTLS
		}

		l := pickListener(lb, port, hi.ServerName)
		if l == nil || l.SSL == nil {
			return nil, errBackendTLS
		}

		d.b.mu.RLock("frontTLS")
		key := d.b.certKey(lbID, l.SSL.CertificateName)
		d.b.mu.RUnlock()

		if key == nil {
			return nil, fmt.Errorf("%w: certificate %s has no private key", errBackendTLS, l.SSL.CertificateName)
		}

		cfg := &tls.Config{
			Certificates: []tls.Certificate{*key},
			MinVersion:   tls.VersionTLS12,
			NextProtos:   []string{"http/1.1"},
		}
		if l.Protocol == protoHTTP2 {
			cfg.NextProtos = []string{"h2", "http/1.1"}
		}

		if l.Protocol == protoTCP {
			cfg.NextProtos = nil
		}

		if len(l.SSL.Protocols) == 0 {
			cfg.MaxVersion = tls.VersionTLS12
		}

		applyProtocols(cfg, l.SSL.Protocols)
		applyCiphers(cfg, lb, l.SSL.CipherSuiteName)
		applyClientAuth(cfg, lb, l.SSL)

		return cfg, nil
	}
}

func applyClientAuth(cfg *tls.Config, lb *LoadBalancer, s *SSLConfig) {
	if !boolOr(s.VerifyPeerCertificate) {
		return
	}

	if c := lb.Certificates[s.CertificateName]; c != nil {
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM([]byte(c.CA))
		cfg.ClientCAs, cfg.ClientAuth = pool, tls.RequireAndVerifyClientCert
	}
}

// activeLB returns the active load balancer, which is immutable once published.
func (b *InMemoryBackend) activeLB(id string) *LoadBalancer {
	b.mu.RLock("activeLB")
	defer b.mu.RUnlock()

	if lb := b.lbs[id]; lb != nil && lb.State == stateActive {
		return lb
	}

	return nil
}
