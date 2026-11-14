package ocidatabase

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	handshakeTimeout = 10 * time.Second
	dialTimeout      = 5 * time.Second
	copyDirections   = 2
)

// aclRules is the client IP access control list of a database.
type aclRules struct {
	nets    []netip.Prefix
	enabled bool
}

func newACL(enabled bool, entries []string) *aclRules {
	r := &aclRules{enabled: enabled}

	for _, e := range entries {
		if p, err := netip.ParsePrefix(e); err == nil {
			r.nets = append(r.nets, p.Masked())
		} else if a, aerr := netip.ParseAddr(e); aerr == nil {
			r.nets = append(r.nets, netip.PrefixFrom(a, a.BitLen()))
		}
	}

	return r
}

func (r *aclRules) allows(a netip.Addr) bool {
	if !r.enabled {
		return true
	}

	a = a.Unmap()
	for _, p := range r.nets {
		if p.Contains(a) {
			return true
		}
	}

	return false
}

// gate is the state shared by the TLS listeners of one database.
type gate struct {
	acl      atomic.Pointer[aclRules]
	target   atomic.Pointer[string]
	sec      *security
	refusals atomic.Int64
	mtls     atomic.Bool
}

// tlsProxy terminates TLS (and, for the mTLS listener, verifies client certificates) in front of the database
// listener.
type tlsProxy struct {
	ln       net.Listener
	g        *gate
	conns    map[net.Conn]struct{}
	mu       *lockmetrics.RWMutex
	wg       sync.WaitGroup
	port     int
	mutual   bool
	closed   atomic.Bool
	closeOne sync.Once
}

// startProxy listens on an ephemeral port; mutual selects the mTLS listener.
func startProxy(ctx context.Context, g *gate, mutual bool) (*tlsProxy, error) {
	ln, port, err := portalloc.ListenTCP(ctx, 0)
	if err != nil {
		return nil, err //nolint:wrapcheck // portalloc errors carry the port
	}

	p := &tlsProxy{
		ln: tls.NewListener(ln, g.sec.serverConfig(mutual)), g: g, port: port, mutual: mutual,
		conns: map[net.Conn]struct{}{}, mu: lockmetrics.New("ocidatabase.proxy"),
	}

	p.wg.Go(p.accept)

	return p, nil
}

func (p *tlsProxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}

		if !p.track(c) {
			_ = c.Close()

			return
		}

		p.wg.Go(func() { p.serve(c) })
	}
}

func (p *tlsProxy) track(c net.Conn) bool {
	p.mu.Lock("track")
	defer p.mu.Unlock()

	if p.closed.Load() {
		return false
	}

	p.conns[c] = struct{}{}

	return true
}

func (p *tlsProxy) untrack(c net.Conn) {
	p.mu.Lock("untrack")
	delete(p.conns, c)
	p.mu.Unlock()
	_ = c.Close()
}

func (p *tlsProxy) permitted(c net.Conn) bool {
	if !p.mutual && p.g.mtls.Load() {
		return false
	}

	if ap, err := netip.ParseAddrPort(c.RemoteAddr().String()); err == nil {
		return p.g.acl.Load().allows(ap.Addr())
	}

	return false
}

func (p *tlsProxy) serve(c net.Conn) {
	defer p.untrack(c)
	defer logger.Recover(context.Background(), "ocidatabase.tlsproxy", func(any) { _ = c.Close() })

	if !p.permitted(c) {
		p.g.refusals.Add(1)

		return
	}

	_ = c.SetDeadline(time.Now().Add(handshakeTimeout))

	tc, ok := c.(*tls.Conn)
	if !ok {
		return
	}

	hctx, cancel := context.WithTimeout(context.Background(), handshakeTimeout)
	err := tc.HandshakeContext(hctx)
	cancel()

	if err != nil {
		return
	}

	_ = c.SetDeadline(time.Time{})

	target := p.g.target.Load()
	if target == nil {
		return
	}

	d := net.Dialer{Timeout: dialTimeout}

	up, derr := d.DialContext(context.Background(), "tcp", *target)
	if derr != nil {
		return
	}

	defer up.Close()

	done := make(chan struct{}, copyDirections)

	p.wg.Go(func() { _, _ = io.Copy(up, c); done <- struct{}{} })
	p.wg.Go(func() { _, _ = io.Copy(c, up); done <- struct{}{} })

	<-done
}

// Close stops the listener and drops every connection.
func (p *tlsProxy) Close() {
	p.closeOne.Do(func() {
		p.mu.Lock("Close")
		p.closed.Store(true)
		_ = p.ln.Close()

		for c := range p.conns {
			_ = c.Close()
		}
		p.mu.Unlock()
		p.wg.Wait()
	})
}
