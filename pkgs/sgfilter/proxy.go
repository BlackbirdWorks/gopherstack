package sgfilter

import (
	"context"
	"io"
	"net"
	"sync"
	"time"
)

// DialTimeout bounds each upstream dial.
const DialTimeout = 10 * time.Second

const pumpDirections = 2

// Dialer opens upstream connections; *net.Dialer satisfies it and tests inject a fake.
type Dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// Proxy relays accepted connections to one upstream address, refusing clients its Admitter rejects.
type Proxy struct {
	ln       net.Listener
	admit    Admitter
	dialer   Dialer
	conns    sync.Map
	upstream string
	wg       sync.WaitGroup
	once     sync.Once
}

// NewProxy fronts upstream on ln. A nil admit admits every client; a nil dialer uses a plain net.Dialer.
func NewProxy(ln net.Listener, upstream string, admit Admitter, dialer Dialer) *Proxy {
	if dialer == nil {
		dialer = &net.Dialer{}
	}

	return &Proxy{ln: ln, upstream: upstream, admit: admit, dialer: dialer}
}

// Addr is the listening address.
func (p *Proxy) Addr() net.Addr { return p.ln.Addr() }

// Start runs the accept loop in the background; Close stops it.
func (p *Proxy) Start() {
	p.wg.Go(p.acceptLoop)
}

func (p *Proxy) acceptLoop() {
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			return
		}

		p.conns.Store(conn, struct{}{})
		p.wg.Go(func() {
			defer p.conns.Delete(conn)

			p.serve(conn)
		})
	}
}

// Close stops accepting, drops open connections and waits for the relays to finish.
func (p *Proxy) Close() {
	p.once.Do(func() {
		_ = p.ln.Close()

		p.conns.Range(func(k, _ any) bool {
			if c, ok := k.(net.Conn); ok {
				_ = c.Close()
			}

			return true
		})
	})
	p.wg.Wait()
}

func (p *Proxy) serve(client net.Conn) {
	defer func() { _ = client.Close() }()

	if p.admit != nil && !p.admit(client.RemoteAddr()) {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), DialTimeout)
	up, err := p.dialer.DialContext(ctx, "tcp", p.upstream)

	cancel()

	if err != nil {
		return
	}

	defer func() { _ = up.Close() }()

	Pipe(client, up)
}

// Pipe copies both directions until either side closes.
func Pipe(client, up net.Conn) {
	done := make(chan struct{}, pumpDirections)

	go func() {
		_, _ = io.Copy(up, client)
		_ = up.Close()

		done <- struct{}{}
	}()

	go func() {
		_, _ = io.Copy(client, up)
		_ = client.Close()

		done <- struct{}{}
	}()

	<-done
	<-done
}
