package rds

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

// tcpForwarder fronts a MySQL-family container so client connections are counted and can resume a paused cluster.
type tcpForwarder struct {
	ln      net.Listener
	be      *InMemoryBackend
	lb      *liveDB
	conns   sync.Map
	wg      sync.WaitGroup
	port    int
	once    sync.Once
	connSeq atomic.Uint32
}

func (f *tcpForwarder) close() {
	f.once.Do(func() {
		_ = f.ln.Close()

		f.conns.Range(func(k, _ any) bool {
			if c, ok := k.(net.Conn); ok {
				_ = c.Close()
			}

			return true
		})

		f.be.engine.releasePort(f.port)
	})
}

func (b *InMemoryBackend) startForwarder(lb *liveDB) error {
	e := b.engine

	port, err := e.acquirePort()
	if err != nil {
		return err
	}

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp",
		net.JoinHostPort(container.BindHostFor(e.cfg.Host), strconv.Itoa(port)))
	if err != nil {
		e.releasePort(port)

		return fmt.Errorf("forwarder listen: %w", err)
	}

	f := &tcpForwarder{be: b, lb: lb, ln: ln, port: port}
	f.wg.Add(1)
	lb.fwd.Store(f)

	select {
	case <-lb.gone:
		f.close()
		f.wg.Done()

		return nil
	default:
	}

	go func() {
		defer f.wg.Done()

		f.acceptLoop()
	}()

	return nil
}

func (f *tcpForwarder) acceptLoop() {
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			return
		}

		f.conns.Store(conn, struct{}{})
		f.wg.Go(func() {
			defer f.conns.Delete(conn)

			f.serve(conn)
		})
	}
}

func (f *tcpForwarder) serve(client net.Conn) {
	defer func() { _ = client.Close() }()

	release, ok := f.be.beginConn(f.lb)
	if !ok {
		return
	}

	defer release()

	if t, found := f.be.proxyTarget(f.lb, f.port); found && t.auth.enabled {
		f.serveIAM(client, t)

		return
	}

	f.be.mu.RLock("rdsForwardTarget")
	addr := net.JoinHostPort(f.be.engine.cfg.Host, strconv.Itoa(f.lb.port))
	f.be.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), proxyUpstreamTimeout)
	up, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)

	cancel()

	if err != nil {
		return
	}

	defer func() { _ = up.Close() }()

	pipeConns(client, up)
}

func pipeConns(client, up net.Conn) {
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
