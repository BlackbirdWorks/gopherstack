package rds

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

// tcpForwarder fronts a MySQL-family container so client connections are counted and can resume a paused cluster.
type tcpForwarder struct {
	ln    net.Listener
	be    *InMemoryBackend
	lb    *liveDB
	conns sync.Map
	wg    sync.WaitGroup
	port  int
	once  sync.Once
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

// wantsForwarder reports whether lb backs an auto-pausable cluster, whose connections must be observable.
func (b *InMemoryBackend) wantsForwarder(lb *liveDB) bool {
	cid, ok := strings.CutPrefix(lb.key, unitClusterPrefix)
	if !ok {
		return false
	}

	b.mu.RLock("rdsWantsForwarder")
	defer b.mu.RUnlock()

	c, found := b.clusters.Get(cid)

	return found && autoPauseConfigured(c)
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
	lb.fwd.Store(f)

	select {
	case <-lb.gone:
		f.close()

		return nil
	default:
	}

	f.wg.Go(f.acceptLoop)

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
