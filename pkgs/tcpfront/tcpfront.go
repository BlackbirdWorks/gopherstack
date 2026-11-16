// Package tcpfront is a loopback TCP listener that forwards every connection to a target resolved per connection,
// so the address clients use survives the target changing.
package tcpfront

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	dialTimeout = 5 * time.Second
	pipes       = 2
	loopback    = "127.0.0.1"
)

// Target resolves where the next connection goes; a zero port refuses it.
type Target func() (host string, port int)

// Front accepts connections on a loopback port and pipes them to the current target.
type Front struct {
	ln     net.Listener
	target Target
	mu     *lockmetrics.RWMutex
	conns  map[net.Conn]struct{}
	wg     sync.WaitGroup
	port   int
}

// New listens on a free loopback port. ctx scopes the logger of the connection handlers.
func New(ctx context.Context, target Target) (*Front, error) {
	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", net.JoinHostPort(loopback, "0"))
	if err != nil {
		return nil, err //nolint:wrapcheck // caller reports it
	}

	f := &Front{ln: ln, target: target, mu: lockmetrics.New("tcpfront"), conns: map[net.Conn]struct{}{}}
	if a, ok := ln.Addr().(*net.TCPAddr); ok {
		f.port = a.Port
	}

	f.wg.Add(1)

	go f.serve(context.WithoutCancel(ctx))

	return f, nil
}

// Addr is the host and port clients connect to.
func (f *Front) Addr() (string, int) { return loopback, f.port }

func (f *Front) serve(ctx context.Context) {
	defer f.wg.Done()

	for {
		c, err := f.ln.Accept()
		if err != nil {
			return
		}

		f.wg.Add(1)

		go f.handle(ctx, c)
	}
}

func (f *Front) track(c net.Conn, add bool) {
	f.mu.Lock("track")
	defer f.mu.Unlock()

	if add {
		f.conns[c] = struct{}{}
	} else {
		delete(f.conns, c)
	}
}

func (f *Front) handle(ctx context.Context, c net.Conn) {
	defer f.wg.Done()
	defer c.Close()
	defer logger.Recover(ctx, "tcpfront", nil)

	host, port := f.target()
	if port == 0 {
		return
	}

	d := net.Dialer{Timeout: dialTimeout}

	up, err := d.DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return
	}
	defer up.Close()

	f.track(c, true)
	f.track(up, true)

	defer f.track(c, false)
	defer f.track(up, false)

	done := make(chan struct{}, pipes)
	pipe := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		_ = dst.Close()
		_ = src.Close()
		done <- struct{}{}
	}

	go pipe(up, c)
	go pipe(c, up)

	<-done
	<-done
}

// Close stops accepting, drops every open connection and waits for the copiers to finish.
func (f *Front) Close() {
	_ = f.ln.Close()

	f.mu.Lock("Close")
	for c := range f.conns {
		_ = c.Close()
	}
	f.mu.Unlock()

	f.wg.Wait()
}
