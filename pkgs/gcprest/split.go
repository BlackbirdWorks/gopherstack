package gcprest

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	prefaceLen  = 4
	sniffWindow = 30 * time.Second
)

var errSplitClosed = errors.New("gcprest: listener closed")

// splitter hands HTTP/2 (gRPC) connections to Accept and serves every other connection with an HTTP/1 server.
type splitter struct {
	net.Listener
	err       error
	rest      *http.Server
	grpcConns chan net.Conn
	restConns chan net.Conn
	done      chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	once      sync.Once
}

func newSplitter(l net.Listener, rest *http.Server) *splitter {
	ctx, cancel := context.WithCancel(context.Background())

	return &splitter{
		ctx:       ctx,
		cancel:    cancel,
		Listener:  l,
		rest:      rest,
		grpcConns: make(chan net.Conn),
		restConns: make(chan net.Conn),
		done:      make(chan struct{}),
	}
}

func (sp *splitter) run() {
	go func() { _ = sp.rest.Serve(&restListener{sp: sp}) }()

	for {
		c, err := sp.Listener.Accept()
		if err != nil {
			sp.fail(err)

			return
		}

		go sp.route(c)
	}
}

func (sp *splitter) fail(err error) {
	sp.once.Do(func() {
		sp.err = err
		close(sp.done)
		sp.cancel()
	})
}

func (sp *splitter) route(c net.Conn) {
	br := bufio.NewReader(c)

	// Close must not strand conns still waiting for their first bytes.
	stop := context.AfterFunc(sp.ctx, func() { _ = c.Close() })

	_ = c.SetReadDeadline(time.Now().Add(sniffWindow))
	head, err := br.Peek(prefaceLen)
	_ = c.SetReadDeadline(time.Time{})

	stop()

	if err != nil {
		_ = c.Close()

		return
	}

	out := sp.restConns
	if string(head) == "PRI " {
		out = sp.grpcConns
	}

	select {
	case out <- &sniffedConn{Conn: c, r: br}:
	case <-sp.done:
		_ = c.Close()
	}
}

// Accept returns the next gRPC connection.
func (sp *splitter) Accept() (net.Conn, error) {
	select {
	case c := <-sp.grpcConns:
		return c, nil
	case <-sp.done:
		return nil, sp.err
	}
}

// Close closes the underlying listener and the REST server.
func (sp *splitter) Close() error {
	err := sp.Listener.Close()
	sp.fail(errSplitClosed)
	_ = sp.rest.Close()

	return err
}

type sniffedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *sniffedConn) Read(p []byte) (int, error) { return c.r.Read(p) }

type restListener struct{ sp *splitter }

func (l *restListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.sp.restConns:
		return c, nil
	case <-l.sp.done:
		return nil, net.ErrClosed
	}
}

func (l *restListener) Close() error   { return nil }
func (l *restListener) Addr() net.Addr { return l.sp.Listener.Addr() }
