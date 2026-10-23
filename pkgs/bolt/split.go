package bolt

import (
	"bufio"
	"errors"
	"net"
	"sync"
	"time"
)

const sniffWindow = 30 * time.Second

var errSplitClosed = errors.New("bolt: listener closed")

// Split diverts connections that start with the Bolt preamble to serve and returns a listener that yields
// every other connection, so an HTTP server can share the port.
func Split(l net.Listener, serve func(net.Conn)) net.Listener {
	s := &splitter{Listener: l, serve: serve, rest: make(chan net.Conn), done: make(chan struct{})}

	go s.run()

	return s
}

type splitter struct {
	net.Listener
	err   error
	serve func(net.Conn)
	rest  chan net.Conn
	done  chan struct{}
	once  sync.Once
}

func (s *splitter) run() {
	for {
		c, err := s.Listener.Accept()
		if err != nil {
			s.once.Do(func() {
				s.err = err
				close(s.done)
			})

			return
		}

		go s.route(c)
	}
}

func (s *splitter) route(c net.Conn) {
	br := bufio.NewReader(c)

	_ = c.SetReadDeadline(time.Now().Add(sniffWindow))
	head, err := br.Peek(Preamble)
	_ = c.SetReadDeadline(time.Time{})

	if err != nil {
		_ = c.Close()

		return
	}

	pc := &peekConn{Conn: c, r: br}

	if [4]byte(head) == magic() {
		s.serve(pc)

		return
	}

	select {
	case s.rest <- pc:
	case <-s.done:
		_ = c.Close()
	}
}

func (s *splitter) Accept() (net.Conn, error) {
	select {
	case c := <-s.rest:
		return c, nil
	case <-s.done:
		if s.err != nil {
			return nil, s.err
		}

		return nil, errSplitClosed
	}
}

type peekConn struct {
	net.Conn
	r *bufio.Reader
}

func (p *peekConn) Read(b []byte) (int, error) { return p.r.Read(b) }
