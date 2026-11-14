package ociapigateway

import (
	"bufio"
	"net"
	"sync"
	"time"
)

const (
	tlsHandshakeByte = 0x16
	sniffTimeout     = 10 * time.Second
)

// chanListener hands out connections that a sniffing accept loop routed to it.
type chanListener struct {
	conns chan net.Conn
	done  chan struct{}
	inner net.Listener
	once  sync.Once
}

func (l *chanListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *chanListener) Close() error {
	l.once.Do(func() {
		close(l.done)
		_ = l.inner.Close()
	})

	return nil
}

func (l *chanListener) Addr() net.Addr { return l.inner.Addr() }

// peekedConn replays the byte consumed while sniffing the protocol.
type peekedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *peekedConn) Read(p []byte) (int, error) { return c.r.Read(p) }

// splitListener serves plain HTTP and TLS on one port by looking at the first byte of each connection.
// Closing either returned listener stops the accept loop and closes the underlying listener.
func splitListener(inner net.Listener, wg *sync.WaitGroup) (*chanListener, *chanListener) {
	mk := func() *chanListener {
		return &chanListener{conns: make(chan net.Conn), done: make(chan struct{}), inner: inner}
	}
	plain, secure := mk(), mk()

	wg.Go(func() {
		for {
			c, err := inner.Accept()
			if err != nil {
				_ = plain.Close()
				_ = secure.Close()

				return
			}

			wg.Go(func() { dispatchConn(c, plain, secure) })
		}
	})

	return plain, secure
}

func dispatchConn(c net.Conn, plain, secure *chanListener) {
	_ = c.SetReadDeadline(time.Now().Add(sniffTimeout))

	br := bufio.NewReader(c)

	first, err := br.Peek(1)
	if err != nil {
		_ = c.Close()

		return
	}

	_ = c.SetReadDeadline(time.Time{})

	dst := plain
	if first[0] == tlsHandshakeByte {
		dst = secure
	}

	select {
	case dst.conns <- &peekedConn{Conn: c, r: br}:
	case <-dst.done:
		_ = c.Close()
	}
}
