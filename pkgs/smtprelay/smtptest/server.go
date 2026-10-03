// Package smtptest provides a minimal in-process SMTP server for tests.
package smtptest

import (
	"context"
	"crypto/tls"
	"net"
	"net/textproto"
	"strings"
	"sync"
	"testing"
)

const sessionBuffer = 64

// Session is one captured SMTP transaction.
type Session struct {
	From      string
	Data      string
	Rcpts     []string
	AuthLines []string
	TLS       bool
}

// Options controls server behaviour.
type Options struct {
	// TLSConfig, when set, advertises STARTTLS.
	TLSConfig *tls.Config
	// AdvertiseAuth advertises AUTH PLAIN.
	AdvertiseAuth bool
	// RejectRcpt makes every RCPT TO fail with 550.
	RejectRcpt bool
}

// Server is a running fake SMTP server.
type Server struct {
	ln       net.Listener
	sessions chan Session
	opts     Options
	wg       sync.WaitGroup
}

// Start listens on 127.0.0.1 and serves until the test ends.
func Start(t *testing.T, opts Options) *Server {
	t.Helper()

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("smtptest listen: %v", err)
	}

	s := &Server{ln: ln, opts: opts, sessions: make(chan Session, sessionBuffer)}
	s.wg.Add(1)

	go s.accept()

	t.Cleanup(func() {
		_ = ln.Close()
		s.wg.Wait()
	})

	return s
}

// Addr returns host:port.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Sessions yields each completed (or attempted) transaction.
func (s *Server) Sessions() <-chan Session { return s.sessions }

func (s *Server) accept() {
	defer s.wg.Done()

	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}

		s.wg.Go(func() { s.serve(c) })
	}
}

type conn struct {
	nc   net.Conn
	tp   *textproto.Conn
	srv  *Server
	sess Session
}

func (s *Server) serve(nc net.Conn) {
	c := &conn{nc: nc, tp: textproto.NewConn(nc), srv: s}

	defer func() {
		_ = c.nc.Close()

		select {
		case s.sessions <- c.sess:
		default:
		}
	}()

	_ = c.tp.PrintfLine("220 fake ESMTP")

	for {
		line, err := c.tp.ReadLine()
		if err != nil || !c.command(line) {
			return
		}
	}
}

// command handles one client line and reports whether the session continues.
func (c *conn) command(line string) bool {
	verb := strings.ToUpper(line)

	switch {
	case strings.HasPrefix(verb, "EHLO"):
		c.ehlo()
	case verb == "STARTTLS":
		return c.startTLS()
	case strings.HasPrefix(verb, "AUTH"):
		c.sess.AuthLines = append(c.sess.AuthLines, line)
		_ = c.tp.PrintfLine("235 ok")
	case strings.HasPrefix(verb, "MAIL FROM:"):
		c.sess.From = angle(line)
		_ = c.tp.PrintfLine("250 ok")
	case strings.HasPrefix(verb, "RCPT TO:"):
		c.rcpt(line)
	case verb == "DATA":
		return c.data()
	case verb == "QUIT":
		_ = c.tp.PrintfLine("221 bye")

		return false
	default:
		_ = c.tp.PrintfLine("250 ok")
	}

	return true
}

func (c *conn) ehlo() {
	_ = c.tp.PrintfLine("250-fake")

	if c.srv.opts.TLSConfig != nil && !c.sess.TLS {
		_ = c.tp.PrintfLine("250-STARTTLS")
	}

	if c.srv.opts.AdvertiseAuth {
		_ = c.tp.PrintfLine("250-AUTH PLAIN")
	}

	_ = c.tp.PrintfLine("250 8BITMIME")
}

func (c *conn) startTLS() bool {
	_ = c.tp.PrintfLine("220 go ahead")

	tc := tls.Server(c.nc, c.srv.opts.TLSConfig)
	if tc.HandshakeContext(context.Background()) != nil {
		return false
	}

	c.nc = tc
	c.tp = textproto.NewConn(tc)
	c.sess.TLS = true

	return true
}

func (c *conn) rcpt(line string) {
	if c.srv.opts.RejectRcpt {
		_ = c.tp.PrintfLine("550 no")

		return
	}

	c.sess.Rcpts = append(c.sess.Rcpts, angle(line))
	_ = c.tp.PrintfLine("250 ok")
}

func (c *conn) data() bool {
	_ = c.tp.PrintfLine("354 go")

	b, err := c.tp.ReadDotBytes()
	if err != nil {
		return false
	}

	c.sess.Data = string(b)
	_ = c.tp.PrintfLine("250 queued")

	return true
}

func angle(line string) string {
	i, j := strings.IndexByte(line, '<'), strings.LastIndexByte(line, '>')
	if i < 0 || j < i {
		return line
	}

	return line[i+1 : j]
}
