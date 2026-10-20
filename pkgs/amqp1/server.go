//nolint:mnd // AMQP window/handshake constants
package amqp1

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"
)

// Defaults for Config.
const (
	DefaultMaxFrameSize   uint32 = 65536
	DefaultMaxMessageSize uint64 = 1 << 20
	handshakeTimeout             = 15 * time.Second
	writeTimeout                 = 30 * time.Second
	maxSASLFrame          uint32 = 4096
)

// Config configures a Server.
type Config struct {
	Handler        Handler
	TLS            *tls.Config
	Logger         *slog.Logger
	ContainerID    string
	MaxFrameSize   uint32
	MaxMessageSize uint64
}

// Server accepts AMQP 1.0 connections.
type Server struct {
	conns  map[*conn]struct{}
	lns    []net.Listener
	cfg    Config
	mu     sync.Mutex
	closed bool
}

// NewServer applies defaults to cfg and returns a Server.
func NewServer(cfg Config) *Server {
	if cfg.MaxFrameSize == 0 {
		cfg.MaxFrameSize = DefaultMaxFrameSize
	}

	cfg.MaxFrameSize = max(cfg.MaxFrameSize, MinMaxFrameSize)

	if cfg.MaxMessageSize == 0 {
		cfg.MaxMessageSize = DefaultMaxMessageSize
	}

	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	if cfg.ContainerID == "" {
		cfg.ContainerID = "gopherstack"
	}

	return &Server{cfg: cfg, conns: map[*conn]struct{}{}}
}

// Serve accepts connections on ln until it is closed. If useTLS is set the
// listener is wrapped with the configured tls.Config.
func (s *Server) Serve(ln net.Listener, useTLS bool) error {
	if useTLS {
		ln = tls.NewListener(ln, s.cfg.TLS)
	}

	s.mu.Lock()

	if s.closed {
		s.mu.Unlock()
		_ = ln.Close()

		return net.ErrClosed
	}

	s.lns = append(s.lns, ln)
	s.mu.Unlock()

	for {
		nc, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}

			return err
		}

		go s.handle(nc)
	}
}

// Close stops all listeners and drops every connection.
func (s *Server) Close() {
	s.mu.Lock()
	s.closed = true
	lns := s.lns
	conns := make([]*conn, 0, len(s.conns))

	for c := range s.conns {
		conns = append(conns, c)
	}

	s.mu.Unlock()

	for _, ln := range lns {
		_ = ln.Close()
	}

	for _, c := range conns {
		c.shutdown()
	}
}

func (s *Server) track(c *conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false
	}

	s.conns[c] = struct{}{}

	return true
}

func (s *Server) untrack(c *conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

func (s *Server) handle(nc net.Conn) {
	c := newConn(s, nc)
	if !s.track(c) {
		_ = nc.Close()

		return
	}

	defer s.untrack(c)
	defer c.shutdown()

	if err := c.handshake(); err != nil {
		s.cfg.Logger.Debug("amqp1: handshake failed", "error", err)

		return
	}

	c.run()
}

// authenticateSASL runs the server side of the SASL exchange.
func (c *conn) authenticateSASL(r *bufio.Reader) error {
	if _, err := c.nc.Write([]byte(HeaderSASL)); err != nil {
		return err
	}

	mechs := &SASLMechanisms{Mechanisms: []Symbol{"ANONYMOUS", "PLAIN"}}
	if err := c.writePerformative(FrameSASL, 0, mechs); err != nil {
		return err
	}

	f, err := ReadFrame(r, maxSASLFrame)
	if err != nil {
		return err
	}

	p, _, err := DecodePerformative(f.Body)
	if err != nil {
		return err
	}

	init, ok := p.(*SASLInit)
	if !ok {
		return ErrBadFrame
	}

	user, pass := parseSASLResponse(init)
	code := SASLOK

	if !c.srv.cfg.Handler.Authenticate(init.Mechanism, user, pass) {
		code = SASLAuth
	}

	if werr := c.writePerformative(FrameSASL, 0, &SASLOutcome{Code: code}); werr != nil {
		return werr
	}

	if code != SASLOK {
		return errAuthFailed
	}

	return c.expectHeader(r, HeaderAMQP)
}

var errAuthFailed = errors.New("amqp1: sasl authentication failed")

func parseSASLResponse(init *SASLInit) (string, string) {
	if init.Mechanism != "PLAIN" {
		return "", ""
	}

	parts := bytes.SplitN(init.InitialResponse, []byte{0}, 3)
	if len(parts) != 3 {
		return "", ""
	}

	return string(parts[1]), string(parts[2])
}

var errBadHeader = errors.New("amqp1: unsupported protocol header")

func (c *conn) expectHeader(r *bufio.Reader, want string) error {
	got := make([]byte, len(want))
	if _, err := readFull(r, got); err != nil {
		return err
	}

	if string(got) != want {
		_, _ = c.nc.Write([]byte(HeaderAMQP))

		return errBadHeader
	}

	return nil
}

func (c *conn) handshake() error {
	_ = c.nc.SetDeadline(time.Now().Add(handshakeTimeout))
	defer func() { _ = c.nc.SetDeadline(time.Time{}) }()

	hdr := make([]byte, len(HeaderAMQP))
	if _, err := readFull(c.r, hdr); err != nil {
		return err
	}

	switch string(hdr) {
	case HeaderSASL:
		if err := c.authenticateSASL(c.r); err != nil {
			return err
		}
	case HeaderAMQP:
	default:
		_, _ = c.nc.Write([]byte(HeaderAMQP))

		return errBadHeader
	}

	if _, err := c.nc.Write([]byte(HeaderAMQP)); err != nil {
		return err
	}

	return c.openExchange()
}

func (c *conn) openExchange() error {
	f, err := ReadFrame(c.r, c.srv.cfg.MaxFrameSize)
	if err != nil {
		return err
	}

	p, _, err := DecodePerformative(f.Body)
	if err != nil {
		return err
	}

	peer, ok := p.(*Open)
	if !ok {
		return ErrBadFrame
	}

	c.maxFrame = min(peer.MaxFrameSize, c.srv.cfg.MaxFrameSize)
	c.maxFrame = max(c.maxFrame, MinMaxFrameSize)
	c.idle = time.Duration(peer.IdleTimeout) * time.Millisecond

	return c.writePerformative(FrameAMQP, 0, &Open{
		ContainerID:  c.srv.cfg.ContainerID,
		MaxFrameSize: c.srv.cfg.MaxFrameSize,
		ChannelMax:   maxChannel,
	})
}

// run is the connection read loop; it returns when the connection ends.
func (c *conn) run() {
	ctx, cancel := context.WithCancel(context.Background())
	c.ctx = ctx

	defer cancel()

	c.ch = c.srv.cfg.Handler.NewConn()
	defer c.ch.Close()
	defer c.closeSessions()

	if c.idle > 0 {
		go c.keepAlive(ctx)
	}

	for {
		f, err := ReadFrame(c.r, c.srv.cfg.MaxFrameSize)
		if err != nil {
			return
		}

		if f.Body == nil {
			continue
		}

		if !c.dispatch(f) {
			return
		}
	}
}

func (c *conn) keepAlive(ctx context.Context) {
	t := time.NewTicker(max(c.idle/2, time.Second))
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if c.writeRaw(AppendFrame(nil, Frame{Type: FrameAMQP})) != nil {
				return
			}
		}
	}
}
