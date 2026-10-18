package ses

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	inboundIdleTimeout = 5 * time.Minute
	inboundMaxRcpts    = 50
	inboundMaxLine     = 4096
	smtpClosing        = 221
	smtpReady          = 220
	smtpOK             = 250
	smtpCannotVRFY     = 252
	smtpStartData      = 354
	smtpTempFail       = 400
	smtpTooManyRcpts   = 452
	smtpSyntaxErr      = 500
	smtpArgErr         = 501
	smtpNotImpl        = 502
	smtpBadSequence    = 503
	smtpMailboxErr     = 550
	smtpTooBig         = 552
)

var errInboundTooBig = errors.New("message exceeds size limit")

// inboundServer is the SMTP listener feeding deliverInbound.
type inboundServer struct {
	b      *InMemoryBackend
	ln     net.Listener
	tlsCfg *tls.Config
	cancel context.CancelFunc
	wg     sync.WaitGroup
	max    int64
}

func startInboundServer(ctx context.Context, b *InMemoryBackend, cfg InboundConfig) (*inboundServer, error) {
	cert, err := devtls.GenerateSelfSignedCert("127.0.0.1", "localhost")
	if err != nil {
		return nil, fmt.Errorf("ses inbound: tls cert: %w", err)
	}

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("ses inbound: listen %s: %w", cfg.Addr, err)
	}

	maxSize := cfg.MaxSize
	if maxSize <= 0 {
		maxSize = defaultInboundMaxSize
	}

	sctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s := &inboundServer{
		b: b, ln: ln, cancel: cancel, max: maxSize,
		tlsCfg: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
	}

	s.wg.Go(func() { s.accept(sctx) })

	return s, nil
}

func (s *inboundServer) addr() string { return s.ln.Addr().String() }

func (s *inboundServer) close() {
	s.cancel()
	_ = s.ln.Close()
	s.wg.Wait()
}

func (s *inboundServer) accept(ctx context.Context) {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}

		s.wg.Go(func() { s.serve(ctx, c) })
	}
}

type smtpSession struct {
	conn     net.Conn
	br       *bufio.Reader
	from     string
	clientIP string
	rcpts    []string
	hasFrom  bool
	tls      bool
	greeted  bool
}

func (s *smtpSession) reply(code int, text string) bool {
	_ = s.conn.SetWriteDeadline(time.Now().Add(inboundIdleTimeout))
	_, err := fmt.Fprintf(s.conn, "%d %s\r\n", code, text)

	return err == nil
}

func (s *smtpSession) readLine() (string, error) {
	_ = s.conn.SetReadDeadline(time.Now().Add(inboundIdleTimeout))

	line, prefix, err := s.br.ReadLine()
	if err != nil {
		return "", err
	}

	if prefix {
		return "", errInboundTooBig
	}

	return string(line), nil
}

func (s *smtpSession) resetTx() {
	s.from, s.rcpts, s.hasFrom = "", nil, false
}

func (s *inboundServer) serve(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()

	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	sess := &smtpSession{conn: conn, br: bufio.NewReaderSize(conn, inboundMaxLine)}

	if ap, err := netip.ParseAddrPort(conn.RemoteAddr().String()); err == nil {
		sess.clientIP = ap.Addr().String()

		if s.b.inboundIPBlocked(ap.Addr()) {
			sess.reply(smtpMailboxErr, "5.7.1 Service unavailable; client host blocked by receipt filter")

			return
		}
	}

	if !sess.reply(smtpReady, "gopherstack ESMTP ses-inbound") {
		return
	}

	for {
		line, err := sess.readLine()
		if err != nil {
			if errors.Is(err, errInboundTooBig) {
				sess.reply(smtpSyntaxErr, "5.5.2 Line too long")
			}

			return
		}

		verb, arg, _ := strings.Cut(line, " ")

		if !s.command(ctx, sess, strings.ToUpper(verb), arg) {
			return
		}
	}
}

// command handles one SMTP verb and reports whether the session continues.
func (s *inboundServer) command(ctx context.Context, sess *smtpSession, verb, arg string) bool {
	switch verb {
	case "EHLO":
		return s.ehlo(sess)
	case "HELO":
		sess.greeted = true
		sess.resetTx()

		return sess.reply(smtpOK, "gopherstack")
	case "MAIL":
		return s.mailFrom(sess, arg)
	case "RCPT":
		return s.rcptTo(sess, arg)
	case "DATA":
		return s.data(ctx, sess)
	case "STARTTLS":
		return s.startTLS(sess)
	case "RSET":
		sess.resetTx()

		return sess.reply(smtpOK, "2.0.0 Ok")
	case "NOOP":
		return sess.reply(smtpOK, "2.0.0 Ok")
	case "VRFY":
		return sess.reply(smtpCannotVRFY, "2.5.2 Cannot VRFY user")
	case "QUIT":
		sess.reply(smtpClosing, "2.0.0 Bye")

		return false
	default:
		return sess.reply(smtpNotImpl, "5.5.2 Command not recognized")
	}
}

func (s *inboundServer) ehlo(sess *smtpSession) bool {
	sess.greeted = true
	sess.resetTx()

	caps := []string{"gopherstack", "SIZE " + strconv.FormatInt(s.max, 10), "8BITMIME", "ENHANCEDSTATUSCODES"}
	if !sess.tls {
		caps = append(caps, "STARTTLS")
	}

	_ = sess.conn.SetWriteDeadline(time.Now().Add(inboundIdleTimeout))

	for i, c := range caps {
		sep := "-"
		if i == len(caps)-1 {
			sep = " "
		}

		if _, err := fmt.Fprintf(sess.conn, "%d%s%s\r\n", smtpOK, sep, c); err != nil {
			return false
		}
	}

	return true
}

func (s *inboundServer) startTLS(sess *smtpSession) bool {
	if sess.tls {
		return sess.reply(smtpBadSequence, "5.5.1 TLS already active")
	}

	if !sess.reply(smtpReady, "2.0.0 Ready to start TLS") {
		return false
	}

	tc := tls.Server(sess.conn, s.tlsCfg)
	_ = tc.SetDeadline(time.Now().Add(inboundIdleTimeout))

	if err := tc.HandshakeContext(context.Background()); err != nil {
		return false
	}

	sess.conn, sess.br, sess.tls, sess.greeted = tc, bufio.NewReaderSize(tc, inboundMaxLine), true, false
	sess.resetTx()

	return true
}

// pathArg extracts the address from "FROM:<a@b> PARAM=x" or "TO:<a@b>".
func pathArg(arg, keyword string) (string, string, bool) {
	k, rest, ok := strings.Cut(arg, ":")
	if !ok || !strings.EqualFold(strings.TrimSpace(k), keyword) {
		return "", "", false
	}

	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "<") {
		return "", "", false
	}

	end := strings.IndexByte(rest, '>')
	if end < 0 {
		return "", "", false
	}

	return rest[1:end], strings.TrimSpace(rest[end+1:]), true
}

func (s *inboundServer) mailFrom(sess *smtpSession, arg string) bool {
	if !sess.greeted {
		return sess.reply(smtpBadSequence, "5.5.1 Send EHLO first")
	}

	addr, params, ok := pathArg(arg, "FROM")
	if !ok {
		return sess.reply(smtpArgErr, "5.5.4 Syntax: MAIL FROM:<address>")
	}

	for p := range strings.FieldsSeq(params) {
		k, v, _ := strings.Cut(p, "=")
		if !strings.EqualFold(k, "SIZE") {
			continue
		}

		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > s.max {
			return sess.reply(smtpTooBig, "5.3.4 Message size exceeds fixed maximum message size")
		}
	}

	sess.from, sess.hasFrom, sess.rcpts = addr, true, nil

	return sess.reply(smtpOK, "2.1.0 Ok")
}

func (s *inboundServer) rcptTo(sess *smtpSession, arg string) bool {
	if !sess.hasFrom {
		return sess.reply(smtpBadSequence, "5.5.1 Need MAIL before RCPT")
	}

	addr, _, ok := pathArg(arg, "TO")
	if !ok || !strings.Contains(addr, "@") {
		return sess.reply(smtpArgErr, "5.1.3 Syntax: RCPT TO:<address>")
	}

	if len(sess.rcpts) >= inboundMaxRcpts {
		return sess.reply(smtpTooManyRcpts, "4.5.3 Too many recipients")
	}

	if why, accept := s.b.inboundAcceptRcpt(addr, sess.tls); !accept {
		return sess.reply(smtpMailboxErr, why)
	}

	sess.rcpts = append(sess.rcpts, addr)

	return sess.reply(smtpOK, "2.1.5 Ok")
}

func (s *inboundServer) data(ctx context.Context, sess *smtpSession) bool {
	if len(sess.rcpts) == 0 {
		return sess.reply(smtpBadSequence, "5.5.1 Need RCPT before DATA")
	}

	if !sess.reply(smtpStartData, "End data with <CR><LF>.<CR><LF>") {
		return false
	}

	_ = sess.conn.SetReadDeadline(time.Now().Add(inboundIdleTimeout))

	body, err := io.ReadAll(io.LimitReader(textproto.NewReader(sess.br).DotReader(), s.max+1))
	if err != nil {
		return false
	}

	body = bytes.ReplaceAll(body, []byte("\n"), []byte("\r\n"))

	if int64(len(body)) > s.max {
		sess.reply(smtpTooBig, "5.3.4 Message size exceeds fixed maximum message size")

		return false
	}

	res := s.b.deliverInbound(ctx, inboundEnvelope{
		from: sess.from, clientIP: sess.clientIP, rcpts: sess.rcpts, data: body, tls: sess.tls,
	})
	sess.resetTx()

	if res.code >= smtpTempFail {
		logger.Load(ctx).DebugContext(ctx, "ses inbound: message rejected", "code", res.code)
	}

	return sess.reply(res.code, res.text)
}

// WithInbound enables the inbound SMTP listener; it starts with StartWorker and stops on Shutdown.
func (h *Handler) WithInbound(cfg InboundConfig) *Handler {
	h.inboundCfg = &cfg

	return h
}

// InboundAddr returns the inbound listener address, or "" when it is not running.
func (h *Handler) InboundAddr() string {
	if h.inbound == nil {
		return ""
	}

	return h.inbound.addr()
}

func (h *Handler) startInbound(ctx context.Context) error {
	ib, ok := h.Backend.(*InMemoryBackend)
	if !ok || h.inboundCfg == nil || h.inbound != nil {
		return nil
	}

	s, err := startInboundServer(ctx, ib, *h.inboundCfg)
	if err != nil {
		return err
	}

	h.inbound = s

	return nil
}

func (h *Handler) stopInbound() {
	if h.inbound != nil {
		h.inbound.close()
		h.inbound = nil
	}
}
