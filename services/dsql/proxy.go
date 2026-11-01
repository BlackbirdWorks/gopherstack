package dsql

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	proxyHandshakeTimeout = 15 * time.Second
	proxyUpstreamTimeout  = 10 * time.Second
	proxyMaxFrame         = 64 << 20
	proxyFrameHeader      = 5
	proxyFrameLenSize     = 4
	pgPasswordFailedCode  = "28P01"
	pgAuthFailedCode      = "28000"
	pgConnFailureCode     = "08006"
	pgNoDatabaseCode      = "3D000"
	pgFatal               = "FATAL"
	pumpDirections        = 2
	frameQuery            = 'Q'
	frameParse            = 'P'
	postgresScheme        = "postgres"
)

var (
	errUnsupportedStartup = errors.New("unsupported startup message")
	errInvalidFrame       = errors.New("invalid frame length")
	errFrameTooLarge      = errors.New("frame too large")
)

type clientConn struct {
	net.Conn
	backend *pgproto3.Backend
	tls     bool
}

func (c *clientConn) fatal(code, msg string) {
	c.backend.Send(&pgproto3.ErrorResponse{Severity: pgFatal, SeverityUnlocalized: pgFatal, Code: code, Message: msg})
	_ = c.backend.Flush()
}

func (i *dockerInstance) acceptLoop() {
	for {
		conn, err := i.ln.Accept()
		if err != nil {
			return
		}

		if !i.conns.add(conn) {
			continue
		}

		i.wg.Go(func() {
			defer i.conns.remove(conn)

			i.serve(conn)
		})
	}
}

func (i *dockerInstance) serve(raw net.Conn) {
	defer func() { _ = raw.Close() }()

	_ = raw.SetDeadline(time.Now().Add(proxyHandshakeTimeout))

	c, start, err := i.startup(raw)
	if err != nil {
		return
	}

	defer func() { _ = c.Close() }()

	up, ok := i.authenticate(c, start)
	if !ok {
		return
	}

	defer func() { _ = up.Close() }()

	_ = c.SetDeadline(time.Time{})

	pump(c.Conn, up)
}

func (i *dockerInstance) startup(conn net.Conn) (*clientConn, *pgproto3.StartupMessage, error) {
	c := &clientConn{Conn: conn, backend: pgproto3.NewBackend(conn, conn)}

	for {
		msg, err := c.backend.ReceiveStartupMessage()
		if err != nil {
			return nil, nil, fmt.Errorf("startup: %w", err)
		}

		switch m := msg.(type) {
		case *pgproto3.SSLRequest:
			if c, err = i.negotiateSSL(c); err != nil {
				return nil, nil, err
			}
		case *pgproto3.GSSEncRequest:
			if _, err = conn.Write([]byte{'N'}); err != nil {
				return nil, nil, fmt.Errorf("gss refuse: %w", err)
			}
		case *pgproto3.StartupMessage:
			return c, m, nil
		case *pgproto3.CancelRequest:
			i.forwardCancel(m)

			return nil, nil, io.EOF
		default:
			return nil, nil, errUnsupportedStartup
		}
	}
}

func (i *dockerInstance) negotiateSSL(c *clientConn) (*clientConn, error) {
	if _, err := c.Conn.Write([]byte{'S'}); err != nil {
		return nil, fmt.Errorf("ssl accept: %w", err)
	}

	tc := tls.Server(c.Conn, i.tlsCfg)
	if err := tc.HandshakeContext(context.Background()); err != nil {
		return nil, fmt.Errorf("tls handshake: %w", err)
	}

	return &clientConn{Conn: tc, backend: pgproto3.NewBackend(tc, tc), tls: true}, nil
}

func (i *dockerInstance) forwardCancel(m *pgproto3.CancelRequest) {
	h, p := i.srv.Endpoint()

	conn, err := (&net.Dialer{Timeout: proxyUpstreamTimeout}).DialContext(context.Background(), "tcp",
		net.JoinHostPort(h, strconv.Itoa(p)))
	if err != nil {
		return
	}

	defer func() { _ = conn.Close() }()

	if buf, eerr := m.Encode(nil); eerr == nil {
		_, _ = conn.Write(buf)
	}
}

type upstreamLogin struct {
	user     string
	password string
}

// authenticate completes the client handshake and returns the authenticated upstream connection.
func (i *dockerInstance) authenticate(c *clientConn, start *pgproto3.StartupMessage) (net.Conn, bool) {
	user := start.Parameters["user"]

	if !c.tls {
		c.fatal(pgAuthFailedCode, "SSL/TLS connection is required")

		return nil, false
	}

	db := start.Parameters["database"]
	if db == "" {
		db = user
	}

	if db != dataPlaneDB {
		c.fatal(pgNoDatabaseCode, fmt.Sprintf("database %q does not exist", db))

		return nil, false
	}

	pw, ok := i.requestPassword(c)
	if !ok {
		return nil, false
	}

	login, ok := i.verify(c, user, pw)
	if !ok {
		return nil, false
	}

	up, hj, ok := i.dialUpstream(c, login, start)
	if !ok {
		return nil, false
	}

	if !completeStartup(c, hj) {
		_ = up.Close()

		return nil, false
	}

	return up, true
}

func (i *dockerInstance) requestPassword(c *clientConn) (string, bool) {
	c.backend.Send(&pgproto3.AuthenticationCleartextPassword{})

	if err := c.backend.Flush(); err != nil {
		return "", false
	}

	if err := c.backend.SetAuthType(pgproto3.AuthTypeCleartextPassword); err != nil {
		return "", false
	}

	msg, err := c.backend.Receive()
	if err != nil {
		return "", false
	}

	pw, ok := msg.(*pgproto3.PasswordMessage)
	if !ok {
		c.fatal(pgAuthFailedCode, "expected password response")

		return "", false
	}

	return pw.Password, true
}

func (i *dockerInstance) verify(c *clientConn, user, password string) (upstreamLogin, bool) {
	deny := func(err error) (upstreamLogin, bool) {
		logger.Load(context.Background()).Info("dsql: IAM authentication refused", "cluster", i.ref.ID,
			"user", user, "error", err)
		c.fatal(pgPasswordFailedCode, fmt.Sprintf("password authentication failed for user %q", user))

		return upstreamLogin{}, false
	}

	cfg := i.l.cfg.Auth()
	if cfg == nil {
		return deny(errTokenUnknownID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), proxyUpstreamTimeout)
	defer cancel()

	var secret string

	granted := func(role, arn string) bool {
		s, ok := i.roleLogin(ctx, role, arn)
		secret = s

		return ok
	}

	ip, _, _ := net.SplitHostPort(c.RemoteAddr().String())
	target := authTarget{endpoint: i.ref.Endpoint, region: i.ref.Region, arn: i.ref.ARN, port: i.port}

	sess, err := authenticateToken(*cfg, target, user, password, ip, i.l.cfg.Now(), granted)
	if err != nil {
		return deny(err)
	}

	if sess.role == adminRole {
		return upstreamLogin{user: adminRole, password: i.adminPW}, true
	}

	return upstreamLogin{user: sess.role, password: secret}, true
}

func (i *dockerInstance) dialUpstream(
	c *clientConn, login upstreamLogin, start *pgproto3.StartupMessage,
) (net.Conn, *pgconn.HijackedConn, bool) {
	h, p := i.srv.Endpoint()
	u := url.URL{
		Scheme: postgresScheme, User: url.UserPassword(login.user, login.password),
		Host: net.JoinHostPort(h, strconv.Itoa(p)), Path: "/" + dataPlaneDB, RawQuery: "sslmode=disable",
	}

	cfg, err := pgconn.ParseConfig(u.String())
	if err != nil {
		c.fatal(pgConnFailureCode, "invalid connection parameters")

		return nil, nil, false
	}

	cfg.ConnectTimeout = proxyUpstreamTimeout

	for k, v := range start.Parameters {
		if k != "user" && k != "database" && k != "replication" && k != "options" {
			cfg.RuntimeParams[k] = v
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), proxyUpstreamTimeout)
	defer cancel()

	pc, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		c.fatal(pgConnFailureCode, "could not connect to the database")

		return nil, nil, false
	}

	var hj *pgconn.HijackedConn

	if err = pc.SyncConn(ctx); err == nil {
		hj, err = pc.Hijack()
	}

	if err != nil {
		_ = pc.Close(ctx)

		c.fatal(pgConnFailureCode, "upstream connection unavailable")

		return nil, nil, false
	}

	return hj.Conn, hj, true
}

func completeStartup(c *clientConn, hj *pgconn.HijackedConn) bool {
	c.backend.Send(&pgproto3.AuthenticationOk{})

	for k, v := range hj.ParameterStatuses {
		c.backend.Send(&pgproto3.ParameterStatus{Name: k, Value: v})
	}

	c.backend.Send(&pgproto3.BackendKeyData{ProcessID: hj.PID, SecretKey: hj.SecretKey})
	c.backend.Send(&pgproto3.ReadyForQuery{TxStatus: hj.TxStatus})

	return c.backend.Flush() == nil
}

func pump(client, upstream net.Conn) {
	done := make(chan struct{}, pumpDirections)

	go func() {
		_, _ = io.Copy(client, upstream)

		done <- struct{}{}
	}()

	go func() {
		_ = relayFrontend(upstream, client)

		done <- struct{}{}
	}()

	<-done

	_ = client.Close()
	_ = upstream.Close()

	<-done
}

// relayFrontend forwards client messages, rewriting Query and Parse statements through rewriteSQL.
func relayFrontend(dst io.Writer, src io.Reader) error {
	hdr := make([]byte, proxyFrameHeader)

	for {
		if _, err := io.ReadFull(src, hdr); err != nil {
			return fmt.Errorf("read frame: %w", err)
		}

		n := int(binary.BigEndian.Uint32(hdr[1:])) - proxyFrameLenSize
		if n < 0 {
			return errInvalidFrame
		}

		if hdr[0] == frameQuery || hdr[0] == frameParse {
			if err := relayStatement(dst, src, hdr[0], n); err != nil {
				return err
			}

			continue
		}

		if _, err := dst.Write(hdr); err != nil {
			return fmt.Errorf("write header: %w", err)
		}

		if _, err := io.CopyN(dst, src, int64(n)); err != nil {
			return fmt.Errorf("copy body: %w", err)
		}
	}
}

func relayStatement(dst io.Writer, src io.Reader, typ byte, n int) error {
	if n > proxyMaxFrame {
		return errFrameTooLarge
	}

	body := make([]byte, n)
	if _, err := io.ReadFull(src, body); err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	body = rewriteFrame(typ, body)

	out := make([]byte, proxyFrameHeader, proxyFrameHeader+len(body))
	out[0] = typ
	binary.BigEndian.PutUint32(
		out[1:],
		uint32(len(body)+proxyFrameLenSize),
	) //nolint:gosec // body bounded by proxyMaxFrame

	if _, err := dst.Write(append(out, body...)); err != nil {
		return fmt.Errorf("write frame: %w", err)
	}

	return nil
}

// rewriteFrame applies rewriteSQL to the statement text of a Query ('Q') or Parse ('P') message body.
func rewriteFrame(typ byte, body []byte) []byte {
	nameEnd := 0

	if typ == frameParse {
		i := bytes.IndexByte(body, 0)
		if i < 0 {
			return body
		}

		nameEnd = i + 1
	}

	end := bytes.IndexByte(body[nameEnd:], 0)
	if end < 0 {
		return body
	}

	orig := string(body[nameEnd : nameEnd+end])

	rewritten := rewriteSQL(orig)
	if rewritten == orig {
		return body
	}

	out := append([]byte(nil), body[:nameEnd]...)
	out = append(out, rewritten...)

	return append(out, body[nameEnd+end:]...)
}
