package rds

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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	proxyHandshakeTimeout = 15 * time.Second
	proxyUpstreamTimeout  = 10 * time.Second
	proxyMaxGuarded       = 64 << 20
	proxyFrameHeader      = 5
	proxyFrameLenSize     = 4
	pgAuthFailedCode      = "28000"
	pgPasswordFailedCode  = "28P01"
	pgConnFailureCode     = "08006"
	pgFatal               = "FATAL"
	pumpDirections        = 2
	hostLocalhost         = "localhost"
)

// guardedSQL matches statements that would let a token session shed the DB user it was authenticated as.
var guardedSQL = regexp.MustCompile(`(?i)session[\s_]+authorization|\breset\s+all\b|\bdiscard\s+all\b`)

const guardReplacement = `DO $$ BEGIN RAISE EXCEPTION 'permission denied to set parameter "session_authorization"' ` +
	`USING ERRCODE = '42501'; END $$`

var (
	errUnsupportedStartup = errors.New("unsupported startup message")
	errInvalidFrame       = errors.New("invalid frame length")
	errFrameTooLarge      = errors.New("frame too large")
)

// pgProxy fronts a Postgres container: TLS termination plus IAM token authentication.
type pgProxy struct {
	ln     net.Listener
	be     *InMemoryBackend
	lb     *liveDB
	tlsCfg *tls.Config
	conns  sync.Map
	wg     sync.WaitGroup
	port   int
	once   sync.Once
}

func (p *pgProxy) close() {
	p.once.Do(func() {
		_ = p.ln.Close()

		p.conns.Range(func(k, _ any) bool {
			if c, ok := k.(net.Conn); ok {
				_ = c.Close()
			}

			return true
		})

		p.be.engine.releasePort(p.port)
	})
}

func (e *dbEngine) serverTLS() *tls.Config {
	e.tlsOnce.Do(func() {
		ca, err := loadOrCreateEngineCA(e.cfg.CADir)
		if err != nil {
			logger.Load(context.Background()).Warn("rds: local CA unavailable, proxy serves without TLS", "error", err)

			return
		}

		cert, err := ca.serverCert(e.cfg.Host)
		if err != nil {
			logger.Load(context.Background()).Warn("rds: proxy certificate unavailable", "error", err)

			return
		}

		e.ca = ca
		e.tlsCfg = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	})

	return e.tlsCfg
}

// EngineCAPEM returns the PEM of the local CA that signs docker-mode database endpoints, or nil without an engine.
func (b *InMemoryBackend) EngineCAPEM() []byte {
	b.mu.RLock("EngineCAPEM")
	e := b.engine
	b.mu.RUnlock()

	if e == nil || e.serverTLS() == nil {
		return nil
	}

	return append([]byte(nil), e.ca.pem...)
}

func (b *InMemoryBackend) startProxy(lb *liveDB) error {
	e := b.engine

	port, err := e.acquirePort()
	if err != nil {
		return err
	}

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp",
		net.JoinHostPort(container.BindHostFor(e.cfg.Host), strconv.Itoa(port)))
	if err != nil {
		e.releasePort(port)

		return fmt.Errorf("proxy listen: %w", err)
	}

	p := &pgProxy{be: b, lb: lb, ln: ln, port: port, tlsCfg: e.serverTLS()}
	lb.proxy.Store(p)

	select {
	case <-lb.gone:
		p.close()

		return nil
	default:
	}

	p.wg.Go(p.acceptLoop)

	return nil
}

func (p *pgProxy) acceptLoop() {
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			return
		}

		p.conns.Store(conn, struct{}{})
		p.wg.Go(func() {
			defer p.conns.Delete(conn)

			p.serve(conn)
		})
	}
}

type proxyTarget struct {
	login EngineLogin
	auth  iamAuthTarget
}

// target snapshots the unit's current IAM and login state; it is read per connection.
func (b *InMemoryBackend) proxyTarget(lb *liveDB, proxyPort int) (proxyTarget, bool) {
	b.mu.RLock("rdsProxyTarget")
	defer b.mu.RUnlock()

	if !b.attachedLocked(lb) {
		return proxyTarget{}, false
	}

	t := proxyTarget{
		login: b.engine.login(lb),
		auth: iamAuthTarget{
			endpoint: net.JoinHostPort(b.engine.cfg.Host, strconv.Itoa(proxyPort)),
			region:   b.region, account: b.accountID, partition: "aws",
		},
	}

	b.fillIAMTarget(lb, &t.auth)

	return t, true
}

func (b *InMemoryBackend) fillIAMTarget(lb *liveDB, a *iamAuthTarget) {
	if strings.HasPrefix(lb.key, unitInstancePrefix) {
		if inst, found := b.instances.Get(normalizeID(lb.resourceID)); found {
			a.enabled, a.resourceID = inst.IAMDatabaseAuthenticationEnabled, inst.DbiResourceID
		}

		return
	}

	if c, found := b.clusters.Get(normalizeID(lb.resourceID)); found {
		a.enabled, a.resourceID = c.IAMDatabaseAuthenticationEnabled, c.DBClusterResourceID
	}
}

func sameEndpoint(tokenHost, advertised string) bool {
	if tokenHost == advertised {
		return true
	}

	th, tp, err1 := net.SplitHostPort(tokenHost)
	ah, ap, err2 := net.SplitHostPort(advertised)

	if err1 != nil || err2 != nil || tp != ap {
		return false
	}

	loop := func(h string) bool {
		ip := net.ParseIP(h)

		return h == hostLocalhost || (ip != nil && ip.IsLoopback())
	}

	return loop(th) && loop(ah)
}

type clientConn struct {
	net.Conn
	backend *pgproto3.Backend
	tls     bool
}

func (c *clientConn) fatal(code, msg string) {
	c.backend.Send(&pgproto3.ErrorResponse{Severity: pgFatal, SeverityUnlocalized: pgFatal, Code: code, Message: msg})
	_ = c.backend.Flush()
}

func (p *pgProxy) startup(conn net.Conn) (*clientConn, *pgproto3.StartupMessage, error) {
	c := &clientConn{Conn: conn, backend: pgproto3.NewBackend(conn, conn)}

	for {
		msg, err := c.backend.ReceiveStartupMessage()
		if err != nil {
			return nil, nil, fmt.Errorf("startup: %w", err)
		}

		switch m := msg.(type) {
		case *pgproto3.SSLRequest:
			if c, err = p.negotiateSSL(c); err != nil {
				return nil, nil, err
			}
		case *pgproto3.GSSEncRequest:
			if _, err = conn.Write([]byte{'N'}); err != nil {
				return nil, nil, fmt.Errorf("gss refuse: %w", err)
			}
		case *pgproto3.StartupMessage:
			return c, m, nil
		case *pgproto3.CancelRequest:
			p.forwardCancel(m)

			return nil, nil, io.EOF
		default:
			return nil, nil, errUnsupportedStartup
		}
	}
}

// negotiateSSL answers an SSLRequest: upgrade to TLS when a certificate is available, else refuse.
func (p *pgProxy) negotiateSSL(c *clientConn) (*clientConn, error) {
	if p.tlsCfg == nil {
		if _, err := c.Conn.Write([]byte{'N'}); err != nil {
			return nil, fmt.Errorf("ssl refuse: %w", err)
		}

		return c, nil
	}

	if _, err := c.Conn.Write([]byte{'S'}); err != nil {
		return nil, fmt.Errorf("ssl accept: %w", err)
	}

	tc := tls.Server(c.Conn, p.tlsCfg)
	if err := tc.HandshakeContext(context.Background()); err != nil {
		return nil, fmt.Errorf("tls handshake: %w", err)
	}

	return &clientConn{Conn: tc, backend: pgproto3.NewBackend(tc, tc), tls: true}, nil
}

func (p *pgProxy) forwardCancel(m *pgproto3.CancelRequest) {
	t, ok := p.be.proxyTarget(p.lb, p.port)
	if !ok {
		return
	}

	conn, err := (&net.Dialer{Timeout: proxyUpstreamTimeout}).DialContext(context.Background(), "tcp", t.login.Addr)
	if err != nil {
		return
	}

	defer func() { _ = conn.Close() }()

	if buf, eerr := m.Encode(nil); eerr == nil {
		_, _ = conn.Write(buf)
	}
}

func (p *pgProxy) serve(raw net.Conn) {
	defer func() { _ = raw.Close() }()

	release, ok := p.be.beginConn(p.lb)
	if !ok {
		return
	}

	defer release()

	_ = raw.SetDeadline(time.Now().Add(proxyHandshakeTimeout))

	c, start, err := p.startup(raw)
	if err != nil {
		return
	}

	defer func() { _ = c.Close() }()

	up, guard, ok := p.authenticate(c, start)
	if !ok {
		return
	}

	defer func() { _ = up.Close() }()

	_ = c.SetDeadline(time.Time{})

	pump(c.Conn, up, guard)
}

// authenticate completes the client handshake and returns the authenticated upstream connection.
func (p *pgProxy) authenticate(c *clientConn, start *pgproto3.StartupMessage) (net.Conn, bool, bool) {
	user := start.Parameters["user"]

	c.backend.Send(&pgproto3.AuthenticationCleartextPassword{})

	if err := c.backend.Flush(); err != nil {
		return nil, false, false
	}

	if err := c.backend.SetAuthType(pgproto3.AuthTypeCleartextPassword); err != nil {
		return nil, false, false
	}

	msg, err := c.backend.Receive()
	if err != nil {
		return nil, false, false
	}

	pw, ok := msg.(*pgproto3.PasswordMessage)
	if !ok {
		c.fatal(pgAuthFailedCode, "expected password response")

		return nil, false, false
	}

	t, ok := p.be.proxyTarget(p.lb, p.port)
	if !ok {
		c.fatal(pgConnFailureCode, "database is shutting down")

		return nil, false, false
	}

	login, tokenUser := t.login, ""

	if t.auth.enabled && looksLikeAuthToken(pw.Password) {
		tokenUser, ok = p.verifyToken(c, t, user, pw.Password)
		if !ok {
			return nil, false, false
		}
	} else {
		login.User, login.Password = user, pw.Password
	}

	up, hj, ok := p.dialUpstream(c, login, start, tokenUser)
	if !ok {
		return nil, false, false
	}

	if !p.completeStartup(c, hj) {
		_ = up.Close()

		return nil, false, false
	}

	return up, tokenUser != "", true
}

func (p *pgProxy) verifyToken(c *clientConn, t proxyTarget, user, password string) (string, bool) {
	deny := func() (string, bool) {
		c.fatal(pgAuthFailedCode, fmt.Sprintf("PAM authentication failed for user %q", user))

		return "", false
	}

	if !c.tls {
		return deny()
	}

	cfg := p.be.iamAuthConfig()
	if cfg == nil {
		return deny()
	}

	ip, _, _ := net.SplitHostPort(c.RemoteAddr().String())

	dbUser, err := authenticateToken(*cfg, t.auth, password, ip, time.Now())
	if err != nil || dbUser != user {
		logger.Load(context.Background()).Info("rds: IAM database authentication refused", "user", user, "error", err)

		return deny()
	}

	return dbUser, true
}

func (p *pgProxy) dialUpstream(
	c *clientConn, login EngineLogin, start *pgproto3.StartupMessage, sessionUser string,
) (net.Conn, *pgconn.HijackedConn, bool) {
	db := start.Parameters["database"]
	if db == "" {
		db = start.Parameters["user"]
	}

	u := url.URL{
		Scheme: postgresScheme, User: url.UserPassword(login.User, login.Password), Host: login.Addr, Path: "/" + db,
		RawQuery: "sslmode=disable",
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
		p.relayConnectError(c, err, sessionUser)

		return nil, nil, false
	}

	if sessionUser != "" && sessionUser != login.User {
		_, err = pc.Exec(ctx, "SET SESSION AUTHORIZATION "+quotePGIdent(sessionUser)).ReadAll()
		if err != nil {
			_ = pc.Close(ctx)

			c.fatal(pgAuthFailedCode, fmt.Sprintf("PAM authentication failed for user %q", sessionUser))

			return nil, nil, false
		}
	}

	hj, err := pc.Hijack()
	if err != nil {
		_ = pc.Close(ctx)

		c.fatal(pgConnFailureCode, "upstream connection unavailable")

		return nil, nil, false
	}

	return hj.Conn, hj, true
}

func (p *pgProxy) relayConnectError(c *clientConn, err error, sessionUser string) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && sessionUser == "" {
		c.fatal(pgErr.Code, pgErr.Message)

		return
	}

	c.fatal(pgConnFailureCode, "could not connect to the database")
}

func (p *pgProxy) completeStartup(c *clientConn, hj *pgconn.HijackedConn) bool {
	c.backend.Send(&pgproto3.AuthenticationOk{})

	for k, v := range hj.ParameterStatuses {
		c.backend.Send(&pgproto3.ParameterStatus{Name: k, Value: v})
	}

	c.backend.Send(&pgproto3.BackendKeyData{ProcessID: hj.PID, SecretKey: hj.SecretKey})
	c.backend.Send(&pgproto3.ReadyForQuery{TxStatus: hj.TxStatus})

	return c.backend.Flush() == nil
}

func quotePGIdent(s string) string {
	return `"` + string(bytes.ReplaceAll([]byte(s), []byte(`"`), []byte(`""`))) + `"`
}

func pump(client, upstream net.Conn, guard bool) {
	done := make(chan struct{}, pumpDirections)

	go func() {
		_, _ = io.Copy(client, upstream)

		done <- struct{}{}
	}()

	go func() {
		_ = relayFrontend(upstream, client, guard)

		done <- struct{}{}
	}()

	<-done

	_ = client.Close()
	_ = upstream.Close()

	<-done
}

// relayFrontend forwards client messages, neutralising session-authorization changes when guard is set.
func relayFrontend(dst io.Writer, src io.Reader, guard bool) error {
	hdr := make([]byte, proxyFrameHeader)

	for {
		if _, err := io.ReadFull(src, hdr); err != nil {
			return fmt.Errorf("read frame: %w", err)
		}

		n := int(binary.BigEndian.Uint32(hdr[1:])) - proxyFrameLenSize
		if n < 0 {
			return errInvalidFrame
		}

		if guard && (hdr[0] == 'Q' || hdr[0] == 'P') {
			if err := relayGuarded(dst, src, hdr[0], n); err != nil {
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

func relayGuarded(dst io.Writer, src io.Reader, typ byte, n int) error {
	if n > proxyMaxGuarded {
		return errFrameTooLarge
	}

	body := make([]byte, n)
	if _, err := io.ReadFull(src, body); err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	body = guardBody(typ, body)

	out := make([]byte, proxyFrameHeader, proxyFrameHeader+len(body))
	out[0] = typ
	total := uint32(len(body) + proxyFrameLenSize) //nolint:gosec // body bounded by proxyMaxGuarded
	binary.BigEndian.PutUint32(out[1:], total)

	if _, err := dst.Write(append(out, body...)); err != nil {
		return fmt.Errorf("write frame: %w", err)
	}

	return nil
}

// guardBody swaps a forbidden Query or Parse statement for one that raises a permission error upstream.
func guardBody(typ byte, body []byte) []byte {
	nameEnd := 0

	if typ == 'P' {
		i := bytes.IndexByte(body, 0)
		if i < 0 {
			return body
		}

		nameEnd = i + 1
	}

	end := bytes.IndexByte(body[nameEnd:], 0)
	if end < 0 || !guardedSQL.Match(body[nameEnd:nameEnd+end]) {
		return body
	}

	out := append([]byte(nil), body[:nameEnd]...)
	out = append(out, guardReplacement...)

	return append(out, body[nameEnd+end:]...)
}
