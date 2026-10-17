package rds

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const myServerCaps = myCapLongPassword | myCapFoundRows | myCapLongFlag | myCapConnectWithDB | myCapProtocol41 |
	myCapTransactions | myCapSecureConn | myCapMultiStatements | myCapMultiResults | myCapPluginAuth |
	myCapConnectAttrs | myCapLenencData | myCapDeprecateEOF

var errMyHandshake = errors.New("mysql handshake refused")

// detachConn lets the database/sql driver authenticate on a socket the proxy then takes back.
type detachConn struct {
	net.Conn
	detached atomic.Bool
}

func (c *detachConn) Write(b []byte) (int, error) {
	if c.detached.Load() {
		return len(b), nil
	}

	return c.Conn.Write(b) //nolint:wrapcheck // transparent conn wrapper
}

func (c *detachConn) Close() error {
	if c.detached.Load() {
		return nil
	}

	return c.Conn.Close() //nolint:wrapcheck // transparent conn wrapper
}

// serveIAM authenticates a client of an IAM-enabled MySQL/MariaDB database, then pipes it to the engine.
// The engine session runs as the master login (or the password user): MySQL has no per-session user switch.
func (f *tcpForwarder) serveIAM(client net.Conn, t proxyTarget) {
	_ = client.SetDeadline(time.Now().Add(proxyHandshakeTimeout))

	c, resp, seq, err := f.clientHandshake(client)
	if err != nil {
		return
	}

	login, ok := f.authorizeMySQL(c, seq, resp, t)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), proxyUpstreamTimeout)
	up, err := dialMySQLUpstream(ctx, login, resp)

	cancel()

	if err != nil {
		_ = writeMyPacket(c, seq+1, upstreamErrPacket(err))

		return
	}

	defer func() { _ = up.Close() }()

	if writeMyPacket(c, seq+1, encodeMyOK()) != nil {
		return
	}

	_ = c.SetDeadline(time.Time{})

	pipeConns(c, up)
}

func (f *tcpForwarder) clientHandshake(client net.Conn) (net.Conn, *myHandshakeResponse, byte, error) {
	cfg := f.be.engine.serverTLS()
	caps := uint32(myServerCaps)

	if cfg != nil {
		caps |= myCapSSL
	}

	scramble := make([]byte, myScrambleLen)
	_, _ = rand.Read(scramble)

	greeting := encodeMyGreeting(f.lb.version, f.connSeq.Add(1), scramble, caps, myPluginClear)
	if err := writeMyPacket(client, 0, greeting); err != nil {
		return nil, nil, 0, err
	}

	seq, body, err := readMyPacket(client)
	if err != nil {
		return nil, nil, 0, err
	}

	resp, err := decodeMyHandshakeResponse(body)
	if err != nil {
		return nil, nil, 0, err
	}

	if resp.caps&myCapDeprecateEOF == 0 {
		return nil, nil, 0, refuseMySQLClient(client, seq)
	}

	c := client

	if resp.caps&myCapSSL != 0 && resp.user == "" {
		if cfg == nil {
			return nil, nil, 0, errMyHandshake
		}

		if c, seq, resp, err = upgradeMyTLS(client, cfg); err != nil {
			return nil, nil, 0, err
		}
	}

	if resp.caps&myCapDeprecateEOF == 0 {
		return nil, nil, 0, refuseMySQLClient(c, seq)
	}

	return c, resp, seq, nil
}

func refuseMySQLClient(c net.Conn, seq byte) error {
	msg := "IAM database authentication requires a client that supports CLIENT_DEPRECATE_EOF"
	_ = writeMyPacket(c, seq+1, encodeMyErr(myErrNotSupported, myStateNoConnect, msg))

	return errMyHandshake
}

func upgradeMyTLS(client net.Conn, cfg *tls.Config) (net.Conn, byte, *myHandshakeResponse, error) {
	tc := tls.Server(client, cfg)
	if err := tc.HandshakeContext(context.Background()); err != nil {
		return nil, 0, nil, fmt.Errorf("tls handshake: %w", err)
	}

	seq, body, err := readMyPacket(tc)
	if err != nil {
		return nil, 0, nil, err
	}

	resp, err := decodeMyHandshakeResponse(body)
	if err != nil {
		return nil, 0, nil, err
	}

	return tc, seq, resp, nil
}

// authorizeMySQL turns the client's cleartext password into the engine login to use, or refuses with an ERR packet.
func (f *tcpForwarder) authorizeMySQL(
	c net.Conn, seq byte, resp *myHandshakeResponse, t proxyTarget,
) (EngineLogin, bool) {
	password := strings.TrimSuffix(string(resp.authData), "\x00")
	login := t.login
	login.DBName = resp.database

	if !looksLikeAuthToken(password) {
		login.User, login.Password = resp.user, password

		return login, true
	}

	_, isTLS := c.(*tls.Conn)
	cfg := f.be.iamAuthConfig()

	if isTLS && cfg != nil {
		ip, _, _ := net.SplitHostPort(c.RemoteAddr().String())

		dbUser, err := authenticateToken(*cfg, t.auth, password, ip, time.Now())
		if err == nil && dbUser == resp.user {
			return login, true
		}

		logger.Load(context.Background()).Info("rds: IAM database authentication refused", "user", resp.user, "error", err)
	}

	msg := fmt.Sprintf("Access denied for user '%s' (using password: YES)", resp.user)
	_ = writeMyPacket(c, seq+1, encodeMyErr(myErrAccessDenied, myStateDenied, msg))

	return EngineLogin{}, false
}

func upstreamErrPacket(err error) []byte {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		state := string(me.SQLState[:])
		if me.SQLState == [5]byte{} {
			state = myStateGeneral
		}

		return encodeMyErr(me.Number, state, me.Message)
	}

	return encodeMyErr(myErrUnknown, myStateGeneral, "could not connect to the database")
}

// dialMySQLUpstream authenticates to the engine with the real driver (native, sha2 and RSA exchanges)
// and returns the raw, session-ready socket.
func dialMySQLUpstream(ctx context.Context, login EngineLogin, resp *myHandshakeResponse) (net.Conn, error) {
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net, cfg.Addr = login.User, login.Password, "tcp", login.Addr
	cfg.DBName, cfg.Timeout = login.DBName, proxyUpstreamTimeout
	cfg.MultiStatements = resp.caps&myCapMultiStatements != 0
	cfg.ClientFoundRows = resp.caps&myCapFoundRows != 0

	var dc *detachConn

	cfg.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		raw, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, fmt.Errorf("dial upstream: %w", err)
		}

		dc = &detachConn{Conn: raw}

		return dc, nil
	}

	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("mysql connector: %w", err)
	}

	drv, err := connector.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("upstream login: %w", err)
	}

	dc.detached.Store(true)
	_ = drv.Close()
	_ = dc.Conn.SetDeadline(time.Time{})

	return dc.Conn, nil
}
