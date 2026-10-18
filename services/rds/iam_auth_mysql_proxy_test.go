package rds //nolint:testpackage // white-box access to the proxy codec and fake engine.

import (
	"context"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	fakeMyMasterPassword = "masterpw1"
	fakeMyNativePlugin   = "mysql_native_password"
	fakeMyComPing        = 0x0e
)

func fakeMyScramble() []byte { return []byte("0123456789abcdefghij") }

// fakeMyRuntime serves a MySQL-protocol listener on the host port a container would publish.
type fakeMyRuntime struct {
	listeners []net.Listener
	mu        sync.Mutex
}

func (f *fakeMyRuntime) CreateAndStart(ctx context.Context, spec container.Spec) (string, error) {
	_, host, _, _ := container.ParsePortSpec(spec.Ports[0])

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:"+host)
	if err != nil {
		return "", err
	}

	f.mu.Lock()
	f.listeners = append(f.listeners, ln)
	f.mu.Unlock()

	go func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}

			go serveFakeMy(conn)
		}
	}()

	return "fake-" + host, nil
}

func (f *fakeMyRuntime) StopAndRemove(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, ln := range f.listeners {
		_ = ln.Close()
	}

	return nil
}

func (f *fakeMyRuntime) StopContainer(context.Context, string) error  { return nil }
func (f *fakeMyRuntime) StartContainer(context.Context, string) error { return nil }

func nativeScramble(password string) []byte {
	h1 := sha1.Sum([]byte(password))
	h2 := sha1.Sum(h1[:])
	h3 := sha1.Sum(append(append([]byte(nil), fakeMyScramble()...), h2[:]...))

	out := make([]byte, len(h1))
	for i := range h1 {
		out[i] = h1[i] ^ h3[i]
	}

	return out
}

func serveFakeMy(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	caps := uint32(myCapProtocol41 | myCapSecureConn | myCapPluginAuth | myCapLongPassword | myCapConnectWithDB)
	if writeMyPacket(conn, 0, encodeMyGreeting("8.4.0", 1, fakeMyScramble(), caps, fakeMyNativePlugin)) != nil {
		return
	}

	_, body, err := readMyPacket(conn)
	if err != nil {
		return
	}

	resp, err := decodeMyHandshakeResponse(body)
	if err != nil {
		return
	}

	if resp.user != "master" || string(resp.authData) != string(nativeScramble(fakeMyMasterPassword)) {
		_ = writeMyPacket(conn, 2, encodeMyErr(myErrAccessDenied, myStateDenied, "Access denied for user"))

		return
	}

	if writeMyPacket(conn, 2, encodeMyOK()) != nil {
		return
	}

	for {
		_, cmd, rerr := readMyPacket(conn)
		if rerr != nil || len(cmd) == 0 {
			return
		}

		if cmd[0] != fakeMyComPing && cmd[0] != 0x03 {
			return
		}

		if writeMyPacket(conn, 1, encodeMyOK()) != nil {
			return
		}
	}
}

type myProxyFixture struct {
	b    *InMemoryBackend
	auth *authFixture
	pool *x509.CertPool
	inst DBInstance
}

func newMyProxyFixture(t *testing.T, iamEnabled bool, policy string) *myProxyFixture {
	t.Helper()

	b := NewInMemoryBackend(authTestAccount, authTestRegion)
	t.Cleanup(b.Close)

	b.EnableEngine(EngineConfig{
		Runtime: &fakeMyRuntime{},
		Probe:   func(context.Context, EngineLogin) error { return nil },
	})

	f := newAuthFixture(t, policy)
	b.SetIAMAuth(func() IAMAuthConfig { return IAMAuthConfig{Identities: f.iam, Enforce: true} })

	_, err := b.CreateDBInstance("mydb", "mysql", "db.t3.micro", "appdb", "master", "", 20,
		DBInstanceOptions{MasterUserPassword: fakeMyMasterPassword, IAMDatabaseAuthenticationEnabled: iamEnabled})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, derr := b.DescribeDBInstances("mydb")

		return derr == nil && len(got) == 1 && got[0].DBInstanceStatus == "available"
	}, 10*time.Second, 5*time.Millisecond)

	got, err := b.DescribeDBInstances("mydb")
	require.NoError(t, err)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(b.EngineCAPEM()))

	return &myProxyFixture{b: b, auth: f, pool: pool, inst: got[0]}
}

func (p *myProxyFixture) token(t *testing.T, signed time.Time) string {
	t.Helper()

	return buildTestToken(t, net.JoinHostPort(p.inst.Endpoint, strconv.Itoa(p.inst.Port)),
		authTestRegion, "bob", p.auth.creds, signed, "900")
}

func (p *myProxyFixture) ping(t *testing.T, user, password string, withTLS bool) error {
	t.Helper()

	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net = user, password, "tcp"
	cfg.Addr = net.JoinHostPort(p.inst.Endpoint, strconv.Itoa(p.inst.Port))
	cfg.DBName, cfg.AllowCleartextPasswords, cfg.Timeout = "appdb", true, 5*time.Second

	if withTLS {
		cfg.TLS = &tls.Config{RootCAs: p.pool, ServerName: p.inst.Endpoint, MinVersion: tls.VersionTLS12}
	}

	conn, err := mysql.NewConnector(cfg)
	require.NoError(t, err)

	db := sql.OpenDB(conn)
	defer func() { _ = db.Close() }()

	if err = db.PingContext(t.Context()); err != nil {
		return err
	}

	_, err = db.ExecContext(t.Context(), "SET @x = 1")

	return err
}

func TestMySQLProxyIAMAuth(t *testing.T) {
	t.Parallel()

	allow := allowPolicy("arn:aws:rds-db:us-east-1:" + authTestAccount + ":dbuser:mydb/bob")

	tests := []struct {
		name     string
		policy   string
		password string
		iam      bool
		tls      bool
		expired  bool
		wantErr  bool
	}{
		{name: "valid token", iam: true, tls: true, policy: allow},
		{name: "expired token", iam: true, tls: true, policy: allow, expired: true, wantErr: true},
		{name: "policy denies", iam: true, tls: true, wantErr: true},
		{name: "no tls", iam: true, tls: false, policy: allow, wantErr: true},
		{name: "iam disabled passes through", iam: false, tls: true, policy: allow, wantErr: true},
		{name: "master password", iam: true, tls: true, password: fakeMyMasterPassword},
		{name: "wrong password", iam: true, tls: true, password: "nope", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := newMyProxyFixture(t, tt.iam, tt.policy)

			user, password := "bob", p.token(t, time.Now().UTC())

			switch {
			case tt.expired:
				password = p.token(t, time.Now().UTC().Add(-time.Hour))
			case tt.password != "":
				user, password = "master", tt.password
			}

			err := p.ping(t, user, password, tt.tls)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestMySQLProxyRefusesLegacyClient(t *testing.T) {
	t.Parallel()

	p := newMyProxyFixture(t, true, "")

	conn, err := (&net.Dialer{}).DialContext(
		t.Context(),
		"tcp",
		net.JoinHostPort(p.inst.Endpoint, strconv.Itoa(p.inst.Port)),
	)
	require.NoError(t, err)

	defer func() { _ = conn.Close() }()

	_, greeting, err := readMyPacket(conn)
	require.NoError(t, err)
	assert.Equal(t, byte(myProtocolV10), greeting[0])
	assert.Contains(t, string(greeting), myPluginClear)

	resp := buildMyResponse(myCapProtocol41|myCapSecureConn|myCapPluginAuth, "bob", []byte("pw"), "", myPluginClear)
	require.NoError(t, writeMyPacket(conn, 1, resp))

	_, body, err := readMyPacket(conn)
	require.NoError(t, err)
	assert.Equal(t, byte(myErrMarker), body[0])
	assert.Contains(t, string(body), "CLIENT_DEPRECATE_EOF")
}
