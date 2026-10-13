package rds //nolint:testpackage // white-box access to the proxy and token verifier.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const fakePGMasterPassword = "masterpw1"

// fakePGRuntime starts an in-process postgres-protocol listener on the host port a container would publish.
type fakePGRuntime struct {
	listeners []net.Listener
	mu        sync.Mutex
}

func (f *fakePGRuntime) CreateAndStart(ctx context.Context, spec container.Spec) (string, error) {
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

			go serveFakePG(conn)
		}
	}()

	return "fake-" + host, nil
}

func (f *fakePGRuntime) StopAndRemove(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, ln := range f.listeners {
		_ = ln.Close()
	}

	return nil
}

func (f *fakePGRuntime) StopContainer(context.Context, string) error  { return nil }
func (f *fakePGRuntime) StartContainer(context.Context, string) error { return nil }

func serveFakePG(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	be := pgproto3.NewBackend(conn, conn)

	msg, err := be.ReceiveStartupMessage()
	if err != nil {
		return
	}

	if _, ok := msg.(*pgproto3.StartupMessage); !ok {
		return
	}

	be.Send(&pgproto3.AuthenticationCleartextPassword{})
	_ = be.Flush()
	_ = be.SetAuthType(pgproto3.AuthTypeCleartextPassword)

	pm, err := be.Receive()
	if err != nil {
		return
	}

	if p, ok := pm.(*pgproto3.PasswordMessage); !ok || p.Password != fakePGMasterPassword {
		be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "28P01", Message: "password authentication failed"})
		_ = be.Flush()

		return
	}

	be.Send(&pgproto3.AuthenticationOk{})
	be.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "17.0"})
	be.Send(&pgproto3.BackendKeyData{ProcessID: 42, SecretKey: []byte{1, 2, 3, 4}})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	_ = be.Flush()

	runFakePGSession(be)
}

func runFakePGSession(be *pgproto3.Backend) {
	session := "master"

	for {
		msg, err := be.Receive()
		if err != nil {
			return
		}

		q, ok := msg.(*pgproto3.Query)
		if !ok {
			return
		}

		sql := strings.ToLower(q.String)

		switch {
		case strings.HasPrefix(sql, "set session authorization"):
			session = strings.Trim(strings.TrimSpace(strings.TrimPrefix(sql, "set session authorization")), `"`)
			be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SET")})
		case strings.HasPrefix(sql, "reset session authorization"):
			session = "master"
			be.Send(&pgproto3.CommandComplete{CommandTag: []byte("RESET")})
		case strings.HasPrefix(sql, "do "):
			be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42501", Message: "permission denied"})
		default:
			be.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("current_user")}}})
			be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(session)}})
			be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		}

		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

		if be.Flush() != nil {
			return
		}
	}
}

type proxyFixture struct {
	b    *InMemoryBackend
	auth *authFixture
	pool *x509.CertPool
	inst DBInstance
}

func newProxyFixture(t *testing.T, iamEnabled bool, policy string) *proxyFixture {
	t.Helper()

	b := NewInMemoryBackend(authTestAccount, authTestRegion)
	t.Cleanup(b.Close)

	b.EnableEngine(EngineConfig{
		Runtime: &fakePGRuntime{},
		Probe:   func(context.Context, EngineLogin) error { return nil },
	})

	f := newAuthFixture(t, policy)
	b.SetIAMAuth(func() IAMAuthConfig { return IAMAuthConfig{Identities: f.iam, Enforce: true} })

	_, err := b.CreateDBInstance("pgdb", "postgres", "db.t3.micro", "appdb", "master", "", 20,
		DBInstanceOptions{MasterUserPassword: fakePGMasterPassword, IAMDatabaseAuthenticationEnabled: iamEnabled})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, derr := b.DescribeDBInstances("pgdb")

		return derr == nil && len(got) == 1 && got[0].DBInstanceStatus == "available"
	}, 10*time.Second, 5*time.Millisecond)

	got, err := b.DescribeDBInstances("pgdb")
	require.NoError(t, err)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(b.EngineCAPEM()))

	return &proxyFixture{b: b, auth: f, pool: pool, inst: got[0]}
}

func (p *proxyFixture) token(t *testing.T, user string, signed time.Time) string {
	t.Helper()

	return buildTestToken(t, net.JoinHostPort(p.inst.Endpoint, strconv.Itoa(p.inst.Port)),
		authTestRegion, user, p.auth.creds, signed, "900")
}

func (p *proxyFixture) connect(t *testing.T, user, password string, withTLS bool) (*pgconn.PgConn, error) {
	t.Helper()

	cfg, err := pgconn.ParseConfig(
		"postgres://" + user + "@" + net.JoinHostPort(p.inst.Endpoint, strconv.Itoa(p.inst.Port)) +
			"/appdb?sslmode=disable&connect_timeout=5",
	)
	require.NoError(t, err)

	cfg.Password = password

	if withTLS {
		cfg.TLSConfig = &tls.Config{RootCAs: p.pool, ServerName: p.inst.Endpoint, MinVersion: tls.VersionTLS12}
	}

	return pgconn.ConnectConfig(t.Context(), cfg)
}

func queryOne(t *testing.T, pc *pgconn.PgConn, sql string) (string, error) {
	t.Helper()

	rs, err := pc.Exec(t.Context(), sql).ReadAll()
	if err != nil {
		return "", err
	}

	return string(rs[0].Rows[0][0]), nil
}

func TestPostgresProxyIAMAuth(t *testing.T) {
	t.Parallel()

	allow := allowPolicy("arn:aws:rds-db:us-east-1:" + authTestAccount + ":dbuser:pgdb/bob")

	tests := []struct {
		name     string
		wantCode string
		policy   string
		iam      bool
		tls      bool
		expired  bool
		bad      bool
		password bool
	}{
		{name: "valid token", iam: true, tls: true, policy: allow},
		{name: "bad signature", iam: true, tls: true, policy: allow, bad: true, wantCode: "28000"},
		{name: "expired token", iam: true, tls: true, policy: allow, expired: true, wantCode: "28000"},
		{name: "policy denies", iam: true, tls: true, wantCode: "28000"},
		{name: "no tls", iam: true, tls: false, policy: allow, wantCode: "28000"},
		{name: "iam disabled", iam: false, tls: true, policy: allow, wantCode: "28P01"},
		{name: "master password", iam: true, tls: true, password: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := newProxyFixture(t, tt.iam, tt.policy)

			user, password := "bob", p.token(t, "bob", time.Now().UTC())

			switch {
			case tt.bad:
				password = strings.Replace(password, "DBUser=bob", "DBUser=bob&X=1", 1)
			case tt.expired:
				password = p.token(t, "bob", time.Now().UTC().Add(-time.Hour))
			case tt.password:
				user, password = "master", fakePGMasterPassword
			}

			pc, err := p.connect(t, user, password, tt.tls)
			if tt.wantCode != "" {
				var pgErr *pgconn.PgError
				require.ErrorAs(t, err, &pgErr)
				assert.Equal(t, tt.wantCode, pgErr.Code)

				return
			}

			require.NoError(t, err)

			t.Cleanup(func() { _ = pc.Close(context.Background()) })

			got, err := queryOne(t, pc, "select current_user")
			require.NoError(t, err)
			assert.Equal(t, user, got)
		})
	}
}

func TestPostgresProxyBlocksSessionAuthorizationReset(t *testing.T) {
	t.Parallel()

	p := newProxyFixture(t, true, allowPolicy("*"))

	pc, err := p.connect(t, "bob", p.token(t, "bob", time.Now().UTC()), true)
	require.NoError(t, err)

	t.Cleanup(func() { _ = pc.Close(context.Background()) })

	for _, stmt := range []string{
		"RESET SESSION AUTHORIZATION",
		"select 1; reset   session  authorization",
		"RESET ALL",
		"select set_config('session_authorization','master',false)",
	} {
		_, err = queryOne(t, pc, stmt)

		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr, stmt)
		assert.Equal(t, "42501", pgErr.Code, stmt)
	}

	got, err := queryOne(t, pc, "select current_user")
	require.NoError(t, err)
	assert.Equal(t, "bob", got)
}

func TestEngineCAPersists(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	first, err := loadOrCreateEngineCA(dir)
	require.NoError(t, err)

	second, err := loadOrCreateEngineCA(dir)
	require.NoError(t, err)
	assert.Equal(t, first.pem, second.pem)

	cert, err := second.serverCert("db.example.test")
	require.NoError(t, err)

	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)

	pool := x509.NewCertPool()
	pool.AddCert(second.cert)

	for _, host := range []string{"db.example.test", "127.0.0.1", "localhost"} {
		_, err = leaf.Verify(x509.VerifyOptions{DNSName: host, Roots: pool})
		assert.NoError(t, err, host)
	}
}
