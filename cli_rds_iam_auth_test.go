package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/rds"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

const (
	rdsAuthMasterPassword = "masterpw1"
	rdsAuthEmptyHash      = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

type rdsFakePGRuntime struct {
	listeners []net.Listener
	mu        sync.Mutex
}

func (f *rdsFakePGRuntime) CreateAndStart(ctx context.Context, spec container.Spec) (string, error) {
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

			go serveRDSFakePG(conn)
		}
	}()

	return "fake-" + host, nil
}

func (f *rdsFakePGRuntime) StopAndRemove(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, ln := range f.listeners {
		_ = ln.Close()
	}

	return nil
}

func (f *rdsFakePGRuntime) StopContainer(context.Context, string) error  { return nil }
func (f *rdsFakePGRuntime) StartContainer(context.Context, string) error { return nil }

func serveRDSFakePG(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	be := pgproto3.NewBackend(conn, conn)

	if _, err := be.ReceiveStartupMessage(); err != nil {
		return
	}

	be.Send(&pgproto3.AuthenticationCleartextPassword{})
	_ = be.Flush()
	_ = be.SetAuthType(pgproto3.AuthTypeCleartextPassword)

	pm, err := be.Receive()
	if err != nil {
		return
	}

	if p, ok := pm.(*pgproto3.PasswordMessage); !ok || p.Password != rdsAuthMasterPassword {
		be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "28P01", Message: "password authentication failed"})
		_ = be.Flush()

		return
	}

	be.Send(&pgproto3.AuthenticationOk{})
	be.Send(&pgproto3.BackendKeyData{ProcessID: 42, SecretKey: []byte{1, 2, 3, 4}})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	_ = be.Flush()

	for {
		if _, err = be.Receive(); err != nil {
			return
		}

		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SET")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

		if be.Flush() != nil {
			return
		}
	}
}

type rdsAuthStack struct {
	e     *echo.Echo
	rdsH  *rds.Handler
	pool  *x509.CertPool
	creds aws.Credentials
	inst  rds.DBInstance
}

func newRDSAuthStack(t *testing.T, wire bool, policy string) *rdsAuthStack {
	t.Helper()

	iamB := iambackend.NewInMemoryBackend()
	_, err := iamB.CreateUser("alice", "/", "")
	require.NoError(t, err)

	ak, err := iamB.CreateAccessKey("alice")
	require.NoError(t, err)

	if policy != "" {
		require.NoError(t, iamB.PutUserPolicy("alice", "p", policy))
	}

	b := rds.NewInMemoryBackend("123456789012", "us-east-1")
	t.Cleanup(b.Close)
	b.EnableEngine(rds.EngineConfig{
		Runtime: &rdsFakePGRuntime{},
		Probe:   func(context.Context, rds.EngineLogin) error { return nil },
	})

	rdsH := rds.NewHandler(b)
	services := []service.Registerable{
		rdsH, iambackend.NewHandler(iamB), stsbackend.NewHandler(stsbackend.NewInMemoryBackend()),
	}

	e := echo.New()
	if wire {
		wireRDSIAMAuth(e, services, true)
	}

	_, err = b.CreateDBInstance("pgdb", "postgres", "db.t3.micro", "appdb", "master", "", 20,
		rds.DBInstanceOptions{MasterUserPassword: rdsAuthMasterPassword, IAMDatabaseAuthenticationEnabled: true})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, derr := b.DescribeDBInstances("pgdb")

		return derr == nil && len(got) == 1 && got[0].DBInstanceStatus == "available"
	}, 10*time.Second, 5*time.Millisecond)

	got, err := b.DescribeDBInstances("pgdb")
	require.NoError(t, err)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(b.EngineCAPEM()))

	return &rdsAuthStack{
		e: e, rdsH: rdsH, inst: got[0], pool: pool,
		creds: aws.Credentials{AccessKeyID: ak.AccessKeyID, SecretAccessKey: ak.SecretAccessKey},
	}
}

func (s *rdsAuthStack) token(t *testing.T, user string) string {
	t.Helper()

	endpoint := net.JoinHostPort(s.inst.Endpoint, strconv.Itoa(s.inst.Port))

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+endpoint, nil)
	require.NoError(t, err)

	q := req.URL.Query()
	q.Set("Action", "connect")
	q.Set("DBUser", user)
	q.Set("X-Amz-Expires", "900")
	req.URL.RawQuery = q.Encode()

	signed, _, err := v4.NewSigner().PresignHTTP(t.Context(), s.creds, req, rdsAuthEmptyHash, "rds-db",
		"us-east-1", time.Now().UTC())
	require.NoError(t, err)

	return strings.TrimPrefix(signed, "https://")
}

func (s *rdsAuthStack) connect(t *testing.T, user, password string) (*pgconn.PgConn, error) {
	t.Helper()

	cfg, err := pgconn.ParseConfig("postgres://" + user + "@" +
		net.JoinHostPort(s.inst.Endpoint, strconv.Itoa(s.inst.Port)) + "/appdb?sslmode=disable&connect_timeout=5")
	require.NoError(t, err)

	cfg.Password = password
	cfg.TLSConfig = &tls.Config{RootCAs: s.pool, ServerName: s.inst.Endpoint, MinVersion: tls.VersionTLS12}

	return pgconn.ConnectConfig(t.Context(), cfg)
}

func TestRDSIAMAuthWiring(t *testing.T) {
	t.Parallel()

	allow := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"rds-db:connect",` +
		`"Resource":"arn:aws:rds-db:us-east-1:123456789012:dbuser:pgdb/bob"}]}`

	tests := []struct {
		name     string
		policy   string
		wire     bool
		wantAuth bool
	}{
		{name: "wired allow", wire: true, policy: allow, wantAuth: true},
		{name: "wired no policy", wire: true},
		{name: "unwired", policy: allow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newRDSAuthStack(t, tt.wire, tt.policy)

			pc, err := s.connect(t, "bob", s.token(t, "bob"))
			if !tt.wantAuth {
				var pgErr *pgconn.PgError
				require.ErrorAs(t, err, &pgErr)
				assert.Equal(t, "28000", pgErr.Code)

				return
			}

			require.NoError(t, err)
			require.NoError(t, pc.Close(t.Context()))
		})
	}
}

func TestRDSCAEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		wire     bool
	}{
		{name: "wired", wire: true, wantCode: http.StatusOK},
		{name: "unwired", wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newRDSAuthStack(t, tt.wire, "")

			rec := httptest.NewRecorder()
			s.e.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, rdsCAPath, nil))
			require.Equal(t, tt.wantCode, rec.Code)

			if !tt.wire {
				return
			}

			pool := x509.NewCertPool()
			assert.True(t, pool.AppendCertsFromPEM(rec.Body.Bytes()))
			assert.Equal(t, string(s.rdsH.Backend.EngineCAPEM()), rec.Body.String())
		})
	}
}

func TestRDSCADirPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		want    string
		persist bool
	}{
		{name: "persist", persist: true, want: "rds-ca"},
		{name: "memory"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			c := &CLI{DataDir: dir, Persist: tt.persist}

			got := c.GetRDSCADir()
			if tt.want == "" {
				assert.Empty(t, got)

				return
			}

			assert.Equal(t, dir+"/"+tt.want, got)
		})
	}
}
