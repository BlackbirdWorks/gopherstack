package ocidatabase_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"software.sslmate.com/src/go-pkcs12"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	svc "github.com/blackbirdworks/gopherstack/services/ocidatabase"
)

const engineWait = 30 * time.Second

var errFake = errors.New("fake failure")

// echoInstance is a database stand-in: a TCP echo server on a loopback port.
type echoInstance struct {
	ln       net.Listener
	launcher *echoLauncher
	conns    map[net.Conn]struct{}
	mu       *lockmetrics.RWMutex
	wg       sync.WaitGroup
	port     int
	running  atomic.Bool
	closed   atomic.Bool
}

func (i *echoInstance) Endpoint() (string, int) { return "127.0.0.1", i.port }

func (i *echoInstance) serve() {
	for {
		c, err := i.ln.Accept()
		if err != nil {
			return
		}

		i.mu.Lock("serve")
		i.conns[c] = struct{}{}
		i.mu.Unlock()

		i.wg.Go(func() {
			defer c.Close()

			buf := make([]byte, 256)

			for {
				n, rerr := c.Read(buf)
				if n > 0 && i.running.Load() {
					_, _ = c.Write(buf[:n])
				}

				if rerr != nil {
					return
				}
			}
		})
	}
}

func (i *echoInstance) Stop(context.Context) error {
	i.running.Store(false)
	i.launcher.stops.Add(1)

	return nil
}

func (i *echoInstance) Start(context.Context) error {
	i.running.Store(true)
	i.launcher.starts.Add(1)

	return nil
}

func (i *echoInstance) SetAdminPassword(_ context.Context, current, next string) error {
	if i.launcher.failPassword.Load() {
		return fmt.Errorf("%w: password rejected", errFake)
	}

	i.launcher.passwords.Store(&[2]string{current, next})

	return nil
}

func (i *echoInstance) Close(context.Context) {
	if !i.closed.CompareAndSwap(false, true) {
		return
	}

	_ = i.ln.Close()

	i.mu.Lock("Close")
	for c := range i.conns {
		_ = c.Close()
	}
	i.mu.Unlock()
	i.wg.Wait()
	i.launcher.closes.Add(1)
}

type echoLauncher struct {
	passwords    atomic.Pointer[[2]string]
	specs        atomic.Pointer[svc.LaunchSpec]
	failLaunch   atomic.Bool
	failPassword atomic.Bool
	launches     atomic.Int32
	stops        atomic.Int32
	starts       atomic.Int32
	closes       atomic.Int32
}

func (l *echoLauncher) Launch(ctx context.Context, spec svc.LaunchSpec) (svc.Instance, error) {
	if l.failLaunch.Load() {
		return nil, fmt.Errorf("%w: image pull failed", errFake)
	}

	ln, port, err := portalloc.ListenTCP(ctx, 0)
	if err != nil {
		return nil, err
	}

	l.specs.Store(&spec)
	l.launches.Add(1)

	i := &echoInstance{
		ln:       ln,
		launcher: l,
		port:     port,
		conns:    map[net.Conn]struct{}{},
		mu:       lockmetrics.New("test.echo"),
	}
	i.running.Store(true)
	i.wg.Go(i.serve)

	return i, nil
}

func newEngineEnv(t *testing.T) (*env, *echoLauncher) {
	t.Helper()

	l := &echoLauncher{}
	e := newEnv(t, envOpts{configure: func(b *svc.InMemoryBackend) {
		b.SetLauncher(l, "test/oracle:1")
		t.Cleanup(b.Close)
	}})

	return e, l
}

func (e *env) await(
	t *testing.T,
	id *string,
	want database.AutonomousDatabaseLifecycleStateEnum,
) database.AutonomousDatabase {
	t.Helper()

	var got database.AutonomousDatabase

	require.Eventually(t, func() bool {
		got = e.get(t, id)

		return got.LifecycleState == want
	}, engineWait, 10*time.Millisecond, "want %s, last %s (%v)", want, got.LifecycleState, got.LifecycleDetails)

	return got
}

// clientTLS builds the client side of a wallet: the client certificate and the CAs to trust.
func clientTLS(t *testing.T, files map[string][]byte) *tls.Config {
	t.Helper()

	key, leaf, cas, err := pkcs12.DecodeChain(files["ewallet.p12"], walletPassword)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	for _, c := range cas {
		pool.AddCert(c)
	}

	return &tls.Config{
		MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: "localhost",
		Certificates: []tls.Certificate{{Certificate: [][]byte{leaf.Raw}, PrivateKey: key}},
	}
}

// echoOnce dials addr with cfg, sends a line and reports whether it came back.
func echoOnce(t *testing.T, addr string, cfg *tls.Config) error {
	t.Helper()

	d := tls.Dialer{Config: cfg, NetDialer: &net.Dialer{Timeout: engineWait}}

	conn, err := d.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	if _, err = conn.Write([]byte("ping\n")); err != nil {
		return err
	}

	buf := make([]byte, 5)

	n, err := conn.Read(buf)
	if err != nil {
		return err
	}

	if string(buf[:n]) != "ping\n" {
		return fmt.Errorf("%w: %q", errFake, buf[:n])
	}

	return nil
}

// ports returns the mTLS and TLS addresses from the connection strings.
func ports(t *testing.T, db database.AutonomousDatabase) (string, string) {
	t.Helper()

	var mutual, server string

	for _, p := range db.ConnectionStrings.Profiles {
		start := strings.Index(*p.Value, "(port=") + len("(port=")
		port := (*p.Value)[start : strings.Index((*p.Value)[start:], ")")+start]
		addr := net.JoinHostPort("127.0.0.1", port)

		if p.TlsAuthentication == database.DatabaseConnectionStringProfileTlsAuthenticationMutual {
			mutual = addr
		} else {
			server = addr
		}
	}

	return mutual, server
}

func TestEngineConnectivity(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	resp := e.mustCreate(t, createOpts{})

	got := e.await(t, resp.Id, database.AutonomousDatabaseLifecycleStateAvailable)
	assert.Equal(t, "SUCCEEDED", string(e.wr(t, resp.OpcWorkRequestId).Status))
	assert.Equal(t, testPassword, l.specs.Load().Password)
	assert.Equal(t, "test/oracle:1", l.specs.Load().Image)

	files, err := e.wallet(t, resp.Id, walletPassword)
	require.NoError(t, err)

	mutual, server := ports(t, got)
	require.NotEmpty(t, mutual)
	require.NotEmpty(t, server)

	assert.Contains(t, *got.ConnectionStrings.High, "(host=localhost)")
	assert.Contains(t, *got.ConnectionStrings.High, "(service_name=FREEPDB1)")

	full := clientTLS(t, files)
	require.NoError(t, echoOnce(t, mutual, full), "mTLS with the wallet certificate")

	noClientCert := full.Clone()
	noClientCert.Certificates = nil
	require.Error(t, echoOnce(t, mutual, noClientCert), "mTLS needs the client certificate")

	require.NoError(t, echoOnce(t, server, noClientCert), "mTLS is optional, so TLS needs no client certificate")

	wrongCA := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "localhost", RootCAs: x509.NewCertPool()}
	require.Error(t, echoOnce(t, server, wrongCA), "the server certificate comes from the database CA")

	_, err = e.update(t, resp.Id, database.UpdateAutonomousDatabaseDetails{IsMtlsConnectionRequired: new(true)})
	require.NoError(t, err)
	e.await(t, resp.Id, database.AutonomousDatabaseLifecycleStateAvailable)
	require.Error(t, echoOnce(t, server, noClientCert), "mTLS required refuses the TLS-only listener")
	require.NoError(t, echoOnce(t, mutual, full))
}

func TestEngineAccessControl(t *testing.T) {
	t.Parallel()

	e, _ := newEngineEnv(t)
	resp := e.mustCreate(t, createOpts{})
	got := e.await(t, resp.Id, database.AutonomousDatabaseLifecycleStateAvailable)

	files, err := e.wallet(t, resp.Id, walletPassword)
	require.NoError(t, err)

	cfg := clientTLS(t, files)
	_, server := ports(t, got)
	require.NoError(t, echoOnce(t, server, cfg))

	tests := []struct {
		name    string
		acl     []string
		allowed bool
	}{
		{name: "other-network", acl: []string{"10.0.0.0/8"}, allowed: false},
		{name: "loopback-cidr", acl: []string{"127.0.0.0/8"}, allowed: true},
		{name: "loopback-address", acl: []string{"192.0.2.1", "127.0.0.1"}, allowed: true},
		{name: "vcn-only", acl: []string{"ocid1.vcn.oc1..v"}, allowed: false},
		{name: "empty-list-disables-control", acl: []string{}, allowed: true},
	}

	for _, tc := range tests {
		_, err = e.update(t, resp.Id, database.UpdateAutonomousDatabaseDetails{WhitelistedIps: tc.acl})
		require.NoError(t, err, tc.name)
		e.await(t, resp.Id, database.AutonomousDatabaseLifecycleStateAvailable)

		err = echoOnce(t, server, cfg)
		if tc.allowed {
			require.NoError(t, err, tc.name)
		} else {
			require.Error(t, err, tc.name)
		}
	}
}

func TestEngineLifecycle(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	resp := e.mustCreate(t, createOpts{})
	got := e.await(t, resp.Id, database.AutonomousDatabaseLifecycleStateAvailable)

	files, err := e.wallet(t, resp.Id, walletPassword)
	require.NoError(t, err)

	cfg := clientTLS(t, files)
	_, server := ports(t, got)
	require.NoError(t, echoOnce(t, server, cfg))

	_, err = e.client.StopAutonomousDatabase(
		t.Context(),
		database.StopAutonomousDatabaseRequest{AutonomousDatabaseId: resp.Id},
	)
	require.NoError(t, err)
	stopped := e.await(t, resp.Id, database.AutonomousDatabaseLifecycleStateStopped)
	assert.EqualValues(t, 1, l.stops.Load())
	require.Error(t, echoOnce(t, server, cfg), "a stopped database refuses connections")
	assert.NotNil(t, stopped.ConnectionStrings, "the endpoints stay stable while stopped")

	_, err = e.client.StartAutonomousDatabase(
		t.Context(),
		database.StartAutonomousDatabaseRequest{AutonomousDatabaseId: resp.Id},
	)
	require.NoError(t, err)
	e.await(t, resp.Id, database.AutonomousDatabaseLifecycleStateAvailable)
	assert.EqualValues(t, 1, l.starts.Load())
	require.NoError(t, echoOnce(t, server, cfg))

	_, err = e.client.RestartAutonomousDatabase(
		t.Context(),
		database.RestartAutonomousDatabaseRequest{AutonomousDatabaseId: resp.Id},
	)
	require.NoError(t, err)
	e.await(t, resp.Id, database.AutonomousDatabaseLifecycleStateAvailable)
	assert.EqualValues(t, 2, l.stops.Load())
	assert.EqualValues(t, 2, l.starts.Load())
	require.NoError(t, echoOnce(t, server, cfg))

	_, err = e.client.DeleteAutonomousDatabase(
		t.Context(),
		database.DeleteAutonomousDatabaseRequest{AutonomousDatabaseId: resp.Id},
	)
	require.NoError(t, err)
	e.await(t, resp.Id, database.AutonomousDatabaseLifecycleStateTerminated)
	assert.EqualValues(t, 1, l.closes.Load(), "terminate removes the database")

	_, err = (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp", server)
	require.Error(t, err, "the listeners are gone")
}

func TestEnginePassword(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	resp := e.mustCreate(t, createOpts{})
	e.await(t, resp.Id, database.AutonomousDatabaseLifecycleStateAvailable)

	_, err := e.update(t, resp.Id, database.UpdateAutonomousDatabaseDetails{AdminPassword: new("N3wPassw0rd_Adb1")})
	require.NoError(t, err)
	e.await(t, resp.Id, database.AutonomousDatabaseLifecycleStateAvailable)
	assert.Equal(t, [2]string{testPassword, "N3wPassw0rd_Adb1"}, *l.passwords.Load())

	l.failPassword.Store(true)

	upd, err := e.update(t, resp.Id, database.UpdateAutonomousDatabaseDetails{AdminPassword: new("An0therPassw0rd_1")})
	require.NoError(t, err)

	got := e.await(t, resp.Id, database.AutonomousDatabaseLifecycleStateUnavailable)
	assert.Contains(t, *got.LifecycleDetails, "password rejected")
	assert.Equal(t, "FAILED", string(e.wr(t, upd.OpcWorkRequestId).Status))
	assert.NotContains(t, *got.LifecycleDetails, "An0therPassw0rd_1")
}

func TestEngineLaunchFailure(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	l.failLaunch.Store(true)

	resp := e.mustCreate(t, createOpts{})
	got := e.await(t, resp.Id, database.AutonomousDatabaseLifecycleStateUnavailable)
	assert.Contains(t, *got.LifecycleDetails, "image pull failed")
	assert.NotContains(t, *got.LifecycleDetails, testPassword)

	wr := e.wr(t, resp.OpcWorkRequestId)
	assert.Equal(t, "FAILED", string(wr.Status))

	list, err := e.wrs.ListWorkRequestErrors(t.Context(), workrequestsErrorsRequest(resp.OpcWorkRequestId))
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "OperationFailed", *list.Items[0].Code)
	assert.Contains(t, *list.Items[0].Message, "image pull failed")
}

func TestEngineRestoredDatabases(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	resp := e.mustCreate(t, createOpts{})
	e.await(t, resp.Id, database.AutonomousDatabaseLifecycleStateAvailable)

	snap := e.backend.Snapshot(t.Context())
	assert.NotContains(t, string(snap), testPassword, "the ADMIN password is not persisted")

	e2, l2 := newEngineEnv(t)
	require.NoError(t, e2.backend.Restore(t.Context(), snap))

	got := e2.get(t, resp.Id)
	assert.Equal(t, database.AutonomousDatabaseLifecycleStateStopped, got.LifecycleState)
	assert.Contains(t, *got.LifecycleDetails, "Restored from a snapshot")
	assert.Nil(t, got.ConnectionStrings, "no listeners until the database runs again")

	_, err := e2.client.StartAutonomousDatabase(
		t.Context(),
		database.StartAutonomousDatabaseRequest{AutonomousDatabaseId: resp.Id},
	)
	assert.Equal(t, 409, svcErr(t, err).GetHTTPStatusCode(), "the password is unknown")

	_, err = e2.update(t, resp.Id, database.UpdateAutonomousDatabaseDetails{AdminPassword: new("N3wPassw0rd_Adb1")})
	require.NoError(t, err)

	_, err = e2.client.StartAutonomousDatabase(
		t.Context(),
		database.StartAutonomousDatabaseRequest{AutonomousDatabaseId: resp.Id},
	)
	require.NoError(t, err)

	started := e2.await(t, resp.Id, database.AutonomousDatabaseLifecycleStateAvailable)
	assert.EqualValues(t, 1, l2.launches.Load())
	assert.Equal(t, "N3wPassw0rd_Adb1", l2.specs.Load().Password)
	assert.NotNil(t, started.ConnectionStrings)
	assert.EqualValues(t, 1, l.launches.Load())
}

func TestEngineWalletRotation(t *testing.T) {
	t.Parallel()

	e, _ := newEngineEnv(t)
	resp := e.mustCreate(t, createOpts{mutate: func(d *database.CreateAutonomousDatabaseDetails) {
		d.IsMtlsConnectionRequired = new(true)
	}})
	got := e.await(t, resp.Id, database.AutonomousDatabaseLifecycleStateAvailable)
	mutual, _ := ports(t, got)

	oldFiles, err := e.wallet(t, resp.Id, walletPassword)
	require.NoError(t, err)

	oldCfg := clientTLS(t, oldFiles)
	require.NoError(t, echoOnce(t, mutual, oldCfg))

	rotate := func(grace *int) {
		_, rerr := e.client.UpdateAutonomousDatabaseWallet(t.Context(), database.UpdateAutonomousDatabaseWalletRequest{
			AutonomousDatabaseId: resp.Id,
			ShouldRotate:         new(true), GracePeriod: grace,
		})
		require.NoError(t, rerr)
	}

	rotate(new(1))
	require.NoError(t, echoOnce(t, mutual, oldCfg), "the old wallet works during the grace period")

	newFiles, err := e.wallet(t, resp.Id, walletPassword)
	require.NoError(t, err)
	require.NoError(t, echoOnce(t, mutual, clientTLS(t, newFiles)))

	e.clock.Advance(2 * time.Hour)
	require.Error(t, echoOnce(t, mutual, oldCfg), "the old wallet stops working after the grace period")
	require.NoError(t, echoOnce(t, mutual, clientTLS(t, newFiles)))

	rotate(nil)
	require.Error(t, echoOnce(t, mutual, clientTLS(t, newFiles)), "no grace period")
}

func TestEngineResetAndClose(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	resp := e.mustCreate(t, createOpts{})
	got := e.await(t, resp.Id, database.AutonomousDatabaseLifecycleStateAvailable)
	_, server := ports(t, got)

	e.backend.Reset()
	assert.EqualValues(t, 1, l.closes.Load())

	_, err := (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp", server)
	require.Error(t, err)

	_, port, splitErr := net.SplitHostPort(server)
	require.NoError(t, splitErr)

	_, convErr := strconv.Atoi(port)
	require.NoError(t, convErr)
	assert.Equal(t, svc.EngineDocker, e.backend.EngineMode())
}
