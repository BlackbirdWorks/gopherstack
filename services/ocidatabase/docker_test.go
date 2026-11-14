package ocidatabase_test

import (
	"context"
	"crypto/tls"
	"database/sql"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/database"
	goora "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	svc "github.com/blackbirdworks/gopherstack/services/ocidatabase"
)

const (
	dockerWait   = 12 * time.Minute
	dockerPoll   = 2 * time.Second
	pullEnv      = "GOPHERSTACK_ORACLE_PULL"
	dockerPw     = "Adb_Passw0rd_12"
	dockerNewPw  = "Adb_N3wPassw0rd_12"
	containerKey = "gopherstack.ocidatabase.id"
)

func dockerOut(t *testing.T, args ...string) string {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", args...).CombinedOutput()
	require.NoError(t, err, string(out))

	return strings.TrimSpace(string(out))
}

// newDockerEnv wires the service to the local container runtime. It skips when docker or the Oracle image is
// missing; the 1.3 GB image is only pulled when GOPHERSTACK_ORACLE_PULL is set.
func newDockerEnv(t *testing.T) *env {
	t.Helper()

	if testing.Short() {
		t.Skip("docker engine test is not part of -short")
	}

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	lr := gcpserve.NewLazyRuntime(nil, "ocidatabase.test.runtime")
	t.Cleanup(func() { _ = lr.Close() })

	if _, err := lr.Get(); err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	if err := exec.CommandContext(t.Context(), "docker", "image", "inspect", svc.DefaultImage).Run(); err != nil {
		if os.Getenv(pullEnv) == "" {
			t.Skipf("%s is not present locally; set %s=1 to pull it", svc.DefaultImage, pullEnv)
		}

		dockerOut(t, "pull", svc.DefaultImage)
	}

	return newEnv(t, envOpts{configure: func(b *svc.InMemoryBackend) {
		b.SetLauncher(svc.NewDockerLauncher(svc.DockerConfig{Runtime: lr.Get}), svc.DefaultImage)
		t.Cleanup(b.Close)
	}})
}

func (e *env) awaitDocker(
	t *testing.T,
	id *string,
	want database.AutonomousDatabaseLifecycleStateEnum,
) database.AutonomousDatabase {
	t.Helper()

	var got database.AutonomousDatabase

	require.Eventually(t, func() bool {
		got = e.get(t, id)

		return got.LifecycleState == want
	}, dockerWait, dockerPoll, "want %s, last %s (%v)", want, got.LifecycleState, got.LifecycleDetails)

	return got
}

// connect opens ADMIN against the TLS profile of the high consumer group, using the connection string the API returns.
func connect(t *testing.T, db database.AutonomousDatabase, password string, cfg *tls.Config) *sql.DB {
	t.Helper()

	var descriptor string

	for _, p := range db.ConnectionStrings.Profiles {
		if p.ConsumerGroup == database.DatabaseConnectionStringProfileConsumerGroupHigh &&
			p.TlsAuthentication == database.DatabaseConnectionStringProfileTlsAuthenticationServer {
			descriptor = *p.Value
		}
	}

	require.NotEmpty(t, descriptor, "a TLS profile for the high consumer group")

	connector, ok := goora.NewConnector(goora.BuildJDBC("ADMIN", password, descriptor, nil)).(*goora.OracleConnector)
	require.True(t, ok)
	connector.WithTLSConfig(cfg)

	conn := sql.OpenDB(connector)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

func TestDockerAutonomousDatabase(t *testing.T) {
	t.Parallel()

	e := newDockerEnv(t)

	resp := e.mustCreate(t, createOpts{mutate: func(d *database.CreateAutonomousDatabaseDetails) {
		d.AdminPassword, d.DbName = new(dockerPw), new("dockerdb")
	}})
	got := e.awaitDocker(t, resp.Id, database.AutonomousDatabaseLifecycleStateAvailable)
	assert.Equal(t, "SUCCEEDED", string(e.wr(t, resp.OpcWorkRequestId).Status))

	files, err := e.wallet(t, resp.Id, walletPassword)
	require.NoError(t, err)

	cfg := clientTLS(t, files)
	cfg.Certificates = nil

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	conn := connect(t, got, dockerPw, cfg)

	_, err = conn.ExecContext(ctx, "CREATE TABLE notes (id NUMBER, body VARCHAR2(50))")
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, "INSERT INTO notes VALUES (1, 'hello from the emulator')")
	require.NoError(t, err)

	selectBody := func(c *sql.DB) (string, error) {
		var body string

		qerr := c.QueryRowContext(ctx, "SELECT body FROM notes WHERE id = 1").Scan(&body)

		return body, qerr
	}

	body, err := selectBody(conn)
	require.NoError(t, err)
	assert.Equal(t, "hello from the emulator", body)

	_, err = e.client.StopAutonomousDatabase(
		t.Context(),
		database.StopAutonomousDatabaseRequest{AutonomousDatabaseId: resp.Id},
	)
	require.NoError(t, err)
	got = e.awaitDocker(t, resp.Id, database.AutonomousDatabaseLifecycleStateStopped)

	stoppedConn := connect(t, got, dockerPw, cfg)
	require.Error(t, stoppedConn.PingContext(ctx), "a stopped database refuses connections")

	_, err = e.client.StartAutonomousDatabase(
		t.Context(),
		database.StartAutonomousDatabaseRequest{AutonomousDatabaseId: resp.Id},
	)
	require.NoError(t, err)
	got = e.awaitDocker(t, resp.Id, database.AutonomousDatabaseLifecycleStateAvailable)

	restarted := connect(t, got, dockerPw, cfg)
	body, err = selectBody(restarted)
	require.NoError(t, err, "data survives stop and start")
	assert.Equal(t, "hello from the emulator", body)

	_, err = e.update(t, resp.Id, database.UpdateAutonomousDatabaseDetails{AdminPassword: new(dockerNewPw)})
	require.NoError(t, err)
	got = e.awaitDocker(t, resp.Id, database.AutonomousDatabaseLifecycleStateAvailable)

	require.Error(t, connect(t, got, dockerPw, cfg).PingContext(ctx), "the old password no longer works")

	renewed := connect(t, got, dockerNewPw, cfg)
	body, err = selectBody(renewed)
	require.NoError(t, err)
	assert.Equal(t, "hello from the emulator", body)

	filter := "label=" + containerKey + "=" + *resp.Id
	require.NotEmpty(t, dockerOut(t, "ps", "-aq", "--filter", filter))

	_, err = e.client.DeleteAutonomousDatabase(
		t.Context(),
		database.DeleteAutonomousDatabaseRequest{AutonomousDatabaseId: resp.Id},
	)
	require.NoError(t, err)
	e.awaitDocker(t, resp.Id, database.AutonomousDatabaseLifecycleStateTerminated)

	assert.Empty(t, dockerOut(t, "ps", "-aq", "--filter", filter), "terminate removes the container")
}
