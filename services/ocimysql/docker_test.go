package ocimysql_test

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"

	ocimysql "github.com/blackbirdworks/gopherstack/services/ocimysql"
)

const dockerWait = 8 * time.Minute

func dockerEnv(t *testing.T) *env {
	t.Helper()

	if testing.Short() {
		t.Skip("docker engine test skipped in short mode")
	}

	rt, err := container.NewRuntime(container.Config{Logger: slog.Default()})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	pctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	if err = rt.Ping(pctx); err != nil {
		_ = rt.Close()
		t.Skipf("container runtime unreachable: %v", err)
	}

	vols, _ := rt.(container.VolumeRuntime)

	e := newEnv(t, envOpts{})
	require.NoError(t, e.backend.EnableDockerEngine(ocimysql.DockerConfig{
		Runtime: rt, Volumes: vols, Logger: slog.Default(), StartTimeout: 5 * time.Minute,
	}))
	t.Cleanup(func() {
		e.backend.Reset()
		e.backend.Close()
		_ = rt.Close()
	})

	e.wait = dockerWait

	return e
}

func openAdmin(t *testing.T, host string, port int, user, pass string) *sql.DB {
	t.Helper()

	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net, cfg.Addr = user, pass, "tcp", fmt.Sprintf("%s:%d", host, port)
	cfg.Timeout = 5 * time.Second
	cfg.InterpolateParams = true

	c, err := mysql.NewConnector(cfg)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	t.Cleanup(func() { _ = db.Close() })

	return db
}

func (e *env) connect(t *testing.T, db my.DbSystem, pass string) *sql.DB {
	t.Helper()

	return openAdmin(t, *db.IpAddress, *db.Port, adminUser, pass)
}

func rowCount(t *testing.T, c *sql.DB) int {
	t.Helper()

	var n int

	require.NoError(t, c.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM shop.orders").Scan(&n))

	return n
}

func dockerList(t *testing.T, kind, filter string) string {
	t.Helper()

	args := []string{kind, "ls", "-q", "--filter", "name=" + filter}
	if kind == "container" {
		args = []string{"ps", "-aq", "--filter", "name=" + filter}
	}

	out, err := exec.CommandContext(t.Context(), "docker", args...).Output()
	require.NoError(t, err)

	return strings.TrimSpace(string(out))
}

func TestDockerEngineEndToEnd(t *testing.T) {
	t.Parallel()

	e := dockerEnv(t)
	ctx := t.Context()

	created := e.createDB(t, dbDetails("real"))
	db := e.awaitState(t, created.Id, my.DbSystemLifecycleStateActive)
	assert.Equal(t, "127.0.0.1", *db.IpAddress)

	c := e.connect(t, db, adminPass)
	require.NoError(t, c.PingContext(ctx), "the admin user created with CreateDbSystem can connect")

	_, err := c.ExecContext(ctx, "CREATE DATABASE shop")
	require.NoError(t, err)
	_, err = c.ExecContext(ctx, "CREATE TABLE shop.orders (id INT PRIMARY KEY, item VARCHAR(40))")
	require.NoError(t, err)
	_, err = c.ExecContext(ctx, "INSERT INTO shop.orders VALUES (1, 'widget')")
	require.NoError(t, err)

	bad := e.connect(t, db, "Wr0ng#Password")
	require.Error(t, bad.PingContext(ctx), "wrong password is rejected")

	_, err = e.db.StopDbSystem(ctx, my.StopDbSystemRequest{
		DbSystemId: db.Id, ShutdownType: my.InnoDbShutdownModeSlow,
	})
	require.NoError(t, err)
	stopped := e.awaitState(t, db.Id, my.DbSystemLifecycleStateInactive)

	down := e.connect(t, stopped, adminPass)
	require.Error(t, down.PingContext(ctx), "a stopped DB system refuses connections")

	_, err = e.db.StartDbSystem(ctx, my.StartDbSystemRequest{DbSystemId: db.Id})
	require.NoError(t, err)

	restarted := e.awaitState(t, db.Id, my.DbSystemLifecycleStateActive)
	c2 := e.connect(t, restarted, adminPass)
	assert.Equal(t, 1, rowCount(t, c2), "data survives stop and start")

	bkResp, err := e.bk.CreateBackup(ctx, my.CreateBackupRequest{
		DbSystemId: db.Id, DisplayName: new("pre"),
	})
	require.NoError(t, err)

	bk := e.awaitBackup(t, bkResp.Id, my.BackupLifecycleStateActive)

	_, err = c2.ExecContext(ctx, "INSERT INTO shop.orders VALUES (2, 'gadget')")
	require.NoError(t, err)

	clone := e.createDB(t, func() my.CreateDbSystemDetails {
		d := dbDetails("clone")
		d.AdminUsername, d.AdminPassword = nil, nil
		d.Source = my.CreateDbSystemSourceFromBackupDetails{BackupId: bk.Id}

		return d
	}())
	cloned := e.awaitState(t, clone.Id, my.DbSystemLifecycleStateActive)

	c3 := e.connect(t, cloned, adminPass)
	assert.Equal(t, 1, rowCount(t, c3), "the restored DB system has the backed-up table and not later writes")
	assert.NotEqual(t, *db.Port, *cloned.Port)

	_, err = e.db.UpdateDbSystem(ctx, my.UpdateDbSystemRequest{
		DbSystemId: clone.Id, AdminPassword: new("N3wPassw0rd#"),
	})
	require.NoError(t, err)
	e.awaitState(t, clone.Id, my.DbSystemLifecycleStateActive)

	require.NoError(t, e.connect(t, cloned, "N3wPassw0rd#").PingContext(ctx))
	require.Error(t, e.connect(t, cloned, adminPass).PingContext(ctx), "the old password stops working")

	for _, id := range []*string{db.Id, clone.Id} {
		_, err = e.db.DeleteDbSystem(ctx, my.DeleteDbSystemRequest{DbSystemId: id})
		require.NoError(t, err)
		e.awaitState(t, id, my.DbSystemLifecycleStateDeleted)
	}

	for _, id := range []*string{db.Id, clone.Id} {
		tail := strings.ToLower((*id)[len(*id)-12:])
		assert.Empty(t, dockerList(t, "container", tail), "deleting a DB system removes its container")
		assert.Empty(t, dockerList(t, "volume", tail), "deleting a DB system removes its volume")
	}
}
