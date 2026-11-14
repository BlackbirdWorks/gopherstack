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

	bk := e.awaitBackup(t, bkResp.Id)

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

func TestDockerReadReplica(t *testing.T) {
	t.Parallel()

	e := dockerEnv(t)
	ctx := t.Context()

	created := e.createDB(t, func() my.CreateDbSystemDetails {
		d := dbDetails("primary")
		d.ShapeName = new(shapeReplica)

		return d
	}())
	db := e.awaitState(t, created.Id, my.DbSystemLifecycleStateActive)

	src := e.connect(t, db, adminPass)
	for _, stmt := range []string{
		"CREATE DATABASE shop",
		"CREATE TABLE shop.orders (id INT PRIMARY KEY, item VARCHAR(40))",
		"INSERT INTO shop.orders VALUES (1, 'widget')",
	} {
		_, err := src.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	rep := e.mkReplica(t, db.Id, "r1")
	require.Eventually(t, func() bool {
		r, err := e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: rep.Id})

		return err == nil && r.LifecycleState == my.ReplicaLifecycleStateActive
	}, e.waitFor(), 500*time.Millisecond)

	got, err := e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: rep.Id})
	require.NoError(t, err)

	r := openAdmin(t, *got.IpAddress, *got.Port, adminUser, adminPass)
	assert.Equal(t, 1, rowCount(t, r), "the replica starts from the source's data")

	_, err = src.ExecContext(ctx, "INSERT INTO shop.orders VALUES (2, 'gadget')")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return rowCount(t, r) == 2 }, time.Minute, 500*time.Millisecond,
		"later writes replicate")

	_, err = r.ExecContext(ctx, "INSERT INTO shop.orders VALUES (3, 'nope')")
	require.Error(t, err, "a read replica refuses writes")

	_, err = e.db.StopDbSystem(ctx, my.StopDbSystemRequest{DbSystemId: db.Id, ShutdownType: my.InnoDbShutdownModeFast})
	require.NoError(t, err)
	e.awaitState(t, db.Id, my.DbSystemLifecycleStateInactive)

	_, err = e.db.StartDbSystem(ctx, my.StartDbSystemRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	restarted := e.awaitState(t, db.Id, my.DbSystemLifecycleStateActive)

	src2 := e.connect(t, restarted, adminPass)
	_, err = src2.ExecContext(ctx, "INSERT INTO shop.orders VALUES (4, 'again')")
	require.NoError(t, err)

	after, err := e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: rep.Id})
	require.NoError(t, err)

	r2 := openAdmin(t, *after.IpAddress, *after.Port, adminUser, adminPass)
	require.Eventually(t, func() bool { return rowCount(t, r2) == 3 }, 2*time.Minute, time.Second,
		"replication resumes after stop and start")

	_, err = e.rp.DeleteReplica(ctx, my.DeleteReplicaRequest{ReplicaId: rep.Id})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		x, gerr := e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: rep.Id})

		return gerr == nil && x.LifecycleState == my.ReplicaLifecycleStateDeleted
	}, e.waitFor(), 500*time.Millisecond)

	tail := strings.ToLower((*rep.Id)[len(*rep.Id)-12:])
	assert.Empty(t, dockerList(t, "container", tail), "deleting a replica removes its container")
	assert.Empty(t, dockerList(t, "volume", tail), "deleting a replica removes its volume")

	_, err = e.db.DeleteDbSystem(ctx, my.DeleteDbSystemRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	e.awaitState(t, db.Id, my.DbSystemLifecycleStateDeleted)
}

func TestDockerPointInTimeRecovery(t *testing.T) {
	t.Parallel()

	e := dockerEnv(t)
	e.backend.SetClock(time.Now)

	ctx := t.Context()

	d := dbDetails("pitr")
	withPitr(&d)

	created := e.createDB(t, d)
	db := e.awaitState(t, created.Id, my.DbSystemLifecycleStateActive)
	require.NotNil(t, db.PointInTimeRecoveryDetails)

	src := e.connect(t, db, adminPass)
	for _, stmt := range []string{
		"CREATE DATABASE shop",
		"CREATE TABLE shop.orders (id INT PRIMARY KEY, item VARCHAR(40))",
		"INSERT INTO shop.orders VALUES (1, 'widget')",
	} {
		_, err := src.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	mark := time.Now().UTC().Add(time.Second).Truncate(time.Second)

	require.Eventually(t, func() bool { return time.Now().After(mark.Add(2 * time.Second)) }, 10*time.Second,
		100*time.Millisecond)

	_, err := src.ExecContext(ctx, "INSERT INTO shop.orders VALUES (2, 'gadget')")
	require.NoError(t, err)

	restore := func(name string, at *time.Time) my.DbSystem {
		resp, cerr := e.createFromPitr(t, name, pitrSource(db.Id, at))
		require.NoError(t, cerr)

		return e.awaitState(t, resp.Id, my.DbSystemLifecycleStateActive)
	}

	earlier := restore("earlier", &mark)
	assert.Equal(t, 1, rowCount(t, e.connect(t, earlier, adminPass)), "recovery stops before the later write")

	latest := restore("latest", nil)
	assert.Equal(t, 2, rowCount(t, e.connect(t, latest, adminPass)), "no recovery point recovers everything")

	_, err = e.connect(t, earlier, adminPass).ExecContext(ctx, "INSERT INTO shop.orders VALUES (3, 'new')")
	require.NoError(t, err, "the recovered DB system is writable and detached from its source")

	for _, id := range []*string{earlier.Id, latest.Id, db.Id} {
		_, derr := e.db.DeleteDbSystem(ctx, my.DeleteDbSystemRequest{DbSystemId: id})
		require.NoError(t, derr)
		e.awaitState(t, id, my.DbSystemLifecycleStateDeleted)
	}
}

func scalarOf(t *testing.T, c *sql.DB, q string) string {
	t.Helper()

	var v string

	require.NoError(t, c.QueryRowContext(t.Context(), q).Scan(&v))

	return v
}

func TestDockerConfigurationStatusEstimateValidate(t *testing.T) {
	t.Parallel()

	e := dockerEnv(t)
	ctx := t.Context()

	cfg := e.mkConfig(t, "tuned", &my.ConfigurationVariables{
		MaxConnections: new(77), InnodbFtMinTokenSize: new(4), SqlMode: new("STRICT_ALL_TABLES"),
	})

	d := dbDetails("real")
	d.ConfigurationId = cfg

	created := e.createDB(t, d)
	db := e.awaitState(t, created.Id, my.DbSystemLifecycleStateActive)
	c := e.connect(t, db, adminPass)

	assert.Equal(t, "77", scalarOf(t, c, "SELECT @@max_connections"), "a dynamic variable is set")
	assert.Equal(
		t,
		"4",
		scalarOf(t, c, "SELECT @@innodb_ft_min_token_size"),
		"a start-time variable takes effect after the restart",
	)
	assert.Equal(t, "STRICT_ALL_TABLES", scalarOf(t, c, "SELECT @@sql_mode"))

	for _, stmt := range []string{
		"CREATE DATABASE shop",
		"CREATE TABLE shop.orders (id INT PRIMARY KEY, item VARCHAR(40), note TEXT)",
		"INSERT INTO shop.orders VALUES (1, 'widget', 'a'), (2, 'gadget', 'b')",
	} {
		_, err := c.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	_, err := e.db.GenerateHeatWaveClusterMemoryEstimate(ctx, my.GenerateHeatWaveClusterMemoryEstimateRequest{
		DbSystemId: db.Id,
	})
	require.NoError(t, err)

	var est my.GetHeatWaveClusterMemoryEstimateResponse

	require.Eventually(t, func() bool {
		var gerr error

		est, gerr = e.db.GetHeatWaveClusterMemoryEstimate(ctx, my.GetHeatWaveClusterMemoryEstimateRequest{
			DbSystemId: db.Id,
		})

		return gerr == nil && est.Status == my.HeatWaveClusterMemoryEstimateStatusSucceeded
	}, time.Minute, 200*time.Millisecond)

	require.Len(t, est.TableSchemas, 1)
	require.Equal(t, "shop", *est.TableSchemas[0].SchemaName)

	tbl := est.TableSchemas[0].PerTableEstimates[0]
	assert.Equal(t, "orders", *tbl.TableName)
	assert.Equal(t, 3, *tbl.ToLoadColumnCount)
	assert.Equal(t, 2, *tbl.VarlenColumnCount, "the VARCHAR and the TEXT column")
	assert.EqualValues(t, 2, *tbl.EstimatedRowCount)

	_, err = e.db.GenerateDbSystemStatus(ctx, my.GenerateDbSystemStatusRequest{DbSystemId: db.Id})
	require.NoError(t, err)

	var status my.GetDbSystemStatusResponse

	require.Eventually(t, func() bool {
		var gerr error

		status, gerr = e.db.GetDbSystemStatus(ctx, my.GetDbSystemStatusRequest{DbSystemId: db.Id})

		return gerr == nil && status.DbSystemStatusResult != nil
	}, time.Minute, 200*time.Millisecond)

	res := status.DbSystemStatusResult
	assert.True(t, *res.CanAcceptClientConnections)
	assert.True(t, *res.IsWritable)
	assert.NotEmpty(t, *res.GtidSet, "the GTID set is read from the server")

	bk, err := e.bk.CreateBackup(ctx, my.CreateBackupRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	e.awaitBackup(t, bk.Id)

	_, err = e.bk.ValidateBackup(ctx, my.ValidateBackupRequest{
		BackupId: bk.Id, IsPreparedBackupRequired: new(false),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		got, gerr := e.bk.GetBackup(ctx, my.GetBackupRequest{BackupId: bk.Id})

		return gerr == nil && got.BackupValidationDetails != nil &&
			got.BackupValidationDetails.ValidationStatus == my.BackupValidationDetailsValidationStatusValidated
	}, 5*time.Minute, time.Second, "the backup restores onto a scratch server")

	plain := e.mkConfigForDocker(t)

	_, err = e.db.UpdateDbSystem(ctx, my.UpdateDbSystemRequest{
		DbSystemId: db.Id, ConfigurationId: plain,
	})
	require.NoError(t, err)
	e.awaitState(t, db.Id, my.DbSystemLifecycleStateActive)

	back := e.connect(t, db, adminPass)
	require.Eventually(t, func() bool { return back.PingContext(ctx) == nil }, time.Minute, 500*time.Millisecond)

	assert.Equal(t, "55", scalarOf(t, back, "SELECT @@max_connections"))
	assert.Equal(t, "3", scalarOf(t, back, "SELECT @@innodb_ft_min_token_size"), "the earlier configuration is dropped")
	assert.NotEqual(t, "STRICT_ALL_TABLES", scalarOf(t, back, "SELECT @@sql_mode"))
	assert.Equal(t, 2, rowCount(t, back), "the data survives the configuration change")

	_, err = e.db.DeleteDbSystem(ctx, my.DeleteDbSystemRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	e.awaitState(t, db.Id, my.DbSystemLifecycleStateDeleted)
}

func (e *env) mkConfigForDocker(t *testing.T) *string {
	t.Helper()

	return e.mkConfig(t, "plain", &my.ConfigurationVariables{MaxConnections: new(55)})
}
