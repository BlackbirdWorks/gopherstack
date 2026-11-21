package azurearm_test

import (
	"context"
	"database/sql"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/mysql/armmysqlflexibleservers/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/postgresql/armpostgresqlflexibleservers/v2"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const flexDockerVersionPG = "16"

func newFlexDockerStack(t *testing.T, kind, image string) *flexDataStack {
	t.Helper()

	if testing.Short() {
		t.Skip("docker engine tests are skipped under -short")
	}

	if err := exec.CommandContext(t.Context(), "docker", "image", "inspect", image).Run(); err != nil {
		t.Skipf("image %s is not present locally", image)
	}

	eng := flexDockerEngine(t, kind)
	s := newFlexStack(t, eng)
	clk := newFlexClock()
	clk.step = time.Second
	dir := t.TempDir()

	s.pg.SetClock(clk.Now)
	s.my.SetClock(clk.Now)
	s.pg.SetDumpDir(dir, 0)
	s.my.SetDumpDir(dir, 0)

	return &flexDataStack{flexStack: s, eng: nil, clk: clk, dir: dir}
}

func flexDockerCtx(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 2*flexDockerStart)
	t.Cleanup(cancel)

	return ctx
}

func pgDockerConn(ctx context.Context, t *testing.T, ep, db string) *pgx.Conn {
	t.Helper()

	conn, err := pgx.Connect(ctx, "postgres://pgadmin:"+flexPassword+"@"+ep+"/"+db+"?sslmode=disable")
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(ctx)) })

	return conn
}

func pgDockerCreate(
	ctx context.Context,
	t *testing.T,
	s *flexStack,
	name string,
	props armpostgresqlflexibleservers.ServerProperties,
) string {
	t.Helper()

	srv := armpostgresqlflexibleservers.Server{
		Location: new(flexLoc), Properties: &props,
		SKU: &armpostgresqlflexibleservers.SKU{
			Name: new("Standard_D2ds_v5"), Tier: new(armpostgresqlflexibleservers.SKUTierGeneralPurpose),
		},
	}

	p, err := s.pgFactory(t).NewServersClient().BeginCreate(ctx, flexRG, name, srv, nil)
	require.NoError(t, err)

	res, err := p.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	return *res.ID
}

func pgDockerCount(ctx context.Context, t *testing.T, ep string) int {
	t.Helper()

	var n int
	require.NoError(t, pgDockerConn(ctx, t, ep, "shop").QueryRow(ctx, "SELECT count(*) FROM items").Scan(&n))

	return n
}

func TestPGFlexDockerBackupsReplicasAndParameters(t *testing.T) {
	t.Parallel()

	d := newFlexDockerStack(t, "postgres", "postgres:"+flexDockerVersionPG)
	ctx := flexDockerCtx(t)
	f := d.pgFactory(t)

	srcID := pgDockerCreate(ctx, t, d.flexStack, "pgsrc", armpostgresqlflexibleservers.ServerProperties{
		AdministratorLogin: new("pgadmin"), AdministratorLoginPassword: new(flexPassword),
		Version: new(armpostgresqlflexibleservers.ServerVersion(flexDockerVersionPG)),
		Storage: &armpostgresqlflexibleservers.Storage{StorageSizeGB: new(int32(32))},
		Backup:  &armpostgresqlflexibleservers.Backup{BackupRetentionDays: new(int32(7))},
	})
	db, err := f.NewDatabasesClient().
		BeginCreate(ctx, flexRG, "pgsrc", "shop", armpostgresqlflexibleservers.Database{}, nil)
	require.NoError(t, err)
	_, err = db.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	ep := flexEndpoint(t, d.flexStack, pgNS, "pgsrc", flexAPIPG)
	c := pgDockerConn(ctx, t, ep, "shop")
	_, err = c.Exec(ctx, "CREATE TABLE items (id int PRIMARY KEY, note text)")
	require.NoError(t, err)
	_, err = c.Exec(ctx, "INSERT INTO items VALUES (1, 'one')")
	require.NoError(t, err)

	d.clk.Advance(25 * time.Hour)
	d.pg.RunBackupCycle(ctx)

	_, err = pgDockerConn(ctx, t, ep, "shop").Exec(ctx, "INSERT INTO items VALUES (2, 'two')")
	require.NoError(t, err)

	restoredID := pgDockerCreate(ctx, t, d.flexStack, "pgpitr", armpostgresqlflexibleservers.ServerProperties{
		CreateMode: new(armpostgresqlflexibleservers.CreateModePointInTimeRestore), SourceServerResourceID: &srcID,
		PointInTimeUTC: new(d.clk.Now()),
	})
	assert.NotEmpty(t, restoredID)

	rep := flexEndpoint(t, d.flexStack, pgNS, "pgpitr", flexAPIPG)
	assert.Equal(t, 1, pgDockerCount(ctx, t, rep), "the restore holds the backup, not later writes")

	_, err = pgDockerConn(ctx, t, rep, "shop").Exec(ctx, "INSERT INTO items VALUES (3, 'three')")
	require.NoError(t, err, "a restored server is writable and the carried admin can write the restored tables")

	pgDockerCreate(ctx, t, d.flexStack, "pgrep", armpostgresqlflexibleservers.ServerProperties{
		CreateMode: new(armpostgresqlflexibleservers.CreateModeReplica), SourceServerResourceID: &srcID,
	})

	repEP := flexEndpoint(t, d.flexStack, pgNS, "pgrep", flexAPIPG)
	assert.Equal(t, 2, pgDockerCount(ctx, t, repEP), "the replica is seeded from the source as it is now")

	_, err = pgDockerConn(ctx, t, repEP, "shop").Exec(ctx, "INSERT INTO items VALUES (9, 'nine')")
	require.Error(t, err, "a replica refuses writes")
	assert.Contains(t, err.Error(), "read-only")

	prom, err := f.NewServersClient().BeginUpdate(ctx, flexRG, "pgrep", armpostgresqlflexibleservers.ServerForUpdate{
		Properties: &armpostgresqlflexibleservers.ServerPropertiesForUpdate{
			ReplicationRole: new(armpostgresqlflexibleservers.ReplicationRoleNone),
		},
	}, nil)
	require.NoError(t, err)
	_, err = prom.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	_, err = pgDockerConn(ctx, t, repEP, "shop").Exec(ctx, "INSERT INTO items VALUES (9, 'nine')")
	require.NoError(t, err, "a promoted replica accepts writes")

	for _, name := range []string{"pgpitr", "pgrep"} {
		del, derr := f.NewServersClient().BeginDelete(ctx, flexRG, name, nil)
		require.NoError(t, derr)
		_, derr = del.PollUntilDone(ctx, pollFast())
		require.NoError(t, derr)
	}
}

func TestPGFlexDockerParametersAndRestart(t *testing.T) {
	t.Parallel()

	d := newFlexDockerStack(t, "postgres", "postgres:"+flexDockerVersionPG)
	ctx := flexDockerCtx(t)
	f := d.pgFactory(t)

	pgDockerCreate(ctx, t, d.flexStack, "pgcfg", armpostgresqlflexibleservers.ServerProperties{
		AdministratorLogin: new("pgadmin"), AdministratorLoginPassword: new(flexPassword),
		Version: new(armpostgresqlflexibleservers.ServerVersion(flexDockerVersionPG)),
		Storage: &armpostgresqlflexibleservers.Storage{StorageSizeGB: new(int32(32))},
	})

	dbc, err := f.NewDatabasesClient().
		BeginCreate(ctx, flexRG, "pgcfg", "shop", armpostgresqlflexibleservers.Database{}, nil)
	require.NoError(t, err)
	_, err = dbc.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	ep := flexEndpoint(t, d.flexStack, pgNS, "pgcfg", flexAPIPG)
	show := func(param string) string {
		var v string
		require.NoError(t, pgDockerConn(ctx, t, ep, "postgres").QueryRow(ctx, "SHOW "+param).Scan(&v))

		return v
	}

	_, err = pgDockerConn(ctx, t, ep, "shop").Exec(ctx, "CREATE TABLE items (id int)")
	require.NoError(t, err)
	_, err = pgDockerConn(ctx, t, ep, "shop").Exec(ctx, "INSERT INTO items VALUES (1)")
	require.NoError(t, err)

	put := func(name, value string) {
		p, perr := f.NewConfigurationsClient().BeginPut(ctx, flexRG, "pgcfg", name,
			armpostgresqlflexibleservers.Configuration{
				Properties: &armpostgresqlflexibleservers.ConfigurationProperties{
					Value: new(value), Source: new("user-override"),
				},
			}, nil)
		require.NoError(t, perr)
		_, perr = p.PollUntilDone(ctx, pollFast())
		require.NoError(t, perr)
	}

	put("work_mem", "8192")
	assert.Equal(t, "8MB", show("work_mem"), "a dynamic parameter applies at once")

	put("max_connections", "77")
	assert.NotEqual(t, "77", show("max_connections"), "a static parameter waits for a restart")

	cfg, err := f.NewConfigurationsClient().Get(ctx, flexRG, "pgcfg", "max_connections", nil)
	require.NoError(t, err)
	assert.True(t, *cfg.Properties.IsConfigPendingRestart)

	rst, err := f.NewServersClient().BeginRestart(ctx, flexRG, "pgcfg", nil)
	require.NoError(t, err)
	_, err = rst.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	assert.Equal(t, "77", show("max_connections"), "the restart applies it")
	assert.Equal(t, "8MB", show("work_mem"), "dynamic values persist across the restart")
	assert.Equal(t, 1, pgDockerCount(ctx, t, ep), "data survives a restart")

	reset, err := f.NewConfigurationsClient().BeginUpdate(ctx, flexRG, "pgcfg", "work_mem",
		armpostgresqlflexibleservers.ConfigurationForUpdate{
			Properties: &armpostgresqlflexibleservers.ConfigurationProperties{Source: new("system-default")},
		}, nil)
	require.NoError(t, err)
	_, err = reset.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	assert.Equal(t, "4MB", show("work_mem"))

	stop, err := f.NewServersClient().BeginStop(ctx, flexRG, "pgcfg", nil)
	require.NoError(t, err)
	_, err = stop.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	short, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	_, err = pgx.Connect(short, "postgres://pgadmin:"+flexPassword+"@"+ep+"/postgres?sslmode=disable")
	require.Error(t, err, "a stopped server refuses connections")

	start, err := f.NewServersClient().BeginStart(ctx, flexRG, "pgcfg", nil)
	require.NoError(t, err)
	_, err = start.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	assert.Equal(t, 1, pgDockerCount(ctx, t, ep), "data survives stop and start")
	assert.Equal(t, "77", show("max_connections"))
}

func myDockerOpen(t *testing.T, ep, db string) *sql.DB {
	t.Helper()

	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net, cfg.Addr, cfg.DBName = "myadmin", flexPassword, "tcp", ep, db
	cfg.Timeout = 5 * time.Second

	c, err := mysql.NewConnector(cfg)
	require.NoError(t, err)

	conn := sql.OpenDB(c)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

func myDockerCreate(
	ctx context.Context,
	t *testing.T,
	s *flexStack,
	name string,
	props armmysqlflexibleservers.ServerProperties,
) {
	t.Helper()

	srv := armmysqlflexibleservers.Server{
		Location: new(flexLoc), Properties: &props,
		SKU: &armmysqlflexibleservers.MySQLServerSKU{
			Name: new("Standard_D2ds_v4"), Tier: new(armmysqlflexibleservers.ServerSKUTierGeneralPurpose),
		},
	}

	p, err := s.myFactory(t).NewServersClient().BeginCreate(ctx, flexRG, name, srv, nil)
	require.NoError(t, err)
	_, err = p.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
}

func myDockerCount(ctx context.Context, t *testing.T, ep string) int {
	t.Helper()

	var n int
	require.NoError(t, myDockerOpen(t, ep, "shop").QueryRowContext(ctx, "SELECT count(*) FROM items").Scan(&n))

	return n
}

func TestMySQLFlexDockerBackupsReplicasAndParameters(t *testing.T) {
	t.Parallel()

	d := newFlexDockerStack(t, "mysql", "mysql:8.4")
	ctx := flexDockerCtx(t)
	f := d.myFactory(t)

	myDockerCreate(ctx, t, d.flexStack, "mysrc", armmysqlflexibleservers.ServerProperties{
		AdministratorLogin: new("myadmin"), AdministratorLoginPassword: new(flexPassword),
		Version: new(myVersion84),
		Storage: &armmysqlflexibleservers.Storage{StorageSizeGB: new(int32(20))},
		Backup:  &armmysqlflexibleservers.Backup{BackupRetentionDays: new(int32(5))},
	})

	srcID := "/subscriptions/" + aciSub + "/resourceGroups/" + flexRG + "/providers/" + myNS + "/flexibleServers/mysrc"

	db, err := f.NewDatabasesClient().
		BeginCreateOrUpdate(ctx, flexRG, "mysrc", "shop", armmysqlflexibleservers.Database{}, nil)
	require.NoError(t, err)
	_, err = db.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	ep := flexEndpoint(t, d.flexStack, myNS, "mysrc", flexAPIMy)
	shop := myDockerOpen(t, ep, "shop")

	_, err = shop.ExecContext(ctx, "CREATE TABLE items (id int PRIMARY KEY, note varchar(20))")
	require.NoError(t, err)
	_, err = shop.ExecContext(ctx, "INSERT INTO items VALUES (1, 'one')")
	require.NoError(t, err)

	d.clk.Advance(time.Hour)

	status, body := d.raw(t, "PUT", flexServerPath(myNS, "mysrc")+"/backups/before-two", "", flexAPIMy)
	require.Equal(t, 200, status, body)

	_, err = shop.ExecContext(ctx, "INSERT INTO items VALUES (2, 'two')")
	require.NoError(t, err)

	d.clk.Advance(time.Hour)

	require.NoError(t, d.mySeed(t, "mypitr", armmysqlflexibleservers.ServerProperties{
		CreateMode: new(armmysqlflexibleservers.CreateModePointInTimeRestore), SourceServerResourceID: &srcID,
		RestorePointInTime: new(d.clk.Now().Add(-30 * time.Minute)),
	}))

	assert.Equal(t, 1, myDockerCount(ctx, t, flexEndpoint(t, d.flexStack, myNS, "mypitr", flexAPIMy)),
		"the restore holds the on-demand backup, not later writes")

	require.NoError(t, d.mySeed(t, "myrep", armmysqlflexibleservers.ServerProperties{
		CreateMode: new(armmysqlflexibleservers.CreateModeReplica), SourceServerResourceID: &srcID,
	}))

	repEP := flexEndpoint(t, d.flexStack, myNS, "myrep", flexAPIMy)
	assert.Equal(t, 2, myDockerCount(ctx, t, repEP))

	_, err = myDockerOpen(t, repEP, "shop").ExecContext(ctx, "INSERT INTO items VALUES (9, 'nine')")
	require.Error(t, err, "a replica refuses writes")

	prom, err := f.NewServersClient().BeginUpdate(ctx, flexRG, "myrep", armmysqlflexibleservers.ServerForUpdate{
		Properties: &armmysqlflexibleservers.ServerPropertiesForUpdate{
			ReplicationRole: new(armmysqlflexibleservers.ReplicationRoleNone),
		},
	}, nil)
	require.NoError(t, err)
	_, err = prom.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	_, err = myDockerOpen(t, repEP, "shop").ExecContext(ctx, "INSERT INTO items VALUES (9, 'nine')")
	require.NoError(t, err, "a promoted replica accepts writes")
}

func TestMySQLFlexDockerParametersAndRestart(t *testing.T) {
	t.Parallel()

	d := newFlexDockerStack(t, "mysql", "mysql:8.4")
	ctx := flexDockerCtx(t)
	f := d.myFactory(t)

	myDockerCreate(ctx, t, d.flexStack, "mycfg", armmysqlflexibleservers.ServerProperties{
		AdministratorLogin: new("myadmin"), AdministratorLoginPassword: new(flexPassword),
		Version: new(myVersion84),
		Storage: &armmysqlflexibleservers.Storage{StorageSizeGB: new(int32(20))},
	})

	ep := flexEndpoint(t, d.flexStack, myNS, "mycfg", flexAPIMy)
	conn := myDockerOpen(t, ep, "")

	global := func(name string) string {
		var v string
		require.NoError(t, myDockerOpen(t, ep, "").QueryRowContext(ctx, "SELECT @@GLOBAL."+name).Scan(&v))

		return v
	}

	_, err := conn.ExecContext(ctx, "CREATE DATABASE keep")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "CREATE TABLE keep.t (id int)")
	require.NoError(t, err)

	status, body := d.raw(t, "POST", flexServerPath(myNS, "mycfg")+"/updateConfigurations",
		`{"value":[{"name":"wait_timeout","properties":{"value":"123"}},`+
			`{"name":"character_set_server","properties":{"value":"latin1"}}]}`, flexAPIMy)
	require.Equal(t, 200, status, body)

	assert.Equal(t, "123", global("wait_timeout"), "a dynamic variable applies at once")
	assert.True(
		t,
		strings.EqualFold("utf8mb4", global("character_set_server")),
		"a static variable waits for a restart",
	)

	rst, err := f.NewServersClient().
		BeginRestart(ctx, flexRG, "mycfg", armmysqlflexibleservers.ServerRestartParameter{}, nil)
	require.NoError(t, err)
	_, err = rst.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	assert.Equal(t, "latin1", global("character_set_server"), "the restart applies it")
	assert.Equal(t, "123", global("wait_timeout"))

	var n int
	require.NoError(t, myDockerOpen(t, ep, "keep").QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&n))
	assert.Zero(t, n, "the schema survives a restart")
}
