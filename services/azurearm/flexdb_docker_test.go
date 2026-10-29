package azurearm_test

import (
	"context"
	"database/sql"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const flexDockerStart = 5 * time.Minute

func flexDockerEngine(t *testing.T, kind string) azurearm.FlexEngine {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	_ = rt.Close()

	eng := azurearm.NewDockerFlexEngine(kind, azurearm.DockerFlexConfig{
		Runtime:      func() (container.Runtime, error) { return container.NewRuntime(container.Config{}) },
		StartTimeout: flexDockerStart,
	})

	if c, ok := eng.(interface{ Close() }); ok {
		t.Cleanup(c.Close)
	}

	return eng
}

func flexEndpoint(t *testing.T, s *flexStack, ns, name, api string) string {
	t.Helper()

	_, body := s.raw(t, http.MethodGet, flexServerPath(ns, name), "", api)
	props, _ := body["properties"].(map[string]any)
	ep, _ := props["gopherstackEndpoint"].(string)
	require.NotEmpty(t, ep, "the docker engine must publish an endpoint")

	return ep
}

func TestPGFlexDockerEngine(t *testing.T) {
	t.Parallel()

	eng := flexDockerEngine(t, "postgres")
	s := newFlexStack(t, nil)
	s.pg.SetEngine(eng)
	f := s.pgFactory(t)
	ctx, cancel := context.WithTimeout(t.Context(), flexDockerStart)
	t.Cleanup(cancel)

	srv := pgServer()
	v := srvVersion16
	srv.Properties.Version = &v

	p, err := f.NewServersClient().BeginCreate(ctx, flexRG, "pg-docker", srv, nil)
	require.NoError(t, err)
	_, err = p.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	ep := flexEndpoint(t, s, pgNS, "pg-docker", flexAPIPG)
	dsn := "postgres://pgadmin:" + flexPassword + "@" + ep + "/postgres?sslmode=disable"

	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)

	var one int
	require.NoError(t, conn.QueryRow(ctx, "SELECT 1").Scan(&one))
	_ = conn.Close(ctx)

	db, err := f.NewDatabasesClient().BeginCreate(ctx, flexRG, "pg-docker", "shop", pgDatabase(), nil)
	require.NoError(t, err)
	_, err = db.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	shop, err := pgx.Connect(ctx, strings.Replace(dsn, "/postgres?", "/shop?", 1))
	require.NoError(t, err)

	var owner string
	require.NoError(t, shop.QueryRow(ctx,
		"SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = 'shop'").Scan(&owner))
	assert.Equal(t, "pgadmin", owner)
	_ = shop.Close(ctx)

	up, err := f.NewServersClient().BeginUpdate(ctx, flexRG, "pg-docker", pgPasswordUpdate("N3wS3cret!pw"), nil)
	require.NoError(t, err)
	_, err = up.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	_, err = pgx.Connect(ctx, dsn)
	require.Error(t, err, "the old password must stop working")

	conn, err = pgx.Connect(ctx, strings.Replace(dsn, flexPassword, "N3wS3cret!pw", 1))
	require.NoError(t, err)
	_ = conn.Close(ctx)

	stop, err := f.NewServersClient().BeginStop(ctx, flexRG, "pg-docker", nil)
	require.NoError(t, err)
	_, err = stop.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	short, cancelShort := context.WithTimeout(ctx, 3*time.Second)
	defer cancelShort()

	_, err = pgx.Connect(short, strings.Replace(dsn, flexPassword, "N3wS3cret!pw", 1))
	require.Error(t, err, "a stopped server must refuse connections")

	start, err := f.NewServersClient().BeginStart(ctx, flexRG, "pg-docker", nil)
	require.NoError(t, err)
	_, err = start.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	conn, err = pgx.Connect(ctx, strings.Replace(dsn, flexPassword, "N3wS3cret!pw", 1))
	require.NoError(t, err)
	_ = conn.Close(ctx)

	del, err := f.NewServersClient().BeginDelete(ctx, flexRG, "pg-docker", nil)
	require.NoError(t, err)
	_, err = del.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
}

func TestMySQLFlexDockerEngine(t *testing.T) {
	t.Parallel()

	eng := flexDockerEngine(t, "mysql")
	s := newFlexStack(t, nil)
	s.my.SetEngine(eng)
	f := s.myFactory(t)
	ctx, cancel := context.WithTimeout(t.Context(), flexDockerStart)
	t.Cleanup(cancel)

	srv := mySrv()
	v := myVersion84
	srv.Properties.Version = &v

	p, err := f.NewServersClient().BeginCreate(ctx, flexRG, "my-docker", srv, nil)
	require.NoError(t, err)
	_, err = p.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	ep := flexEndpoint(t, s, myNS, "my-docker", flexAPIMy)

	open := func(pw, dbName string) *sql.DB {
		cfg := mysql.NewConfig()
		cfg.User, cfg.Passwd, cfg.Net, cfg.Addr, cfg.DBName = "myadmin", pw, "tcp", ep, dbName
		cfg.Timeout = 5 * time.Second

		c, cErr := mysql.NewConnector(cfg)
		require.NoError(t, cErr)

		db := sql.OpenDB(c)
		t.Cleanup(func() { _ = db.Close() })

		return db
	}

	require.NoError(t, open(flexPassword, "").PingContext(ctx))

	db, err := f.NewDatabasesClient().BeginCreateOrUpdate(ctx, flexRG, "my-docker", "shop", myDatabase(), nil)
	require.NoError(t, err)
	_, err = db.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	assert.Contains(t, mysqlDatabases(ctx, t, open(flexPassword, "")), "shop")

	require.NoError(t, open(flexPassword, "shop").PingContext(ctx))

	drop, err := f.NewDatabasesClient().BeginDelete(ctx, flexRG, "my-docker", "shop", nil)
	require.NoError(t, err)
	_, err = drop.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	assert.NotContains(t, mysqlDatabases(ctx, t, open(flexPassword, "")), "shop")

	up, err := f.NewServersClient().BeginUpdate(ctx, flexRG, "my-docker", myPasswordUpdate("N3wS3cret!pw"), nil)
	require.NoError(t, err)
	_, err = up.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	require.Error(t, open(flexPassword, "").PingContext(ctx))
	require.NoError(t, open("N3wS3cret!pw", "").PingContext(ctx))

	stop, err := f.NewServersClient().BeginStop(ctx, flexRG, "my-docker", nil)
	require.NoError(t, err)
	_, err = stop.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	require.Error(t, open("N3wS3cret!pw", "").PingContext(ctx))

	start, err := f.NewServersClient().BeginStart(ctx, flexRG, "my-docker", nil)
	require.NoError(t, err)
	_, err = start.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	require.NoError(t, open("N3wS3cret!pw", "").PingContext(ctx))

	del, err := f.NewServersClient().BeginDelete(ctx, flexRG, "my-docker", nil)
	require.NoError(t, err)
	_, err = del.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
}

func mysqlDatabases(ctx context.Context, t *testing.T, db *sql.DB) []string {
	t.Helper()

	rows, err := db.QueryContext(ctx, "SHOW DATABASES")
	require.NoError(t, err)

	defer rows.Close()

	var names []string

	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))

		names = append(names, n)
	}

	require.NoError(t, rows.Err())

	return names
}
