package rds_test

import (
	"database/sql"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

const (
	sqlServerDockerEnv  = "RDS_SQLSERVER_DOCKER_TEST"
	sqlServerDockerWait = 8 * time.Minute
)

func openSQLServerDB(t *testing.T, inst rds.DBInstance, password string) *sql.DB {
	t.Helper()

	u := url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword("master", password),
		Host:     inst.Endpoint + ":" + strconv.Itoa(inst.Port),
		RawQuery: "encrypt=disable",
	}

	db, err := sql.Open("sqlserver", u.String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return db
}

func TestEngineDockerSQLServerSnapshotRestore(t *testing.T) {
	t.Parallel()

	if os.Getenv(sqlServerDockerEnv) == "" {
		t.Skipf("set %s=1 to run (pulls the ~1.5GB amd64-only SQL Server image)", sqlServerDockerEnv)
	}

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	require.NoError(t, err)

	er, ok := rt.(rds.EngineRuntime)
	require.True(t, ok)
	require.NoError(t, rt.Ping(t.Context()))

	rec := &recordingEngineRuntime{EngineRuntime: er}
	b := rds.NewInMemoryBackend("123456789012", "us-east-1")
	b.EnableEngine(rds.EngineConfig{Runtime: rec, SQLServer: true, StartTimeout: sqlServerDockerWait})
	t.Cleanup(b.Close)

	const password = "Sup3rS3cret-pw"

	_, err = b.CreateDBInstance("mssql1", "sqlserver-ex", "db.t3.micro", "", "master", "", 20,
		rds.DBInstanceOptions{EngineVersion: "16.00.4115.5.v1", MasterUserPassword: password})
	require.NoError(t, err)

	waitAvailable := func(id string) rds.DBInstance {
		var inst rds.DBInstance

		require.Eventually(t, func() bool {
			got, derr := b.DescribeDBInstances(id)
			require.NoError(t, derr)
			inst = got[0]
			requireNotFailed(t, b, id, inst.DBInstanceStatus)

			return inst.DBInstanceStatus == "available"
		}, sqlServerDockerWait, time.Second)

		return inst
	}

	src := waitAvailable("mssql1")
	db := openSQLServerDB(t, src, password)

	var edition, login string
	require.NoError(t, db.QueryRowContext(dbCtx(t),
		"SELECT CAST(SERVERPROPERTY('Edition') AS nvarchar(100)), SUSER_SNAME()").Scan(&edition, &login))
	assert.Contains(t, edition, "Express")
	assert.Equal(t, "master", login)

	for _, stmt := range []string{
		"CREATE DATABASE appdb",
		"CREATE TABLE appdb.dbo.items (id int primary key, name nvarchar(50))",
		"INSERT INTO appdb.dbo.items VALUES (1, N'alpha')",
	} {
		_, err = db.ExecContext(dbCtx(t), stmt)
		require.NoError(t, err, stmt)
	}

	_, err = b.CreateDBSnapshot("mssql-snap", "mssql1")
	require.NoError(t, err)

	_, err = b.RestoreDBInstanceFromDBSnapshot("mssql2", "mssql-snap", rds.DBInstanceOptions{})
	require.NoError(t, err)

	restored := waitAvailable("mssql2")
	assert.NotEqual(t, src.Port, restored.Port)

	var name string

	require.NoError(t, openSQLServerDB(t, restored, password).QueryRowContext(dbCtx(t),
		"SELECT name FROM appdb.dbo.items WHERE id = 1").Scan(&name))
	assert.Equal(t, "alpha", name)

	_, err = b.ModifyDBInstance("mssql1", "", 0,
		rds.DBInstanceOptions{MasterUserPassword: "Another-pw-2", ApplyImmediately: true})
	require.NoError(t, err)
	require.NoError(t, openSQLServerDB(t, src, "Another-pw-2").PingContext(dbCtx(t)))
	require.Error(t, openSQLServerDB(t, src, password).PingContext(dbCtx(t)))

	ids := rec.containerIDs()
	require.Len(t, ids, 2)

	for _, id := range []string{"mssql2", "mssql1"} {
		_, err = b.DeleteDBInstanceWithOptions(id, true, "", true)
		require.NoError(t, err)
	}

	for _, id := range ids {
		require.Eventually(t, func() bool { return !dockerContainerExists(t, id) }, time.Minute, 250*time.Millisecond)
	}
}
