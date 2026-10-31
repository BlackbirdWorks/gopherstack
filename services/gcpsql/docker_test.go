package gcpsql_test

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	sqladmin "google.golang.org/api/sqladmin/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/gcpsql"
)

const dockerWait = 8 * time.Minute

type dockerCase struct {
	name    string
	version string
	flag    gcpsql.Flag
	showSQL string
	want    string
}

func dockerEnv(t *testing.T) *env {
	t.Helper()

	if testing.Short() {
		t.Skip("docker engine test skipped in short mode")
	}

	h := newDockerHandler(t)

	objects := &fakeObjects{data: map[string][]byte{}}
	h.Backend.SetObjectStore(objects)
	t.Cleanup(h.Backend.Close)
	t.Cleanup(h.Backend.Reset)

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	svc, err := sqladmin.NewService(t.Context(), option.WithEndpoint(srv.URL), option.WithoutAuthentication())
	require.NoError(t, err)

	return &env{svc: svc, backend: h.Backend, objects: objects, url: srv.URL, timeout: dockerWait}
}

func newDockerHandler(t *testing.T) *gcpsql.Handler {
	t.Helper()

	lr := gcpserve.NewLazyRuntime(slog.Default(), "gcpsql.test.runtime")
	t.Cleanup(func() { _ = lr.Close() })

	if _, err := lr.Get(); err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	reg, err := (&gcpsql.Provider{}).Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)

	h, ok := reg.(*gcpsql.Handler)
	require.True(t, ok)

	h.Backend.SetStartupTimeout(3 * time.Minute)

	return h
}

func openDB(t *testing.T, version, endpoint, user, pass string) *sql.DB {
	t.Helper()

	const db = "app"

	var conn *sql.DB

	var err error

	if strings.HasPrefix(version, "POSTGRES") {
		conn, err = sql.Open("pgx", fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, pass, endpoint, db))
	} else {
		cfg := mysql.NewConfig()
		cfg.User, cfg.Passwd, cfg.Net, cfg.Addr, cfg.DBName = user, pass, "tcp", endpoint, db
		conn, err = sql.Open("mysql", cfg.FormatDSN())
	}

	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

func scalar(t *testing.T, db *sql.DB, query string) string {
	t.Helper()

	var v string

	require.NoError(t, db.QueryRowContext(t.Context(), query).Scan(&v))

	return v
}

func (e *env) endpoint(t *testing.T, name string) string {
	t.Helper()

	in, err := e.backend.GetInstance(project, name)
	require.NoError(t, err)
	require.NotEmpty(t, in.GopherstackEndpoint)

	return in.GopherstackEndpoint
}

func TestDockerEngines(t *testing.T) {
	t.Parallel()

	cases := []dockerCase{
		{"postgres", "POSTGRES_16", gcpsql.Flag{Name: "max_connections", Value: "150"}, "SHOW max_connections", "150"},
		{"mysql", "MYSQL_8_4", gcpsql.Flag{Name: "max_connections", Value: "151"}, "SELECT @@max_connections", "151"},
	}

	assertNoContainers(t, "dpostgres", "dmysql")

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := dockerEnv(t)
			ctx, cancel := context.WithTimeout(t.Context(), dockerWait)
			defer cancel()

			runDockerFlow(ctx, t, e, tc)
		})
	}
}

func runDockerFlow(ctx context.Context, t *testing.T, e *env, tc dockerCase) {
	t.Helper()

	name := "d" + tc.name
	create := &sqladmin.DatabaseInstance{
		Name: name, DatabaseVersion: tc.version, Region: "us-central1", RootPassword: "root-pw",
		Settings: &sqladmin.Settings{
			Tier:          "db-custom-1-3840",
			DatabaseFlags: []*sqladmin.DatabaseFlags{{Name: tc.flag.Name, Value: tc.flag.Value}},
		},
	}

	op, err := e.svc.Instances.Insert(project, create).Context(ctx).Do()
	e.mustWait(t, op, err)

	ep := e.endpoint(t, name)

	op, err = e.svc.Databases.Insert(project, name, &sqladmin.Database{Name: "app"}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.Users.Insert(project, name, &sqladmin.User{Name: "alice", Host: "%", Password: "alice-pw"}).
		Context(ctx).Do()
	e.mustWait(t, op, err)

	assert.Equal(t, tc.want, scalar(t, openDB(t, tc.version, ep, "alice", "alice-pw"), tc.showSQL),
		"database flags are applied by restart")

	db := openDB(t, tc.version, ep, "alice", "alice-pw")
	_, err = db.ExecContext(ctx, "CREATE TABLE items (id int primary key, name varchar(50))")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO items VALUES (1, 'alpha'), (2, 'beta')")
	require.NoError(t, err)

	verifyExportImportClone(ctx, t, e, tc, name)
}

func verifyExportImportClone(ctx context.Context, t *testing.T, e *env, tc dockerCase, name string) {
	t.Helper()

	count := "SELECT count(*) FROM items"

	op, err := e.svc.Instances.Export(project, name, exportReq(&sqladmin.ExportContext{
		FileType: "SQL", Uri: "gs://bkt/app.sql", Databases: []string{"app"},
	})).
		Context(ctx).
		Do()
	e.mustWait(t, op, err)
	assert.Contains(t, string(e.objects.get("bkt/app.sql")), "alpha")

	op, err = e.svc.BackupRuns.Insert(project, name, &sqladmin.BackupRun{}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.Instances.Clone(project, name, &sqladmin.InstancesCloneRequest{
		CloneContext: &sqladmin.CloneContext{DestinationInstanceName: name + "-clone"},
	}).Context(ctx).Do()
	e.mustWait(t, op, err)

	clone := openDB(t, tc.version, e.endpoint(t, name+"-clone"), "alice", "alice-pw")
	assert.Equal(t, "2", scalar(t, clone, count), "clone carries data and accounts")

	importInto(ctx, t, e, tc, name)

	src := openDB(t, tc.version, e.endpoint(t, name), "alice", "alice-pw")
	_, err = src.ExecContext(ctx, "DELETE FROM items")
	require.NoError(t, err)

	runs, err := e.svc.BackupRuns.List(project, name).Context(ctx).Do()
	require.NoError(t, err)
	require.Len(t, runs.Items, 1)

	op, err = e.svc.Instances.RestoreBackup(project, name, &sqladmin.InstancesRestoreBackupRequest{
		RestoreBackupContext: &sqladmin.RestoreBackupContext{BackupRunId: runs.Items[0].Id},
	}).Context(ctx).Do()
	e.mustWait(t, op, err)

	restored := openDB(t, tc.version, e.endpoint(t, name), "alice", "alice-pw")
	assert.Equal(t, "2", scalar(t, restored, count), "restoreBackup brings the rows back")
}

func importInto(ctx context.Context, t *testing.T, e *env, tc dockerCase, from string) {
	t.Helper()

	target := from + "-imp"
	op, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: target, DatabaseVersion: tc.version, RootPassword: "root-pw",
		Settings: &sqladmin.Settings{Tier: "db-custom-1-3840"},
	}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.Databases.Insert(project, target, &sqladmin.Database{Name: "app"}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.Instances.Import(project, target, importReq(&sqladmin.ImportContext{
		FileType: "SQL", Uri: "gs://bkt/app.sql", Database: "app",
	})).
		Context(ctx).
		Do()
	e.mustWait(t, op, err)

	admin := "root"
	if strings.HasPrefix(tc.version, "POSTGRES") {
		admin = "postgres"
	}

	imp := openDB(t, tc.version, e.endpoint(t, target), admin, "root-pw")
	assert.Equal(t, "alpha", scalar(t, imp, "SELECT name FROM items WHERE id = 1"))
}

// assertNoContainers fails the test if containers, volumes or networks of the named instances outlive it.
func assertNoContainers(t *testing.T, instances ...string) {
	t.Helper()

	for _, in := range instances {
		assertNoDockerObjects(t, "container", "ps", "-aq", "--filter", "name=gopherstack-gcpsql-"+in+"-")
		assertNoDockerObjects(t, "volume", "ls", "-q", "--filter", "name=gopherstack-gcpsql-data-"+project+"-"+in)
		assertNoDockerObjects(t, "volume", "ls", "-q", "--filter", "name=gopherstack-gcpsql-pitr-"+project+"-"+in)
		assertNoDockerObjects(t, "network", "ls", "-q", "--filter", "name=gopherstack-gcpsql-net-"+in)
	}
}

func assertNoDockerObjects(t *testing.T, what string, args ...string) {
	t.Helper()

	t.Cleanup(func() {
		out, err := exec.CommandContext(context.Background(), "docker", args...).Output()
		if err == nil {
			assert.Empty(t, strings.TrimSpace(string(out)), what+"s left behind")
		}
	})
}
