package gcpsql_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"

	"github.com/blackbirdworks/gopherstack/services/gcpsql"
)

func TestSnapshotRestoreRelaunchesEngine(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "p1", "POSTGRES_16", nil)

	op, err := e.svc.Databases.Insert(project, "p1", &sqladmin.Database{Name: "app"}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.Users.Insert(project, "p1", &sqladmin.User{Name: "alice", Password: "pw"}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.BackupRuns.Insert(project, "p1", &sqladmin.BackupRun{}).Context(ctx).Do()
	e.mustWait(t, op, err)

	snap := e.backend.Snapshot(ctx)
	require.NotEmpty(t, snap)

	fresh := &fakeLauncher{}
	b := gcpsql.NewInMemoryBackend()
	b.SetLauncher(fresh)
	t.Cleanup(b.Close)

	require.NoError(t, b.Restore(ctx, snap))

	require.Eventually(t, func() bool {
		in, gerr := b.GetInstance(project, "p1")

		return gerr == nil && in.State == "RUNNABLE"
	}, 10*time.Second, 5*time.Millisecond)

	eng := fresh.engine(0).snapshot()
	assert.True(t, eng.dbs["app"])
	assert.Equal(t, "pw", eng.users["alice@"])
	assert.Equal(t, "pw-root", fresh.specs[0].Password)

	runs, err := b.ListBackupRuns(project, "p1")
	require.NoError(t, err)
	assert.Len(t, runs, 1)

	ops := b.ListOperations(project, "p1", "", 0)
	assert.Len(t, ops.Data, 4)
}

func TestRestoreRejectsGarbage(t *testing.T) {
	t.Parallel()

	b := gcpsql.NewInMemoryBackend()
	assert.Error(t, b.Restore(t.Context(), []byte("not json")))
}

func TestResetStopsEngines(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "r1", "MYSQL_8_0", nil)

	e.backend.Reset()
	assert.True(t, e.launcher.engine(0).snapshot().closed)
	assert.Empty(t, e.backend.ListInstances(project, "", 0).Data)
}
