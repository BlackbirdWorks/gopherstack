package ocimysql_test

import (
	"errors"
	"testing"

	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errDockerExploded = errors.New("docker exploded")

func newEngineEnv(t *testing.T) (*env, *fakeLauncher) {
	t.Helper()

	l := newFakeLauncher()

	return newEnv(t, envOpts{launcher: l}), l
}

func (e *env) mkEngineDB(t *testing.T, name string, mod func(*my.CreateDbSystemDetails)) my.DbSystem {
	t.Helper()

	d := dbDetails(name)
	if mod != nil {
		mod(&d)
	}

	resp := e.createDB(t, d)

	return e.awaitState(t, resp.Id, my.DbSystemLifecycleStateActive)
}

func TestEngineLifecycle(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	ctx := t.Context()

	created := e.createDB(t, dbDetails("eng"))
	assert.Equal(t, my.DbSystemLifecycleStateCreating, created.LifecycleState)

	db := e.awaitState(t, created.Id, my.DbSystemLifecycleStateActive)
	assert.Equal(t, "127.0.0.1", *db.IpAddress, "the engine publishes on loopback")
	assert.Greater(t, *db.Port, 40000)
	assert.Equal(t, *db.IpAddress, *db.Endpoints[0].IpAddress)
	assert.Equal(t, *db.Port, *db.Endpoints[0].Port)

	wr, err := e.wr.GetWorkRequest(ctx, my.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, my.WorkRequestOperationStatusSucceeded, wr.Status)

	inst := l.inst(*db.Id)
	require.NotNil(t, inst)
	assert.Equal(t, adminPass, inst.admins[adminUser])
	assert.True(t, l.specs[0].Fresh)
	assert.Equal(t, "mysql:8.4", l.specs[0].Image)
	assert.NotEmpty(t, l.specs[0].RootSecret)

	_, err = e.db.StopDbSystem(ctx, my.StopDbSystemRequest{
		DbSystemId: db.Id, ShutdownType: my.InnoDbShutdownModeFast,
	})
	require.NoError(t, err)

	stopped := e.awaitState(t, db.Id, my.DbSystemLifecycleStateInactive)
	assert.Equal(t, my.DbSystemEndpointStatusInactive, stopped.Endpoints[0].Status)

	l.mu.Lock()
	assert.True(t, inst.stopped)
	l.mu.Unlock()

	_, err = e.db.StartDbSystem(ctx, my.StartDbSystemRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	e.awaitState(t, db.Id, my.DbSystemLifecycleStateActive)

	l.mu.Lock()
	assert.False(t, inst.stopped)
	assert.Equal(t, 1, inst.starts)
	assert.Len(t, l.specs, 1, "stop and start reuse the container")
	l.mu.Unlock()

	_, err = e.db.RestartDbSystem(ctx, my.RestartDbSystemRequest{
		DbSystemId:   db.Id,
		ShutdownType: my.InnoDbShutdownModeImmediate,
	})
	require.NoError(t, err)
	e.awaitState(t, db.Id, my.DbSystemLifecycleStateActive)

	_, err = e.db.DeleteDbSystem(ctx, my.DeleteDbSystemRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	e.awaitState(t, db.Id, my.DbSystemLifecycleStateDeleted)

	l.mu.Lock()
	assert.True(t, inst.closed)
	assert.True(t, inst.purged, "delete removes the data volume")
	l.mu.Unlock()
}

func TestEngineLaunchFailure(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	l.failLaunch = errDockerExploded

	created := e.createDB(t, dbDetails("bad"))
	db := e.awaitState(t, created.Id, my.DbSystemLifecycleStateFailed)
	assert.Contains(t, *db.LifecycleDetails, "docker exploded")

	wr, err := e.wr.GetWorkRequest(t.Context(), my.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, my.WorkRequestOperationStatusFailed, wr.Status)

	errs, err := e.wr.ListWorkRequestErrors(
		t.Context(),
		my.ListWorkRequestErrorsRequest{WorkRequestId: created.OpcWorkRequestId},
	)
	require.NoError(t, err)
	require.Len(t, errs.Items, 1)
	assert.Contains(t, *errs.Items[0].Message, "docker exploded")

	logs, err := e.wr.ListWorkRequestLogs(
		t.Context(),
		my.ListWorkRequestLogsRequest{WorkRequestId: created.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Len(t, logs.Items, 2)

	_, err = e.db.DeleteDbSystem(t.Context(), my.DeleteDbSystemRequest{DbSystemId: db.Id})
	require.NoError(t, err, "FAILED DB systems can be deleted")
	e.awaitState(t, db.Id, my.DbSystemLifecycleStateDeleted)

	l.mu.Lock()
	assert.Contains(t, l.removed, *db.Id, "a never-launched DB system still has its volume removed")
	l.mu.Unlock()
}

func TestEngineBackupAndRestore(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	ctx := t.Context()

	src := e.mkEngineDB(t, "src", nil)
	l.addTable(*src.Id, testTable)
	l.addTable(*src.Id, "customers")

	resp, err := e.bk.CreateBackup(ctx, my.CreateBackupRequest{
		DbSystemId: src.Id, DisplayName: new("b1"),
	})
	require.NoError(t, err)
	assert.Equal(t, my.BackupLifecycleStateCreating, resp.LifecycleState)

	bk := e.awaitBackup(t, resp.Id, my.BackupLifecycleStateActive)
	assert.GreaterOrEqual(t, *bk.BackupSizeInGBs, 1)

	l.addTable(*src.Id, "after_backup")

	dst := e.mkEngineDB(t, "dst", func(d *my.CreateDbSystemDetails) {
		d.AdminUsername, d.AdminPassword = nil, nil
		d.Source = my.CreateDbSystemSourceFromBackupDetails{BackupId: bk.Id}
	})

	inst := l.inst(*dst.Id)
	require.NotNil(t, inst)

	l.mu.Lock()
	assert.Equal(
		t,
		[]string{testTable, "customers"},
		inst.tables,
		"the restore carries the data of the backup, not later writes",
	)
	assert.Equal(t, adminPass, inst.admins[adminUser], "admin credentials follow the backup")
	l.mu.Unlock()

	t.Run("backup of a stopped system is refused", func(t *testing.T) {
		t.Parallel()

		_, serr := e.db.StopDbSystem(ctx, my.StopDbSystemRequest{
			DbSystemId: dst.Id, ShutdownType: my.InnoDbShutdownModeFast,
		})
		require.NoError(t, serr)
		e.awaitState(t, dst.Id, my.DbSystemLifecycleStateInactive)

		_, serr = e.bk.CreateBackup(
			ctx,
			my.CreateBackupRequest{DbSystemId: dst.Id},
		)
		assert.Equal(t, 409, svcErr(t, serr).GetHTTPStatusCode())
	})
}

func TestEngineFinalBackupOnDelete(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	ctx := t.Context()

	db := e.mkEngineDB(t, "fin", func(d *my.CreateDbSystemDetails) {
		d.DeletionPolicy = &my.CreateDeletionPolicyDetails{
			FinalBackup: my.CreateDeletionPolicyDetailsFinalBackupRequireFinalBackup,
		}
	})
	l.addTable(*db.Id, testTable)

	_, err := e.db.StopDbSystem(ctx, my.StopDbSystemRequest{
		DbSystemId: db.Id, ShutdownType: my.InnoDbShutdownModeFast,
	})
	require.NoError(t, err)
	e.awaitState(t, db.Id, my.DbSystemLifecycleStateInactive)

	_, err = e.db.DeleteDbSystem(ctx, my.DeleteDbSystemRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	e.awaitState(t, db.Id, my.DbSystemLifecycleStateDeleted)

	lst, err := e.bk.ListBackups(ctx, my.ListBackupsRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.Len(t, lst.Items, 1)
	assert.Equal(t, my.BackupLifecycleStateActive, lst.Items[0].LifecycleState)

	restored := e.mkEngineDB(t, "back", func(d *my.CreateDbSystemDetails) {
		d.Source = my.CreateDbSystemSourceFromBackupDetails{BackupId: lst.Items[0].Id}
	})

	l.mu.Lock()
	assert.Equal(
		t,
		[]string{testTable},
		l.insts[*restored.Id].tables,
		"the final backup is restorable after the system is gone",
	)
	l.mu.Unlock()
}

func TestEngineUpdate(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	ctx := t.Context()
	db := e.mkEngineDB(t, "upd", nil)
	l.addTable(*db.Id, testTable)

	_, err := e.db.UpdateDbSystem(ctx, my.UpdateDbSystemRequest{
		DbSystemId: db.Id, AdminPassword: new("N3wPassw0rd#"),
	})
	require.NoError(t, err)
	e.awaitState(t, db.Id, my.DbSystemLifecycleStateActive)

	l.mu.Lock()
	assert.Equal(t, "N3wPassw0rd#", l.insts[*db.Id].admins[adminUser])
	l.mu.Unlock()

	_, err = e.db.UpdateDbSystem(ctx, my.UpdateDbSystemRequest{
		DbSystemId: db.Id, MysqlVersion: new("9.4.0"),
	})
	require.NoError(t, err)

	up := e.awaitState(t, db.Id, my.DbSystemLifecycleStateActive)
	assert.Equal(t, "9.4.0", *up.MysqlVersion)

	l.mu.Lock()
	defer l.mu.Unlock()

	require.Len(t, l.specs, 2)
	assert.False(t, l.specs[1].Fresh, "an in-place upgrade keeps the data volume")
	assert.Equal(t, "mysql:9", l.specs[1].Image)
	assert.Equal(t, l.specs[0].RootSecret, l.specs[1].RootSecret)
}

func TestEngineSnapshotRestoreRelaunches(t *testing.T) {
	t.Parallel()

	e, _ := newEngineEnv(t)
	db := e.mkEngineDB(t, "persist", nil)

	stopped := e.mkEngineDB(t, "parked", nil)
	_, err := e.db.StopDbSystem(t.Context(), my.StopDbSystemRequest{
		DbSystemId: stopped.Id, ShutdownType: my.InnoDbShutdownModeFast,
	})
	require.NoError(t, err)
	e.awaitState(t, stopped.Id, my.DbSystemLifecycleStateInactive)

	snap := e.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	l2 := newFakeLauncher()
	r := newEnv(t, envOpts{launcher: l2})
	require.NoError(t, r.backend.Restore(t.Context(), snap))

	got := r.awaitState(t, db.Id, my.DbSystemLifecycleStateActive)
	assert.Equal(t, "127.0.0.1", *got.IpAddress)

	l2.mu.Lock()
	require.Len(t, l2.specs, 1, "only the running system is relaunched")
	assert.False(t, l2.specs[0].Fresh, "restore reuses the data volume")
	assert.Equal(t, *db.Id, l2.specs[0].ID)
	l2.mu.Unlock()

	assert.Equal(t, my.DbSystemLifecycleStateInactive, r.getDB(t, stopped.Id).LifecycleState)

	_, err = r.db.StartDbSystem(t.Context(), my.StartDbSystemRequest{DbSystemId: stopped.Id})
	require.NoError(t, err)
	r.awaitState(t, stopped.Id, my.DbSystemLifecycleStateActive)

	l2.mu.Lock()
	assert.Len(t, l2.specs, 2, "starting a system with no server launches one on its volume")
	l2.mu.Unlock()
}

func TestResetClosesEngineServers(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	db := e.mkEngineDB(t, "reset", nil)
	inst := l.inst(*db.Id)

	e.backend.Reset()

	l.mu.Lock()
	defer l.mu.Unlock()

	assert.True(t, inst.closed)
	assert.True(t, inst.purged)
}
