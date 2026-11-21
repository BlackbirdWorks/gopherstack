package gcpsql_test

import (
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	sqladmin "google.golang.org/api/sqladmin/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/gcpsql"
)

var errFakeDump = errors.New("dump refused")

func (e *env) settle(t *testing.T, op *sqladmin.Operation, err error) *sqladmin.Operation {
	t.Helper()
	require.NoError(t, err)
	e.backend.Drain()

	got, gerr := e.svc.Operations.Get(project, op.Name).Context(t.Context()).Do()
	require.NoError(t, gerr)
	require.Equal(t, "DONE", got.Status)

	return got
}

func (e *env) createProjectBackup(t *testing.T, instance string, b *sqladmin.Backup) *sqladmin.Backup {
	t.Helper()

	b.Instance = instance
	op, err := e.svc.Backups.CreateBackup("projects/"+project, b).Context(t.Context()).Do()
	done := e.settle(t, op, err)
	require.Nil(t, done.Error)

	list, lerr := e.svc.Backups.ListBackups("projects/" + project).Context(t.Context()).Do()
	require.NoError(t, lerr)
	require.NotEmpty(t, list.Backups)

	return list.Backups[0]
}

func apiStatus(err error) int {
	if ge, ok := errors.AsType[*googleapi.Error](err); ok {
		return ge.Code
	}

	return 0
}

func TestProjectBackupLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "pb1", "POSTGRES_16", nil)

	op, err := e.svc.Databases.Insert(project, "pb1", &sqladmin.Database{Name: "app"}).Context(ctx).Do()
	e.mustWait(t, op, err)

	bk := e.createProjectBackup(t, "pb1", &sqladmin.Backup{Description: "before upgrade"})

	assert.Equal(t, "sql#backup", bk.Kind)
	assert.Equal(t, "ON_DEMAND", bk.Type)
	assert.Equal(t, "SUCCESSFUL", bk.State)
	assert.Equal(t, "pb1", bk.Instance)
	assert.Equal(t, "POSTGRES_16", bk.DatabaseVersion)
	assert.Equal(t, "before upgrade", bk.Description)
	assert.True(t, strings.HasPrefix(bk.Name, "projects/"+project+"/backups/"), bk.Name)
	assert.NotEmpty(t, bk.BackupInterval.EndTime)
	assert.Positive(t, bk.MaxChargeableBytes)

	got, err := e.svc.Backups.GetBackup(bk.Name).Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, bk.Name, got.Name)

	_, err = e.svc.Backups.UpdateBackup(bk.Name, &sqladmin.Backup{Description: "x"}).Context(ctx).Do()
	assert.Equal(t, 400, apiStatus(err), "only final backups are updatable")

	op, err = e.svc.Backups.DeleteBackup(bk.Name).Context(ctx).Do()
	done := e.settle(t, op, err)
	assert.Equal(t, "DELETE_BACKUP", done.OperationType)

	_, err = e.svc.Backups.GetBackup(bk.Name).Context(ctx).Do()
	assert.Equal(t, 404, apiStatus(err))
}

func TestProjectBackupCreateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		in     sqladmin.Backup
		status int
	}{
		{name: "unknown instance", in: sqladmin.Backup{Instance: "nope"}, status: 404},
		{name: "bad instance name", in: sqladmin.Backup{Instance: "Bad_Name"}, status: 400},
		{name: "ttl too long", in: sqladmin.Backup{Instance: "pv", TtlDays: 366}, status: 400},
		{
			name:   "ttl and expiry",
			in:     sqladmin.Backup{Instance: "pv", TtlDays: 2, ExpiryTime: "2099-01-01T00:00:00Z"},
			status: 400,
		},
		{
			name:   "expiry in the past",
			in:     sqladmin.Backup{Instance: "pv", ExpiryTime: "2001-01-01T00:00:00Z"},
			status: 400,
		},
		{name: "ok with ttl", in: sqladmin.Backup{Instance: "pv", TtlDays: 7}, status: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createInstance(t, "pv", "POSTGRES_16", nil)

			_, err := e.svc.Backups.CreateBackup("projects/"+project, &tt.in).Context(t.Context()).Do()
			if tt.status == 200 {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.status, apiStatus(err))
		})
	}
}

func TestProjectBackupListFilterAndPaging(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "fa", "POSTGRES_16", nil)
	e.createInstance(t, "fb", "POSTGRES_16", nil)

	for _, in := range []string{"fa", "fa", "fb"} {
		e.createProjectBackup(t, in, &sqladmin.Backup{})
	}

	parent := "projects/" + project

	tests := []struct {
		name   string
		filter string
		want   int
	}{
		{name: "all", want: 3},
		{name: "by instance", filter: "instance:fa", want: 2},
		{name: "by type", filter: "instance:fb AND type:ON_DEMAND", want: 1},
		{name: "no match", filter: "type:FINAL", want: 0},
		{name: "future start", filter: "backupInterval.startTime>=2099-01-01T00:00:00Z", want: 0},
	}

	for _, tt := range tests {
		got, err := e.svc.Backups.ListBackups(parent).Filter(tt.filter).Context(ctx).Do()
		require.NoError(t, err, tt.name)
		assert.Len(t, got.Backups, tt.want, tt.name)
	}

	_, err := e.svc.Backups.ListBackups(parent).Filter("bogus:1").Context(ctx).Do()
	assert.Equal(t, 400, apiStatus(err))

	first, err := e.svc.Backups.ListBackups(parent).PageSize(2).Context(ctx).Do()
	require.NoError(t, err)
	require.Len(t, first.Backups, 2)
	require.NotEmpty(t, first.NextPageToken)

	second, err := e.svc.Backups.ListBackups(parent).PageSize(2).PageToken(first.NextPageToken).Context(ctx).Do()
	require.NoError(t, err)
	assert.Len(t, second.Backups, 1)
	assert.Empty(t, second.NextPageToken)
}

func TestFinalBackupOutlivesInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		settings *sqladmin.Settings
		name     string
		wantDays int
		enable   bool
		wantNone bool
	}{
		{name: "query flag", enable: true, wantDays: 30},
		{
			name: "settings config",
			settings: &sqladmin.Settings{
				FinalBackupConfig: &sqladmin.FinalBackupConfig{Enabled: true, RetentionDays: 10},
			},
			wantDays: 10,
		},
		{name: "not requested", wantNone: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clock := newFakeClock()
			e := newEnv(t)
			e.backend.SetClock(clock.now)
			e.createInstance(t, "fin", "POSTGRES_16", tt.settings)

			call := e.svc.Instances.Delete(project, "fin")
			if tt.enable {
				call = call.EnableFinalBackup(true).FinalBackupDescription("bye")
			}

			op, err := call.Context(t.Context()).Do()
			e.settle(t, op, err)

			_, err = e.svc.Instances.Get(project, "fin").Context(t.Context()).Do()
			assert.Equal(t, 404, apiStatus(err))

			list, err := e.svc.Backups.ListBackups("projects/" + project).Filter("type:FINAL").Context(t.Context()).Do()
			require.NoError(t, err)

			if tt.wantNone {
				assert.Empty(t, list.Backups)

				return
			}

			require.Len(t, list.Backups, 1)

			fb := list.Backups[0]
			assert.Equal(t, "FINAL", fb.Type)
			assert.Equal(t, "fin", fb.Instance)
			assert.Equal(t, "SUCCESSFUL", fb.State)
			assert.NotEmpty(t, fb.InstanceDeletionTime)

			want := clock.now().
				Add(time.Duration(tt.wantDays) * 24 * time.Hour).
				UTC().
				Format("2006-01-02T15:04:05.000Z")
			assert.Equal(t, want, fb.ExpiryTime)

			upd, uerr := e.svc.Backups.UpdateBackup(fb.Name, &sqladmin.Backup{Description: "kept", TtlDays: 60}).
				UpdateMask("description,ttlDays").Context(t.Context()).Do()
			e.settle(t, upd, uerr)

			got, gerr := e.svc.Backups.GetBackup(fb.Name).Context(t.Context()).Do()
			require.NoError(t, gerr)
			assert.Equal(t, "kept", got.Description)
			assert.Equal(t, clock.now().Add(60*24*time.Hour).UTC().Format("2006-01-02T15:04:05.000Z"), got.ExpiryTime)

			_, uerr = e.svc.Backups.UpdateBackup(fb.Name, &sqladmin.Backup{State: "FAILED"}).
				UpdateMask("state").Context(t.Context()).Do()
			assert.Equal(t, 400, apiStatus(uerr))
		})
	}
}

func TestFinalBackupFailureKeepsInstance(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "keep", "POSTGRES_16", nil)
	e.launcher.engine(0).setFailures(errFakeDump, "")

	op, err := e.svc.Instances.Delete(project, "keep").EnableFinalBackup(true).Context(t.Context()).Do()
	require.NoError(t, err)
	e.backend.Drain()

	got, err := e.svc.Operations.Get(project, op.Name).Context(t.Context()).Do()
	require.NoError(t, err)
	require.NotNil(t, got.Error, "the delete must fail when the final backup cannot be taken")

	inst, err := e.svc.Instances.Get(project, "keep").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "RUNNABLE", inst.State)
	assert.False(t, e.launcher.engine(0).snapshot().purged)

	list, err := e.svc.Backups.ListBackups("projects/" + project).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Empty(t, list.Backups)
}

type fakeClock struct{ ns atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC).UnixNano())

	return c
}

func (c *fakeClock) now() time.Time { return time.Unix(0, c.ns.Load()).UTC() }

func (c *fakeClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

func TestProjectBackupExpiresOnBackendClock(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	e := newEnv(t)
	e.backend.SetClock(clock.now)
	e.createInstance(t, "ex", "POSTGRES_16", nil)

	bk := e.createProjectBackup(t, "ex", &sqladmin.Backup{TtlDays: 2})
	keep := e.createProjectBackup(t, "ex", &sqladmin.Backup{})

	clock.advance(36 * time.Hour)

	_, err := e.svc.Backups.GetBackup(bk.Name).Context(t.Context()).Do()
	require.NoError(t, err, "not expired yet")

	clock.advance(36 * time.Hour)

	_, err = e.svc.Backups.GetBackup(bk.Name).Context(t.Context()).Do()
	assert.Equal(t, 404, apiStatus(err), "expired backups are invisible")

	list, err := e.svc.Backups.ListBackups("projects/" + project).Context(t.Context()).Do()
	require.NoError(t, err)
	require.Len(t, list.Backups, 1)
	assert.Equal(t, keep.Name, list.Backups[0].Name)

	e.backend.RunScheduledBackups(clock.now())
	assert.Equal(t, 1, e.backend.BackupCount(project), "the sweep removes the expired record")
}

func TestProjectBackupLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  string
		count int
		bytes int64
	}{
		{name: "count", count: 2, want: "429"},
		{name: "bytes", bytes: 10, want: "FAILED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.backend.SetBackupLimits(tt.count, tt.bytes)
			e.createInstance(t, "lim", "POSTGRES_16", nil)

			if tt.count > 0 {
				for range tt.count {
					e.createProjectBackup(t, "lim", &sqladmin.Backup{})
				}

				_, err := e.svc.Backups.CreateBackup("projects/"+project, &sqladmin.Backup{Instance: "lim"}).
					Context(t.Context()).Do()
				assert.Equal(t, 429, apiStatus(err))

				return
			}

			op, err := e.svc.Backups.CreateBackup("projects/"+project, &sqladmin.Backup{Instance: "lim"}).
				Context(t.Context()).Do()
			done := e.settle(t, op, err)
			require.NotNil(t, done.Error, "an oversized dump fails the operation")

			list, lerr := e.svc.Backups.ListBackups("projects/" + project).Context(t.Context()).Do()
			require.NoError(t, lerr)
			require.Len(t, list.Backups, 1)
			assert.Equal(t, tt.want, list.Backups[0].State)
			assert.Zero(t, list.Backups[0].MaxChargeableBytes)
		})
	}
}

func TestProjectBackupLimitIsSerialised(t *testing.T) {
	t.Parallel()

	const limit, workers, each = 20, 8, 5

	e := newEnv(t)
	e.backend.SetBackupLimits(limit, 0)
	e.createInstance(t, "race", "POSTGRES_16", nil)

	var (
		wg       sync.WaitGroup
		accepted atomic.Int32
	)

	for range workers {
		wg.Go(func() {
			for range each {
				_, err := e.backend.CreateBackup(project, gcpsql.BackupInput{Instance: "race"})
				if err == nil {
					accepted.Add(1)
				}
			}
		})
	}

	wg.Wait()
	e.backend.Drain()

	assert.Equal(t, int32(limit), accepted.Load())
	assert.Equal(t, limit, e.backend.BackupCount(project))
}

func TestRestoreFromProjectBackup(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "src", "POSTGRES_16", nil)
	e.createInstance(t, "other", "POSTGRES_15", nil)
	e.createInstance(t, "dst", "POSTGRES_16", nil)

	op, err := e.svc.Databases.Insert(project, "src", &sqladmin.Database{Name: "app"}).Context(ctx).Do()
	e.mustWait(t, op, err)

	bk := e.createProjectBackup(t, "src", &sqladmin.Backup{})

	op, err = e.svc.Instances.Delete(project, "src").Context(ctx).Do()
	e.settle(t, op, err)

	op, err = e.svc.Instances.RestoreBackup(project, "dst", &sqladmin.InstancesRestoreBackupRequest{Backup: bk.Name}).
		Context(ctx).Do()
	done := e.settle(t, op, err)
	require.Nil(t, done.Error)
	assert.Equal(t, "RESTORE_VOLUME", done.OperationType)

	restored := e.launcher.engine(2).snapshot().restored
	require.Len(t, restored, 1)
	assert.Contains(t, restored[0], "-- dump of app")

	tests := []struct {
		name   string
		target string
		req    sqladmin.InstancesRestoreBackupRequest
		status int
	}{
		{name: "unknown backup", target: "dst", status: 404,
			req: sqladmin.InstancesRestoreBackupRequest{Backup: "projects/" + project + "/backups/999"}},
		{name: "malformed name", target: "dst", status: 400,
			req: sqladmin.InstancesRestoreBackupRequest{Backup: "backups/1"}},
		{name: "other project", target: "dst", status: 400,
			req: sqladmin.InstancesRestoreBackupRequest{Backup: "projects/elsewhere/backups/" + idOf(bk.Name)}},
		{name: "version mismatch", target: "other", status: 400,
			req: sqladmin.InstancesRestoreBackupRequest{Backup: bk.Name}},
		{
			name:   "dr backup",
			target: "dst",
			status: 400,
			req: sqladmin.InstancesRestoreBackupRequest{
				BackupdrBackup: "projects/p/locations/l/backupVaults/v/dataSources/d/backups/b",
			},
		},
		{
			name:   "two sources",
			target: "dst",
			status: 400,
			req: sqladmin.InstancesRestoreBackupRequest{
				Backup:               bk.Name,
				RestoreBackupContext: &sqladmin.RestoreBackupContext{BackupRunId: 1},
			},
		},
		{name: "no source", target: "dst", status: 400},
	}

	for _, tt := range tests {
		_, err = e.svc.Instances.RestoreBackup(project, tt.target, &tt.req).Context(ctx).Do()
		assert.Equal(t, tt.status, apiStatus(err), tt.name)
	}
}

func idOf(name string) string { return name[strings.LastIndex(name, "/")+1:] }

func TestRestoreFromTamperedBackupTouchesNothing(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "tsrc", "POSTGRES_16", nil)
	bk := e.createProjectBackup(t, "tsrc", &sqladmin.Backup{})

	var snap map[string]any

	require.NoError(t, json.Unmarshal(e.backend.Snapshot(ctx), &snap, jsonx.V1()))

	backups, _ := snap["projectBackups"].(map[string]any)
	require.Len(t, backups, 1)

	for _, v := range backups {
		v.(map[string]any)["dump"] = base64.StdEncoding.EncodeToString([]byte("DROP DATABASE postgres;"))
	}

	tampered, err := json.Marshal(snap, jsonx.V1())
	require.NoError(t, err)

	fresh := &fakeLauncher{}
	b := gcpsql.NewInMemoryBackend()
	b.SetLauncher(fresh)
	t.Cleanup(b.Close)
	require.NoError(t, b.Restore(ctx, tampered))
	b.Drain()

	op, err := b.RestoreProjectBackup(project, "tsrc", bk.Name)
	require.NoError(t, err)
	b.Drain()

	got, err := b.GetOperation(project, op.Name)
	require.NoError(t, err)
	require.NotNil(t, got.Error)
	assert.Contains(t, got.Error.Errors[0].Message, "SHA-256")
	assert.Empty(t, fresh.engine(0).snapshot().restored, "the engine must not be touched")
}

func TestFailedRestoreRollsBack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantState    string
		failRollback bool
	}{
		{name: "rolled back", wantState: "RUNNABLE"},
		{name: "rollback fails", failRollback: true, wantState: "FAILED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			ctx := t.Context()
			e.createInstance(t, "rb", "POSTGRES_16", nil)

			for _, db := range []string{"keepme", "poison"} {
				op, err := e.svc.Databases.Insert(project, "rb", &sqladmin.Database{Name: db}).Context(ctx).Do()
				e.mustWait(t, op, err)
			}

			bk := e.createProjectBackup(t, "rb", &sqladmin.Backup{})

			op, err := e.svc.Databases.Delete(project, "rb", "poison").Context(ctx).Do()
			e.mustWait(t, op, err)

			eng := e.launcher.engine(0)
			fail := "-- dump of poison"

			if tt.failRollback {
				fail = "-- dump of keepme"
			}

			eng.setFailures(nil, fail)

			op, err = e.svc.Instances.RestoreBackup(project, "rb", &sqladmin.InstancesRestoreBackupRequest{Backup: bk.Name}).
				Context(ctx).
				Do()
			require.NoError(t, err)
			e.backend.Drain()

			got, err := e.svc.Operations.Get(project, op.Name).Context(ctx).Do()
			require.NoError(t, err)
			require.NotNil(t, got.Error)

			restored := eng.snapshot().restored
			require.Len(t, restored, 2, "the failed load and the rollback to the pre-restore state")
			assert.Contains(t, restored[0], "-- dump of poison")
			assert.NotContains(t, restored[1], "-- dump of poison")

			inst, err := e.svc.Instances.Get(project, "rb").Context(ctx).Do()
			require.NoError(t, err)
			assert.Equal(t, tt.wantState, inst.State)
		})
	}
}

func TestProjectBackupSurvivesSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "ps", "POSTGRES_16", nil)

	bk := e.createProjectBackup(t, "ps", &sqladmin.Backup{Description: "persist me", TtlDays: 5})

	fresh := &fakeLauncher{}
	b := gcpsql.NewInMemoryBackend()
	b.SetLauncher(fresh)
	t.Cleanup(b.Close)
	require.NoError(t, b.Restore(ctx, e.backend.Snapshot(ctx)))
	b.Drain()

	_, id, ok := gcpsql.ParseBackupName(bk.Name)
	require.True(t, ok)

	got, err := b.GetBackup(project, id)
	require.NoError(t, err)
	assert.Equal(t, "persist me", got.Description)
	assert.Equal(t, bk.ExpiryTime, got.ExpiryTime)
	assert.Equal(t, strconv.FormatInt(bk.MaxChargeableBytes, 10), got.MaxChargeableBytes)

	op, err := b.RestoreProjectBackup(project, "ps", bk.Name)
	require.NoError(t, err)
	b.Drain()

	done, err := b.GetOperation(project, op.Name)
	require.NoError(t, err)
	assert.Nil(t, done.Error)
	assert.Len(t, fresh.engine(0).snapshot().restored, 1)
}
