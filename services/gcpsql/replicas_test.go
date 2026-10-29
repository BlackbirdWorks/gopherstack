package gcpsql_test

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	sqladmin "google.golang.org/api/sqladmin/v1"

	"github.com/blackbirdworks/gopherstack/services/gcpsql"
)

func (l *fakeLauncher) engineFor(name string) *fakeEngine {
	l.mu.Lock()
	defer l.mu.Unlock()

	for i, s := range l.specs {
		if s.ID == name {
			return l.engines[i]
		}
	}

	return nil
}

func (l *fakeLauncher) specFor(name string) gcpsql.LaunchSpec {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, s := range l.specs {
		if s.ID == name {
			return s
		}
	}

	return gcpsql.LaunchSpec{}
}

func binlogSettings() *sqladmin.Settings {
	return &sqladmin.Settings{BackupConfiguration: &sqladmin.BackupConfiguration{Enabled: true, BinaryLogEnabled: true}}
}

func (e *env) createReplica(t *testing.T, name, master, version string) *sqladmin.DatabaseInstance {
	t.Helper()

	op, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: name, DatabaseVersion: version, MasterInstanceName: master,
		Settings: &sqladmin.Settings{Tier: "db-custom-1-3840"},
	}).Context(t.Context()).Do()
	done := e.mustWait(t, op, err)
	assert.Equal(t, "CREATE_REPLICA", done.OperationType)

	inst, err := e.svc.Instances.Get(project, name).Context(t.Context()).Do()
	require.NoError(t, err)

	return inst
}

func statusOf(t *testing.T, err error) int {
	t.Helper()

	var ge *googleapi.Error

	require.ErrorAs(t, err, &ge)

	return ge.Code
}

func TestReplicaCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		prim    *sqladmin.Settings
		master  string
	}{
		{name: "postgres bare name", version: "POSTGRES_16", master: "prim"},
		{name: "mysql project prefix", version: "MYSQL_8_4", prim: binlogSettings(), master: project + ":prim"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := &fakeLauncher{}
			e := newEnvWith(t, l)
			e.createInstance(t, "prim", tt.version, tt.prim)

			rep := e.createReplica(t, "rep", tt.master, tt.version)
			assert.Equal(t, "READ_REPLICA_INSTANCE", rep.InstanceType)
			assert.Equal(t, project+":prim", rep.MasterInstanceName)
			assert.Equal(t, "us-central1", rep.Region, "region follows the primary")
			assert.Equal(t, "db-custom-1-3840", rep.Settings.Tier)

			prim, err := e.svc.Instances.Get(project, "prim").Context(t.Context()).Do()
			require.NoError(t, err)
			assert.Equal(t, []string{"rep"}, prim.ReplicaNames)
			assert.Equal(t, "CLOUD_SQL_INSTANCE", prim.InstanceType)

			spec := l.specFor("rep")
			require.NotNil(t, spec.Replica, "replica engine streams from the primary")
			assert.Equal(t, "repl-pw", spec.Replica.Password)

			eng := l.engineFor("rep").snapshot()
			assert.Equal(t, 1, eng.attached)
			assert.Equal(t, spec.Replica.Host, eng.replSrc.Host)
		})
	}
}

func TestReplicaCreateErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantMsg string
		in      sqladmin.DatabaseInstance
		want    int
	}{
		{
			name: "missing primary",
			in:   sqladmin.DatabaseInstance{MasterInstanceName: "ghost"},
			want: http.StatusNotFound,
		},
		{
			name: "mysql without binlog",
			in:   sqladmin.DatabaseInstance{MasterInstanceName: "my", DatabaseVersion: "MYSQL_8_0"},
			want: http.StatusBadRequest, wantMsg: "Binary logging",
		},
		{
			name: "version mismatch",
			in:   sqladmin.DatabaseInstance{MasterInstanceName: "pg", DatabaseVersion: "POSTGRES_15"},
			want: http.StatusBadRequest, wantMsg: "must match",
		},
		{
			name: "other project", in: sqladmin.DatabaseInstance{MasterInstanceName: "elsewhere:pg"},
			want: http.StatusBadRequest, wantMsg: "another project",
		},
		{
			name: "replica of replica", in: sqladmin.DatabaseInstance{MasterInstanceName: "pgrep"},
			want: http.StatusBadRequest, wantMsg: "Replicas of replicas",
		},
		{
			name: "wrong instance type",
			in:   sqladmin.DatabaseInstance{MasterInstanceName: "pg", InstanceType: "CLOUD_SQL_INSTANCE"},
			want: http.StatusBadRequest, wantMsg: "instanceType",
		},
		{name: "replica type without primary", in: sqladmin.DatabaseInstance{InstanceType: "READ_REPLICA_INSTANCE"},
			want: http.StatusBadRequest, wantMsg: "masterInstanceName"},
	}

	e := newEnv(t)
	e.createInstance(t, "pg", "POSTGRES_16", nil)
	e.createInstance(t, "my", "MYSQL_8_0", nil)
	e.createReplica(t, "pgrep", "pg", "POSTGRES_16")

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := tt.in
			in.Name = "bad" + strconv.Itoa(i)
			in.Settings = &sqladmin.Settings{Tier: "db-custom-1-3840"}

			_, err := e.svc.Instances.Insert(project, &in).Context(t.Context()).Do()
			require.Error(t, err)
			assert.Equal(t, tt.want, statusOf(t, err))

			if tt.wantMsg != "" {
				assert.Contains(t, err.Error(), tt.wantMsg)
			}
		})
	}
}

func TestReplicaLifecycle(t *testing.T) {
	t.Parallel()

	l := &fakeLauncher{}
	e := newEnvWith(t, l)
	ctx := t.Context()

	e.createInstance(t, "prim", "POSTGRES_16", nil)
	e.createReplica(t, "rep", "prim", "POSTGRES_16")

	_, err := e.svc.Instances.Delete(project, "prim").Context(ctx).Do()
	require.Error(t, err)
	assert.Equal(t, http.StatusBadRequest, statusOf(t, err), "a primary with replicas cannot be deleted")

	_, err = e.svc.Instances.StartReplica(project, "prim").Context(ctx).Do()
	require.Error(t, err)
	assert.Equal(t, http.StatusBadRequest, statusOf(t, err), "a primary has no replication to start")

	op, err := e.svc.Instances.StopReplica(project, "rep").Context(ctx).Do()
	done := e.mustWait(t, op, err)
	assert.Equal(t, "STOP_REPLICA", done.OperationType)

	op, err = e.svc.Instances.StartReplica(project, "rep").Context(ctx).Do()
	done = e.mustWait(t, op, err)
	assert.Equal(t, "START_REPLICA", done.OperationType)
	assert.Equal(t, []bool{false, true}, l.engineFor("rep").snapshot().replCalls)

	op, err = e.svc.Instances.PromoteReplica(project, "rep").Context(ctx).Do()
	done = e.mustWait(t, op, err)
	assert.Equal(t, "PROMOTE_REPLICA", done.OperationType)
	assert.Equal(t, 1, l.engineFor("rep").snapshot().promoted)

	rep, err := e.svc.Instances.Get(project, "rep").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "CLOUD_SQL_INSTANCE", rep.InstanceType)
	assert.Empty(t, rep.MasterInstanceName)

	prim, err := e.svc.Instances.Get(project, "prim").Context(ctx).Do()
	require.NoError(t, err)
	assert.Empty(t, prim.ReplicaNames, "the promoted replica leaves the primary")

	_, err = e.svc.Instances.PromoteReplica(project, "rep").Context(ctx).Do()
	require.Error(t, err)
	assert.Equal(t, http.StatusBadRequest, statusOf(t, err), "a standalone instance cannot be promoted")

	op, err = e.svc.Instances.Delete(project, "prim").Context(ctx).Do()
	e.mustWait(t, op, err)
	assert.True(t, l.engineFor("prim").snapshot().purged, "delete purges the engine volumes")
}

func TestReplicaDeleteUnlinks(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "prim", "POSTGRES_16", nil)
	e.createReplica(t, "rep", "prim", "POSTGRES_16")

	op, err := e.svc.Instances.Delete(project, "rep").Context(t.Context()).Do()
	e.mustWait(t, op, err)

	prim, err := e.svc.Instances.Get(project, "prim").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Empty(t, prim.ReplicaNames)
}

func TestReplicaRecordsFollowPrimary(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()

	e.createInstance(t, "prim", "POSTGRES_16", nil)
	e.createReplica(t, "rep", "prim", "POSTGRES_16")

	op, err := e.svc.Databases.Insert(project, "prim", &sqladmin.Database{Name: "app"}).Context(ctx).Do()
	e.mustWait(t, op, err)

	require.Eventually(t, func() bool {
		dbs, lerr := e.svc.Databases.List(project, "rep").Context(ctx).Do()

		return lerr == nil && len(dbs.Items) > 0 && func() bool {
			for _, d := range dbs.Items {
				if d.Name == "app" {
					return true
				}
			}

			return false
		}()
	}, e.timeout, 5*time.Millisecond, "databases created on the primary appear on the replica")

	_, err = e.svc.Databases.Insert(project, "rep", &sqladmin.Database{Name: "nope"}).Context(ctx).Do()
	require.Error(t, err)
	assert.Equal(t, http.StatusBadRequest, statusOf(t, err), "replicas are read-only")

	_, err = e.svc.Users.Insert(project, "rep", &sqladmin.User{Name: "u", Password: "p"}).Context(ctx).Do()
	require.Error(t, err)
	assert.Equal(t, http.StatusBadRequest, statusOf(t, err))
}

func TestReplicaOfStateOnlyPrimary(t *testing.T) {
	t.Parallel()

	e := newEnvWith(t, nil)
	e.createInstance(t, "prim", "POSTGRES_16", nil)
	rep := e.createReplica(t, "rep", "prim", "POSTGRES_16")
	assert.Equal(t, "RUNNABLE", rep.State)
	assert.Equal(t, "READ_REPLICA_INSTANCE", rep.InstanceType)

	op, err := e.svc.Instances.PromoteReplica(project, "rep").Context(t.Context()).Do()
	e.mustWait(t, op, err)
}
