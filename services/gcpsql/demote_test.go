package gcpsql_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"

	"github.com/blackbirdworks/gopherstack/services/gcpsql"
)

func (e *env) demote(t *testing.T, name, source string, master bool) (*sqladmin.Operation, error) {
	t.Helper()

	if master {
		return e.svc.Instances.DemoteMaster(project, name, &sqladmin.InstancesDemoteMasterRequest{
			DemoteMasterContext: &sqladmin.DemoteMasterContext{MasterInstanceName: source},
		}).Context(t.Context()).Do()
	}

	return e.svc.Instances.Demote(project, name, &sqladmin.InstancesDemoteRequest{
		DemoteContext: &sqladmin.DemoteContext{SourceRepresentativeInstanceName: source},
	}).Context(t.Context()).Do()
}

func TestDemoteMakesReadOnlyReplicaOfExternalSource(t *testing.T) {
	t.Parallel()

	for _, master := range []bool{false, true} {
		name := "demote"
		if master {
			name = "demoteMaster"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			ctx := t.Context()
			e.createInstance(t, "dm", "POSTGRES_16", nil)

			op, err := e.demote(t, "dm", "onprem-src", master)
			done := e.settle(t, op, err)
			require.Nil(t, done.Error)
			assert.Equal(t, "DEMOTE_MASTER", done.OperationType)

			inst, err := e.svc.Instances.Get(project, "dm").Context(ctx).Do()
			require.NoError(t, err)
			assert.Equal(t, "READ_REPLICA_INSTANCE", inst.InstanceType)
			assert.Equal(t, project+":onprem-src", inst.MasterInstanceName)
			assert.Equal(t, "RUNNABLE", inst.State)
			assert.Equal(t, []bool{true}, e.launcher.engine(0).readOnlyCalls())

			_, err = e.svc.Databases.Insert(project, "dm", &sqladmin.Database{Name: "x"}).Context(ctx).Do()
			assert.Equal(t, 400, apiStatus(err), "a demoted instance rejects writes")

			_, err = e.svc.Instances.StopReplica(project, "dm").Context(ctx).Do()
			assert.Equal(t, 400, apiStatus(err), "there is no replication stream to stop")

			op, err = e.svc.Instances.PromoteReplica(project, "dm").Context(ctx).Do()
			done = e.settle(t, op, err)
			require.Nil(t, done.Error)

			inst, err = e.svc.Instances.Get(project, "dm").Context(ctx).Do()
			require.NoError(t, err)
			assert.Equal(t, "CLOUD_SQL_INSTANCE", inst.InstanceType)
			assert.Empty(t, inst.MasterInstanceName)
			assert.Equal(t, []bool{true, false}, e.launcher.engine(0).readOnlyCalls())
			assert.Zero(t, e.launcher.engine(0).snapshot().promoted, "a primary engine is not in recovery")

			op, err = e.svc.Databases.Insert(project, "dm", &sqladmin.Database{Name: "x"}).Context(ctx).Do()
			e.mustWait(t, op, err)
		})
	}
}

func TestDemoteValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		setup  func(t *testing.T, e *env)
		target string
		source string
		status int
	}{
		{name: "bad source", target: "dm", source: "Not_Valid", status: 400},
		{name: "source is self", target: "dm", source: "dm", status: 400},
		{name: "source is a cloud sql instance", target: "dm", source: "other", status: 400},
		{name: "unknown instance", target: "ghost", source: "ext", status: 404},
		{
			name: "has replicas", target: "dm", source: "ext", status: 400,
			setup: func(t *testing.T, e *env) {
				t.Helper()

				op, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
					Name: "rep", MasterInstanceName: "dm", Region: "us-central1",
				}).Context(t.Context()).Do()
				e.mustWait(t, op, err)
			},
		},
		{
			name: "is a replica", target: "rep", source: "ext", status: 400,
			setup: func(t *testing.T, e *env) {
				t.Helper()

				op, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
					Name: "rep", MasterInstanceName: "dm", Region: "us-central1",
				}).Context(t.Context()).Do()
				e.mustWait(t, op, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createInstance(t, "dm", "POSTGRES_16", nil)
			e.createInstance(t, "other", "POSTGRES_16", nil)

			if tt.setup != nil {
				tt.setup(t, e)
			}

			_, err := e.demote(t, tt.target, tt.source, false)
			assert.Equal(t, tt.status, apiStatus(err))
		})
	}

	e := newEnv(t)
	e.createInstance(t, "dm", "POSTGRES_16", nil)

	_, err := e.svc.Instances.Demote(project, "dm", &sqladmin.InstancesDemoteRequest{}).Context(t.Context()).Do()
	assert.Equal(t, 400, apiStatus(err), "demoteContext is required")
}

func TestDemotionSurvivesSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "dm", "POSTGRES_16", nil)

	op, err := e.demote(t, "dm", "ext", false)
	e.settle(t, op, err)

	fresh := &fakeLauncher{}
	b := gcpsql.NewInMemoryBackend()
	b.SetLauncher(fresh)
	t.Cleanup(b.Close)
	require.NoError(t, b.Restore(ctx, e.backend.Snapshot(ctx)))
	b.Drain()

	inst, err := b.GetInstance(project, "dm")
	require.NoError(t, err)
	assert.Equal(t, "RUNNABLE", inst.State)
	assert.Equal(t, "READ_REPLICA_INSTANCE", inst.InstanceType)
	assert.Nil(t, fresh.specs[0].Replica, "no engine-level replication source exists for an external demotion")
	assert.Equal(t, []bool{true}, fresh.engine(0).readOnlyCalls())
}

func TestConcurrentDemoteSerialised(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "dm", "POSTGRES_16", nil)

	var wg sync.WaitGroup

	for range 8 {
		wg.Go(func() { _, _ = e.backend.DemoteInstance(project, "dm", gcpsql.DemoteRequest{Source: "ext"}) })
	}

	wg.Wait()
	e.backend.Drain()

	ok := 0

	for _, op := range e.backend.ListOperations(project, "dm", "", 0).Data {
		if op.OperationType == "DEMOTE_MASTER" && op.Error == nil {
			ok++
		}
	}

	assert.Equal(t, 1, ok)
	assert.Equal(t, []bool{true}, e.launcher.engine(0).readOnlyCalls())
}
