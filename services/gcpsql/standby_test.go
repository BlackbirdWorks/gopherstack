package gcpsql_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"

	"github.com/blackbirdworks/gopherstack/services/gcpsql"
)

func regionalSettings() *sqladmin.Settings {
	return &sqladmin.Settings{AvailabilityType: "REGIONAL"}
}

func (l *fakeLauncher) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return len(l.engines)
}

func TestStandbyLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		avail     string
		version   string
		wantEng   int
		wantStdby bool
	}{
		{"regional postgres", "REGIONAL", "POSTGRES_16", 2, true},
		{"regional mysql", "REGIONAL", "MYSQL_8_4", 2, true},
		{"zonal", "ZONAL", "POSTGRES_16", 1, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createInstance(t, "ha", tt.version, &sqladmin.Settings{AvailabilityType: tt.avail})

			require.Equal(t, tt.wantEng, e.launcher.count())

			if !tt.wantStdby {
				return
			}

			spec := e.launcher.specs[1]
			assert.True(t, spec.Standby)
			assert.Equal(t, "b", spec.Slot)
			require.NotNil(t, spec.Replica)
			assert.NotEqual(t, e.launcher.specs[0].ServerID, spec.ServerID)
			assert.Equal(t, 1, e.launcher.engine(1).snapshot().attached)
		})
	}
}

func TestStandbyFollowsAvailability(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "ha", "POSTGRES_16", nil)
	require.Equal(t, 1, e.launcher.count())

	op, err := e.svc.Instances.Patch(project, "ha", &sqladmin.DatabaseInstance{Settings: regionalSettings()}).
		Context(t.Context()).Do()
	e.mustWait(t, op, err)
	require.Equal(t, 2, e.launcher.count(), "REGIONAL starts a standby")

	op, err = e.svc.Instances.Patch(project, "ha", &sqladmin.DatabaseInstance{Settings: &sqladmin.Settings{
		AvailabilityType: "ZONAL",
	}}).Context(t.Context()).Do()
	e.mustWait(t, op, err)
	assert.True(t, e.launcher.engine(1).snapshot().purged, "ZONAL removes the standby")
}

func TestStandbyRemovedWithInstance(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "ha", "MYSQL_8_4", regionalSettings())

	op, err := e.svc.Instances.Delete(project, "ha").Context(t.Context()).Do()
	e.mustWait(t, op, err)

	assert.True(t, e.launcher.engine(0).snapshot().purged)
	assert.True(t, e.launcher.engine(1).snapshot().purged)
}

func TestFailoverPromotesStandby(t *testing.T) {
	t.Parallel()

	for _, version := range []string{"POSTGRES_16", "MYSQL_8_4"} {
		t.Run(version, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			before := e.createInstance(t, "ha", version, regionalSettings())

			cur, err := e.backend.GetInstance(project, "ha")
			require.NoError(t, err)

			beforeEP := cur.GopherstackEndpoint

			op, err := e.svc.Instances.Failover(project, "ha", &sqladmin.InstancesFailoverRequest{
				FailoverContext: &sqladmin.FailoverContext{SettingsVersion: before.Settings.SettingsVersion},
			}).Context(t.Context()).Do()
			e.mustWait(t, op, err)

			oldEng, standby := e.launcher.engine(0).snapshot(), e.launcher.engine(1).snapshot()
			assert.Equal(t, 1, e.launcher.engine(0).quiesced)
			assert.True(t, oldEng.purged, "the former primary is removed")
			assert.Equal(
				t,
				[]string{"pos-40000"},
				e.launcher.engine(1).waited,
				"standby waited for the primary's position",
			)
			assert.Equal(t, 1, standby.promoted)
			assert.Equal(t, 1, e.launcher.engine(0).sealed, "the old primary is shut down cleanly first")
			assert.Equal(t, 1, e.launcher.engine(1).settled)
			require.Equal(t, 3, e.launcher.count(), "a new standby is seeded from the promoted engine")
			assert.True(t, e.launcher.specs[2].Standby)
			assert.Empty(t, e.launcher.specs[2].Slot, "the new standby takes the freed slot")

			after, err := e.svc.Instances.Get(project, "ha").Context(t.Context()).Do()
			require.NoError(t, err)
			assert.Equal(t, "RUNNABLE", after.State)
			assert.Equal(t, before.SecondaryGceZone, after.GceZone)

			in, err := e.backend.GetInstance(project, "ha")
			require.NoError(t, err)
			assert.Equal(t, beforeEP, in.GopherstackEndpoint, "clients keep the same address across a failover")

			snap := string(e.backend.Snapshot(t.Context()))
			assert.Contains(t, snap, `"slot":"b"`, "the active data slot is persisted")
		})
	}
}

func TestFailoverFailureKeepsPrimary(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "ha", "POSTGRES_16", regionalSettings())
	e.launcher.engine(1).waitErr = errFakeRole

	op, err := e.svc.Instances.Failover(project, "ha", &sqladmin.InstancesFailoverRequest{}).
		Context(t.Context()).Do()
	require.NoError(t, err)

	done := e.wait(t, op)
	require.NotNil(t, done.Error)
	assert.Equal(t, 1, e.launcher.engine(0).unquiesced, "the primary accepts writes again")
	assert.Zero(t, e.launcher.engine(1).snapshot().promoted)
	assert.False(t, e.launcher.engine(0).snapshot().purged)
}

func TestSwitchover(t *testing.T) {
	t.Parallel()

	for _, version := range []string{"POSTGRES_16", "MYSQL_8_4"} {
		t.Run(version, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createInstance(t, "prim", version, binlogSettings())
			e.createReplica(t, "rep", "prim", version)
			e.createReplica(t, "sib", "prim", version)

			op, err := e.svc.Instances.Switchover(project, "rep").Context(t.Context()).Do()
			done := e.mustWait(t, op, err)
			assert.Equal(t, "SWITCHOVER", done.OperationType)

			rep, err := e.svc.Instances.Get(project, "rep").Context(t.Context()).Do()
			require.NoError(t, err)
			assert.Equal(t, "CLOUD_SQL_INSTANCE", rep.InstanceType)
			assert.Empty(t, rep.MasterInstanceName)
			assert.ElementsMatch(t, []string{"prim", "sib"}, rep.ReplicaNames)

			prim, err := e.svc.Instances.Get(project, "prim").Context(t.Context()).Do()
			require.NoError(t, err)
			assert.Equal(t, "READ_REPLICA_INSTANCE", prim.InstanceType)
			assert.Equal(t, project+":rep", prim.MasterInstanceName)
			assert.Empty(t, prim.ReplicaNames)
			assert.Equal(t, "RUNNABLE", prim.State)

			sib, err := e.svc.Instances.Get(project, "sib").Context(t.Context()).Do()
			require.NoError(t, err)
			assert.Equal(t, project+":rep", sib.MasterInstanceName)

			assert.Equal(t, 1, e.launcher.engineFor("rep").snapshot().promoted)
			assert.Equal(t, 1, e.launcher.engineFor("prim").quiesced)
			assert.True(t, e.launcher.engineFor("prim").snapshot().purged)
			assert.GreaterOrEqual(
				t,
				e.launcher.engineFor("sib").snapshot().attached,
				2,
				"siblings follow the new primary",
			)

			last := e.launcher.specs[e.launcher.count()-1]
			assert.Equal(t, "prim", last.ID)
			require.NotNil(t, last.Replica, "the old primary relaunches as a replica of the promoted one")
			assert.True(t, last.Fresh)
		})
	}
}

func TestSwitchoverRejectsPrimary(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "prim", "POSTGRES_16", nil)

	_, err := e.svc.Instances.Switchover(project, "prim").Context(t.Context()).Do()
	require.Error(t, err)
	assert.Equal(t, http.StatusBadRequest, statusOf(t, err))
}

func TestDRReplica(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "prim", "POSTGRES_16", nil)
	e.createReplica(t, "dr", "prim", "POSTGRES_16")
	e.createReplica(t, "plain", "prim", "POSTGRES_16")

	_, err := e.svc.Instances.PromoteReplica(project, "dr").Failover(true).Context(t.Context()).Do()
	require.Error(t, err, "failover promote needs the DR designation")
	assert.Equal(t, http.StatusBadRequest, statusOf(t, err))

	_, err = e.svc.Instances.Patch(project, "prim", &sqladmin.DatabaseInstance{
		ReplicationCluster: &sqladmin.ReplicationCluster{FailoverDrReplicaName: "ghost"},
	}).Context(t.Context()).Do()
	require.Error(t, err)

	op, err := e.svc.Instances.Patch(project, "prim", &sqladmin.DatabaseInstance{
		ReplicationCluster: &sqladmin.ReplicationCluster{FailoverDrReplicaName: "dr"},
	}).Context(t.Context()).Do()
	e.mustWait(t, op, err)

	prim, err := e.svc.Instances.Get(project, "prim").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "dr", prim.ReplicationCluster.FailoverDrReplicaName)

	dr, err := e.svc.Instances.Get(project, "dr").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.True(t, dr.ReplicationCluster.DrReplica)

	_, err = e.svc.Instances.PromoteReplica(project, "plain").Failover(true).Context(t.Context()).Do()
	require.Error(t, err, "only the designated replica can fail over")

	op, err = e.svc.Instances.PromoteReplica(project, "dr").Failover(true).Context(t.Context()).Do()
	done := e.mustWait(t, op, err)
	assert.Equal(t, "PROMOTE_REPLICA", done.OperationType)

	newPrim, err := e.svc.Instances.Get(project, "dr").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "prim", newPrim.ReplicationCluster.FailoverDrReplicaName, "the old primary becomes the DR replica")

	old, err := e.svc.Instances.Get(project, "prim").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.True(t, old.ReplicationCluster.DrReplica)
	assert.Equal(t, project+":dr", old.MasterInstanceName)
}

func TestRestoreAfterFailoverUsesPromotedSlot(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "ha", "POSTGRES_16", regionalSettings())

	op, err := e.svc.Instances.Failover(project, "ha", &sqladmin.InstancesFailoverRequest{}).Context(t.Context()).Do()
	e.mustWait(t, op, err)

	fresh := &fakeLauncher{}
	b := gcpsql.NewInMemoryBackend()
	b.SetLauncher(fresh)
	t.Cleanup(b.Close)

	require.NoError(t, b.Restore(t.Context(), e.backend.Snapshot(t.Context())))
	require.Eventually(t, func() bool {
		in, gerr := b.GetInstance(project, "ha")

		return gerr == nil && in.State == "RUNNABLE"
	}, 10*time.Second, 5*time.Millisecond)
	b.Drain()

	require.GreaterOrEqual(t, fresh.count(), 2)
	assert.Equal(t, "b", fresh.specs[0].Slot, "the primary relaunches on the volume the promoted standby filled")
	assert.False(t, fresh.specs[0].Standby)
	assert.Empty(t, fresh.specs[1].Slot)
	assert.True(t, fresh.specs[1].Standby)
}

func TestCascadingReplicas(t *testing.T) {
	t.Parallel()

	for _, version := range []string{"POSTGRES_16", "MYSQL_8_4"} {
		t.Run(version, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createInstance(t, "prim", version, binlogSettings())

			op, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
				Name: "mid", DatabaseVersion: version, MasterInstanceName: "prim",
				Settings: &sqladmin.Settings{
					Tier:                "db-custom-1-3840",
					BackupConfiguration: &sqladmin.BackupConfiguration{BinaryLogEnabled: true},
				},
			}).Context(t.Context()).Do()
			e.mustWait(t, op, err)

			leaf := e.createReplica(t, "leaf", "mid", version)
			assert.Equal(t, project+":mid", leaf.MasterInstanceName)

			mid, err := e.svc.Instances.Get(project, "mid").Context(t.Context()).Do()
			require.NoError(t, err)
			assert.Equal(t, []string{"leaf"}, mid.ReplicaNames)

			spec := e.launcher.specFor("leaf")
			require.NotNil(t, spec.Replica)

			op, err = e.svc.Databases.Insert(project, "prim", &sqladmin.Database{Name: "app"}).Context(t.Context()).Do()
			e.mustWait(t, op, err)

			require.Eventually(t, func() bool {
				_, gerr := e.svc.Databases.Get(project, "leaf", "app").Context(t.Context()).Do()

				return gerr == nil
			}, e.timeout, 5*time.Millisecond, "records flow down the whole hierarchy")

			_, err = e.svc.Instances.Delete(project, "mid").Context(t.Context()).Do()
			require.Error(t, err, "a replica with replicas cannot be deleted")

			_, err = e.svc.Instances.Switchover(project, "leaf").Context(t.Context()).Do()
			require.Error(t, err, "switchover needs a replica of a primary")
		})
	}
}
