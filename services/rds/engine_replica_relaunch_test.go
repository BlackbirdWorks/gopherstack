package rds_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func (f *streamFixture) createBackedUpPair(t *testing.T, engine string) {
	t.Helper()

	f.rt.setDump("v1")

	_, err := f.b.CreateDBInstance("src", engine, "db.t3.micro", "appdb", "master", "", 20,
		rds.DBInstanceOptions{MasterUserPassword: "pw", BackupRetentionPeriod: 1})
	require.NoError(t, err)
	f.waitAvailable(t, "src")

	_, err = f.b.CreateDBInstanceReadReplica("rep", "src", "", "", "", rds.DBInstanceOptions{})
	require.NoError(t, err)
	f.waitAvailable(t, "rep")

	rds.RecordRestorePointsForTest(f.b)
}

func (f *streamFixture) restoreInto(t *testing.T, dumpDir string) *streamFixture {
	t.Helper()

	snap := f.b.Snapshot(t.Context())
	g := newStreamFixtureIn(t, dumpDir)
	require.NoError(t, g.b.Restore(t.Context(), snap))

	return g
}

func TestEngineRelaunchRebuildsReplicaFromSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		engine   string
		wantExec string
	}{
		{name: "postgres", engine: "postgres", wantExec: "CREATE ROLE rdsrepl"},
		{name: "mysql", engine: "mysql", wantExec: "CHANGE REPLICATION SOURCE TO"},
		{name: "mariadb", engine: "mariadb", wantExec: "CHANGE MASTER TO"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			f := newStreamFixtureIn(t, dir)
			f.createBackedUpPair(t, tt.engine)

			g := f.restoreInto(t, dir)
			require.Eventually(t, func() bool { return g.rt.loadedBy("ctr-1") == "v1" },
				engineWait, 5*time.Millisecond, "source gets its persisted data back")
			require.Eventually(t, func() bool { return g.rt.containers() == 2 }, engineWait, 5*time.Millisecond)

			src, rep := g.rt.spec(0), g.rt.spec(1)
			assert.Empty(t, src.Network, "source launches first, outside the network")
			assert.NotEmpty(t, rep.Network)

			nets, connected, _ := g.rt.netState()
			require.Len(t, nets, 1)
			assert.Equal(t, []string{"net-" + nets[0] + "/ctr-1"}, connected)
			assert.True(t, g.rt.execMentions(tt.wantExec), tt.wantExec)

			require.Eventually(t, func() bool {
				got, err := g.b.DescribeDBInstances("rep")
				if err != nil || len(got[0].StatusInfos) != 1 {
					return false
				}

				_, ok := g.b.ReplicaLag("rep")

				return ok && got[0].StatusInfos[0].Status == "replicating"
			}, engineWait, 5*time.Millisecond)

			got, err := g.b.DescribeDBInstances("rep")
			require.NoError(t, err)
			assert.Equal(t, "src", got[0].ReplicaSourceDBInstanceIdentifier)
		})
	}
}

func dumpDirHolds(dir, text string) bool {
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		sub, _ := os.ReadDir(filepath.Join(dir, e.Name()))
		for _, s := range sub {
			raw, _ := os.ReadFile(filepath.Join(dir, e.Name(), s.Name()))
			if strings.Contains(string(raw), text) {
				return true
			}
		}
	}

	return false
}

func TestEngineRelaunchPromotedReplicaKeepsOwnData(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	f := newStreamFixtureIn(t, dir)
	f.createBackedUpPair(t, "postgres")

	f.rt.setDump("promoted-data")
	_, err := f.b.PromoteReadReplica("rep")
	require.NoError(t, err)
	f.waitAvailable(t, "rep")
	require.Eventually(t, func() bool { return dumpDirHolds(dir, "promoted-data") }, engineWait, 5*time.Millisecond)

	g := f.restoreInto(t, dir)
	require.Eventually(t, func() bool { return len(g.rt.allLoaded()) == 2 }, engineWait, 5*time.Millisecond)
	g.waitAvailable(t, "src")
	g.waitAvailable(t, "rep")

	nets, _, _ := g.rt.netState()
	assert.Empty(t, nets, "no replica remains to need a network")

	for i := range 2 {
		assert.Empty(t, g.rt.spec(i).Network)
		assert.Empty(t, g.rt.spec(i).Entrypoint)
	}

	assert.ElementsMatch(t, []string{"v1", "promoted-data"}, g.rt.allLoaded())

	got, err := g.b.DescribeDBInstances("rep")
	require.NoError(t, err)
	assert.Empty(t, got[0].StatusInfos)
	assert.Empty(t, got[0].ReplicaSourceDBInstanceIdentifier)
}

func TestEngineSourceDeletedWhileReplicaCreatingPromotes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		engine string
	}{
		{name: "postgres", engine: "postgres"},
		{name: "mysql", engine: "mysql"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var hold atomic.Bool

			hold.Store(true)

			f := newStreamFixture(t)
			f.b.EnableEngine(rds.EngineConfig{
				Runtime:             f.rt,
				StartTimeout:        engineWait,
				ReplicaPollInterval: 5 * time.Millisecond,
				Probe: func(context.Context, rds.EngineLogin) error {
					if hold.Load() {
						return errNetDown
					}

					return nil
				},
			})

			_, err := f.b.CreateDBInstance("src", tt.engine, "db.t3.micro", "appdb", "master", "", 20,
				rds.DBInstanceOptions{MasterUserPassword: "pw"})
			require.NoError(t, err)
			_, err = f.b.CreateDBInstanceReadReplica("rep", "src", "", "", "", rds.DBInstanceOptions{})
			require.NoError(t, err)

			_, err = f.b.DeleteDBInstanceWithOptions("src", true, "", true)
			require.NoError(t, err)
			hold.Store(false)

			var got rds.DBInstance

			require.Eventually(t, func() bool {
				d, derr := f.b.DescribeDBInstances("rep")
				require.NoError(t, derr)
				got = d[0]

				return got.Endpoint == "127.0.0.1"
			}, engineWait, 5*time.Millisecond, "the replica's own engine never came up")

			assert.Equal(t, "available", got.DBInstanceStatus)
			assert.Empty(t, got.ReplicaSourceDBInstanceIdentifier)
			assert.Empty(t, got.StatusInfos)
		})
	}
}

func TestEngineReplicaNetworkFailureIsRetried(t *testing.T) {
	t.Parallel()

	f := newStreamFixture(t)
	f.rt.failNets = 1

	_, err := f.b.CreateDBInstance("src", "postgres", "db.t3.micro", "appdb", "master", "", 20,
		rds.DBInstanceOptions{MasterUserPassword: "pw"})
	require.NoError(t, err)
	f.waitAvailable(t, "src")

	_, err = f.b.CreateDBInstanceReadReplica("bad", "src", "", "", "", rds.DBInstanceOptions{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, derr := f.b.DescribeDBInstances("bad")

		return derr == nil && got[0].DBInstanceStatus == "failed"
	}, engineWait, 5*time.Millisecond)

	_, err = f.b.CreateDBInstanceReadReplica("good", "src", "", "", "", rds.DBInstanceOptions{})
	require.NoError(t, err)
	f.waitAvailable(t, "good")

	nets, connected, _ := f.rt.netState()
	assert.Len(t, nets, 1)
	assert.Len(t, connected, 1)
}
