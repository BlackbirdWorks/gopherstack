package rds_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

var errNetDown = errors.New("network daemon down")

type streamFakeRuntime struct {
	*fakeEngineRuntime
	nets      []string
	connected []string
	removed   []string
	netMu     sync.Mutex
	failNets  int
}

func (s *streamFakeRuntime) CreateNetwork(_ context.Context, name string) (string, error) {
	s.netMu.Lock()
	defer s.netMu.Unlock()

	if s.failNets > 0 {
		s.failNets--

		return "", errNetDown
	}

	s.nets = append(s.nets, name)

	return "net-" + name, nil
}

func (s *streamFakeRuntime) ConnectNetwork(_ context.Context, networkID, containerID string) error {
	s.netMu.Lock()
	defer s.netMu.Unlock()

	s.connected = append(s.connected, networkID+"/"+containerID)

	return nil
}

func (s *streamFakeRuntime) RemoveNetwork(_ context.Context, networkID string) error {
	s.netMu.Lock()
	defer s.netMu.Unlock()

	s.removed = append(s.removed, networkID)

	return nil
}

func (s *streamFakeRuntime) Exec(ctx context.Context, id string, cmd []string) (container.ExecResult, error) {
	if slices.Contains(cmd, "SELECT pg_promote(true, 60)") {
		_, _ = s.fakeEngineRuntime.Exec(ctx, id, cmd)

		return container.ExecResult{Stdout: "t\n"}, nil
	}

	return s.fakeEngineRuntime.Exec(ctx, id, cmd)
}

func (s *streamFakeRuntime) ExecEnv(
	ctx context.Context, id string, cmd, env []string,
) (container.ExecResult, error) {
	s.mu.Lock()
	s.envs = append(s.envs, slices.Clone(env))
	s.mu.Unlock()

	return s.Exec(ctx, id, cmd)
}

func (s *streamFakeRuntime) netState() ([]string, []string, []string) {
	s.netMu.Lock()
	defer s.netMu.Unlock()

	return slices.Clone(s.nets), slices.Clone(s.connected), slices.Clone(s.removed)
}

type streamFixture struct {
	rt     *streamFakeRuntime
	b      *rds.InMemoryBackend
	status atomic.Pointer[rds.ReplicaStatus]
}

func newStreamFixture(t *testing.T) *streamFixture {
	t.Helper()

	return newStreamFixtureIn(t, "")
}

func newStreamFixtureIn(t *testing.T, dumpDir string) *streamFixture {
	t.Helper()

	f := &streamFixture{
		rt: &streamFakeRuntime{fakeEngineRuntime: newFakeEngineRuntime()},
		b:  rds.NewInMemoryBackend("123456789012", "us-east-1"),
	}
	f.status.Store(&rds.ReplicaStatus{Replicating: true, LagSeconds: 2})
	t.Cleanup(f.b.Close)
	f.b.EnableEngine(rds.EngineConfig{
		Runtime:             f.rt,
		StartTimeout:        engineWait,
		ReplicaPollInterval: 5 * time.Millisecond,
		DumpDir:             dumpDir,
		Probe:               func(context.Context, rds.EngineLogin) error { return nil },
		ReplicaStatus: func(context.Context, rds.EngineLogin) (rds.ReplicaStatus, error) {
			return *f.status.Load(), nil
		},
	})

	return f
}

func (f *streamFixture) waitAvailable(t *testing.T, id string) rds.DBInstance {
	t.Helper()

	var inst rds.DBInstance

	require.Eventually(t, func() bool {
		got, err := f.b.DescribeDBInstances(id)
		require.NoError(t, err)
		inst = got[0]

		return inst.DBInstanceStatus == "available"
	}, engineWait, 5*time.Millisecond, "instance %s never became available", id)

	return inst
}

func (f *streamFixture) createPair(t *testing.T, engine string) {
	t.Helper()

	_, err := f.b.CreateDBInstance("src", engine, "db.t3.micro", "appdb", "master", "", 20,
		rds.DBInstanceOptions{MasterUserPassword: "pw"})
	require.NoError(t, err)
	f.waitAvailable(t, "src")

	_, err = f.b.CreateDBInstanceReadReplica("rep", "src", "", "", "", rds.DBInstanceOptions{})
	require.NoError(t, err)
	f.waitAvailable(t, "rep")
}

func (f *fakeEngineRuntime) execMentions(sub string) bool {
	return slices.ContainsFunc(f.allExecs(), func(c []string) bool {
		return slices.ContainsFunc(c, func(a string) bool { return strings.Contains(a, sub) })
	})
}

func flagValue(args []string, prefix string) string {
	for _, a := range args {
		if v, ok := strings.CutPrefix(a, prefix); ok {
			return v
		}
	}

	return ""
}

func TestEngineStreamingReplicaContainers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		engine     string
		sourceExec []string
	}{
		{name: "postgres", engine: "postgres", sourceExec: []string{"CREATE ROLE rdsrepl", "pg_reload_conf", "pg_hba"}},
		{name: "mysql", engine: "mysql", sourceExec: []string{"CREATE USER IF NOT EXISTS", "--source-data=2"}},
		{name: "mariadb", engine: "mariadb", sourceExec: []string{"CREATE USER IF NOT EXISTS", "--master-data=1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newStreamFixture(t)
			f.createPair(t, tt.engine)

			nets, connected, _ := f.rt.netState()
			require.Len(t, nets, 1)
			require.Len(t, connected, 1)
			assert.Equal(t, "net-"+nets[0]+"/ctr-1", connected[0])

			src, rep := f.rt.spec(0), f.rt.spec(1)
			assert.Empty(t, src.Network, "the source joins the shared network after it starts")
			assert.Equal(t, nets[0], rep.Network)
			assert.NotEqual(t, src.Name, rep.Name)

			for _, sub := range tt.sourceExec {
				assert.True(t, f.rt.execMentions(sub), sub)
			}

			if tt.engine == "postgres" {
				assert.Contains(t, src.Cmd, "wal_level=replica")
				assert.Contains(t, src.Cmd, "max_wal_senders=10")
				require.GreaterOrEqual(t, len(rep.Entrypoint), 5)
				assert.Contains(t, rep.Entrypoint[2], "pg_basebackup")
				assert.Contains(t, rep.Entrypoint[2], "-R -X stream")
				assert.Equal(t, src.Name, rep.Entrypoint[4])

				return
			}

			srcID, repID := flagValue(src.Cmd, "--server-id="), flagValue(rep.Cmd, "--server-id=")
			assert.NotEmpty(t, srcID)
			assert.NotEqual(t, srcID, repID)
			assert.Contains(t, src.Cmd, "--log-bin=mysql-bin")
			assert.Empty(t, rep.Entrypoint)
		})
	}
}

func TestEngineStreamingMySQLReplicaCommands(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		engine string
		want   []string
	}{
		{name: "mysql", engine: "mysql", want: []string{
			"RESET BINARY LOGS AND GTIDS", "SOURCE_AUTO_POSITION=1", "SET GLOBAL super_read_only=ON",
		}},
		{name: "mariadb", engine: "mariadb", want: []string{
			"RESET MASTER", "MASTER_USE_GTID=slave_pos", "SET GLOBAL read_only=ON",
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newStreamFixture(t)
			f.createPair(t, tt.engine)

			for _, w := range tt.want {
				assert.True(t, f.rt.execMentions(w), w)
			}
		})
	}
}

func TestEngineStreamingReplicaStatus(t *testing.T) {
	t.Parallel()

	f := newStreamFixture(t)
	f.createPair(t, "postgres")

	src, err := f.b.DescribeDBInstances("src")
	require.NoError(t, err)
	assert.Empty(t, src[0].StatusInfos)

	tests := []struct {
		wantStatus string
		wantMsg    string
		status     rds.ReplicaStatus
		wantLag    time.Duration
		wantNormal bool
	}{
		{
			status:     rds.ReplicaStatus{Replicating: true, LagSeconds: 3},
			wantStatus: "replicating", wantNormal: true, wantLag: 3 * time.Second,
		},
		{
			status:     rds.ReplicaStatus{Message: "WAL receiver status: stopping"},
			wantStatus: "error", wantMsg: "WAL receiver status: stopping",
		},
	}

	for _, tt := range tests {
		f.status.Store(&tt.status)

		require.Eventually(t, func() bool {
			got, derr := f.b.DescribeDBInstances("rep")
			require.NoError(t, derr)

			lag, _ := f.b.ReplicaLag("rep")

			return len(got[0].StatusInfos) == 1 && got[0].StatusInfos[0].Status == tt.wantStatus &&
				got[0].StatusInfos[0].Message == tt.wantMsg && lag == tt.wantLag
		}, engineWait, 5*time.Millisecond)

		got, derr := f.b.DescribeDBInstances("rep")
		require.NoError(t, derr)
		assert.Equal(t, "read replication", got[0].StatusInfos[0].StatusType)
		assert.Equal(t, tt.wantNormal, got[0].StatusInfos[0].Normal)

		lag, ok := f.b.ReplicaLag("rep")
		assert.True(t, ok)
		assert.Equal(t, tt.wantLag, lag)
	}
}

func TestEngineStreamingPromote(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		engine   string
		wantExec string
	}{
		{name: "postgres", engine: "postgres", wantExec: "SELECT pg_promote(true, 60)"},
		{name: "mysql", engine: "mysql", wantExec: "STOP REPLICA; RESET REPLICA ALL; SET GLOBAL read_only=OFF; " +
			"SET GLOBAL super_read_only=OFF"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newStreamFixture(t)
			f.createPair(t, tt.engine)

			require.Eventually(t, func() bool {
				got, err := f.b.DescribeDBInstances("rep")
				require.NoError(t, err)

				return len(got[0].StatusInfos) == 1
			}, engineWait, 5*time.Millisecond)

			_, err := f.b.PromoteReadReplica("rep")
			require.NoError(t, err)

			require.Eventually(t, func() bool { return f.rt.execSeen(tt.wantExec) }, engineWait, 5*time.Millisecond)

			got := f.waitAvailable(t, "rep")
			assert.Empty(t, got.ReplicaSourceDBInstanceIdentifier)
			assert.Empty(t, got.StatusInfos)

			_, ok := f.b.ReplicaLag("rep")
			assert.False(t, ok)

			src, err := f.b.DescribeDBInstances("src")
			require.NoError(t, err)
			assert.Empty(t, src[0].ReadReplicaIdentifiers)
		})
	}
}

func TestEngineStreamingDeleteSourcePromotesReplicasAndFreesNetwork(t *testing.T) {
	t.Parallel()

	f := newStreamFixture(t)
	f.createPair(t, "postgres")

	_, err := f.b.DeleteDBInstanceWithOptions("src", true, "", true)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return f.rt.execSeen("SELECT pg_promote(true, 60)") },
		engineWait, 5*time.Millisecond)

	got := f.waitAvailable(t, "rep")
	assert.Empty(t, got.ReplicaSourceDBInstanceIdentifier)

	_, _, removed := f.rt.netState()
	assert.Empty(t, removed, "the network stays while the promoted replica still uses it")

	_, err = f.b.DeleteDBInstanceWithOptions("rep", true, "", true)
	require.NoError(t, err)

	nets, _, _ := f.rt.netState()
	require.Eventually(t, func() bool {
		_, _, rm := f.rt.netState()

		return slices.Equal(rm, []string{"net-" + nets[0]})
	}, engineWait, 5*time.Millisecond)
	require.Eventually(t, func() bool { return f.rt.containers() == 0 }, engineWait, 5*time.Millisecond)
}

func TestEngineReplicaFallsBackToCopyWithoutNetworks(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t, engineWait)
	f.readySource(t, "postgres")

	_, err := f.b.CreateDBInstanceReadReplica("rep", "src", "", "", "", rds.DBInstanceOptions{})
	require.NoError(t, err)
	f.waitInstance(t, "rep", "available")

	assert.Empty(t, f.rt.spec(1).Network)
	assert.Empty(t, f.rt.spec(1).Entrypoint)
	assert.Empty(t, f.instance(t, "rep").StatusInfos)
}
