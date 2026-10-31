package gcpsql_test

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"

	"github.com/blackbirdworks/gopherstack/services/gcpsql"
)

func TestTierReachesEngine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tier    string
		wantCPU int64
		wantMem int64
	}{
		{tier: "db-custom-1-3840", wantCPU: 1, wantMem: 3840 << 20},
		{tier: "db-f1-micro", wantCPU: 1, wantMem: 614 << 20},
		{tier: "db-n1-standard-1", wantCPU: 1, wantMem: 3840 << 20},
		{tier: "db-bogus", wantCPU: 0, wantMem: 0},
	}

	for _, tt := range tests {
		t.Run(tt.tier, func(t *testing.T) {
			t.Parallel()

			l := &fakeLauncher{}
			e := newEnvWith(t, l)
			e.createInstance(t, "a", "POSTGRES_16", &sqladmin.Settings{Tier: tt.tier})

			spec := l.specFor("a")
			assert.Equal(t, tt.wantCPU*1_000_000_000, spec.NanoCPUs)
			assert.Equal(t, tt.wantMem, spec.MemoryBytes)
			assert.True(t, spec.Persist, "engine data lives in a volume")
			assert.Equal(t, project, spec.Project)
			assert.NotZero(t, spec.ServerID)
		})
	}
}

func TestTierChangeRelaunchesEngine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		persist      bool
		replica      bool
		wantLaunches int
	}{
		{name: "persistent", persist: true, wantLaunches: 2},
		{name: "not persistent", persist: false, wantLaunches: 1},
		{name: "has replica", persist: true, replica: true, wantLaunches: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := &fakeLauncher{persist: tt.persist}
			e := newEnvWith(t, l)
			e.createInstance(t, "a", "POSTGRES_16", &sqladmin.Settings{Tier: "db-custom-1-3840"})

			if tt.replica {
				e.createReplica(t, "r", "a", "POSTGRES_16")
			}

			op, err := e.svc.Instances.Patch(project, "a", &sqladmin.DatabaseInstance{
				Settings: &sqladmin.Settings{Tier: "db-custom-2-7680"},
			}).Context(t.Context()).Do()
			e.mustWait(t, op, err)

			l.mu.Lock()
			defer l.mu.Unlock()

			launches := 0
			for _, s := range l.specs {
				if s.ID == "a" {
					launches++
				}
			}

			assert.Equal(t, tt.wantLaunches, launches)

			if tt.wantLaunches == 2 {
				last := l.specs[len(l.specs)-1]
				assert.Equal(t, int64(min(2, runtime.NumCPU()))*1_000_000_000, last.NanoCPUs)
				assert.Equal(t, int64(7680)<<20, last.MemoryBytes)
				assert.True(t, l.engines[0].closed)
				assert.False(t, l.engines[0].purged, "a relaunch keeps the data volume")
			}
		})
	}
}

func TestVolumeLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		end       func(*env)
		name      string
		wantPurge bool
	}{
		{name: "reset removes volumes", end: func(e *env) { e.backend.Reset() }, wantPurge: true},
		{name: "close keeps volumes", end: func(e *env) { e.backend.Close() }, wantPurge: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := &fakeLauncher{}
			e := newEnvWith(t, l)
			e.createInstance(t, "a", "POSTGRES_16", nil)

			tt.end(e)

			eng := l.engineFor("a").snapshot()
			assert.True(t, eng.closed)
			assert.Equal(t, tt.wantPurge, eng.purged)
		})
	}
}

func TestStaleVolumesSweptOnce(t *testing.T) {
	t.Parallel()

	l := &fakeLauncher{}
	e := newEnvWith(t, l)
	e.createInstance(t, "a", "POSTGRES_16", nil)
	e.createInstance(t, "b", "MYSQL_8_0", nil)

	l.mu.Lock()
	defer l.mu.Unlock()

	require.Len(t, l.swept, 1, "the sweep runs before the first launch only")

	names := make([]string, 0, len(l.swept[0]))
	for n := range l.swept[0] {
		names = append(names, n)
	}

	joined := strings.Join(names, " ")
	assert.Contains(t, joined, "gopherstack-gcpsql-data-test-project-a")
}

func TestSnapshotRestoreRelaunchesReplicaAfterPrimary(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "prim", "POSTGRES_16", nil)
	e.createReplica(t, "rep", "prim", "POSTGRES_16")

	op, err := e.svc.Instances.StopReplica(project, "rep").Context(ctx).Do()
	e.mustWait(t, op, err)

	snap := e.backend.Snapshot(ctx)

	fresh := &fakeLauncher{}
	b := gcpsql.NewInMemoryBackend()
	b.SetLauncher(fresh)
	t.Cleanup(b.Close)

	require.NoError(t, b.Restore(ctx, snap))

	require.Eventually(t, func() bool {
		in, gerr := b.GetInstance(project, "rep")

		return gerr == nil && in.State == "RUNNABLE"
	}, 10*time.Second, 5*time.Millisecond)

	in, err := b.GetInstance(project, "rep")
	require.NoError(t, err)
	assert.Equal(t, "READ_REPLICA_INSTANCE", in.InstanceType)

	prim, err := b.GetInstance(project, "prim")
	require.NoError(t, err)
	assert.Equal(t, []string{"rep"}, prim.ReplicaNames)

	require.NotNil(t, fresh.specFor("rep").Replica)

	rep := fresh.engineFor("rep").snapshot()
	assert.Equal(t, 1, rep.attached)
	assert.Equal(t, []bool{false}, rep.replCalls, "a stopped replica stays stopped across a restart")
}
