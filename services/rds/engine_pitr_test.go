package rds_test

import (
	"context"
	"maps"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

type fakeClock struct {
	now time.Time
	mu  sync.Mutex
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

type pitrFixture struct {
	*engineFixture
	clock *fakeClock
	t0    time.Time
}

func newPITRFixture(t *testing.T, dumpDir string) *pitrFixture {
	t.Helper()

	clock := &fakeClock{now: time.Now().UTC().Truncate(time.Second)}
	f := &engineFixture{rt: newFakeEngineRuntime(), b: rds.NewInMemoryBackend("123456789012", "us-east-1")}
	t.Cleanup(f.b.Close)
	f.b.EnableEngine(rds.EngineConfig{
		Runtime:              f.rt,
		StartTimeout:         engineWait,
		Now:                  clock.Now,
		DumpDir:              dumpDir,
		RestorePointInterval: -1,
		Probe:                func(context.Context, rds.EngineLogin) error { return nil },
	})

	return &pitrFixture{engineFixture: f, clock: clock, t0: clock.Now()}
}

// tick moves the clock forward and takes one restore point, as the periodic loop would.
func (f *pitrFixture) tick(d time.Duration) {
	f.clock.advance(d)
	rds.RecordRestorePointsForTest(f.b)
}

func (f *pitrFixture) createSource(t *testing.T, engine string, retention int) {
	t.Helper()

	_, err := f.b.CreateDBInstance("src", engine, "db.t3.micro", "appdb", "master", "", 20,
		rds.DBInstanceOptions{MasterUserPassword: "pw", BackupRetentionPeriod: retention})
	require.NoError(t, err)
	f.waitInstance(t, "src", "available")

	// Wait for the first point to be stored, not just for its dump to start: a tick that advances the clock
	// mid-dump stamps that point late and drops t0 from the window.
	if retention > 0 {
		require.Eventually(t, func() bool { return !f.instance(t, "src").LatestRestorableTime.IsZero() },
			engineWait, 5*time.Millisecond)
	}
}

func (f *fakeEngineRuntime) allExecs() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([][]string(nil), f.execs...)
}

func TestEnginePointInTimeRestoreUsesRequestedTime(t *testing.T) {
	t.Parallel()

	for _, tt := range dataCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newPITRFixture(t, "")
			f.createSource(t, tt.engine, 7)

			f.rt.setDump("v1")
			f.tick(10 * time.Minute)
			f.rt.setDump("v2")
			f.tick(10 * time.Minute)

			tests := []struct {
				wantErr error
				name    string
				want    string
				at      time.Duration
			}{
				{name: "first point", at: 5 * time.Minute, want: tt.dump},
				{name: "between points", at: 15 * time.Minute, want: "v1"},
				{name: "latest point", at: 20 * time.Minute, want: "v2"},
				{name: "after latest", at: 25 * time.Minute, wantErr: rds.ErrInvalidRestore},
				{name: "before earliest", at: -time.Hour, wantErr: rds.ErrInvalidRestore},
			}

			for i, c := range tests {
				id := "pitr" + string(rune('a'+i))
				_, err := f.b.RestoreDBInstanceToPointInTime(id, "src",
					rds.DBInstanceOptions{RestoreTime: f.t0.Add(c.at)})

				if c.wantErr != nil {
					require.ErrorIs(t, err, c.wantErr, c.name)

					continue
				}

				require.NoError(t, err, c.name)
				f.waitInstance(t, id, "available")
				assert.Equal(t, c.want, f.rt.loadedBy("ctr-"+string(rune('2'+countOK(tests[:i])))), c.name)
			}
		})
	}
}

func countOK(cases []struct {
	wantErr error
	name    string
	want    string
	at      time.Duration
},
) int {
	n := 0

	for _, c := range cases {
		if c.wantErr == nil {
			n++
		}
	}

	return n
}

func TestEnginePointInTimeRestoreLatestAndDisabled(t *testing.T) {
	t.Parallel()

	f := newPITRFixture(t, "")
	f.createSource(t, "postgres", 7)
	f.rt.setDump("live")

	_, err := f.b.RestoreDBInstanceToPointInTime("latest", "src", rds.DBInstanceOptions{UseLatestRestorableTime: true})
	require.NoError(t, err)
	f.waitInstance(t, "latest", "available")
	assert.Equal(t, "live", f.rt.loadedBy("ctr-2"))

	g := newPITRFixture(t, "")
	g.createSource(t, "postgres", 0)

	_, err = g.b.RestoreDBInstanceToPointInTime("x", "src", rds.DBInstanceOptions{RestoreTime: g.t0})
	require.ErrorIs(t, err, rds.ErrPointInTimeRestoreNotEnabled)
}

func TestEngineRestorableWindowIsReported(t *testing.T) {
	t.Parallel()

	f := newPITRFixture(t, "")
	f.createSource(t, "postgres", 7)

	f.rt.setDump("v1")
	f.tick(10 * time.Minute)
	f.tick(10 * time.Minute)

	inst := f.instance(t, "src")
	assert.Equal(t, f.t0.Add(20*time.Minute), inst.LatestRestorableTime)

	f.readyClusterWithRetention(t, "c1", 7)
	require.Eventually(t, func() bool { return !f.cluster(t, "c1").LatestRestorableTime.IsZero() },
		engineWait, 5*time.Millisecond)
	f.tick(5 * time.Minute)

	c := f.cluster(t, "c1")
	assert.Equal(t, f.t0.Add(20*time.Minute), c.EarliestRestorableTime)
	assert.Equal(t, f.t0.Add(25*time.Minute), c.LatestRestorableTime)
}

func (f *pitrFixture) readyClusterWithRetention(t *testing.T, id string, retention int) {
	t.Helper()

	_, err := f.b.CreateDBCluster(id, "aurora-mysql", "master", "appdb", "", 0, nil,
		rds.DBClusterOptions{
			MasterUserPassword: "pw", EngineVersion: "8.0.mysql_aurora.3.05.2", BackupRetentionPeriod: retention,
		})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return f.cluster(t, id).Status == "available" },
		engineWait, 5*time.Millisecond)
}

func TestEngineClusterPointInTimeRestoreUsesRequestedTime(t *testing.T) {
	t.Parallel()

	f := newPITRFixture(t, "")
	f.readyClusterWithRetention(t, "c1", 7)
	require.Eventually(t, func() bool { return len(f.rt.allExecs()) > 0 }, engineWait, 5*time.Millisecond)

	f.rt.setDump("c-v1")
	f.tick(10 * time.Minute)
	f.rt.setDump("c-v2")
	f.tick(10 * time.Minute)

	_, err := f.b.RestoreDBClusterToPointInTime("c2", "c1",
		rds.DBClusterOptions{RestoreTime: f.t0.Add(12 * time.Minute)})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return f.cluster(t, "c2").Status == "available" },
		engineWait, 5*time.Millisecond)
	assert.Equal(t, "c-v1", f.rt.loadedBy("ctr-2"))

	_, err = f.b.RestoreDBClusterToPointInTime("c3", "c1", rds.DBClusterOptions{RestoreTime: f.t0.Add(time.Hour)})
	require.ErrorIs(t, err, rds.ErrInvalidRestore)
}

func TestEngineRestorePointsPruneToRetention(t *testing.T) {
	t.Parallel()

	f := newPITRFixture(t, "")
	f.createSource(t, "postgres", 1)

	f.rt.setDump("d1")
	f.tick(26 * time.Hour)
	f.rt.setDump("d2")
	f.tick(26 * time.Hour)

	_, err := f.b.RestoreDBInstanceToPointInTime("old", "src",
		rds.DBInstanceOptions{RestoreTime: f.t0.Add(27 * time.Hour)})
	require.ErrorIs(t, err, rds.ErrInvalidRestore)

	_, err = f.b.RestoreDBInstanceToPointInTime("ok", "src",
		rds.DBInstanceOptions{RestoreTime: f.t0.Add(29 * time.Hour)})
	require.NoError(t, err)
	f.waitInstance(t, "ok", "available")
	assert.Equal(t, "d1", f.rt.loadedBy("ctr-2"))
}

func TestEngineUnchangedDumpExtendsLatestRestorableTime(t *testing.T) {
	t.Parallel()

	f := newPITRFixture(t, "")
	f.createSource(t, "postgres", 7)

	f.tick(10 * time.Minute)
	f.tick(10 * time.Minute)

	assert.Equal(t, f.t0.Add(20*time.Minute), f.instance(t, "src").LatestRestorableTime)

	_, err := f.b.RestoreDBInstanceToPointInTime("mid", "src",
		rds.DBInstanceOptions{RestoreTime: f.t0.Add(15 * time.Minute)})
	require.NoError(t, err)
}

func TestEngineSnapshotDumpsSurviveRestart(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	f := newPITRFixture(t, dir)
	f.createSource(t, "postgres", 0)
	f.rt.setDump("persisted-dump")

	_, err := f.b.CreateDBSnapshot("snap1", "src")
	require.NoError(t, err)
	_, err = f.b.CreateDBSnapshot("snap2", "src")
	require.NoError(t, err)

	state := f.b.Snapshot(context.Background())
	require.NotEmpty(t, state)

	entries, err := os.ReadDir(dir + "/us-east-1")
	require.NoError(t, err)
	assert.Len(t, entries, 2)

	g := newPITRFixture(t, dir)
	require.NoError(t, g.b.Restore(context.Background(), state))

	_, err = g.b.RestoreDBInstanceFromDBSnapshot("copy", "snap1", rds.DBInstanceOptions{})
	require.NoError(t, err)
	g.waitInstance(t, "copy", "available")
	assert.Contains(t, g.rt.allLoaded(), "persisted-dump")

	_, err = g.b.DeleteDBSnapshot("snap2")
	require.NoError(t, err)

	entries, err = os.ReadDir(dir + "/us-east-1")
	require.NoError(t, err)
	assert.Len(t, entries, 1)
}

func TestEngineRestorePointsSurviveRestart(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	f := newPITRFixture(t, dir)
	f.createSource(t, "postgres", 7)
	f.rt.setDump("before-restart")
	f.tick(10 * time.Minute)

	state := f.b.Snapshot(context.Background())

	g := newPITRFixture(t, dir)
	require.NoError(t, g.b.Restore(context.Background(), state))
	g.waitInstance(t, "src", "available")
	assert.Eventually(t, func() bool { return slices.Contains(g.rt.allLoaded(), "before-restart") },
		engineWait, 5*time.Millisecond)
}

func (f *fakeEngineRuntime) allLoaded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Collect(maps.Values(f.loaded))
}
