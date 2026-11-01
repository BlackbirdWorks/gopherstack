package dms_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/dmsengine"
	"github.com/blackbirdworks/gopherstack/services/dms"
)

var errUnreachable = errors.New("dial tcp 127.0.0.1:1: connect: connection refused")

type engineClock struct {
	t  time.Time
	mu sync.Mutex
}

func (c *engineClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *engineClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(d)
}

type stubSource struct {
	hold     chan struct{}
	changes  chan dmsengine.Change
	pingErr  error
	startAts chan dmsengine.StreamStart
	tables   []dmsengine.Table
}

func newStubSource() *stubSource {
	return &stubSource{
		hold: make(
			chan struct{},
		),
		changes:  make(chan dmsengine.Change, 8),
		startAts: make(chan dmsengine.StreamStart, 8),
		tables: []dmsengine.Table{{Schema: "app", Name: "items", PK: []string{"id"}, Columns: []dmsengine.Column{
			{Name: "id", Native: "int"}, {Name: "name", Native: "text"},
		}}},
	}
}

func (s *stubSource) Ping(context.Context) error                        { return s.pingErr }
func (s *stubSource) Tables(context.Context) ([]dmsengine.Table, error) { return s.tables, nil }
func (s *stubSource) Position(context.Context) (string, error)          { return "bin.1:4", nil }
func (s *stubSource) Release(context.Context) error                     { return nil }
func (s *stubSource) Close() error                                      { return nil }

func (s *stubSource) Scan(ctx context.Context, _ dmsengine.Table, fn func(dmsengine.Row) error) error {
	select {
	case <-s.hold:
	case <-ctx.Done():
		return ctx.Err()
	}

	for _, r := range []dmsengine.Row{{"1", "a"}, {"2", "b"}, {"3", "c"}} {
		if err := fn(r); err != nil {
			return err
		}
	}

	return nil
}

func (s *stubSource) Stream(
	ctx context.Context, start dmsengine.StreamStart, _ func(string, string) (dmsengine.Table, bool),
	fn func(dmsengine.Change) error,
) error {
	s.startAts <- start

	for {
		select {
		case <-ctx.Done():
			return nil
		case c := <-s.changes:
			if err := fn(c); err != nil {
				return err
			}
		}
	}
}

type stubTarget struct{ pingErr error }

func (stubTarget) Close() error                 { return nil }
func (t stubTarget) Ping(context.Context) error { return t.pingErr }
func (stubTarget) Prepare(context.Context, dmsengine.TargetTable, dmsengine.PrepMode) error {
	return nil
}
func (stubTarget) Load(context.Context, dmsengine.TargetTable, []dmsengine.Row) error   { return nil }
func (stubTarget) EndLoad(context.Context, dmsengine.TargetTable) error                 { return nil }
func (stubTarget) Apply(context.Context, dmsengine.TargetTable, dmsengine.Change) error { return nil }
func (stubTarget) Commit(context.Context) error                                         { return nil }

type stubOpener struct {
	tgt      stubTarget
	src      *stubSource
	released chan string
	seen     dmsengine.EndpointConfig
	mu       sync.Mutex
}

func (o *stubOpener) OpenSource(_ context.Context, ep dmsengine.EndpointConfig) (dmsengine.Source, error) {
	o.mu.Lock()
	o.seen = ep
	o.mu.Unlock()

	return o.src, nil
}

func (o *stubOpener) OpenTarget(context.Context, dmsengine.EndpointConfig, dmsengine.Family) (dmsengine.Target, error) {
	return o.tgt, nil
}

func (o *stubOpener) ReleaseSlot(_ context.Context, ep dmsengine.EndpointConfig) error {
	o.released <- ep.SlotName

	return nil
}

type engineEnv struct {
	b     *dms.InMemoryBackend
	op    *stubOpener
	clock *engineClock
	task  string
	src   string
	tgt   string
	ri    string
}

const selectApp = `{"rules":[{"rule-type":"selection","rule-id":"1","rule-name":"1",
	"object-locator":{"schema-name":"app","table-name":"%"},"rule-action":"include"}]}`

func newEngineEnv(t *testing.T, srcEngine, migration string) *engineEnv {
	t.Helper()

	b := dms.NewInMemoryBackend("123456789012", "us-east-1")
	t.Cleanup(b.Close)

	e := &engineEnv{
		b: b, op: &stubOpener{src: newStubSource(), released: make(chan string, 4)},
		clock: &engineClock{t: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	b.EnableEngine(dms.EngineConfig{Opener: e.op, Now: e.clock.Now})

	ctx := t.Context()
	ri, err := b.CreateReplicationInstance(ctx, "ri", "dms.t3.micro", "", "", 0, false, false, false, nil,
		dms.ReplicationInstanceSettings{})
	require.NoError(t, err)

	src, err := b.CreateEndpoint(ctx, "src", "source", srcEngine, "127.0.0.1", "app", "u", "secret-pass", 3306, nil,
		dms.EndpointConnectionSettings{})
	require.NoError(t, err)

	tgt, err := b.CreateEndpoint(ctx, "tgt", "target", "postgres", "127.0.0.1", "app", "u", "secret-pass", 5432, nil,
		dms.EndpointConnectionSettings{})
	require.NoError(t, err)

	task, err := b.CreateReplicationTask(ctx, "task", src.EndpointArn, tgt.EndpointArn, ri.ReplicationInstanceArn,
		migration, selectApp, "", nil, dms.ReplicationTaskCDCSettings{})
	require.NoError(t, err)

	e.ri, e.src, e.tgt, e.task = ri.ReplicationInstanceArn, src.EndpointArn, tgt.EndpointArn, task.ReplicationTaskArn

	return e
}

func (e *engineEnv) describe(t *testing.T) *dms.ReplicationTask {
	t.Helper()

	tasks, err := e.b.DescribeReplicationTasks(t.Context(), dms.NewIdentifierFilter("replication-task-arn", e.task))
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	return tasks[0]
}

func (e *engineEnv) await(t *testing.T, status string) *dms.ReplicationTask {
	t.Helper()

	var got *dms.ReplicationTask

	require.Eventually(t, func() bool {
		got = e.describe(t)

		return got.Status == status
	}, 10*time.Second, 5*time.Millisecond, "status %s", status)

	return got
}

func (e *engineEnv) start(t *testing.T, cdc dms.ReplicationTaskCDCSettings) *dms.ReplicationTask {
	t.Helper()

	rt, err := e.b.StartReplicationTask(t.Context(), e.task, cdc)
	require.NoError(t, err)

	return rt
}

func TestEngineFullLoadLifecycle(t *testing.T) {
	t.Parallel()

	e := newEngineEnv(t, "mysql", "full-load")
	started := e.start(t, dms.ReplicationTaskCDCSettings{})
	assert.Equal(t, "starting", started.Status)

	running := e.await(t, "running")
	assert.False(t, running.StartDate.IsZero())
	assert.Zero(t, running.ElapsedTimeMillis)

	require.Eventually(t, func() bool {
		ts := e.describe(t).TableStats

		return len(ts) == 1 && ts[0].TableState == "Full load"
	}, 10*time.Second, 5*time.Millisecond)

	e.clock.Advance(90 * time.Second)
	assert.EqualValues(t, 90_000, e.describe(t).ElapsedTimeMillis)

	e.clock.Advance(30 * time.Second)
	close(e.op.src.hold)

	done := e.await(t, "stopped")
	assert.Equal(t, "Stop Reason FULL_LOAD_ONLY_FINISHED", done.StopReason)
	assert.EqualValues(t, 120_000, done.ElapsedTimeMillis)

	e.clock.Advance(time.Hour)
	assert.EqualValues(t, 120_000, e.describe(t).ElapsedTimeMillis, "elapsed freezes at stop")

	require.Len(t, done.TableStats, 1)
	st := done.TableStats[0]
	assert.Equal(t, "app", st.SchemaName)
	assert.Equal(t, "items", st.TableName)
	assert.Equal(t, "Table completed", st.TableState)
	assert.EqualValues(t, 3, st.FullLoadRows)
	assert.False(t, done.FullLoadFinishDate.IsZero())
}

func TestEngineFailureAndRestart(t *testing.T) {
	t.Parallel()

	e := newEngineEnv(t, "mysql", "full-load")
	e.op.src.pingErr = errUnreachable
	e.start(t, dms.ReplicationTaskCDCSettings{})

	failed := e.await(t, "failed")
	assert.Equal(t, "Stop Reason FATAL_ERROR", failed.StopReason)
	assert.Contains(t, failed.LastFailureMessage, "connection refused")
	assert.NotContains(t, failed.LastFailureMessage, "secret-pass")

	e.op.src.pingErr = nil
	close(e.op.src.hold)
	e.start(t, dms.ReplicationTaskCDCSettings{})

	done := e.await(t, "stopped")
	assert.Empty(t, done.LastFailureMessage)
	assert.Equal(t, "Stop Reason FULL_LOAD_ONLY_FINISHED", done.StopReason)
}

func TestEngineCDCStopAndResume(t *testing.T) {
	t.Parallel()

	e := newEngineEnv(t, "mysql", "cdc")
	e.start(t, dms.ReplicationTaskCDCSettings{CdcStartPosition: "bin.1:100"})
	e.await(t, "running")

	assert.Equal(t, dmsengine.StreamStart{Position: "bin.1:100"}, <-e.op.src.startAts)

	e.op.src.changes <- dmsengine.Change{Commit: true, Position: "bin.1:555"}
	require.Eventually(t, func() bool { return e.describe(t).RecoveryCheckpoint == "bin.1:555" },
		10*time.Second, 5*time.Millisecond)

	stopping, err := e.b.StopReplicationTask(t.Context(), e.task)
	require.NoError(t, err)
	assert.Equal(t, "stopping", stopping.Status)

	stopped := e.await(t, "stopped")
	assert.Equal(t, "Stop Reason NORMAL", stopped.StopReason)

	_, err = e.b.StopReplicationTask(t.Context(), e.task)
	require.ErrorIs(t, err, dms.ErrInvalidState)

	e.start(t, dms.ReplicationTaskCDCSettings{StartType: "resume-processing"})
	e.await(t, "running")
	assert.Equal(t, dmsengine.StreamStart{Position: "bin.1:555"}, <-e.op.src.startAts)
}

func TestEngineReloadTables(t *testing.T) {
	t.Parallel()

	e := newEngineEnv(t, "mysql", "full-load-and-cdc")
	_, err := e.b.ReloadTables(t.Context(), e.task, nil)
	require.ErrorIs(t, err, dms.ErrInvalidState, "task is not running yet")

	close(e.op.src.hold)
	e.start(t, dms.ReplicationTaskCDCSettings{})

	require.Eventually(t, func() bool {
		ts := e.describe(t).TableStats

		return e.describe(t).Status == "running" && len(ts) == 1 && ts[0].TableState == "Table completed"
	}, 10*time.Second, 5*time.Millisecond)

	_, err = e.b.ReloadTables(t.Context(), e.task, []dmsengine.TableRef{{Schema: "app", Name: "nope"}})
	require.ErrorIs(t, err, dms.ErrNotFound)

	_, err = e.b.ReloadTables(t.Context(), e.task, []dmsengine.TableRef{{Schema: "app", Name: "items"}})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		ts := e.describe(t).TableStats

		return len(ts) == 1 && ts[0].FullLoadReloaded && ts[0].TableState == "Table completed"
	}, 10*time.Second, 5*time.Millisecond)
}

func TestEngineTestConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		srcErr     error
		tgtErr     error
		endpoint   func(*engineEnv) string
		wantStatus string
		wantMsg    string
	}{
		{name: "source ok", endpoint: func(e *engineEnv) string { return e.src }, wantStatus: "successful"},
		{
			name: "source unreachable", srcErr: errUnreachable, endpoint: func(e *engineEnv) string { return e.src },
			wantStatus: "failed", wantMsg: "Failed to connect to database. " + errUnreachable.Error(),
		},
		{
			name: "target unreachable", tgtErr: errUnreachable, endpoint: func(e *engineEnv) string { return e.tgt },
			wantStatus: "failed", wantMsg: errUnreachable.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEngineEnv(t, "mysql", "full-load")
			e.op.src.pingErr, e.op.tgt.pingErr = tt.srcErr, tt.tgtErr

			conn, err := e.b.TestConnection(t.Context(), e.ri, tt.endpoint(e))
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, conn.Status)
			assert.Contains(t, conn.LastFailureMessage, tt.wantMsg)
			assert.NotContains(t, conn.LastFailureMessage, "secret-pass")

			listed, err := e.b.DescribeConnections(t.Context(), dms.DescribeFilters{})
			require.NoError(t, err)
			require.Len(t, listed, 1)
			assert.Equal(t, tt.wantStatus, listed[0].Status)
		})
	}
}

func TestEngineFallbackIsStateOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		srcEngine string
		engineOn  bool
	}{
		{name: "engine disabled", engineOn: false, srcEngine: "mysql"},
		{name: "unsupported source engine", engineOn: true, srcEngine: "oracle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEngineEnv(t, tt.srcEngine, "full-load")
			if !tt.engineOn {
				e.b = dms.NewInMemoryBackend("123456789012", "us-east-1")
				e = rebuildWithoutEngine(t, e.b)
			}

			started := e.start(t, dms.ReplicationTaskCDCSettings{})
			assert.Equal(t, "starting", started.Status)
			e.describe(t)
			assert.Equal(t, "running", e.describe(t).Status, "state-only tasks flip to running once described")
			assert.Empty(t, e.op.src.startAts)

			stopped, err := e.b.StopReplicationTask(t.Context(), e.task)
			require.NoError(t, err)
			assert.Equal(t, "stopped", stopped.Status)
			assert.Equal(t, "Stop Reason NORMAL", stopped.StopReason)
		})
	}
}

func rebuildWithoutEngine(t *testing.T, b *dms.InMemoryBackend) *engineEnv {
	t.Helper()

	ctx := t.Context()
	ri, err := b.CreateReplicationInstance(ctx, "ri", "dms.t3.micro", "", "", 0, false, false, false, nil,
		dms.ReplicationInstanceSettings{})
	require.NoError(t, err)

	src, err := b.CreateEndpoint(
		ctx,
		"src",
		"source",
		"mysql",
		"127.0.0.1",
		"app",
		"u",
		"p",
		3306,
		nil,
		dms.EndpointConnectionSettings{},
	)
	require.NoError(t, err)

	tgt, err := b.CreateEndpoint(
		ctx,
		"tgt",
		"target",
		"postgres",
		"127.0.0.1",
		"app",
		"u",
		"p",
		5432,
		nil,
		dms.EndpointConnectionSettings{},
	)
	require.NoError(t, err)

	task, err := b.CreateReplicationTask(ctx, "task", src.EndpointArn, tgt.EndpointArn, ri.ReplicationInstanceArn,
		"full-load", selectApp, "", nil, dms.ReplicationTaskCDCSettings{})
	require.NoError(t, err)

	return &engineEnv{
		b: b, op: &stubOpener{src: newStubSource()}, clock: &engineClock{t: time.Now()},
		ri: ri.ReplicationInstanceArn, src: src.EndpointArn, tgt: tgt.EndpointArn, task: task.ReplicationTaskArn,
	}
}

func TestEngineSnapshotSettlesRunningTasks(t *testing.T) {
	t.Parallel()

	e := newEngineEnv(t, "mysql", "cdc")
	e.start(t, dms.ReplicationTaskCDCSettings{})
	e.await(t, "running")

	snap := e.b.Snapshot(t.Context())

	restored := dms.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), snap))

	tasks, err := restored.DescribeReplicationTasks(t.Context(), dms.DescribeFilters{})
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, "stopped", tasks[0].Status)
	assert.NotEmpty(t, tasks[0].StopReason)
}

func TestEngineDeleteReleasesPostgresSlot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		srcEngine string
		wantSlot  bool
	}{
		{name: "postgres source", srcEngine: "postgres", wantSlot: true},
		{name: "mysql source", srcEngine: "mysql"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEngineEnv(t, tt.srcEngine, "full-load")
			_, err := e.b.DeleteReplicationTask(t.Context(), e.task)
			require.NoError(t, err)

			if !tt.wantSlot {
				select {
				case slot := <-e.op.released:
					assert.Failf(t, "unexpected release", "slot %s", slot)
				default:
				}

				return
			}

			select {
			case slot := <-e.op.released:
				assert.Regexp(t, `^dms_[0-9a-f]{16}$`, slot)
			case <-time.After(10 * time.Second):
				require.FailNow(t, "slot was not released")
			}
		})
	}
}

func TestEngineEndpointCertificateReachesTheEngine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sslMode string
		wantPEM string
		cert    bool
	}{
		{name: "require with certificate", sslMode: "require", cert: true, wantPEM: "PEM-DATA"},
		{name: "none with certificate still passes it along", sslMode: "none", cert: true, wantPEM: "PEM-DATA"},
		{name: "require without certificate", sslMode: "require"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEngineEnv(t, "mysql", "full-load")
			settings := dms.EndpointConnectionSettings{SslMode: tt.sslMode}

			if tt.cert {
				cert, err := e.b.ImportCertificate(t.Context(), "ca", "PEM-DATA", "", "", nil)
				require.NoError(t, err)

				settings.CertificateArn = cert.CertificateArn
			}

			ep, err := e.b.CreateEndpoint(t.Context(), "tls-src", "source", "mysql", "db.local", "app", "u", "p", 3306,
				nil, settings)
			require.NoError(t, err)

			conn, err := e.b.TestConnection(t.Context(), e.ri, ep.EndpointArn)
			require.NoError(t, err)
			assert.Equal(t, "successful", conn.Status)

			e.op.mu.Lock()
			defer e.op.mu.Unlock()

			assert.Equal(t, tt.sslMode, e.op.seen.SSLMode)
			assert.Equal(t, tt.wantPEM, e.op.seen.CertificatePEM)
		})
	}
}
