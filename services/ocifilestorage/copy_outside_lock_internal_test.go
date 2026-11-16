package ocifilestorage

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	copyComp  = "ocid1.compartment.oc1..comp"
	copyAD    = "Uocm:IAD-AD-1"
	copyGuard = 60 * time.Second
)

// hookPlane runs a hook before each data copy so a test can hold the copy open.
type hookPlane struct {
	*NFSPlane
	take      func(context.Context)
	clone     func(context.Context)
	replicate func(context.Context)
	usage     func()
}

func (h *hookPlane) TakeSnapshot(ctx context.Context, fsID, name string) error {
	if h.take != nil {
		h.take(ctx)
	}

	return h.NFSPlane.TakeSnapshot(ctx, fsID, name)
}

func (h *hookPlane) CloneFrom(ctx context.Context, srcFS, snapshot, dstFS string) error {
	if h.clone != nil {
		h.clone(ctx)
	}

	return h.NFSPlane.CloneFrom(ctx, srcFS, snapshot, dstFS)
}

func (h *hookPlane) ReplicateSnapshot(ctx context.Context, srcFS, snapshot, dstFS string) (int64, error) {
	if h.replicate != nil {
		h.replicate(ctx)
	}

	return h.NFSPlane.ReplicateSnapshot(ctx, srcFS, snapshot, dstFS)
}

func (h *hookPlane) Usage(id string) int64 {
	if h.usage != nil {
		h.usage()
	}

	return h.NFSPlane.Usage(id)
}

func (h *hookPlane) SnapshotExclusiveBytes(id, name string) int64 {
	if h.usage != nil {
		h.usage()
	}

	return h.NFSPlane.SnapshotExclusiveBytes(id, name)
}

// blocker holds a copy open until release is closed.
type blocker struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlocker() *blocker {
	return &blocker{entered: make(chan struct{}), release: make(chan struct{})}
}

func (bl *blocker) hook(context.Context) {
	bl.once.Do(func() { close(bl.entered) })
	<-bl.release
}

type copyEnv struct {
	b     *InMemoryBackend
	hp    *hookPlane
	clock *atomic.Int64
	src   FileSystem
}

func newCopyEnv(t *testing.T) *copyEnv {
	t.Helper()

	plane, err := NewNFSPlane(NFSConfig{Dir: t.TempDir()})
	require.NoError(t, err)

	hp := &hookPlane{NFSPlane: plane}
	clock := &atomic.Int64{}
	clock.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())

	b := NewInMemoryBackend()
	b.SetTransition(0)
	b.SetClock(func() time.Time { return time.Unix(0, clock.Load()).UTC() })
	b.SetDataPlane(hp)
	t.Cleanup(b.Close)

	e := &copyEnv{b: b, hp: hp, clock: clock}
	e.src = e.newFS(t, "src")
	require.NoError(t, os.WriteFile(filepath.Join(plane.fsDir(e.src.ID), "f"), []byte("data"), 0o600))

	return e
}

func (e *copyEnv) newFS(t *testing.T, name string) FileSystem {
	t.Helper()

	fs, _, err := e.b.CreateFileSystem(createFSIn{
		AvailabilityDomain: copyAD, CompartmentID: copyComp, DisplayName: &name,
	}, "")
	require.NoError(t, err)

	return fs
}

func (e *copyEnv) snapshot(t *testing.T, name string) Snapshot {
	t.Helper()

	sn, _, err := e.b.CreateSnapshot(createSnapshotIn{FileSystemID: e.src.ID, Name: name}, "")
	require.NoError(t, err)

	return sn
}

func (e *copyEnv) listFS(t *testing.T) []map[string]any {
	t.Helper()

	pg, err := e.b.ListFileSystems(FSFilter{Compartment: copyComp, AD: copyAD}, ListOpts{})
	require.NoError(t, err)

	return pg.Data
}

// within fails the test when fn does not return: a backend call blocked behind a copy never would.
func within(t *testing.T, what string, fn func()) {
	t.Helper()

	done := make(chan struct{})

	go func() {
		defer close(done)

		fn()
	}()

	select {
	case <-done:
	case <-time.After(copyGuard):
		t.Fatalf("%s did not return while a copy was in flight", what)
	}
}

func (e *copyEnv) otherCallsProceed(t *testing.T) {
	t.Helper()

	within(t, "GetFileSystem", func() {
		_, _, err := e.b.GetFileSystem(e.src.ID)
		assert.NoError(t, err)
	})
	within(t, "ListFileSystems", func() { assert.NotEmpty(t, e.listFS(t)) })
}

func (e *copyEnv) replicationEnv(t *testing.T) Replication {
	t.Helper()

	dst := e.newFS(t, "dst")
	interval := int64(minReplicationMinutes)

	r, _, err := e.b.CreateReplication(createReplicationIn{
		CompartmentID: copyComp, SourceID: e.src.ID, TargetID: dst.ID, ReplicationInterval: &interval,
	}, "")
	require.NoError(t, err)

	return r
}

func (e *copyEnv) replication(t *testing.T, id string) Replication {
	t.Helper()

	r, _, err := e.b.GetReplication(id)
	require.NoError(t, err)

	return r
}

// asyncSweep runs Sweep on its own goroutine and reports when it returns.
func (e *copyEnv) asyncSweep() <-chan struct{} {
	done := make(chan struct{})

	go func() {
		defer close(done)

		e.b.Sweep()
	}()

	return done
}

func TestDataCopiesRunOutsideTheBackendLock(t *testing.T) {
	t.Parallel()

	t.Run("snapshot", func(t *testing.T) {
		t.Parallel()

		e := newCopyEnv(t)
		bl := newBlocker()
		e.hp.take = bl.hook

		done := make(chan error, 1)

		go func() {
			_, _, err := e.b.CreateSnapshot(createSnapshotIn{FileSystemID: e.src.ID, Name: "s1"}, "")
			done <- err
		}()

		<-bl.entered
		e.otherCallsProceed(t)

		pg, err := e.b.ListSnapshots(SnapshotFilter{FileSystem: e.src.ID}, ListOpts{})
		require.NoError(t, err)
		require.Len(t, pg.Data, 1)
		assert.Equal(t, stCreating, pg.Data[0]["lifecycleState"])

		close(bl.release)
		require.NoError(t, <-done)

		pg, err = e.b.ListSnapshots(SnapshotFilter{FileSystem: e.src.ID}, ListOpts{})
		require.NoError(t, err)
		assert.Equal(t, stActive, pg.Data[0]["lifecycleState"])
		assert.FileExists(t, filepath.Join(e.hp.snapDir(e.src.ID, "s1"), "f"))
	})

	t.Run("clone", func(t *testing.T) {
		t.Parallel()

		e := newCopyEnv(t)
		sn := e.snapshot(t, "s1")
		bl := newBlocker()
		e.hp.clone = bl.hook

		done := make(chan error, 1)

		go func() {
			_, _, err := e.b.CreateFileSystem(createFSIn{
				AvailabilityDomain: copyAD, CompartmentID: copyComp, SourceSnapshotID: &sn.ID,
			}, "")
			done <- err
		}()

		<-bl.entered
		e.otherCallsProceed(t)

		var cloneID string

		for _, m := range e.listFS(t) {
			if m["lifecycleState"] == stCreating {
				cloneID, _ = m["id"].(string)
			}
		}

		require.NotEmpty(t, cloneID, "the clone is visible as CREATING while its data is copied")

		close(bl.release)
		require.NoError(t, <-done)

		got, _, err := e.b.GetFileSystem(cloneID)
		require.NoError(t, err)
		assert.Equal(t, stActive, got.State)
		assert.FileExists(t, filepath.Join(e.hp.fsDir(cloneID), "f"))
	})

	t.Run("replication capture", func(t *testing.T) {
		t.Parallel()

		e := newCopyEnv(t)
		r := e.replicationEnv(t)
		bl := newBlocker()
		e.hp.take = bl.hook

		swept := e.asyncSweep()

		<-bl.entered
		e.otherCallsProceed(t)
		assert.Equal(t, deltaCapturing, e.replication(t, r.ID).DeltaStatus)

		close(bl.release)
		<-swept

		pg, err := e.b.ListSnapshots(SnapshotFilter{FileSystem: e.src.ID}, ListOpts{})
		require.NoError(t, err)
		require.Len(t, pg.Data, 1)
		assert.Equal(t, stActive, pg.Data[0]["lifecycleState"])
	})

	t.Run("replication apply", func(t *testing.T) {
		t.Parallel()

		e := newCopyEnv(t)
		r := e.replicationEnv(t)

		for range 3 {
			e.b.Sweep()
		}

		require.Equal(t, deltaApplying, e.replication(t, r.ID).DeltaStatus)

		bl := newBlocker()
		e.hp.replicate = bl.hook

		swept := e.asyncSweep()

		<-bl.entered
		e.otherCallsProceed(t)
		assert.Equal(t, deltaApplying, e.replication(t, r.ID).DeltaStatus)

		close(bl.release)
		<-swept

		assert.Equal(t, deltaIdle, e.replication(t, r.ID).DeltaStatus)
		assert.FileExists(t, filepath.Join(e.hp.fsDir(r.TargetID), "f"))
	})
}

func (e *copyEnv) noOrphans(t *testing.T, fsID string) {
	t.Helper()

	assert.NoDirExists(t, e.hp.fsDir(fsID))
	assert.NoDirExists(t, e.hp.snapRoot(fsID))
}

func TestDeletionDuringCopyLeavesNoOrphans(t *testing.T) {
	t.Parallel()

	t.Run("file system during snapshot", func(t *testing.T) {
		t.Parallel()

		e := newCopyEnv(t)
		bl := newBlocker()
		e.hp.take = bl.hook

		done := make(chan error, 1)

		go func() {
			_, _, err := e.b.CreateSnapshot(createSnapshotIn{FileSystemID: e.src.ID, Name: "s1"}, "")
			done <- err
		}()

		<-bl.entered
		within(t, "DeleteFileSystem", func() {
			assert.NoError(t, e.b.DeleteFileSystem(e.src.ID, "", false, false))
		})

		close(bl.release)
		require.NoError(t, <-done)
		e.noOrphans(t, e.src.ID)
	})

	t.Run("snapshot during snapshot", func(t *testing.T) {
		t.Parallel()

		e := newCopyEnv(t)
		bl := newBlocker()
		e.hp.take = bl.hook

		done := make(chan error, 1)

		go func() {
			_, _, err := e.b.CreateSnapshot(createSnapshotIn{FileSystemID: e.src.ID, Name: "s1"}, "")
			done <- err
		}()

		<-bl.entered

		pg, err := e.b.ListSnapshots(SnapshotFilter{FileSystem: e.src.ID}, ListOpts{})
		require.NoError(t, err)

		id, _ := pg.Data[0]["id"].(string)

		_, _, err = e.b.CreateSnapshot(createSnapshotIn{FileSystemID: e.src.ID, Name: "s1"}, "")
		require.Error(t, err, "the name stays taken while the copy runs")

		within(t, "DeleteSnapshot", func() { assert.NoError(t, e.b.DeleteSnapshot(id, "", false)) })

		_, _, err = e.b.CreateSnapshot(createSnapshotIn{FileSystemID: e.src.ID, Name: "s1"}, "")
		require.Error(t, err, "the name stays reserved until the abandoned copy is cleaned up")

		close(bl.release)
		require.NoError(t, <-done)
		assert.NoDirExists(t, e.hp.snapDir(e.src.ID, "s1"))
		assert.DirExists(t, e.hp.fsDir(e.src.ID))

		_, _, err = e.b.CreateSnapshot(createSnapshotIn{FileSystemID: e.src.ID, Name: "s1"}, "")
		require.NoError(t, err)
	})

	t.Run("clone during clone", func(t *testing.T) {
		t.Parallel()

		e := newCopyEnv(t)
		sn := e.snapshot(t, "s1")
		bl := newBlocker()
		e.hp.clone = bl.hook

		done := make(chan error, 1)

		go func() {
			_, _, err := e.b.CreateFileSystem(createFSIn{
				AvailabilityDomain: copyAD, CompartmentID: copyComp, SourceSnapshotID: &sn.ID,
			}, "")
			done <- err
		}()

		<-bl.entered

		var cloneID string

		for _, m := range e.listFS(t) {
			if m["lifecycleState"] == stCreating {
				cloneID, _ = m["id"].(string)
			}
		}

		require.NotEmpty(t, cloneID)
		within(t, "DeleteFileSystem", func() {
			assert.NoError(t, e.b.DeleteFileSystem(cloneID, "", false, false))
		})

		close(bl.release)
		require.NoError(t, <-done)
		e.noOrphans(t, cloneID)
	})

	t.Run("parent during clone", func(t *testing.T) {
		t.Parallel()

		e := newCopyEnv(t)
		sn := e.snapshot(t, "s1")
		bl := newBlocker()
		e.hp.clone = bl.hook

		done := make(chan error, 1)

		go func() {
			_, _, err := e.b.CreateFileSystem(createFSIn{
				AvailabilityDomain: copyAD, CompartmentID: copyComp, SourceSnapshotID: &sn.ID,
			}, "")
			done <- err
		}()

		<-bl.entered
		within(t, "DeleteSnapshot", func() { assert.NoError(t, e.b.DeleteSnapshot(sn.ID, "", false)) })

		close(bl.release)
		require.Error(t, <-done, "a clone whose source vanished fails")

		parent, _, err := e.b.GetFileSystem(e.src.ID)
		require.NoError(t, err)
		assert.Zero(t, parent.CloneCount)
		assert.False(t, parent.IsCloneParent)

		for _, m := range e.listFS(t) {
			assert.NotEqual(t, stCreating, m["lifecycleState"])
		}
	})
}

func TestCopyingBackendClosesCleanly(t *testing.T) {
	t.Parallel()

	e := newCopyEnv(t)
	entered := make(chan struct{})
	e.hp.take = func(ctx context.Context) {
		close(entered)
		<-ctx.Done()
	}

	done := make(chan error, 1)

	go func() {
		_, _, err := e.b.CreateSnapshot(createSnapshotIn{FileSystemID: e.src.ID, Name: "s1"}, "")
		done <- err
	}()

	<-entered

	closed := make(chan struct{})

	go func() {
		defer close(closed)

		e.b.Close()
	}()

	<-closed
	require.Error(t, <-done, "the copy honours the cancelled context")
	assert.NoDirExists(t, e.hp.snapDir(e.src.ID, "s1"))
}

func TestWriteSnapshotCloneReplicationAndQuotaLookupDoNotDeadlock(t *testing.T) {
	t.Parallel()

	const rounds = 12

	e := newCopyEnv(t)
	yes := true
	limit := 1 << 20

	quotaFS, _, err := e.b.CreateFileSystem(createFSIn{
		AvailabilityDomain: copyAD, CompartmentID: copyComp, AreQuotaRulesEnabled: &yes,
	}, "")
	require.NoError(t, err)

	_, _, err = e.b.CreateQuotaRule(quotaFS.ID, createQuotaIn{
		PrincipalType: qFileSystemLevel, IsHardQuota: &yes, QuotaLimitGB: &limit,
	})
	require.NoError(t, err)

	e.src = quotaFS
	base := e.snapshot(t, "base")
	r := e.replicationEnv(t)

	v, err := e.hp.view(ExportMount{
		ExportID: "x1", FileSystemID: quotaFS.ID, Quotas: e.b.quotaSource(quotaFS.ID),
	}, false)
	require.NoError(t, err)

	var wg sync.WaitGroup

	loop := func(fn func(i int)) {
		wg.Go(func() {
			for i := range rounds {
				fn(i)
			}
		})
	}

	for w := range 4 {
		loop(func(i int) {
			f, werr := v.OpenFile("/w"+strconv.Itoa(w)+"-"+strconv.Itoa(i), os.O_RDWR|os.O_CREATE, 0o644)
			if assert.NoError(t, werr) {
				_, werr = f.Write([]byte("0123456789"))
				assert.NoError(t, werr)
			}
		})
	}

	loop(func(i int) {
		_, _, serr := e.b.CreateSnapshot(createSnapshotIn{FileSystemID: quotaFS.ID, Name: "s" + strconv.Itoa(i)}, "")
		if serr != nil {
			t.Error(serr)
		}
	})
	loop(func(int) {
		_, _, cerr := e.b.CreateFileSystem(createFSIn{
			AvailabilityDomain: copyAD, CompartmentID: copyComp, SourceSnapshotID: &base.ID,
		}, "")
		if cerr != nil {
			t.Error(cerr)
		}
	})
	loop(func(int) {
		e.clock.Add(int64(time.Duration(minReplicationMinutes+1) * time.Minute))
		e.b.Sweep()
	})
	loop(func(int) {
		_, qerr := e.b.ListQuotaRules(quotaFS.ID, QuotaFilter{}, ListOpts{})
		if qerr != nil {
			t.Error(qerr)
		}

		_, _, gerr := e.b.GetFileSystem(quotaFS.ID)
		if gerr != nil {
			t.Error(gerr)
		}

		_, _, rerr := e.b.GetReplication(r.ID)
		if rerr != nil {
			t.Error(rerr)
		}
	})

	finished := make(chan struct{})

	go func() {
		defer close(finished)

		wg.Wait()
	}()

	select {
	case <-finished:
	case <-time.After(copyGuard):
		t.Fatal("writers, snapshots, clones, replication and quota lookups deadlocked")
	}
}

func TestRestoreFailsCopiesTheSnapshotInterrupted(t *testing.T) {
	t.Parallel()

	e := newCopyEnv(t)
	bl := newBlocker()
	e.hp.take = bl.hook

	done := make(chan error, 1)

	go func() {
		_, _, err := e.b.CreateSnapshot(createSnapshotIn{FileSystemID: e.src.ID, Name: "s1"}, "")
		done <- err
	}()

	<-bl.entered

	data := e.b.Snapshot(t.Context())

	close(bl.release)
	require.NoError(t, <-done)

	other := newCopyEnv(t)
	require.NoError(t, other.b.Restore(t.Context(), data))

	pg, err := other.b.ListSnapshots(SnapshotFilter{FileSystem: e.src.ID}, ListOpts{})
	require.NoError(t, err)
	require.Len(t, pg.Data, 1)
	assert.Equal(t, stFailed, pg.Data[0]["lifecycleState"])
}
