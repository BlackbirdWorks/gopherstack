package ocifilestorage

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func putFile(t *testing.T, p *NFSPlane, name string, size int, mtime time.Time) {
	t.Helper()

	full := filepath.Join(p.fsDir("fs1"), name)
	require.NoError(t, os.WriteFile(full, make([]byte, size), 0o600))
	require.NoError(t, os.Chtimes(full, mtime, mtime))
}

func TestMeteringCountsDifferentiatedSnapshotData(t *testing.T) {
	t.Parallel()

	p, err := NewNFSPlane(NFSConfig{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(p.Close)
	require.NoError(t, p.CreateFS("fs1"))

	t0 := time.Unix(1_700_000_000, 0)
	putFile(t, p, "a.txt", 10, t0)
	require.NoError(t, p.TakeSnapshot("fs1", "s1"))

	assert.EqualValues(t, 10, p.Usage("fs1"), "an unchanged snapshot adds nothing")
	assert.Zero(t, p.SnapshotExclusiveBytes("fs1", "s1"))

	putFile(t, p, "a.txt", 25, t0.Add(time.Second))
	assert.EqualValues(t, 35, p.Usage("fs1"))
	assert.EqualValues(t, 10, p.SnapshotExclusiveBytes("fs1", "s1"))

	require.NoError(t, p.TakeSnapshot("fs1", "s2"))
	assert.EqualValues(t, 35, p.Usage("fs1"))
	assert.Zero(t, p.SnapshotExclusiveBytes("fs1", "s2"))

	require.NoError(t, os.Remove(filepath.Join(p.fsDir("fs1"), "a.txt")))
	assert.EqualValues(t, 35, p.Usage("fs1"), "deleted live data stays metered while snapshots hold it")
	assert.EqualValues(t, 25, p.SnapshotExclusiveBytes("fs1", "s2"))

	p.DropSnapshot("fs1", "s1")
	assert.EqualValues(t, 25, p.Usage("fs1"))
}

func TestSnapshotWaitsForInFlightWrites(t *testing.T) {
	t.Parallel()

	p, err := NewNFSPlane(NFSConfig{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(p.Close)
	require.NoError(t, p.CreateFS("fs1"))

	gate := p.gate("fs1")
	gate.RLock("test write")

	done := make(chan error, 1)

	go func() { done <- p.TakeSnapshot("fs1", "s1") }()

	for gate.WriteWaiters() == 0 {
		runtime.Gosched()
	}

	select {
	case <-done:
		t.Fatal("snapshot completed while a write was in flight")
	default:
	}

	gate.RUnlock()
	require.NoError(t, <-done)
	assert.DirExists(t, p.snapDir("fs1", "s1"))
}
