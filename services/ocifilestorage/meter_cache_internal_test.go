package ocifilestorage

import (
	"os"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const probeGuard = 10 * time.Second

func (e *copyEnv) view(t *testing.T, quotas func() []QuotaLimit) *vfs {
	t.Helper()

	v, err := e.hp.view(ExportMount{ExportID: "x1", FileSystemID: e.src.ID, Quotas: quotas}, false)
	require.NoError(t, err)

	return v
}

func (e *copyEnv) readViews(t *testing.T) {
	t.Helper()

	_, _, err := e.b.GetFileSystem(e.src.ID)
	require.NoError(t, err)
	e.listFS(t)

	pg, err := e.b.ListSnapshots(SnapshotFilter{FileSystem: e.src.ID}, ListOpts{})
	require.NoError(t, err)

	for _, m := range pg.Data {
		id, _ := m["id"].(string)
		_, _, err = e.b.GetSnapshot(id)
		require.NoError(t, err)
	}
}

func (e *copyEnv) walks(t *testing.T) int {
	t.Helper()

	q := e.hp.stateOf(e.src.ID)
	require.NotNil(t, q)

	return q.meter.walks
}

func TestMeterIsNotWalkedPerView(t *testing.T) {
	t.Parallel()

	e := newCopyEnv(t)

	for i := range 5 {
		e.snapshot(t, "s"+strconv.Itoa(i))
	}

	base := e.walks(t)

	for range 3 {
		e.readViews(t)
	}

	assert.Equal(t, base, e.walks(t), "repeated views share the cached walk")

	v := e.view(t, nil)
	f, err := v.OpenFile("/g", os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	_, err = f.Write([]byte("0123456789"))
	require.NoError(t, err)

	fs, _, err := e.b.GetFileSystem(e.src.ID)
	require.NoError(t, err)
	assert.EqualValues(t, 14, fs.MeteredBytes)
	assert.Equal(t, base+1, e.walks(t), "a write invalidates the cache")

	acc := e.view(t, hardFSRule)
	_, err = acc.OpenFile("/g", os.O_RDWR, 0o644)
	require.NoError(t, err)
	e.readViews(t)
	assert.Equal(t, base+1, e.walks(t))

	h, err := acc.OpenFile("/g", os.O_RDWR, 0o644)
	require.NoError(t, err)
	_, err = h.Write([]byte("abc"))
	require.NoError(t, err)

	fs, _, err = e.b.GetFileSystem(e.src.ID)
	require.NoError(t, err)
	assert.EqualValues(t, 14, fs.MeteredBytes, "an in-place accounted write keeps the file version count")
	assert.Equal(t, base+2, e.walks(t), "an accounted write invalidates the cache too")
}

func TestMeteringDoesNotHoldTheBackendLock(t *testing.T) {
	t.Parallel()

	e := newCopyEnv(t)
	sn := e.snapshot(t, "s1")

	var probes, stuck atomic.Int32

	e.hp.usage = func() {
		probes.Add(1)

		done := make(chan struct{})

		go func() {
			defer close(done)

			e.b.mu.Lock("probe")
			e.b.mu.Unlock()
		}()

		select {
		case <-done:
		case <-time.After(probeGuard):
			stuck.Add(1)
		}
	}

	e.readViews(t)

	_, _, err := e.b.CreateSnapshot(createSnapshotIn{FileSystemID: e.src.ID, Name: "s2"}, "")
	require.NoError(t, err)
	_, _, err = e.b.UpdateSnapshot(sn.ID, "", updateSnapshotIn{})
	require.NoError(t, err)
	_, _, err = e.b.CreateFileSystem(createFSIn{
		AvailabilityDomain: copyAD, CompartmentID: copyComp, SourceSnapshotID: &sn.ID,
	}, "")
	require.NoError(t, err)

	assert.Positive(t, probes.Load())
	assert.Zero(t, stuck.Load(), "a data plane walk ran while the backend lock was held")
}

func TestQuotaAppliesToTempFilesAndAppendOpens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		open func(v *vfs) (growable, error)
		name string
	}{
		{name: "temp file", open: func(v *vfs) (growable, error) { return v.TempFile("/", "t-") }},
		{name: "append reopen", open: func(v *vfs) (growable, error) {
			first, err := v.OpenFile("/a", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
			if err != nil {
				return nil, err
			}

			if _, err = first.Write(make([]byte, raceLimit*6/10)); err != nil {
				return nil, err
			}

			return v.OpenFile("/a", os.O_WRONLY|os.O_APPEND, 0o644)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, v := exportView(t, hardFSRule)

			f, err := tt.open(v)
			require.NoError(t, err)

			if tt.name == "temp file" {
				_, err = f.Write(make([]byte, raceLimit*6/10))
				require.NoError(t, err)
			}

			_, err = f.Write(make([]byte, raceLimit*6/10))
			require.ErrorIs(t, err, syscall.EDQUOT)
		})
	}
}

func TestTempFileReportsAnExportRelativeName(t *testing.T) {
	t.Parallel()

	_, v := exportView(t, hardFSRule)

	f, err := v.TempFile("/", "t-")
	require.NoError(t, err)

	assert.Regexp(t, `^/t-`, f.Name())
	assert.FileExists(t, v.live.Root()+f.Name())
}
