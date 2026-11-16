package ocifilestorage

import (
	"os"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const raceLimit = 100

type growable interface {
	Seek(int64, int) (int64, error)
	Write([]byte) (int, error)
	Truncate(int64) error
}

func hardFSRule() []QuotaLimit {
	return []QuotaLimit{{Type: qFileSystemLevel, LimitBytes: raceLimit, Hard: true}}
}

func exportView(t *testing.T, quotas func() []QuotaLimit) (*NFSPlane, *vfs) {
	t.Helper()

	p, err := NewNFSPlane(NFSConfig{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(p.Close)
	require.NoError(t, p.CreateFS("fs1"))

	v, err := p.view(ExportMount{ExportID: "x1", FileSystemID: "fs1", Quotas: quotas}, false)
	require.NoError(t, err)

	return p, v
}

func TestConcurrentWritesStayWithinHardQuota(t *testing.T) {
	t.Parallel()

	const writers = 40

	_, v := exportView(t, hardFSRule)

	var (
		wg sync.WaitGroup
		ok = make(chan struct{}, writers)
	)

	for i := range writers {
		wg.Go(func() {
			f, err := v.OpenFile("/f"+strconv.Itoa(i), os.O_RDWR|os.O_CREATE, 0o644)
			if err != nil {
				return
			}

			if _, err = f.Write(make([]byte, 10)); err == nil {
				ok <- struct{}{}
			}
		})
	}

	wg.Wait()
	close(ok)

	assert.Len(t, ok, raceLimit/10)
}

func TestSnapshotDoesNotDeadlockWithQuotaLookup(t *testing.T) {
	t.Parallel()

	backend := lockmetrics.New("test.backend")
	reached, release := make(chan struct{}), make(chan struct{})

	p, v := exportView(t, func() []QuotaLimit {
		close(reached)
		<-release
		backend.RLock("quotas")
		defer backend.RUnlock()

		return hardFSRule()
	})

	f, err := v.OpenFile("/f", os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)

	wrote := make(chan error, 1)

	go func() {
		_, werr := f.Write([]byte("x"))
		wrote <- werr
	}()

	<-reached

	snapped := make(chan error, 1)

	go func() {
		backend.Lock("snapshot")
		defer backend.Unlock()

		snapped <- p.TakeSnapshot(t.Context(), "fs1", "s1")
	}()

	gate := p.gate("fs1")
	for gate.WriteWaiters() == 0 && len(snapped) == 0 {
		runtime.Gosched()
	}

	close(release)

	for range 2 {
		select {
		case err = <-wrote:
			require.NoError(t, err)
		case err = <-snapped:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("write and snapshot deadlocked")
		}
	}
}

func TestQuotaUsageIsNotWalkedPerWrite(t *testing.T) {
	t.Parallel()

	_, v := exportView(t, func() []QuotaLimit {
		return []QuotaLimit{{Type: qFileSystemLevel, LimitBytes: 1 << 20, Hard: true}}
	})

	f, err := v.OpenFile("/f", os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)

	for range 20 {
		_, err = f.Write([]byte("0123456789"))
		require.NoError(t, err)
	}

	assert.Equal(t, 1, v.qs.walks)
	assert.EqualValues(t, 200, v.qs.usage.Total)

	require.NoError(t, v.Remove("/f"))

	g, err := v.OpenFile("/g", os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)

	_, err = g.Write([]byte("abc"))
	require.NoError(t, err)

	assert.Equal(t, 2, v.qs.walks)
	assert.EqualValues(t, 3, v.qs.usage.Total)
}

func TestHardQuotaCountsSparseAndTruncatedGrowth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act     func(f growable) error
		name    string
		blocked bool
	}{
		{name: "sparse write past limit", blocked: true, act: func(f growable) error {
			_, _ = f.Seek(raceLimit*2, 0)
			_, err := f.Write([]byte("x"))

			return err
		}},
		{name: "truncate extend past limit", blocked: true, act: func(f growable) error {
			return f.Truncate(raceLimit * 2)
		}},
		{name: "truncate within limit", blocked: false, act: func(f growable) error {
			return f.Truncate(raceLimit / 2)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, v := exportView(t, hardFSRule)

			f, err := v.OpenFile("/f", os.O_RDWR|os.O_CREATE, 0o644)
			require.NoError(t, err)

			err = tt.act(f)
			if tt.blocked {
				require.ErrorIs(t, err, syscall.EDQUOT)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
