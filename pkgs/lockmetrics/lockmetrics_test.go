package lockmetrics_test

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRWMutex_New(t *testing.T) {
	t.Parallel()

	m := lockmetrics.New("test.lock")
	require.NotNil(t, m)
}

func TestRWMutex_LockUnlock(t *testing.T) {
	t.Parallel()

	m := lockmetrics.New("test.write")
	m.Lock("TestOp")
	m.Unlock()
}

func TestRWMutex_RLockRUnlock(t *testing.T) {
	t.Parallel()

	m := lockmetrics.New("test.read")
	m.RLock("TestReadOp")
	m.RUnlock()
}

func TestRWMutex_MutualExclusion(t *testing.T) {
	t.Parallel()

	m := lockmetrics.New("test.exclusive")
	var counter int
	var wg sync.WaitGroup

	const goroutines = 50
	for range goroutines {
		wg.Go(func() {
			m.Lock("increment")
			counter++
			m.Unlock()
		})
	}

	wg.Wait()
	assert.Equal(t, goroutines, counter)
}

func TestRWMutex_ReadersCanCoexist(t *testing.T) {
	t.Parallel()

	m := lockmetrics.New("test.readers")
	m.Lock("setup")
	m.Unlock()

	var wg sync.WaitGroup
	started := make(chan struct{})

	const readers = 10
	for range readers {
		wg.Go(func() {
			m.RLock("concurrent-read")
			started <- struct{}{}
			time.Sleep(10 * time.Millisecond)
			m.RUnlock()
		})
	}

	go func() {
		wg.Wait()
		close(started)
	}()

	count := 0
	for range started {
		count++
	}
	assert.Equal(t, readers, count)
}

func TestRWMutex_WriteBlocksRead(t *testing.T) {
	t.Parallel()

	m := lockmetrics.New("test.write-blocks-read")
	m.Lock("holding")

	done := make(chan struct{})
	go func() {
		m.RLock("waiting-reader")
		m.RUnlock()
		close(done)
	}()

	select {
	case <-done:
		require.FailNow(t, "reader should be blocked while write lock is held")
	case <-time.After(20 * time.Millisecond):
		// expected: reader is waiting
	}

	m.Unlock()

	select {
	case <-done:
		// reader unblocked after write lock released
	case <-time.After(time.Second):
		require.FailNow(t, "reader should have unblocked after write lock released")
	}
}

func TestRWMutex_DeferPattern(t *testing.T) {
	t.Parallel()

	m := lockmetrics.New("test.defer")

	func() {
		m.Lock("deferOp")
		defer m.Unlock()
	}()

	acquired := make(chan struct{})
	go func() {
		m.Lock("afterDefer")
		defer m.Unlock()
		close(acquired)
	}()

	select {
	case <-acquired:
	case <-time.After(time.Second):
		require.FailNow(t, "lock should have been acquirable after deferred unlock")
	}
}

func TestRWMutex_DeferRLockPattern(t *testing.T) {
	t.Parallel()

	m := lockmetrics.New("test.defer-rlock")

	func() {
		m.RLock("readOp")
		defer m.RUnlock()
	}()

	acquired := make(chan struct{})
	go func() {
		m.Lock("afterRUnlock")
		defer m.Unlock()
		close(acquired)
	}()

	select {
	case <-acquired:
	case <-time.After(time.Second):
		require.FailNow(t, "write lock should be acquirable after read lock is released")
	}
}

func TestRWMutex_WriteWaiters(t *testing.T) {
	t.Parallel()

	m := lockmetrics.New("test.write-waiters")

	m.Lock("holder")

	done := make(chan struct{})
	go func() {
		m.Lock("waiter")
		defer m.Unlock()
		close(done)
	}()

	const pollTimeout = time.Second
	deadline := time.Now().Add(pollTimeout)
	for time.Now().Before(deadline) && m.WriteWaiters() == 0 {
		runtime.Gosched()
	}

	assert.EqualValues(t, 1, m.WriteWaiters(), "expected 1 write waiter while lock is held")

	m.Unlock()
	<-done
	assert.EqualValues(t, 0, m.WriteWaiters(), "expected 0 write waiters after lock released")
}

func TestRWMutex_ReadWaiters(t *testing.T) {
	t.Parallel()

	m := lockmetrics.New("test.read-waiters")

	m.Lock("holder")

	done := make(chan struct{})
	go func() {
		m.RLock("reader")
		defer m.RUnlock()
		close(done)
	}()

	const pollTimeout = time.Second
	deadline := time.Now().Add(pollTimeout)
	for time.Now().Before(deadline) && m.ReadWaiters() == 0 {
		runtime.Gosched()
	}

	assert.EqualValues(t, 1, m.ReadWaiters(), "expected 1 read waiter while write lock is held")

	m.Unlock()
	<-done
	assert.EqualValues(t, 0, m.ReadWaiters(), "expected 0 read waiters after write lock released")
}

func TestRWMutex_GetLockStatus(t *testing.T) {
	t.Parallel()

	m := lockmetrics.New("test.lock-status")

	// Initially unlocked
	locked, ww, rw := m.GetLockStatus()
	assert.False(t, locked)
	assert.EqualValues(t, 0, ww)
	assert.EqualValues(t, 0, rw)

	// Lock it
	m.Lock("holder")
	locked, ww, rw = m.GetLockStatus()
	assert.True(t, locked)
	assert.EqualValues(t, 0, ww)
	assert.EqualValues(t, 0, rw)

	// Add a waiter
	done := make(chan struct{})
	go func() {
		m.Lock("waiter")
		m.Unlock()
		close(done)
	}()

	// Poll until waiter is registered
	for range 10 {
		_, ww, _ = m.GetLockStatus()
		if ww > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	locked, ww, rw = m.GetLockStatus()
	assert.True(t, locked)
	assert.EqualValues(t, 1, ww)
	assert.EqualValues(t, 0, rw)

	// Unlock
	m.Unlock()
	<-done

	locked, ww, rw = m.GetLockStatus()
	assert.False(t, locked)
	assert.EqualValues(t, 0, ww)
	assert.EqualValues(t, 0, rw)
}

func TestRWMutex_Close(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, m *lockmetrics.RWMutex)
		name  string
	}{
		{name: "new"},
		{
			name: "after_use",
			setup: func(_ *testing.T, m *lockmetrics.RWMutex) {
				m.Lock("op")
				m.Unlock()
				m.RLock("op")
				m.RUnlock()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := lockmetrics.New("test." + tt.name)
			if tt.setup != nil {
				tt.setup(t, m)
			}

			m.Close()
		})
	}
}

func TestRWMutex_CloseRemovesLabelValues(t *testing.T) {
	t.Parallel()

	// Use a unique name so the test is isolated from parallel runs.
	name := "test.close.leak." + t.Name()
	m := lockmetrics.New(name)

	// Perform operations that create labelled series.
	m.Lock("op-a")
	m.Unlock()
	m.RLock("op-b")
	m.RUnlock()

	// Gather before close to confirm the series exist.
	beforeMFs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	beforeCount := countSeriesForLock(beforeMFs, name)
	assert.Positive(t, beforeCount, "expected at least one series before Close")

	m.Close()

	// Gather after close — all series for this lock should be gone.
	afterMFs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	afterCount := countSeriesForLock(afterMFs, name)
	assert.Equal(t, 0, afterCount, "expected no series after Close")
}

// countSeriesForLock counts metric series whose "lock" label equals name.
func countSeriesForLock(mfs []*dto.MetricFamily, name string) int {
	count := 0

	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "lock" && lp.GetValue() == name {
					count++
				}
			}
		}
	}

	return count
}

func TestRWMutex_CollectMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(*lockmetrics.RWMutex)
		name  string
	}{
		{
			name:  "unlocked_mutex_collects_without_error",
			setup: nil,
		},
		{
			name: "locked_mutex_emits_held_gauge",
			setup: func(m *lockmetrics.RWMutex) {
				m.Lock("test-op")
				// unlocked after gather below
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := lockmetrics.New("test.collect." + tt.name)
			defer m.Close()

			if tt.setup != nil {
				tt.setup(m)
			}

			// Trigger Collect by gathering from the default registry.
			// The liveCollector is registered globally and iterates allMutexes.
			mfs, err := prometheus.DefaultGatherer.Gather()
			require.NoError(t, err)

			// At minimum the wait/hold histograms and live gauges should appear.
			assert.NotEmpty(t, mfs)

			if tt.name == "locked_mutex_emits_held_gauge" {
				m.Unlock()
			}
		})
	}
}
