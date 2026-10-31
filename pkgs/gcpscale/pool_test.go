package gcpscale_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

var errStart = errors.New("boom")

func TestMain(m *testing.M) { testleak.VerifyTestMain(m) }

type timer struct {
	c  chan time.Time
	fn func()
	at time.Duration
}

func (t *timer) C() <-chan time.Time { return t.c }
func (t *timer) Stop() bool {
	t.fn, t.at = nil, -1

	return true
}

// clock hands out timers that only fire when the test calls fire.
type clock struct {
	timers []*timer
	mu     sync.Mutex
}

func (c *clock) Now() time.Time { return time.Unix(0, 0) }

func (c *clock) NewTimer(d time.Duration) gcpscale.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()

	t := &timer{c: make(chan time.Time, 1), at: d}
	c.timers = append(c.timers, t)

	return t
}

func (c *clock) AfterFunc(d time.Duration, f func()) gcpscale.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()

	t := &timer{c: make(chan time.Time), fn: f, at: d}
	c.timers = append(c.timers, t)

	return t
}

func (c *clock) fireAll() {
	c.mu.Lock()
	ts := append([]*timer(nil), c.timers...)
	c.timers = nil
	c.mu.Unlock()

	for _, t := range ts {
		switch {
		case t.fn != nil:
			t.fn()
		case t.at >= 0:
			t.c <- time.Time{}
		}
	}
}

func (c *clock) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.timers)
}

type fixture struct {
	started atomic.Int32
	stopped atomic.Int32
	failNow atomic.Bool
}

func (f *fixture) pool(l gcpscale.Limits, clk gcpscale.Clock) *gcpscale.Pool[int] {
	return gcpscale.New("test", gcpscale.Config[int]{
		Start: func(context.Context) (int, error) {
			if f.failNow.Load() {
				return 0, errStart
			}

			return int(f.started.Add(1)), nil
		},
		Stop:        func(int) { f.stopped.Add(1) },
		Limits:      func() gcpscale.Limits { return l },
		Clock:       clk,
		IdleTimeout: time.Minute,
	})
}

func TestPool_ConcurrencyAndMax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		limits      gcpscale.Limits
		acquires    int
		wantStarted int32
	}{
		{"default concurrency is one", gcpscale.Limits{}, 3, 3},
		{"concurrency shares", gcpscale.Limits{Concurrency: 3}, 3, 1},
		{"concurrency spills", gcpscale.Limits{Concurrency: 2}, 3, 2},
		{"max caps instances", gcpscale.Limits{Max: 2, Concurrency: 5}, 5, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := &fixture{}
			p := f.pool(tt.limits, &clock{})
			t.Cleanup(p.Close)

			for range tt.acquires {
				_, release, err := p.Acquire(t.Context())
				require.NoError(t, err)
				t.Cleanup(release)
			}

			assert.Equal(t, tt.wantStarted, f.started.Load())
		})
	}
}

func TestPool_SaturationQueuesThenFails(t *testing.T) {
	t.Parallel()

	f := &fixture{}
	clk := &clock{}
	p := f.pool(gcpscale.Limits{Max: 1, Concurrency: 1}, clk)
	t.Cleanup(p.Close)

	_, release, err := p.Acquire(t.Context())
	require.NoError(t, err)

	errs := make(chan error, 1)

	go func() {
		_, _, aerr := p.Acquire(t.Context())
		errs <- aerr
	}()

	require.Eventually(t, func() bool { return clk.count() == 1 }, 5*time.Second, time.Millisecond)
	clk.mu.Lock()
	assert.Equal(t, 10*time.Second, clk.timers[0].at, "queue waits at least 10s")
	clk.mu.Unlock()
	clk.fireAll()
	require.ErrorIs(t, <-errs, gcpscale.ErrSaturated)

	release()
	_, release2, err := p.Acquire(t.Context())
	require.NoError(t, err)
	release2()
	assert.Equal(t, int32(1), f.started.Load(), "the freed slot is reused")
}

func TestPool_QueuedRequestTakesFreedSlot(t *testing.T) {
	t.Parallel()

	f := &fixture{}
	p := f.pool(gcpscale.Limits{Max: 1, Concurrency: 1}, &clock{})
	t.Cleanup(p.Close)

	_, release, err := p.Acquire(t.Context())
	require.NoError(t, err)

	got := make(chan int, 1)

	go func() {
		v, rel, aerr := p.Acquire(t.Context())
		if aerr == nil {
			rel()
			got <- v
		}
	}()

	release()
	assert.Equal(t, 1, <-got)
}

func TestPool_IdleScaleDownKeepsMinimum(t *testing.T) {
	t.Parallel()

	f := &fixture{}
	clk := &clock{}
	p := f.pool(gcpscale.Limits{Min: 1, Max: 5, Concurrency: 1}, clk)
	t.Cleanup(p.Close)

	require.NoError(t, p.Ensure(t.Context(), 1))

	releases := make([]func(), 0, 3)

	for range 3 {
		_, rel, err := p.Acquire(t.Context())
		require.NoError(t, err)

		releases = append(releases, rel)
	}

	assert.Equal(t, 3, p.Live())

	for _, rel := range releases {
		rel()
	}

	clk.fireAll()
	assert.Equal(t, 1, p.Live(), "idle instances above the minimum go away")
	assert.Equal(t, int32(2), f.stopped.Load())
}

func TestPool_StartFailureIsReportedAndRetried(t *testing.T) {
	t.Parallel()

	f := &fixture{}
	p := f.pool(gcpscale.Limits{}, &clock{})
	t.Cleanup(p.Close)

	f.failNow.Store(true)
	_, _, err := p.Acquire(t.Context())
	require.ErrorContains(t, err, "boom")
	assert.Zero(t, p.Live())

	f.failNow.Store(false)
	_, release, err := p.Acquire(t.Context())
	require.NoError(t, err)
	release()
}

func TestPool_CloseStopsInstancesAndRejects(t *testing.T) {
	t.Parallel()

	f := &fixture{}
	p := f.pool(gcpscale.Limits{Min: 2}, &clock{})
	require.NoError(t, p.Ensure(t.Context(), 2))

	p.Close()
	assert.Equal(t, int32(2), f.stopped.Load())

	_, _, err := p.Acquire(t.Context())
	require.ErrorIs(t, err, gcpscale.ErrClosed)
	require.ErrorIs(t, p.Ensure(t.Context(), 1), gcpscale.ErrClosed)
}

func TestPool_CancelledWhileQueued(t *testing.T) {
	t.Parallel()

	f := &fixture{}
	p := f.pool(gcpscale.Limits{Max: 1}, &clock{})
	t.Cleanup(p.Close)

	_, release, err := p.Acquire(t.Context())
	require.NoError(t, err)
	t.Cleanup(release)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, _, err = p.Acquire(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestPool_DrainKeepsPoolOpen(t *testing.T) {
	t.Parallel()

	f := &fixture{}
	p := f.pool(gcpscale.Limits{}, &clock{})

	require.NoError(t, p.Ensure(t.Context(), 2))
	p.Drain()
	assert.Equal(t, int32(2), f.stopped.Load())
	assert.Zero(t, p.Live())

	_, release, err := p.Acquire(t.Context())
	require.NoError(t, err)
	release()
	assert.Equal(t, 1, p.Live(), "a request after a drain starts a fresh instance")
	p.Close()
}

func TestPool_RemoveStopsMatchingInstance(t *testing.T) {
	t.Parallel()

	f := &fixture{}
	p := f.pool(gcpscale.Limits{}, &clock{})

	require.NoError(t, p.Ensure(t.Context(), 2))
	assert.True(t, p.Remove(func(v int) bool { return v == 1 }))
	assert.False(t, p.Remove(func(v int) bool { return v == 1 }), "already removed")
	assert.Equal(t, int32(1), f.stopped.Load())
	assert.Equal(t, 1, p.Live())
	p.Close()
}

func TestPool_RetiredWhileStarting(t *testing.T) {
	t.Parallel()

	var stopped atomic.Int32

	entered := make(chan struct{})
	p := gcpscale.New("test", gcpscale.Config[int]{
		Start: func(ctx context.Context) (int, error) {
			close(entered)
			<-ctx.Done()

			return 1, nil
		},
		Stop:   func(int) { stopped.Add(1) },
		Limits: func() gcpscale.Limits { return gcpscale.Limits{} },
	})

	errc := make(chan error, 1)

	go func() {
		_, _, err := p.Acquire(t.Context())
		errc <- err
	}()

	<-entered
	p.Drain()

	require.ErrorIs(t, <-errc, gcpscale.ErrRetired)
	p.Close()
	assert.Equal(t, int32(1), stopped.Load(), "an instance that finished starting after the drain is stopped")
}
