package worker_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

func TestGroupGoIsJoinedByStop(t *testing.T) {
	t.Parallel()

	g := worker.NewGroup("svc")

	var stopped atomic.Bool
	g.Go("comp", func(ctx context.Context) {
		<-ctx.Done()
		stopped.Store(true)
	})

	g.Stop()
	assert.True(t, stopped.Load(), "Stop must cancel and join the goroutine")
}

func TestGroupTickerSweepsThenStops(t *testing.T) {
	t.Parallel()

	g := worker.NewGroup("svc")

	var count atomic.Int64
	g.Ticker("comp", time.Millisecond, 0, func(context.Context) { count.Add(1) })

	require.Eventually(t, func() bool { return count.Load() >= 2 }, time.Second, time.Millisecond)
	g.Stop()

	after := count.Load()
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, after, count.Load(), "ticker must not fire after Stop")
}

func TestGroupAfterFiresAndIsCancellableByStop(t *testing.T) {
	t.Parallel()

	g := worker.NewGroup("svc")

	fired := make(chan struct{}, 1)
	g.After("comp", time.Millisecond, func() { fired <- struct{}{} })

	select {
	case <-fired:
	case <-time.After(time.Second):
		t.Fatal("After callback never fired")
	}

	// A long-delay timer must be cancelled by Stop, never firing.
	g2 := worker.NewGroup("svc")
	var late atomic.Bool
	g2.After("comp", time.Hour, func() { late.Store(true) })
	g2.Stop()
	time.Sleep(20 * time.Millisecond)
	assert.False(t, late.Load(), "Stop must cancel pending timers")
}

func TestGroupAfterIsNoOpAfterStop(t *testing.T) {
	t.Parallel()

	g := worker.NewGroup("svc")
	g.Stop()

	var ran atomic.Bool
	g.After("comp", time.Millisecond, func() { ran.Store(true) })
	time.Sleep(20 * time.Millisecond)
	assert.False(t, ran.Load(), "After must be a no-op after Stop")
}

func TestGroupRecoversFromCallbackPanics(t *testing.T) {
	t.Parallel()

	g := worker.NewGroup("svc")

	// A panicking Go goroutine and a panicking After callback must both be
	// recovered so Stop still completes cleanly.
	g.Go("comp", func(context.Context) { panic("go boom") })
	g.After("comp", time.Millisecond, func() { panic("after boom") })

	time.Sleep(20 * time.Millisecond)

	require.NotPanics(t, g.Stop)
}

func TestGroupStopIsIdempotent(t *testing.T) {
	t.Parallel()

	g := worker.NewGroup("svc")
	g.Ticker("comp", time.Millisecond, 0, func(context.Context) {})

	g.Stop()
	require.NotPanics(t, g.Stop)
}
