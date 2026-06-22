package worker_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

func TestRunTickerNRunsEachSweepAndStopsOnCancel(t *testing.T) {
	t.Parallel()

	var a, b atomic.Int64
	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	go func() {
		worker.RunTickerN(ctx, "svc",
			worker.Sweep{Component: "a", Interval: time.Millisecond, Fn: func(context.Context) { a.Add(1) }},
			worker.Sweep{Component: "b", Interval: time.Millisecond, Fn: func(context.Context) { b.Add(1) }},
		)
		close(done)
	}()

	require.Eventually(t, func() bool { return a.Load() >= 2 && b.Load() >= 2 }, time.Second, time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunTickerN did not return after cancellation")
	}
}

func TestRunTickerNSkipsInvalidSweeps(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	done := make(chan struct{})
	go func() {
		// nil Fn and non-positive interval are skipped; with all skipped and a
		// cancelled ctx, RunTickerN returns immediately.
		worker.RunTickerN(ctx, "svc",
			worker.Sweep{Component: "nilfn", Interval: time.Millisecond},
			worker.Sweep{Component: "badinterval", Interval: 0, Fn: func(context.Context) {}},
		)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunTickerN did not return")
	}
}
