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

func TestRunTickerSweepsThenStopsOnCancel(t *testing.T) {
	t.Parallel()

	var count atomic.Int64
	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	go func() {
		worker.RunTicker(ctx, "svc", "comp", time.Millisecond, 0, func(context.Context) {
			count.Add(1)
		})
		close(done)
	}()

	// Wait until at least a couple of sweeps have run.
	require.Eventually(t, func() bool { return count.Load() >= 2 }, time.Second, time.Millisecond)

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunTicker did not return after context cancellation")
	}
}

func TestRunTickerAppliesPerSweepTimeout(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	gotDeadline := make(chan bool, 1)
	go worker.RunTicker(ctx, "svc", "comp", time.Millisecond, 50*time.Millisecond,
		func(sweepCtx context.Context) {
			_, ok := sweepCtx.Deadline()
			select {
			case gotDeadline <- ok:
			default:
			}
		})

	select {
	case ok := <-gotDeadline:
		assert.True(t, ok, "sweep context should carry a deadline when timeout > 0")
	case <-time.After(time.Second):
		t.Fatal("sweep was never invoked")
	}
}

func TestRunTickerRecoversFromPanicAndKeepsSweeping(t *testing.T) {
	t.Parallel()

	var count atomic.Int64
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan struct{})
	go func() {
		worker.RunTicker(ctx, "svc", "comp", time.Millisecond, 0, func(context.Context) {
			// Every sweep panics; the worker must recover and keep ticking.
			count.Add(1)
			panic("boom")
		})
		close(done)
	}()

	// If panics were not recovered the goroutine would die after one sweep.
	require.Eventually(t, func() bool { return count.Load() >= 3 }, time.Second, time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunTicker did not return after cancellation")
	}
}

func TestRunTickerWithImmediateSweepsBeforeFirstTick(t *testing.T) {
	t.Parallel()

	var count atomic.Int64
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// A long interval means only the immediate pass can run within the deadline.
	go worker.RunTicker(ctx, "svc", "comp", time.Hour, 0, func(context.Context) {
		count.Add(1)
	}, worker.WithImmediate())

	require.Eventually(t, func() bool { return count.Load() == 1 }, time.Second, time.Millisecond)
}

func TestRunTickerReturnsImmediatelyIfCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	done := make(chan struct{})
	go func() {
		worker.RunTicker(ctx, "svc", "comp", time.Hour, 0, func(context.Context) {
			t.Error("sweep should not run when context is already cancelled")
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunTicker did not return for an already-cancelled context")
	}
}
