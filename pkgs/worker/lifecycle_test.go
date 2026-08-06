package worker_test

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

type fakeRunner struct {
	started chan struct{}
	exited  atomic.Bool
	runs    atomic.Int32
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{started: make(chan struct{}, 1)}
}

func (f *fakeRunner) Run(ctx context.Context) {
	f.runs.Add(1)
	select {
	case f.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	f.exited.Store(true)
}

func TestSingleRunStartsAndStops(t *testing.T) {
	t.Parallel()

	var sr worker.SingleRun

	r := newFakeRunner()
	sr.Start(t.Context(), r)

	select {
	case <-r.started:
	case <-time.After(time.Second):
		t.Fatal("runner never started")
	}

	sr.Stop(t.Context())
	assert.True(t, r.exited.Load(), "Stop must cancel the run and wait for it to exit")
	assert.Equal(t, int32(1), r.runs.Load())
}

func TestSingleRunSecondStartIsNoOpWhileActive(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var sr worker.SingleRun

		r := newFakeRunner()
		sr.Start(t.Context(), r)

		select {
		case <-r.started:
		case <-time.After(time.Second):
			t.Fatal("runner never started")
		}

		// A second Start while the first run is still active must not spawn a
		// second run.
		sr.Start(t.Context(), r)
		synctest.Wait()
		assert.Equal(t, int32(1), r.runs.Load(), "Start must be a no-op while a run is active")

		sr.Stop(t.Context())
	})
}

func TestSingleRunStopIsIdempotent(t *testing.T) {
	t.Parallel()

	var sr worker.SingleRun

	r := newFakeRunner()
	sr.Start(t.Context(), r)

	select {
	case <-r.started:
	case <-time.After(time.Second):
		t.Fatal("runner never started")
	}

	sr.Stop(t.Context())
	require.NotPanics(t, func() { sr.Stop(t.Context()) })
}

func TestSingleRunStopWithoutStartIsNoOp(t *testing.T) {
	t.Parallel()

	var sr worker.SingleRun
	require.NotPanics(t, func() { sr.Stop(t.Context()) })
}

func TestSingleRunStopReturnsWhenCallerContextDone(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var sr worker.SingleRun

		// blockingRunner never exits on its own; Stop must still return once the
		// caller's ctx is done, without waiting forever for the run to finish.
		blocking := blockingRunner{}
		sr.Start(t.Context(), blocking)

		synctest.Wait()

		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()

		done := make(chan struct{})
		go func() {
			sr.Stop(ctx)
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Stop did not return when caller ctx was done")
		}

		// blockingRunner's goroutine is still asleep past ctx's deadline (that's
		// the point of this test); let it actually finish so every goroutine in
		// the bubble exits before Test returns, instead of deadlocking.
		time.Sleep(200 * time.Millisecond)
	})
}

// blockingRunner lingers briefly past context cancellation, simulating a run
// that outlives the cancellation signal by a bit longer than Stop is willing
// to wait for it.
type blockingRunner struct{}

func (blockingRunner) Run(ctx context.Context) {
	<-ctx.Done()
	time.Sleep(200 * time.Millisecond)
}
