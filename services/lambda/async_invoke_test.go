package lambda_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// asyncInvokePort* constants define the ports used by async invocation tests.
// They must not collide with ports used in other test files.
const (
	asyncInvokeQueueBehaviorBase  = 18200 // 18200–18201 reserved
	asyncInvokePendingCleanupBase = 18202 // 18202 reserved
	asyncInvokeSlotLifetimeBase   = 18203 // 18203–18204 reserved
)

// newAsyncTestBackend returns a backend with no Docker/port-alloc so that
// getOrCreateRuntime returns an error quickly. Tests that call EnqueueAsync directly
// bypass getOrCreateRuntime and provide their own runtime server.
func newAsyncTestBackend() *lambda.InMemoryBackend {
	return lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
}

// startAsyncTestServer creates and starts an ExportedRuntimeServer on the given port,
// registering a cleanup hook to stop it when the test finishes.
func startAsyncTestServer(t *testing.T, port int) *lambda.ExportedRuntimeServer {
	t.Helper()

	srv := lambda.NewExportedRuntimeServer(port)
	require.NoError(t, srv.Start(t.Context()))

	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
		defer stopCancel()
		srv.Stop(stopCtx)
	})

	return srv
}

// TestEnqueueAsync_QueueBehavior verifies fast-path (direct enqueue when queue has space)
// and slow-path (background goroutine when queue is full) behaviours of enqueueAsyncInvocation.
func TestEnqueueAsync_QueueBehavior(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		fillCount     int // pre-fill the queue with this many dummy entries (0 = empty)
		wantQueueLen  int // expected queue length once the new invocation is enqueued
		wantNoDropped bool
	}{
		{
			name:          "fast_path_enqueues_immediately_when_queue_empty",
			fillCount:     0,
			wantQueueLen:  1,
			wantNoDropped: true,
		},
		{
			name:          "slow_path_enqueues_after_queue_drains",
			fillCount:     lambda.RuntimeQueueSize,
			wantQueueLen:  1,
			wantNoDropped: true,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Use a non-started server: no HTTP listener needed for queue tests.
			srv := lambda.NewExportedRuntimeServer(asyncInvokeQueueBehaviorBase + i)
			bk := newAsyncTestBackend()

			// Pre-fill the queue to the requested depth.
			if tt.fillCount > 0 {
				filled := lambda.FillQueue(srv, tt.fillCount)
				require.Equal(t, tt.fillCount, filled, "should have filled queue to capacity")
			}

			// Enqueue an invocation — it must not be dropped, even when the queue is full.
			requestID := lambda.EnqueueAsync(t.Context(), bk, srv, "fn-queue", []byte(`{}`), time.Minute, false)
			require.NotEmpty(t, requestID)

			if tt.fillCount > 0 {
				// Drain all pre-filled items to make room for the goroutine.
				drained := lambda.DrainQueue(srv)
				assert.Equal(t, tt.fillCount, drained, "should drain all pre-filled items")
			}

			// The new invocation should eventually appear in the queue.
			require.Eventually(t, func() bool {
				return lambda.QueueLen(srv) == tt.wantQueueLen
			}, 2*time.Second, 10*time.Millisecond,
				"invocation should be enqueued (wantQueueLen=%d)", tt.wantQueueLen)

			assert.True(t, tt.wantNoDropped, "invocation must not be dropped")
		})
	}
}

// TestEnqueueAsync_PendingCleanup verifies that when a container picks up an async
// invocation via /next (storing it in srv.pending) but never calls /response or /error,
// the entry is removed from srv.pending once the function timeout elapses, preventing
// a memory leak.
func TestEnqueueAsync_PendingCleanup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		port              int
		invocationTimeout time.Duration
		wantPendingBefore int // expected PendingLen immediately after container calls /next
		wantPendingAfter  int // expected PendingLen after invocation timeout elapses
	}{
		{
			name:              "stale_entry_removed_after_timeout",
			port:              asyncInvokePendingCleanupBase,
			invocationTimeout: 100 * time.Millisecond,
			wantPendingBefore: 1,
			wantPendingAfter:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := startAsyncTestServer(t, tt.port)
			bk := newAsyncTestBackend()

			lambda.EnqueueAsync(t.Context(), bk, srv, "fn-clean", []byte(`{}`), tt.invocationTimeout, false)

			// Simulate the container calling /next — stores the entry in srv.pending.
			requestID := simulateContainerNext(t, tt.port)
			require.NotEmpty(t, requestID)

			assert.Equal(t, tt.wantPendingBefore, lambda.PendingLen(srv),
				"pending entry should exist immediately after /next")

			// Container does NOT call /response. After the timeout the goroutine cleans up.
			require.Eventually(t, func() bool {
				return lambda.PendingLen(srv) == tt.wantPendingAfter
			}, 3*time.Second, 10*time.Millisecond,
				"pending entry should be removed after invocation timeout")
		})
	}
}

// TestEnqueueAsync_ConcurrencySlotLifetime verifies that when trackConcurrency is true
// the concurrency slot is held for the full execution duration — released only after the
// container posts a response (or after the function timeout if it never does).
func TestEnqueueAsync_ConcurrencySlotLifetime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		port             int
		invTimeout       time.Duration
		simulateResponse bool // true: simulate /next + /response; false: simulate /next only
		wantPendingAfter int  // expected PendingLen after slot is released
	}{
		{
			name:             "slot_released_after_container_response",
			port:             asyncInvokeSlotLifetimeBase,
			invTimeout:       5 * time.Second,
			simulateResponse: true,
			wantPendingAfter: 0, // handleInvocationResult removes the pending entry on response
		},
		{
			name:             "slot_and_pending_released_after_timeout",
			port:             asyncInvokeSlotLifetimeBase + 1,
			invTimeout:       100 * time.Millisecond,
			simulateResponse: false,
			wantPendingAfter: 0, // waitAndCleanPending removes the stale entry on timeout
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := startAsyncTestServer(t, tt.port)
			bk := newAsyncTestBackend()

			const fnName = "fn-slot"

			require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{FunctionName: fnName}))

			_, err := bk.PutFunctionConcurrency(fnName, 1)
			require.NoError(t, err)

			// Acquire the single slot, simulating what InvokeFunctionWithQualifier does
			// before calling enqueueAsyncInvocation.
			acquired, acquireErr := lambda.AcquireConcurrencySlot(bk, fnName)
			require.NoError(t, acquireErr)
			require.True(t, acquired, "should acquire the sole slot")

			// Confirm: no more slots available.
			_, err = lambda.AcquireConcurrencySlot(bk, fnName)
			require.Error(t, err, "slot should be exhausted before enqueue")

			// Enqueue async with trackConcurrency=true — the goroutine holds the slot.
			lambda.EnqueueAsync(t.Context(), bk, srv, fnName, []byte(`{}`), tt.invTimeout, true)

			// Slot should still be held immediately after enqueue (release is deferred).
			_, err = lambda.AcquireConcurrencySlot(bk, fnName)
			require.Error(t, err, "slot should still be held immediately after enqueue")

			// Drive the container interaction.
			requestID := simulateContainerNext(t, tt.port)
			require.NotEmpty(t, requestID)

			if tt.simulateResponse {
				simulateContainerResponse(t, tt.port, requestID, `{"ok":true}`)
			}
			// else: container is silent; the timeout will fire.

			// The slot must eventually be released (by response or timeout).
			require.Eventually(t, func() bool {
				ok, tryErr := lambda.AcquireConcurrencySlot(bk, fnName)
				if tryErr != nil {
					return false
				}

				if ok {
					lambda.ReleaseConcurrencySlot(bk, fnName)
				}

				return true
			}, 3*time.Second, 5*time.Millisecond, "concurrency slot should be released")

			assert.Equal(t, tt.wantPendingAfter, lambda.PendingLen(srv),
				"pending entry state after slot release")
		})
	}
}
