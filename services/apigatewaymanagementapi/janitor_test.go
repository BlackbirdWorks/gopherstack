package apigatewaymanagementapi_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
)

// TestJanitor_SweepIdleConnections locks in the fix for the unbounded-growth
// leak in b.connections: nothing called PruneIdle automatically, so a
// long-running emulator would accumulate every WebSocket connection ever
// created. Real API Gateway disconnects an idle WebSocket connection after 10
// minutes; the janitor now enforces the same default via PruneIdle.
func TestJanitor_SweepIdleConnections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		idleTimeout time.Duration
		sleep       time.Duration
		wantPruned  bool
	}{
		{
			name:        "evicted_past_idle_timeout",
			idleTimeout: 10 * time.Minute,
			sleep:       11 * time.Minute,
			wantPruned:  true,
		},
		{
			name:        "kept_within_idle_timeout",
			idleTimeout: 10 * time.Minute,
			sleep:       time.Minute,
			wantPruned:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := apigatewaymanagementapi.NewInMemoryBackend()

				_, err := b.CreateConnection("conn-1", "10.0.0.1", "ua", nil)
				require.NoError(t, err)

				time.Sleep(tt.sleep)

				j := apigatewaymanagementapi.NewJanitor(b, time.Minute, tt.idleTimeout)
				j.SweepOnce(t.Context())

				_, err = b.GetConnection("conn-1")
				if tt.wantPruned {
					assert.Error(t, err, "idle connection should have been evicted")
				} else {
					assert.NoError(t, err, "connection within idle window should not have been evicted")
				}
			})
		})
	}
}

// TestJanitor_TouchConnection_KeepsActiveConnectionAlive proves that a
// connection receiving inbound frames (simulated via TouchConnection, as the
// apigatewayv2 read loop calls it per frame) survives sweeps past the idle
// timeout, while one that never sends anything is pruned. Real API Gateway
// counts inbound client frames as activity, not just outbound posts.
func TestJanitor_TouchConnection_KeepsActiveConnectionAlive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		touch       bool
		wantPresent bool
	}{
		{name: "inbound_active_survives", touch: true, wantPresent: true},
		{name: "idle_connection_pruned", touch: false, wantPresent: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := apigatewaymanagementapi.NewInMemoryBackend()

				_, err := b.CreateConnection("conn-1", "10.0.0.1", "ua", nil)
				require.NoError(t, err)

				j := apigatewaymanagementapi.NewJanitor(b, time.Minute, 10*time.Minute)

				for range 12 {
					time.Sleep(time.Minute)

					if tt.touch {
						require.NoError(t, b.TouchConnection("conn-1"))
					}

					j.SweepOnce(t.Context())
				}

				_, err = b.GetConnection("conn-1")
				if tt.wantPresent {
					assert.NoError(t, err, "connection receiving inbound frames must survive sweeps")
				} else {
					assert.Error(t, err, "connection with no inbound activity must be pruned")
				}
			})
		})
	}
}

// TestTouchConnection_NoLifecycleEvent verifies TouchConnection refreshes
// LastActiveAt without recording a timeline event, unlike PingConnection.
func TestTouchConnection_NoLifecycleEvent(t *testing.T) {
	t.Parallel()

	b := apigatewaymanagementapi.NewInMemoryBackend()

	_, err := b.CreateConnection("conn-1", "10.0.0.1", "ua", nil)
	require.NoError(t, err)

	before := len(b.GetTimeline("conn-1"))

	require.NoError(t, b.TouchConnection("conn-1"))

	assert.Len(t, b.GetTimeline("conn-1"), before, "TouchConnection must not record a lifecycle event")
}

// TestJanitor_Run_ExitsOnCancel verifies the janitor loop exits promptly when
// the parent context is cancelled, leaving no goroutine behind.
func TestJanitor_Run_ExitsOnCancel(t *testing.T) {
	t.Parallel()

	b := apigatewaymanagementapi.NewInMemoryBackend()
	j := apigatewaymanagementapi.NewJanitor(b, 10*time.Millisecond, time.Hour)

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})

	go func() {
		j.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.Fail(t, "janitor did not exit after context cancellation")
	}
}
