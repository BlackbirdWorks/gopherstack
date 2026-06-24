package xray_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

// TestXRayJanitor_TaskTimeout_WithJanitor verifies that WithJanitor propagates
// the variadic taskTimeout into the janitor's TaskTimeout field.
func TestXRayJanitor_TaskTimeout_WithJanitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		traceTTL    time.Duration
		taskTimeout time.Duration
		wantTTL     time.Duration
		wantTimeout time.Duration
	}{
		{
			name:        "no_timeout_zero",
			traceTTL:    30 * time.Minute,
			taskTimeout: 0,
			wantTTL:     30 * time.Minute,
			wantTimeout: 0,
		},
		{
			name:        "custom_ttl_and_timeout",
			traceTTL:    time.Hour,
			taskTimeout: 30 * time.Second,
			wantTTL:     time.Hour,
			wantTimeout: 30 * time.Second,
		},
		{
			name:        "zero_ttl_uses_default",
			traceTTL:    0,
			taskTimeout: 0,
			wantTTL:     30 * time.Minute,
			wantTimeout: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := xray.NewHandler(xray.NewInMemoryBackend("000000000000", "us-east-1"))
			h.WithJanitor(time.Minute, tt.traceTTL, tt.taskTimeout)

			assert.Equal(t, tt.wantTTL, h.GetJanitorTraceTTL())
			assert.Equal(t, tt.wantTimeout, h.GetJanitorTaskTimeout())
		})
	}
}

// TestXRayJanitor_SweepOnce_EvictsExpiredTraces verifies that SweepOnce removes
// traces older than TraceTTL.
func TestXRayJanitor_SweepOnce_EvictsExpiredTraces(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		traceTTL      time.Duration
		traceAge      time.Duration
		expectEvicted bool
	}{
		{
			name:          "old_trace_evicted",
			traceTTL:      time.Minute,
			traceAge:      2 * time.Minute,
			expectEvicted: true,
		},
		{
			name:          "fresh_trace_kept",
			traceTTL:      time.Minute,
			traceAge:      10 * time.Second,
			expectEvicted: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := xray.NewInMemoryBackend("000000000000", "us-east-1")

			startTime := time.Now().Add(-tt.traceAge)
			traceID := b.PutTraceForTest(startTime)

			j := xray.NewJanitor(b, time.Minute, tt.traceTTL)
			j.SweepOnce(t.Context())

			exists := b.TraceExistsForTest(traceID)
			if tt.expectEvicted {
				assert.False(t, exists, "expected trace to be evicted")
			} else {
				assert.True(t, exists, "expected trace to still exist")
			}
		})
	}
}

// TestXRayJanitor_Run_ExitsOnCancel verifies that the janitor goroutine exits
// promptly when the parent context is cancelled.
func TestXRayJanitor_Run_ExitsOnCancel(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")
	j := xray.NewJanitor(b, 10*time.Millisecond, 0)
	j.TaskTimeout = 30 * time.Second

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

// TestXRayJanitor_DefaultInterval verifies that a zero interval in WithJanitor
// results in the default interval being used.
func TestXRayJanitor_DefaultInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval time.Duration
		want     time.Duration
	}{
		{
			name:     "zero_uses_default",
			interval: 0,
			want:     xray.DefaultJanitorInterval,
		},
		{
			name:     "custom_interval_propagated",
			interval: 5 * time.Minute,
			want:     5 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := xray.NewHandler(xray.NewInMemoryBackend("000000000000", "us-east-1"))
			h.WithJanitor(tt.interval, 0)

			assert.Equal(t, tt.want, h.GetJanitorInterval())
		})
	}
}
