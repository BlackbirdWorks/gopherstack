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

// TestRetrievalCleanup_JanitorSweepsOldTokens verifies that the janitor removes
// retrieval tokens that have exceeded the trace TTL.
func TestRetrievalCleanup_JanitorSweepsOldTokens(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")

	// Add a trace and start a retrieval.
	traceID := b.PutTraceForTest(time.Now().Add(-2 * time.Hour))
	token := b.StartTraceRetrieval([]string{traceID}, time.Now().Add(-3*time.Hour), time.Now())

	// Back-date the retrieval token creation time so it appears old.
	b.SetRetrievalTimeForTest(token, time.Now().Add(-2*time.Hour))

	// Verify retrieval exists before sweep.
	status, traces, err := b.ListRetrievedTraces(token)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETE", status)
	assert.NotNil(t, traces)

	// Run janitor with a 30-minute TTL — token is 2h old so it gets swept.
	j := xray.NewJanitor(b, time.Minute, 30*time.Minute)
	j.SweepOnce(context.Background())

	// After sweep, the token no longer resolves: real AWS returns
	// ResourceNotFoundException for an unknown/expired RetrievalToken.
	_, _, err = b.ListRetrievedTraces(token)
	require.Error(t, err, "retrieval state should have been cleaned up by janitor")
	assert.ErrorIs(t, err, xray.ErrTraceRetrievalNotFound)
}

// TestJanitor_CleansUpSegmentIndexes verifies janitor removes parsedSegments and traceSegments.
func TestJanitor_CleansUpSegmentIndexes(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")

	// Seed a trace with a parsed segment.
	now := float64(time.Now().Unix())
	seg := segJSON("1-janitor-001", "s1", "", "svc", now-1, now, false, false, false)
	_ = b.PutTraceSegments([]string{seg})

	require.Equal(t, 1, b.TraceCount())
	require.NotEmpty(t, b.GetParsedSegments("1-janitor-001"))

	// Run janitor with a TTL of 0 to evict immediately.
	j := xray.NewJanitor(b, time.Millisecond, time.Millisecond)
	j.SweepOnce(t.Context())

	// Trace and segment indexes should all be gone.
	assert.Equal(t, 0, b.TraceCount())
	assert.Empty(t, b.GetParsedSegments("1-janitor-001"), "parsedSegments should be cleaned up by janitor")
}
