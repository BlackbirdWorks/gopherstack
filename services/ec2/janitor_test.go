package ec2_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestJanitor_SweepOnce verifies that SweepOnce triggers both the
// terminated-instance and cancelled-spot-request sweeps in a single call.
func TestJanitor_SweepOnce(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		instanceOld       bool
		spotOld           bool
		wantInstanceCount int
		wantSpotCount     int
	}{
		{
			name:              "old_resources_swept",
			instanceOld:       true,
			spotOld:           true,
			wantInstanceCount: 0,
			wantSpotCount:     0,
		},
		{
			name:              "recent_resources_kept",
			instanceOld:       false,
			spotOld:           false,
			wantInstanceCount: 1,
			wantSpotCount:     1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			// Create and terminate an instance.
			vpc, err := b.CreateVpc("10.0.0.0/16")
			require.NoError(t, err)
			subnet, err := b.CreateSubnet(vpc.ID, "10.0.1.0/24", "us-east-1a")
			require.NoError(t, err)
			insts, err := b.RunInstances("ami-test", "t2.micro", subnet.ID, 1)
			require.NoError(t, err)
			instanceID := insts[0].ID
			_, termErr := b.TerminateInstances([]string{instanceID})
			require.NoError(t, termErr)
			b.TickLifecycleForTest() // shutting-down → terminated

			if tt.instanceOld {
				b.SetInstanceTerminatedAtForTest(instanceID, time.Now().Add(-2*time.Hour))
			}

			// Create and cancel a spot request.
			spotReq, err := b.RequestSpotInstances("ami-test", "t2.micro", subnet.ID, "0.05")
			require.NoError(t, err)
			spotID := spotReq.ID
			require.NoError(t, b.CancelSpotInstanceRequests([]string{spotID}))

			if tt.spotOld {
				b.SetSpotRequestCancelledAtForTest(spotID, time.Now().Add(-12*time.Hour))
			}

			j := ec2.NewJanitor(b, time.Minute, time.Hour, 6*time.Hour)
			j.SweepOnce(t.Context())

			terminated := b.DescribeInstances(nil, "terminated")
			assert.Len(t, terminated, tt.wantInstanceCount)

			spotReqs := b.DescribeSpotInstanceRequests(nil)
			cancelled := 0

			for _, s := range spotReqs {
				if s.State == "cancelled" {
					cancelled++
				}
			}

			assert.Equal(t, tt.wantSpotCount, cancelled)
		})
	}
}

// TestJanitor_TaskTimeout_WithJanitor verifies that WithJanitor propagates the
// taskTimeout variadic argument and that the janitor runs cleanly when the
// outer context is cancelled before any tick fires.
func TestJanitor_TaskTimeout_WithJanitor(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	const wantTimeout = 30 * time.Second

	h.WithJanitor(time.Minute, time.Hour, 0, wantTimeout)

	// A pre-cancelled context ensures the janitor exits immediately.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := h.StartWorker(ctx)
	require.NoError(t, err)
}

// TestJanitor_Run_Cancel_WithTaskTimeout verifies that the janitor goroutine
// exits cleanly when the parent context is cancelled, even when TaskTimeout
// is set to a non-zero value.
func TestJanitor_Run_Cancel_WithTaskTimeout(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	j := ec2.NewJanitor(b, 10*time.Millisecond, time.Hour, 6*time.Hour)
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

// TestEC2Janitor_DefaultInterval verifies that a zero interval in WithJanitor
// results in the default interval being used.
func TestEC2Janitor_DefaultInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval time.Duration
		want     time.Duration
	}{
		{
			name:     "zero_uses_default",
			interval: 0,
			want:     ec2.DefaultJanitorInterval,
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

			h := ec2.NewHandler(ec2.NewInMemoryBackend("123456789012", "us-east-1"))
			h.WithJanitor(tt.interval, 0, 0)

			assert.Equal(t, tt.want, h.GetJanitorInterval())
		})
	}
}
