package kms_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// TestKMSJanitor_TaskTimeout_WithJanitor verifies that WithJanitor propagates
// the variadic taskTimeout into the janitor's TaskTimeout field.
func TestKMSJanitor_TaskTimeout_WithJanitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		taskTimeout time.Duration
		want        time.Duration
	}{
		{
			name:        "no_timeout_zero",
			taskTimeout: 0,
			want:        0,
		},
		{
			name:        "with_30s_timeout",
			taskTimeout: 30 * time.Second,
			want:        30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := kms.NewHandler(kms.NewInMemoryBackend())
			h.WithJanitor(time.Minute, tt.taskTimeout)

			assert.Equal(t, tt.want, h.GetJanitorTaskTimeout())
		})
	}
}

// TestKMSJanitor_SweepOnce_DeletesExpiredKeys verifies that SweepOnce removes
// keys that have passed their scheduled deletion date.
func TestKMSJanitor_SweepOnce_DeletesExpiredKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		deletionOffset   time.Duration
		expectKeyDeleted bool
	}{
		{
			name:             "expired_key_is_deleted",
			deletionOffset:   -1 * time.Second,
			expectKeyDeleted: true,
		},
		{
			name:             "future_key_is_not_deleted",
			deletionOffset:   1 * time.Hour,
			expectKeyDeleted: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()

			// Create a key and schedule it for deletion.
			createOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
				Description: "test-key",
			})
			require.NoError(t, err)

			keyID := createOut.KeyMetadata.KeyID

			_, err = b.ScheduleKeyDeletion(context.Background(), &kms.ScheduleKeyDeletionInput{
				KeyID:               keyID,
				PendingWindowInDays: 7,
			})
			require.NoError(t, err)

			// Directly backdating the deletion date to simulate expiry.
			b.SetDeletionDateForTest(keyID, time.Now().Add(tt.deletionOffset))

			j := kms.NewJanitor(b, time.Minute)
			j.SweepOnce(t.Context())

			meta, err := b.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: keyID})
			if tt.expectKeyDeleted {
				require.Error(t, err, "expected key to be deleted")
				assert.Nil(t, meta)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, meta)
			}
		})
	}
}

// TestKMSJanitor_Run_ExitsOnCancel verifies that the janitor goroutine exits
// promptly when the parent context is cancelled.
func TestKMSJanitor_Run_ExitsOnCancel(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	j := kms.NewJanitor(b, 10*time.Millisecond)
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

// TestKMSJanitor_DefaultInterval verifies that a zero interval in WithJanitor
// results in the default interval being used.
func TestKMSJanitor_DefaultInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval time.Duration
		want     time.Duration
	}{
		{
			name:     "zero_uses_default",
			interval: 0,
			want:     kms.DefaultJanitorInterval,
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

			h := kms.NewHandler(kms.NewInMemoryBackend())
			h.WithJanitor(tt.interval)

			assert.Equal(t, tt.want, h.GetJanitorInterval())
		})
	}
}
