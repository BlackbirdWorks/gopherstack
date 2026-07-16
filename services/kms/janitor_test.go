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

func TestJanitor_HeapSweep_PurgesExpiredKey(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	jan := kms.NewJanitor(b, time.Minute)

	keyID := mustCreateSymKey(t, b)

	// Schedule deletion in the past via ScheduleKeyDeletion + backdating.
	_, err := b.ScheduleKeyDeletion(
		context.Background(),
		&kms.ScheduleKeyDeletionInput{KeyID: keyID, PendingWindowInDays: 7},
	)
	require.NoError(t, err)
	b.SetDeletionDateForTest(keyID, time.Now().Add(-time.Second))

	// Pre-load heap entry.
	pastTime := float64(time.Now().Add(-2*time.Second).UnixNano()) / 1e9
	jan.ScheduleJanitorExpiry(keyID, pastTime, true)

	jan.SweepOnce(t.Context())

	// Key should be gone.
	_, err = b.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: keyID})
	require.Error(t, err)
}

func TestJanitor_FallbackLinearSweep(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	jan := kms.NewJanitor(b, time.Minute)

	keyID := mustCreateSymKey(t, b)
	_, err := b.ScheduleKeyDeletion(
		context.Background(),
		&kms.ScheduleKeyDeletionInput{KeyID: keyID, PendingWindowInDays: 7},
	)
	require.NoError(t, err)
	b.SetDeletionDateForTest(keyID, time.Now().Add(-time.Second))

	// Do NOT pre-load heap — tests fallback linear path.
	jan.SweepOnce(t.Context())

	_, err = b.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: keyID})
	require.Error(t, err)
}

// TestKMSJanitorSweepExpiredKeys verifies the janitor purges keys past their deletion date,
// including key material, aliases, and grants.
func TestKMSJanitorSweepExpiredKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		pendingWindowDay int
		advanceTime      bool
		wantKeyDeleted   bool
	}{
		{
			name:             "expired_key_deleted",
			pendingWindowDay: 7,
			advanceTime:      true,
			wantKeyDeleted:   true,
		},
		{
			name:             "not_yet_expired_key_kept",
			pendingWindowDay: 30,
			advanceTime:      false,
			wantKeyDeleted:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			require.NoError(t, b.CreateAlias(context.Background(), &kms.CreateAliasInput{
				AliasName:   "alias/janitor-test",
				TargetKeyID: key.KeyMetadata.KeyID,
			}))

			_, err = b.CreateGrant(context.Background(), &kms.CreateGrantInput{
				KeyID:            key.KeyMetadata.KeyID,
				GranteePrincipal: "arn:aws:iam::000000000000:role/test",
				Operations:       []string{"Decrypt"},
			})
			require.NoError(t, err)

			_, err = b.ScheduleKeyDeletion(context.Background(), &kms.ScheduleKeyDeletionInput{
				KeyID:               key.KeyMetadata.KeyID,
				PendingWindowInDays: tt.pendingWindowDay,
			})
			require.NoError(t, err)

			if tt.advanceTime {
				// Back-date the deletion date by injecting a past timestamp.
				b.SetDeletionDateForTest(key.KeyMetadata.KeyID, time.Now().Add(-time.Hour))
			}

			janitor := kms.NewJanitor(b, 0)
			janitor.SweepOnce(t.Context())

			if tt.wantKeyDeleted {
				// Key should be gone.
				_, descErr := b.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: key.KeyMetadata.KeyID})
				require.Error(t, descErr)
				require.ErrorIs(t, descErr, kms.ErrKeyNotFound)

				// Alias should be gone.
				aliases, listErr := b.ListAliases(context.Background(), &kms.ListAliasesInput{})
				require.NoError(t, listErr)
				assert.Empty(t, aliases.Aliases)

				// Grants should be gone.
				grants, grantsErr := b.ListGrants(
					context.Background(),
					&kms.ListGrantsInput{KeyID: key.KeyMetadata.KeyID},
				)
				// Key is gone so lookup fails — that's acceptable.
				if grantsErr == nil {
					assert.Empty(t, grants.Grants)
				}
			} else {
				_, descErr := b.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: key.KeyMetadata.KeyID})
				require.NoError(t, descErr)
			}
		})
	}
}

// TestKMSJanitorRunContext verifies that the janitor Run loop respects context cancellation.
func TestKMSJanitorRunContext(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	janitor := kms.NewJanitor(b, 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	done := make(chan struct{})

	go func() {
		janitor.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "janitor did not exit after context cancellation")
	}
}
