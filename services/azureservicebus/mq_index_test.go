package azureservicebus_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureservicebus"
)

func newIndexedBackend(
	t *testing.T,
	ids ...string,
) (*azureservicebus.InMemoryBackend, azureservicebus.EntityRef, *time.Time) {
	t.Helper()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b := azureservicebus.NewInMemoryBackend()
	azureservicebus.SetNowFunc(b, func() time.Time { return now })

	_, err := b.CreateQueue("q")
	require.NoError(t, err)

	ref := azureservicebus.EntityRef{Queue: "q"}

	for _, id := range ids {
		_, err = b.Send(ref, azureservicebus.NewMessage{MessageID: id, Body: []byte(id)})
		require.NoError(t, err)
	}

	return b, ref, &now
}

func TestPeekLock_ScanHintStaysSound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *azureservicebus.InMemoryBackend, ref azureservicebus.EntityRef, now *time.Time)
		name string
	}{
		{
			name: "lock_expiry_restores_fifo",
			run: func(t *testing.T, b *azureservicebus.InMemoryBackend, ref azureservicebus.EntityRef, now *time.Time) {
				t.Helper()

				for range 2 {
					_, err := b.PeekLock(ref, false, time.Minute)
					require.NoError(t, err)
				}

				third, err := b.PeekLock(ref, false, time.Minute)
				require.NoError(t, err)
				assert.Equal(t, "m2", third.MessageID)

				*now = now.Add(2 * time.Minute)
				again, err := b.PeekLock(ref, false, time.Minute)
				require.NoError(t, err)
				assert.Equal(t, "m0", again.MessageID)
			},
		},
		{
			name: "abandon_inside_prefix",
			run: func(t *testing.T, b *azureservicebus.InMemoryBackend, ref azureservicebus.EntityRef, _ *time.Time) {
				t.Helper()

				first, err := b.PeekLock(ref, false, time.Hour)
				require.NoError(t, err)

				_, err = b.PeekLock(ref, false, time.Hour)
				require.NoError(t, err)
				_, err = b.PeekLock(ref, false, time.Hour)
				require.NoError(t, err)

				require.NoError(t, b.Abandon(ref, false, first.MessageID, first.LockToken))

				got, err := b.PeekLock(ref, false, time.Hour)
				require.NoError(t, err)
				assert.Equal(t, "m0", got.MessageID)
			},
		},
		{
			name: "complete_shifts_hint",
			run: func(t *testing.T, b *azureservicebus.InMemoryBackend, ref azureservicebus.EntityRef, _ *time.Time) {
				t.Helper()

				first, err := b.PeekLock(ref, false, time.Hour)
				require.NoError(t, err)
				_, err = b.PeekLock(ref, false, time.Hour)
				require.NoError(t, err)
				require.NoError(t, b.Complete(ref, false, first.MessageID, first.LockToken))

				got, err := b.PeekLock(ref, false, time.Hour)
				require.NoError(t, err)
				assert.Equal(t, "m2", got.MessageID)
			},
		},
		{
			name: "sweep_resets_hint",
			run: func(t *testing.T, b *azureservicebus.InMemoryBackend, ref azureservicebus.EntityRef, now *time.Time) {
				t.Helper()

				for range 3 {
					_, err := b.PeekLock(ref, false, time.Minute)
					require.NoError(t, err)
				}

				*now = now.Add(2 * time.Minute)
				azureservicebus.SweepOnce(b, *now)

				got, err := b.PeekLock(ref, false, time.Minute)
				require.NoError(t, err)
				assert.Equal(t, "m0", got.MessageID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			t.Parallel()

			b, ref, now := newIndexedBackend(t, "m0", "m1", "m2", "m3")
			tt.run(t, b, ref, now)
		})
	}
}

func TestComplete_DuplicateMessageIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "first_match_semantics"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			t.Parallel()

			b, ref, _ := newIndexedBackend(t, "dup", "other", "dup")

			first, err := b.PeekLock(ref, false, time.Hour)
			require.NoError(t, err)
			require.NoError(t, b.Complete(ref, false, "dup", first.LockToken))

			second, err := b.PeekLock(ref, false, time.Hour)
			require.NoError(t, err)
			assert.Equal(t, "other", second.MessageID)

			third, err := b.PeekLock(ref, false, time.Hour)
			require.NoError(t, err)
			require.NoError(t, b.Complete(ref, false, "dup", third.LockToken))
			require.ErrorIs(
				t,
				b.Complete(ref, false, "dup", third.LockToken),
				azureservicebus.ErrMessageNotFound,
				tt.name,
			)
		})
	}
}

func TestComplete_ErrorsAndRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "after_restore"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			t.Parallel()

			b, ref, now := newIndexedBackend(t, "a", "b", "c")
			locked, err := b.PeekLock(ref, false, time.Hour)
			require.NoError(t, err)

			require.ErrorIs(t, b.Complete(ref, false, "b", "x"), azureservicebus.ErrMessageNotLocked)
			require.ErrorIs(t, b.Complete(ref, false, "zzz", "x"), azureservicebus.ErrMessageNotFound)
			require.ErrorIs(t, b.Complete(ref, false, "a", "wrong"), azureservicebus.ErrLockTokenMismatch)

			snap := b.Snapshot(context.Background())
			b2 := azureservicebus.NewInMemoryBackend()
			azureservicebus.SetNowFunc(b2, func() time.Time { return *now })
			require.NoError(t, b2.Restore(context.Background(), snap))

			require.ErrorIs(t, b2.Complete(ref, false, "b", "x"), azureservicebus.ErrMessageNotLocked, tt.name)
			require.NoError(t, b2.Complete(ref, false, "a", locked.LockToken))
			require.ErrorIs(t, b2.Complete(ref, false, "a", locked.LockToken), azureservicebus.ErrMessageNotFound)

			got, err := b2.PeekLock(ref, false, time.Hour)
			require.NoError(t, err)
			assert.Equal(t, "b", got.MessageID, strconv.FormatInt(got.SequenceNumber, 10))
		})
	}
}
