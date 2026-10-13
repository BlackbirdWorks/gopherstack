package azurequeue_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurequeue"
)

func newIndexBackend(t *testing.T, n int) (*azurequeue.InMemoryBackend, *time.Time) {
	t.Helper()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b := azurequeue.NewInMemoryBackend()
	azurequeue.SetNowFunc(b, func() time.Time { return now })

	_, err := b.CreateQueue("q")
	require.NoError(t, err)

	for i := range n {
		_, err = b.PutMessage("q", fmt.Sprintf("m%d", i), 0, 0)
		require.NoError(t, err)
	}

	return b, &now
}

func TestQueueIndex_HintStaysSound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *azurequeue.InMemoryBackend, now *time.Time)
		name string
	}{
		{name: "reappear_in_fifo_order", run: func(t *testing.T, b *azurequeue.InMemoryBackend, now *time.Time) {
			t.Helper()

			first, err := b.GetMessages("q", 2, time.Minute)
			require.NoError(t, err)
			require.Len(t, first, 2)

			mid, err := b.GetMessages("q", 1, time.Minute)
			require.NoError(t, err)
			assert.Equal(t, "m2", mid[0].Text)

			*now = now.Add(2 * time.Minute)
			got, err := b.PeekMessages("q", 3)
			require.NoError(t, err)
			require.Len(t, got, 3)
			assert.Equal(t, []string{"m0", "m1", "m2"}, []string{got[0].Text, got[1].Text, got[2].Text})
		}},
		{name: "update_zero_vt_makes_visible", run: func(t *testing.T, b *azurequeue.InMemoryBackend, _ *time.Time) {
			t.Helper()

			first, err := b.GetMessages("q", 3, time.Hour)
			require.NoError(t, err)

			_, err = b.GetMessages("q", 1, time.Hour)
			require.NoError(t, err)

			_, err = b.UpdateMessage("q", first[1].ID, first[1].PopReceipt, 0, nil)
			require.NoError(t, err)

			got, err := b.PeekMessages("q", 1)
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, "m1", got[0].Text)
		}},
		{name: "delete_shifts_hint", run: func(t *testing.T, b *azurequeue.InMemoryBackend, _ *time.Time) {
			t.Helper()

			first, err := b.GetMessages("q", 2, time.Hour)
			require.NoError(t, err)
			require.NoError(t, b.DeleteMessage("q", first[0].ID, first[0].PopReceipt))

			got, err := b.GetMessages("q", 5, time.Hour)
			require.NoError(t, err)
			require.Len(t, got, 2)
			assert.Equal(t, "m2", got[0].Text)
		}},
		{name: "time_going_backwards", run: func(t *testing.T, b *azurequeue.InMemoryBackend, now *time.Time) {
			t.Helper()

			*now = now.Add(time.Hour)
			_, err := b.GetMessages("q", 2, time.Minute)
			require.NoError(t, err)

			*now = now.Add(-30 * time.Minute)
			got, err := b.PeekMessages("q", 1)
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, "m2", got[0].Text)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			t.Parallel()

			b, now := newIndexBackend(t, 4)
			tt.run(t, b, now)
		})
	}
}

func TestQueueIndex_DuplicateIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "linear_fallback"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			t.Parallel()

			b := azurequeue.NewInMemoryBackend()
			ids := []string{"dup", "r1", "dup", "r2", "r3", "r4"}
			i := 0
			azurequeue.SetIDFunc(b, func() string {
				i++

				return ids[(i-1)%len(ids)]
			})

			_, err := b.CreateQueue("q")
			require.NoError(t, err)
			_, err = b.PutMessage("q", "a", 0, 0)
			require.NoError(t, err)
			_, err = b.PutMessage("q", "b", 0, 0)
			require.NoError(t, err)

			azurequeue.SetIDFunc(b, func() string { return "r1" })
			require.NoError(t, b.DeleteMessage("q", "dup", "r1"))

			got, err := b.PeekMessages("q", 5)
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, "b", got[0].Text, tt.name)
		})
	}
}

func TestQueueIndex_RestoreAndSweep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "rebuilds_index"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			t.Parallel()

			b, now := newIndexBackend(t, 5)
			msgs, err := b.GetMessages("q", 3, time.Hour)
			require.NoError(t, err)

			snap := b.Snapshot(context.Background())
			b2 := azurequeue.NewInMemoryBackend()
			azurequeue.SetNowFunc(b2, func() time.Time { return *now })
			require.NoError(t, b2.Restore(context.Background(), snap))

			require.NoError(t, b2.DeleteMessage("q", msgs[2].ID, msgs[2].PopReceipt))
			require.ErrorIs(t, b2.DeleteMessage("q", msgs[2].ID, msgs[2].PopReceipt), azurequeue.ErrMessageNotFound)

			assert.Equal(t, 4, azurequeue.SweepExpired(b2, now.Add(azurequeue.DefaultMessageTTL+time.Second)), tt.name)
			require.ErrorIs(t, b2.DeleteMessage("q", msgs[0].ID, msgs[0].PopReceipt), azurequeue.ErrMessageNotFound)
		})
	}
}
