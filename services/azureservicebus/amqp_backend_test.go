package azureservicebus_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureservicebus"
)

func TestAMQPBackend_DeferAndReceiveDeferred(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		lock     bool
		wantLock bool
		wantGone bool
	}{
		{name: "lock deferred", lock: true, wantLock: true},
		{name: "receive and delete deferred", lock: false, wantGone: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newQueueBackend(t)
			ref := azureservicebus.EntityRef{Queue: "q"}
			_, err := b.Send(ref, azureservicebus.NewMessage{MessageID: "m1", Body: []byte("x")})
			require.NoError(t, err)

			locked, err := b.PeekLock(ref, false, 0)
			require.NoError(t, err)
			require.NoError(t, b.Defer(ref, "m1", locked.LockToken))

			_, err = b.PeekLock(ref, false, 0)
			require.ErrorIs(t, err, azureservicebus.ErrMessageNotFound, "deferred message must not be delivered")

			got, err := b.ReceiveDeferred(ref, []int64{locked.SequenceNumber, 999}, tt.lock)
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.True(t, got[0].Deferred)
			assert.Equal(t, tt.wantLock, got[0].LockToken != "")

			if tt.wantLock {
				require.NoError(t, b.Complete(ref, false, "m1", got[0].LockToken))
			}

			info, err := b.GetQueueInfo("q")
			require.NoError(t, err)
			assert.Zero(t, info.ActiveMessageCount)
		})
	}
}

func TestAMQPBackend_RenewLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		token   func(locked string) string
		name    string
		wantErr bool
	}{
		{name: "valid token", token: func(l string) string { return l }},
		{name: "unknown token", token: func(string) string { return "nope" }, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newQueueBackend(t)
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			azureservicebus.SetNowFunc(b, func() time.Time { return now })

			ref := azureservicebus.EntityRef{Queue: "q"}
			_, err := b.Send(ref, azureservicebus.NewMessage{Body: []byte("x")})
			require.NoError(t, err)

			locked, err := b.PeekLock(ref, false, 0)
			require.NoError(t, err)

			now = now.Add(30 * time.Second)

			until, err := b.RenewLock(ref, tt.token(locked.LockToken))
			if tt.wantErr {
				require.ErrorIs(t, err, azureservicebus.ErrMessageNotFound)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, now.Add(azureservicebus.DefaultLockDuration), until)
			assert.True(t, until.After(locked.LockedUntil))
		})
	}
}

func TestAMQPBackend_ScheduledVisibility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		advance     time.Duration
		cancel      bool
		wantVisible bool
	}{
		{name: "held before due", advance: 0},
		{name: "visible once due", advance: 2 * time.Hour, wantVisible: true},
		{name: "cancelled never visible", advance: 2 * time.Hour, cancel: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newQueueBackend(t)
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			azureservicebus.SetNowFunc(b, func() time.Time { return now })

			ref := azureservicebus.EntityRef{Queue: "q"}
			info, err := b.Send(ref, azureservicebus.NewMessage{Body: []byte("x"), ScheduledFor: now.Add(time.Hour)})
			require.NoError(t, err)

			if tt.cancel {
				require.NoError(t, b.CancelScheduled(ref, []int64{info.SequenceNumber}))
				require.ErrorIs(
					t,
					b.CancelScheduled(ref, []int64{info.SequenceNumber}),
					azureservicebus.ErrMessageNotFound,
				)
			}

			now = now.Add(tt.advance)

			_, err = b.PeekLock(ref, false, 0)
			if tt.wantVisible {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, azureservicebus.ErrMessageNotFound)
		})
	}
}

func TestAMQPBackend_CancelScheduledOnTopicRemovesAllCopies(t *testing.T) {
	t.Parallel()

	b := azureservicebus.NewInMemoryBackend()
	_, err := b.CreateTopic("t")
	require.NoError(t, err)

	for _, s := range []string{"a", "b"} {
		_, err = b.CreateSubscription("t", s)
		require.NoError(t, err)
	}

	info, err := b.Send(azureservicebus.EntityRef{Topic: "t"},
		azureservicebus.NewMessage{Body: []byte("x"), ScheduledFor: time.Now().Add(time.Hour)})
	require.NoError(t, err)
	require.NoError(t, b.CancelScheduled(azureservicebus.EntityRef{Topic: "t"}, []int64{info.SequenceNumber}))

	for _, s := range []string{"a", "b"} {
		msgs, perr := b.Peek(azureservicebus.EntityRef{Topic: "t", Subscription: s}, false, 0, 10)
		require.NoError(t, perr)
		assert.Empty(t, msgs, s)
	}
}

func TestAMQPBackend_FindByLockToken(t *testing.T) {
	t.Parallel()

	b := newQueueBackend(t)
	ref := azureservicebus.EntityRef{Queue: "q"}
	_, err := b.Send(ref, azureservicebus.NewMessage{MessageID: "m1", Body: []byte("x")})
	require.NoError(t, err)

	locked, err := b.PeekLock(ref, false, 0)
	require.NoError(t, err)

	info, dl, err := b.FindByLockToken(ref, locked.LockToken)
	require.NoError(t, err)
	assert.False(t, dl)
	assert.Equal(t, "m1", info.MessageID)

	require.NoError(t, b.DeadLetter(ref, "m1", locked.LockToken, "r", "d"))

	dlLocked, err := b.PeekLock(ref, true, 0)
	require.NoError(t, err)

	_, dl, err = b.FindByLockToken(ref, dlLocked.LockToken)
	require.NoError(t, err)
	assert.True(t, dl)

	_, _, err = b.FindByLockToken(ref, "missing")
	require.ErrorIs(t, err, azureservicebus.ErrMessageNotFound)
}

func TestAMQPBackend_SnapshotKeepsAMQPFields(t *testing.T) {
	t.Parallel()

	b := newQueueBackend(t)
	ref := azureservicebus.EntityRef{Queue: "q"}
	until := time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond)

	_, err := b.Send(ref, azureservicebus.NewMessage{
		MessageID: "m1", To: "dest", AppProps: map[string]any{"k": "v"}, ScheduledFor: until,
	})
	require.NoError(t, err)

	restored := azureservicebus.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), b.Snapshot(t.Context())))

	msgs, err := restored.Peek(ref, false, 0, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "dest", msgs[0].To)
	assert.Equal(t, "v", msgs[0].AppProps["k"])
	assert.True(t, until.Equal(msgs[0].ScheduledFor))
}
