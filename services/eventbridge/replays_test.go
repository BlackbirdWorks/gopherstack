package eventbridge_test

import (
	"context"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartReplay_ValidatesDestination(t *testing.T) {
	t.Parallel()
	b := newBackend()

	b.AddArchiveInternal(&eventbridge.Archive{
		ArchiveName:    "test-archive",
		ArchiveArn:     "arn:aws:events:us-east-1:123456789012:archive/test-archive",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
		State:          "ACTIVE",
	})

	_, err := b.StartReplay(context.Background(), eventbridge.StartReplayInput{
		ReplayName:     "replay-1",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:archive/test-archive",
		EventStartTime: time.Now().Add(-time.Hour),
		EventEndTime:   time.Now(),
		Destination: &eventbridge.ReplayDestination{
			Arn: "arn:aws:events:us-east-1:123456789012:event-bus/nonexistent",
		},
	})
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

func TestStartReplay_ValidDestinationAccepted(t *testing.T) {
	t.Parallel()
	b := newBackend()

	b.AddArchiveInternal(&eventbridge.Archive{
		ArchiveName:    "test-archive",
		ArchiveArn:     "arn:aws:events:us-east-1:123456789012:archive/test-archive",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
		State:          "ACTIVE",
	})

	defaultBusARN := "arn:aws:events:us-east-1:123456789012:event-bus/default"

	replay, err := b.StartReplay(context.Background(), eventbridge.StartReplayInput{
		ReplayName:     "replay-2",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:archive/test-archive",
		EventStartTime: time.Now().Add(-time.Hour),
		EventEndTime:   time.Now(),
		Destination:    &eventbridge.ReplayDestination{Arn: defaultBusARN},
	})
	require.NoError(t, err)
	assert.Equal(t, "STARTING", replay.State)
}

func TestStartReplay_NoDestinationAccepted(t *testing.T) {
	t.Parallel()
	b := newBackend()

	b.AddArchiveInternal(&eventbridge.Archive{
		ArchiveName:    "arc",
		ArchiveArn:     "arn:aws:events:us-east-1:123456789012:archive/arc",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
		State:          "ACTIVE",
	})

	replay, err := b.StartReplay(context.Background(), eventbridge.StartReplayInput{
		ReplayName:     "replay-3",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:archive/arc",
		EventStartTime: time.Now().Add(-time.Hour),
		EventEndTime:   time.Now(),
	})
	require.NoError(t, err)
	assert.Equal(t, "STARTING", replay.State)
}

func TestReplay_CancelNonRunningFails(t *testing.T) {
	t.Parallel()
	b := newBackend()

	b.AddArchiveInternal(&eventbridge.Archive{
		ArchiveName:    "arc",
		ArchiveArn:     "arn:aws:events:us-east-1:123456789012:archive/arc",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
		State:          "ACTIVE",
	})

	_, err := b.StartReplay(context.Background(), eventbridge.StartReplayInput{
		ReplayName:     "my-replay",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:archive/arc",
		EventStartTime: time.Now().Add(-time.Hour),
		EventEndTime:   time.Now(),
	})
	require.NoError(t, err)

	// Wait for the background goroutine to finish the replay.
	require.Eventually(t, func() bool {
		r, descErr := b.DescribeReplay(context.Background(), "my-replay")

		return descErr == nil && r.State == "COMPLETED"
	}, 5*time.Second, 10*time.Millisecond)

	// Cancelling a COMPLETED replay must fail.
	_, err = b.CancelReplay(context.Background(), "my-replay")
	require.ErrorIs(t, err, eventbridge.ErrInvalidState)
}

func TestReplay_DuplicateNameFails(t *testing.T) {
	t.Parallel()
	b := newBackend()

	b.AddArchiveInternal(&eventbridge.Archive{
		ArchiveName:    "arc2",
		ArchiveArn:     "arn:aws:events:us-east-1:123456789012:archive/arc2",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
		State:          "ACTIVE",
	})

	_, err := b.StartReplay(context.Background(), eventbridge.StartReplayInput{
		ReplayName:     "dup-replay",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:archive/arc2",
		EventStartTime: time.Now().Add(-time.Hour),
		EventEndTime:   time.Now(),
	})
	require.NoError(t, err)

	_, err = b.StartReplay(context.Background(), eventbridge.StartReplayInput{
		ReplayName:     "dup-replay",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:archive/arc2",
		EventStartTime: time.Now().Add(-time.Hour),
		EventEndTime:   time.Now(),
	})
	require.ErrorIs(t, err, eventbridge.ErrAlreadyExists)
}

func TestReplay_ListWithPrefix(t *testing.T) {
	t.Parallel()
	b := newBackend()

	b.AddArchiveInternal(&eventbridge.Archive{
		ArchiveName:    "arc3",
		ArchiveArn:     "arn:aws:events:us-east-1:123456789012:archive/arc3",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
		State:          "ACTIVE",
	})

	for _, name := range []string{"prod-replay-1", "prod-replay-2", "dev-replay-1"} {
		_, err := b.StartReplay(context.Background(), eventbridge.StartReplayInput{
			ReplayName:     name,
			EventSourceArn: "arn:aws:events:us-east-1:123456789012:archive/arc3",
			EventStartTime: time.Now().Add(-time.Hour),
			EventEndTime:   time.Now(),
		})
		require.NoError(t, err)
	}

	replays, _, err := b.ListReplays(context.Background(), "prod-", "")
	require.NoError(t, err)
	assert.Len(t, replays, 2)
}

func TestStartReplay_TimeOrdering(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	earlier := now.Add(-2 * time.Hour)
	later := now.Add(2 * time.Hour)

	tests := []struct {
		start   time.Time
		end     time.Time
		wantErr error
		name    string
	}{
		{name: "start before end ok", start: earlier, end: now, wantErr: nil},
		{name: "start equals end rejected", start: now, end: now, wantErr: eventbridge.ErrInvalidParameter},
		{name: "start after end rejected", start: later, end: earlier, wantErr: eventbridge.ErrInvalidParameter},
		{name: "both zero ok", start: time.Time{}, end: time.Time{}, wantErr: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.StartReplay(context.Background(), eventbridge.StartReplayInput{
				ReplayName:     "r-" + tt.name,
				EventSourceArn: "arn:aws:events:us-east-1:123:archive/my-archive",
				EventStartTime: tt.start,
				EventEndTime:   tt.end,
			})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestReplayCRUD(t *testing.T) {
	t.Parallel()

	t.Run("describe not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.DescribeReplay(context.Background(), "missing")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()
		b := newBackend()

		start := time.Now().Add(-2 * time.Hour)
		end := time.Now().Add(-1 * time.Hour)

		created, err := b.StartReplay(context.Background(), eventbridge.StartReplayInput{
			ReplayName:     "my-replay",
			EventSourceArn: "arn:aws:events:us-east-1:123:archive/my-archive",
			EventStartTime: start,
			EventEndTime:   end,
		})
		require.NoError(t, err)
		assert.Equal(t, "my-replay", created.ReplayName)
		assert.Equal(t, "STARTING", created.State)

		got, err := b.DescribeReplay(context.Background(), "my-replay")
		require.NoError(t, err)
		assert.Equal(t, "my-replay", got.ReplayName)

		replays, _, err := b.ListReplays(context.Background(), "my-", "")
		require.NoError(t, err)
		assert.Len(t, replays, 1)

		// Duplicate name rejected.
		_, err = b.StartReplay(context.Background(), eventbridge.StartReplayInput{
			ReplayName:     "my-replay",
			EventSourceArn: "arn:aws:events:us-east-1:123:archive/my-archive",
		})
		require.ErrorIs(t, err, eventbridge.ErrAlreadyExists)
	})
}

// TestCancelReplay_NotFound verifies non-existent replay returns ErrNotFound.
// TestCancelReplay covers not-found, terminal-state rejection, and the
// RUNNING/STARTING -> CANCELLING transition.
func TestCancelReplay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		seedState    string // empty means no replay is seeded (not-found case)
		wantErr      error
		wantState    string
		wantNoReplay bool
	}{
		{name: "not_found", wantNoReplay: true, wantErr: eventbridge.ErrNotFound},
		{name: "completed_is_terminal", seedState: "COMPLETED", wantErr: eventbridge.ErrInvalidState},
		{name: "cancelled_is_terminal", seedState: "CANCELLED", wantErr: eventbridge.ErrInvalidState},
		{name: "failed_is_terminal", seedState: "FAILED", wantErr: eventbridge.ErrInvalidState},
		{name: "running_transitions_to_cancelling", seedState: "RUNNING", wantState: "CANCELLING"},
		{name: "starting_transitions_to_cancelling", seedState: "STARTING", wantState: "CANCELLING"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := eventbridge.NewInMemoryBackend()
			if !tt.wantNoReplay {
				b.AddReplayInternal(&eventbridge.Replay{ReplayName: "r1", State: tt.seedState})
			}

			name := "r1"
			if tt.wantNoReplay {
				name = "no-such-replay"
			}

			replay, err := b.CancelReplay(context.Background(), name)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantState, replay.State)
		})
	}
}
