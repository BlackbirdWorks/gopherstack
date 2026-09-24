package eventbridge_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// pastReplayTerminalTTL is strictly greater than the modeled one-hour
// replayTerminalTTL, so pruneStaleReplaysLocked always evicts a terminal
// replay by the time it fires.
const pastReplayTerminalTTL = time.Hour + time.Second

// TestReplay_EvictedAfterTTL locks in the fix for the unbounded-memory-growth
// leak in b.replays: a COMPLETED/CANCELLED replay was kept in the map
// forever.
func TestReplay_EvictedAfterTTL(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := eventbridge.NewInMemoryBackend()
		ctx := context.Background()

		b.AddReplayInternal(&eventbridge.Replay{
			ReplayName:    "evict-replay",
			State:         "CANCELLED",
			ReplayEndTime: time.Now(),
		})

		require.Equal(t, 1, b.ReplayCount(), "the terminal replay must still be in the map before the TTL")

		time.Sleep(pastReplayTerminalTTL)

		// The prune runs lazily on the next write-locked op.
		_, err := b.StartReplay(ctx, eventbridge.StartReplayInput{
			ReplayName:     "new-replay",
			EventSourceArn: "arn:aws:events:us-east-1:000000000000:archive/none",
		})
		require.NoError(t, err)

		assert.Equal(t, 1, b.ReplayCount(), "the expired terminal replay must be evicted, leaving only the new one")
	})
}

// TestReplay_KeptWithinTTL proves the eviction above does not fire early.
func TestReplay_KeptWithinTTL(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := eventbridge.NewInMemoryBackend()
		ctx := context.Background()

		b.AddReplayInternal(&eventbridge.Replay{
			ReplayName:    "keep-replay",
			State:         "CANCELLED",
			ReplayEndTime: time.Now(),
		})

		time.Sleep(time.Hour - time.Second)

		_, err := b.StartReplay(ctx, eventbridge.StartReplayInput{
			ReplayName:     "new-replay",
			EventSourceArn: "arn:aws:events:us-east-1:000000000000:archive/none",
		})
		require.NoError(t, err)

		assert.Equal(t, 2, b.ReplayCount(), "the terminal replay must still be in the map within the TTL")
	})
}
