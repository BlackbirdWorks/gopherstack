package cloudcontrol

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClientTokens_TTLBoundsMapGrowth locks in the fix for b.clientTokens growing
// unbounded: it had no expiry check at all, so a single ClientToken could replay
// forever and a long-running backend fed unique ClientTokens leaked memory
// forever. rememberClientToken now sweeps entries past clientTokenTTL on every
// write, and cachedEventForToken now honors that TTL on lookup.
func TestClientTokens_TTLBoundsMapGrowth(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := NewInMemoryBackend("123456789012", "us-east-1")

		first, err := b.CreateResource("AWS::Test::Thing", `{"Name":"one"}`, "tok-1")
		require.NoError(t, err)
		assert.Len(t, b.clientTokens, 1, "one entry recorded after the first CreateResource call")

		// Still inside the window: same token + same desiredState replays the original event.
		time.Sleep(clientTokenTTL - time.Second)

		replay, err := b.CreateResource("AWS::Test::Thing", `{"Name":"one"}`, "tok-1")
		require.NoError(t, err)
		assert.Equal(t, first.Identifier, replay.Identifier,
			"a replay inside the window must return the original resource, not create a new one")
		assert.Len(t, b.clientTokens, 1, "replay must not grow the map")

		// Cross the window and write a new token: the sweep must reclaim tok-1's
		// now-expired entry, leaving only the new one behind.
		time.Sleep(2 * time.Second)

		second, err := b.CreateResource("AWS::Test::Thing", `{"Name":"two"}`, "tok-2")
		require.NoError(t, err)
		assert.NotEqual(t, first.Identifier, second.Identifier)
		assert.Len(t, b.clientTokens, 1, "expired tok-1 entry must be swept, not accumulate")

		_, hasExpired := b.clientTokens["tok-1"]
		assert.False(t, hasExpired, "expired entry must be deleted from the map")

		// A retry of the expired token, now with different content, creates a
		// genuinely new resource instead of replaying the (no longer remembered)
		// original -- proving the token itself, not just its old fingerprint, was
		// forgotten.
		third, err := b.CreateResource("AWS::Test::Thing", `{"Name":"three"}`, "tok-1")
		require.NoError(t, err)
		assert.NotEqual(t, first.Identifier, third.Identifier,
			"an expired token must not replay the original resource")
	})
}
