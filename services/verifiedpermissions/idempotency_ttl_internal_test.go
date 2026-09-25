package verifiedpermissions

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClientTokens_TTLBoundsMapGrowth locks in the fix for clientTokens growing
// unbounded: checkClientToken already treated an entry past idempotencyWindow as
// absent, but never deleted it, so a long-running backend fed unique ClientTokens
// leaked memory forever. recordClientToken now sweeps expired entries on every
// write (see sweepClientTokensLocked).
func TestClientTokens_TTLBoundsMapGrowth(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := NewInMemoryBackend("123456789012", "us-east-1")

		first, err := b.CreatePolicyStore("store one", nil, "OFF", "", "tok-1")
		require.NoError(t, err)
		assert.Len(t, b.clientTokens, 1, "one entry recorded after the first Create call")

		// Still inside the window: same token + same params replays the original.
		time.Sleep(idempotencyWindow - time.Second)

		replay, err := b.CreatePolicyStore("store one", nil, "OFF", "", "tok-1")
		require.NoError(t, err)
		assert.Equal(t, first.PolicyStoreID, replay.PolicyStoreID,
			"a replay inside the window must return the original resource, not create a new one")
		assert.Len(t, b.clientTokens, 1, "replay must not grow the map")

		// Cross the window and write a new token: the sweep must reclaim tok-1's
		// now-expired entry, leaving only the new one behind.
		time.Sleep(2 * time.Second)

		second, err := b.CreatePolicyStore("store two", nil, "OFF", "", "tok-2")
		require.NoError(t, err)
		assert.NotEqual(t, first.PolicyStoreID, second.PolicyStoreID)
		assert.Len(t, b.clientTokens, 1, "expired tok-1 entry must be swept, not accumulate")

		_, hasExpired := b.clientTokens["CreatePolicyStore:tok-1"]
		assert.False(t, hasExpired, "expired entry must be deleted from the map")

		// A retry of the expired token now creates a genuinely new resource
		// instead of replaying the (no longer remembered) original.
		third, err := b.CreatePolicyStore("store one", nil, "OFF", "", "tok-1")
		require.NoError(t, err)
		assert.NotEqual(t, first.PolicyStoreID, third.PolicyStoreID,
			"an expired token must not replay the original resource")
	})
}
