package organizations_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// pastHandshakeTerminalRetention is strictly greater than the modeled 30-day
// handshakeTerminalRetention, so pruneStaleHandshakesLocked always evicts a
// terminal handshake by the time it fires.
const pastHandshakeTerminalRetention = 30*24*time.Hour + time.Second

// TestCancelHandshake_EvictedAfterRetention locks in the fix for the
// unbounded-memory-growth leak in b.handshakes: AWS documents that CANCELED,
// ACCEPTED, DECLINED, or EXPIRED handshakes "show up in lists for only 30
// days after entering that state. After that they are deleted."
// (API_Handshake.html). This backend used to keep them forever.
func TestCancelHandshake_EvictedAfterRetention(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := organizations.NewInMemoryBackend("000000000000", "us-east-1")

		_, _, err := b.CreateOrganization("ALL")
		require.NoError(t, err)

		h, err := b.InviteAccountToOrganization(
			organizations.HandshakeParty{ID: "123456789012", Type: "ACCOUNT"}, "", nil,
		)
		require.NoError(t, err)

		_, err = b.CancelHandshake(h.ID)
		require.NoError(t, err)

		require.Equal(t, 1, organizations.HandshakeCount(b),
			"the tombstone row must still be in the map before the retention window")

		time.Sleep(pastHandshakeTerminalRetention)

		// Pruning runs lazily on the next write-locked op.
		_, err = b.InviteAccountToOrganization(
			organizations.HandshakeParty{ID: "123456789013", Type: "ACCOUNT"}, "", nil,
		)
		require.NoError(t, err)

		assert.Equal(t, 1, organizations.HandshakeCount(b),
			"the stale CANCELED handshake must be evicted, leaving only the new invite")
	})
}

// TestCancelHandshake_KeptWithinRetention proves the eviction above does not
// fire early.
func TestCancelHandshake_KeptWithinRetention(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := organizations.NewInMemoryBackend("000000000000", "us-east-1")

		_, _, err := b.CreateOrganization("ALL")
		require.NoError(t, err)

		h, err := b.InviteAccountToOrganization(
			organizations.HandshakeParty{ID: "123456789012", Type: "ACCOUNT"}, "", nil,
		)
		require.NoError(t, err)

		_, err = b.CancelHandshake(h.ID)
		require.NoError(t, err)

		time.Sleep(30*24*time.Hour - time.Second)

		_, err = b.InviteAccountToOrganization(
			organizations.HandshakeParty{ID: "123456789013", Type: "ACCOUNT"}, "", nil,
		)
		require.NoError(t, err)

		assert.Equal(t, 2, organizations.HandshakeCount(b),
			"the CANCELED handshake must still be in the map within the retention window")
	})
}
