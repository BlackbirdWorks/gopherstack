package ram_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

// pastRAMInvitationTerminalTTL is strictly greater than the modeled one-hour
// ramInvitationTerminalTTL, so pruneTerminalInvitationsLocked always evicts a
// terminal invitation by the time it fires.
const pastRAMInvitationTerminalTTL = time.Hour + time.Second

// TestRejectResourceShareInvitation_EvictedAfterTTL locks in the fix for the
// unbounded-memory-growth leak in b.invitations: RejectResourceShareInvitation
// (and AcceptResourceShareInvitation) transition an invitation to a terminal
// status but the row was kept in the map forever.
func TestRejectResourceShareInvitation_EvictedAfterTTL(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := ram.NewInMemoryBackend("000000000000", "us-east-1")

		inv := b.CreateInvitation("arn:aws:ram:us-east-1:000000000000:resource-share/evict-share",
			"evict-share", "000000000000", "111111111111")

		_, err := b.RejectResourceShareInvitation(inv.InvitationARN)
		require.NoError(t, err)

		require.Equal(t, 1, ram.InvitationCount(b),
			"the terminal invitation must still be in the map before the TTL")

		time.Sleep(pastRAMInvitationTerminalTTL)

		// The prune runs lazily on the next write-locked op.
		b.CreateInvitation("arn:aws:ram:us-east-1:000000000000:resource-share/evict-share-2",
			"evict-share-2", "000000000000", "111111111111")

		assert.Equal(t, 1, ram.InvitationCount(b),
			"the expired terminal invitation must be evicted, leaving only the new one")
	})
}

// TestRejectResourceShareInvitation_KeptWithinTTL proves the eviction above
// does not fire early.
func TestRejectResourceShareInvitation_KeptWithinTTL(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := ram.NewInMemoryBackend("000000000000", "us-east-1")

		inv := b.CreateInvitation("arn:aws:ram:us-east-1:000000000000:resource-share/keep-share",
			"keep-share", "000000000000", "111111111111")

		_, err := b.RejectResourceShareInvitation(inv.InvitationARN)
		require.NoError(t, err)

		time.Sleep(time.Hour - time.Second)

		b.CreateInvitation("arn:aws:ram:us-east-1:000000000000:resource-share/keep-share-2",
			"keep-share-2", "000000000000", "111111111111")

		assert.Equal(t, 2, ram.InvitationCount(b),
			"the terminal invitation must still be in the map within the TTL")
	})
}
