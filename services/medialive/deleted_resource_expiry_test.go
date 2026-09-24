package medialive_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

// pastMedialiveDeletedTTL is strictly greater than the modeled one-hour
// medialiveDeletedTTL, so the lazy prune always evicts a DELETED entry by
// the time it fires.
const pastMedialiveDeletedTTL = time.Hour + time.Second

// TestDeleteInputSecurityGroup_EvictedAfterTTL locks in the fix for
// gopherstack-f9w3k: DeleteInputSecurityGroup used to mark the group DELETED
// forever, growing the table unbounded in a long-running emulator. It now
// evicts the group medialiveDeletedTTL past the delete.
func TestDeleteInputSecurityGroup_EvictedAfterTTL(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := medialive.NewInMemoryBackend("000000000000", "us-east-1")

		g, err := b.CreateInputSecurityGroup(nil, nil)
		require.NoError(t, err)

		require.NoError(t, b.DeleteInputSecurityGroup(g.ID))

		got, err := b.DescribeInputSecurityGroup(g.ID)
		require.NoError(t, err)
		assert.Equal(t, "DELETED", got.State)

		time.Sleep(pastMedialiveDeletedTTL)

		// The prune runs lazily on the next locked op -- a no-op List call
		// (rather than another Describe, which would itself find nothing and
		// prove nothing) drives the eviction.
		_, _, err = b.ListInputSecurityGroups(20, "")
		require.NoError(t, err)

		_, err = b.DescribeInputSecurityGroup(g.ID)
		require.ErrorIs(t, err, medialive.ErrNotFound)
		assert.Equal(t, 0, medialive.InputSecurityGroupCount(b))
	})
}

// TestDeleteInputSecurityGroup_KeptWithinTTL proves the eviction above does
// not fire early: a DELETED group must stay describable for the whole of
// medialiveDeletedTTL, matching terraform-provider-aws's delete waiter
// (waitInputSecurityGroupDeleted), which polls for State=="DELETED".
func TestDeleteInputSecurityGroup_KeptWithinTTL(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := medialive.NewInMemoryBackend("000000000000", "us-east-1")

		g, err := b.CreateInputSecurityGroup(nil, nil)
		require.NoError(t, err)

		require.NoError(t, b.DeleteInputSecurityGroup(g.ID))

		time.Sleep(time.Hour - time.Second)

		got, err := b.DescribeInputSecurityGroup(g.ID)
		require.NoError(t, err)
		assert.Equal(t, "DELETED", got.State)
	})
}

// TestDeleteMultiplex_EvictedAfterTTL locks in the fix for gopherstack-f9w3k
// for Multiplexes: DeleteMultiplex used to mark the multiplex DELETED
// forever. It now evicts it medialiveDeletedTTL past the delete.
func TestDeleteMultiplex_EvictedAfterTTL(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := medialive.NewInMemoryBackend("000000000000", "us-east-1")

		m, err := b.CreateMultiplex("evict-mux", []string{"us-east-1a"}, medialive.MultiplexSettings{
			TransportStreamID: 1,
		}, nil)
		require.NoError(t, err)

		_, err = b.DeleteMultiplex(m.ID)
		require.NoError(t, err)

		got, err := b.DescribeMultiplex(m.ID)
		require.NoError(t, err)
		assert.Equal(t, "DELETED", got.State)

		time.Sleep(pastMedialiveDeletedTTL)

		_, _, err = b.ListMultiplexes(20, "")
		require.NoError(t, err)

		_, err = b.DescribeMultiplex(m.ID)
		require.ErrorIs(t, err, medialive.ErrNotFound)
		assert.Equal(t, 0, medialive.MultiplexCount(b))
	})
}

// TestDeleteMultiplex_KeptWithinTTL proves the eviction above does not fire
// early, matching terraform-provider-aws's delete waiter (waitMultiplexDeleted).
func TestDeleteMultiplex_KeptWithinTTL(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := medialive.NewInMemoryBackend("000000000000", "us-east-1")

		m, err := b.CreateMultiplex("keep-mux", []string{"us-east-1a"}, medialive.MultiplexSettings{
			TransportStreamID: 1,
		}, nil)
		require.NoError(t, err)

		_, err = b.DeleteMultiplex(m.ID)
		require.NoError(t, err)

		time.Sleep(time.Hour - time.Second)

		got, err := b.DescribeMultiplex(m.ID)
		require.NoError(t, err)
		assert.Equal(t, "DELETED", got.State)
	})
}
