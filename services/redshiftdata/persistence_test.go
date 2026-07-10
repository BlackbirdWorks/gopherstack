package redshiftdata //nolint:testpackage // needs the unexported region ctx key, see isolation_test.go's ctxRegion.

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const persistenceTestAccountID = "000000000000"

// seedStatements adds n statements with sequential IDs "seed-0000".."seed-<n-1>"
// to the given region via the internal seed helper, returning the IDs in
// insertion order.
func seedStatements(b *InMemoryBackend, region string, n int) []string {
	ids := make([]string, n)

	for i := range n {
		id := fmt.Sprintf("seed-%04d", i)
		AddStatementInternal(b, region, id, "SELECT 1", "mydb", "FINISHED", false)
		ids[i] = id
	}

	return ids
}

// Test_Persistence_FullStateRoundTrip verifies every statement across every
// region round-trips through Snapshot/Restore, including fields that aren't
// exercised by the simpler refinement1 round-trip test.
func Test_Persistence_FullStateRoundTrip(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend(persistenceTestAccountID, "us-east-1")

	regions := []string{"us-east-1", "eu-west-1", "ap-south-1"}
	wantIDs := make(map[string][]string, len(regions))

	for _, region := range regions {
		wantIDs[region] = seedStatements(b, region, 5)
	}

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := NewInMemoryBackend(persistenceTestAccountID, "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, b.StatementCount(), b2.StatementCount())

	for region, ids := range wantIDs {
		ctx := ctxRegion(region)

		for _, id := range ids {
			got, err := b2.DescribeStatement(ctx, id)
			require.NoError(t, err, "region %s statement %s", region, id)
			assert.Equal(t, id, got.ID)
			assert.Equal(t, "SELECT 1", got.QueryString)
			assert.Equal(t, "FINISHED", got.Status)
		}
	}
}

// Test_Persistence_RingBufferOrderSurvivesRoundTrip proves the FIFO eviction
// order the ring buffer implements (see backend.go's regionStore doc) is
// reconstructed exactly by Restore, not just the surviving statement set.
// It seeds more than the per-region cap so the oldest entries are evicted
// during seeding, round-trips through Snapshot/Restore, then adds one more
// statement and checks that the correct next-oldest entry -- and only that
// one -- is evicted.
func Test_Persistence_RingBufferOrderSurvivesRoundTrip(t *testing.T) {
	t.Parallel()

	const (
		region   = "us-east-1"
		overflow = 50
	)

	historyCap := MaxStatementHistoryForTest
	b := NewInMemoryBackend(persistenceTestAccountID, region)
	ids := seedStatements(b, region, historyCap+overflow)

	require.Equal(t, historyCap, b.StatementCount())

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := NewInMemoryBackend(persistenceTestAccountID, region)
	require.NoError(t, b2.Restore(t.Context(), snap))
	require.Equal(t, historyCap, b2.StatementCount())

	ctx := ctxRegion(region)

	// The first `overflow` seeded IDs were already evicted before the
	// snapshot; they must stay gone after restore.
	_, err := b2.DescribeStatement(ctx, ids[0])
	require.ErrorIs(t, err, ErrNotFound)

	// The oldest surviving entry is ids[overflow]; it must still be present
	// immediately after restore...
	_, err = b2.DescribeStatement(ctx, ids[overflow])
	require.NoError(t, err)

	// ...but inserting one more statement must evict exactly that entry, and
	// no other, proving the ring buffer's head/order was reconstructed
	// correctly rather than e.g. reset to an arbitrary map-iteration order.
	AddStatementInternal(b2, region, "post-restore", "SELECT 1", "mydb", "FINISHED", false)

	_, err = b2.DescribeStatement(ctx, ids[overflow])
	require.ErrorIs(t, err, ErrNotFound)

	_, err = b2.DescribeStatement(ctx, ids[overflow+1])
	require.NoError(t, err, "next-oldest entry must survive the single post-restore eviction")
}

// Test_Persistence_VersionMismatch verifies Restore discards an
// incompatible (old, absent, or future) snapshot version by resetting to
// empty rather than partially decoding it, matching the sesv2/dax/waf
// Phase 3.3 convention. A malformed-JSON payload is covered separately since
// it must error rather than reset.
func Test_Persistence_VersionMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(raw map[string]any)
		name   string
	}{
		{mutate: func(raw map[string]any) { raw["version"] = 999 }, name: "future version"},
		{mutate: func(raw map[string]any) { delete(raw, "version") }, name: "absent version (pre-Phase-3.3 snapshot)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runVersionMismatchCase(t, tt.mutate)
		})
	}
}

func runVersionMismatchCase(t *testing.T, mutate func(raw map[string]any)) {
	t.Helper()

	const region = "us-east-1"

	seed := NewInMemoryBackend(persistenceTestAccountID, region)
	seedStatements(seed, region, 3)

	target := NewInMemoryBackend(persistenceTestAccountID, region)
	seedStatements(target, region, 1) // pre-existing state must be discarded, not merged.

	snap := seed.Snapshot(t.Context())
	require.NotNil(t, snap)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(snap, &raw))
	mutate(raw)

	data, err := json.Marshal(raw)
	require.NoError(t, err)

	require.NoError(t, target.Restore(t.Context(), data))
	assert.Equal(t, 0, target.StatementCount(), "incompatible snapshot must reset to empty")
}

// Test_Persistence_MalformedJSON verifies Restore surfaces an error (rather
// than silently resetting) when the payload isn't valid JSON at all.
func Test_Persistence_MalformedJSON(t *testing.T) {
	t.Parallel()

	target := NewInMemoryBackend(persistenceTestAccountID, "us-east-1")
	seedStatements(target, "us-east-1", 1)

	err := target.Restore(t.Context(), []byte("{not valid json"))
	require.Error(t, err)
	// A malformed payload must not clobber existing state.
	assert.Equal(t, 1, target.StatementCount())
}

// Test_Persistence_HandlerDelegates verifies Handler.Snapshot/Handler.Restore
// delegate to the backend -- the dead-wiring fix: previously Handler had no
// Snapshot/Restore at all, so persistence.Manager's type assertion to
// persistence.Persistable silently excluded this service even though
// InMemoryBackend fully implemented both methods.
func Test_Persistence_HandlerDelegates(t *testing.T) {
	t.Parallel()

	const region = "us-east-1"

	b := NewInMemoryBackend(persistenceTestAccountID, region)
	h := NewHandler(b)

	seedStatements(b, region, 4)

	// Handler must satisfy persistence.Persistable via a plain method-set
	// interface check (mirrors how persistence.Manager registers services).
	var persistable interface {
		Snapshot(ctx context.Context) []byte
		Restore(ctx context.Context, data []byte) error
	} = h

	snap := persistable.Snapshot(t.Context())
	require.NotNil(t, snap)
	assert.Equal(t, b.Snapshot(t.Context()), snap, "Handler.Snapshot must return exactly what the backend produces")

	b2 := NewInMemoryBackend(persistenceTestAccountID, region)
	h2 := NewHandler(b2)

	require.NoError(t, h2.Restore(t.Context(), snap))
	assert.Equal(t, b.StatementCount(), b2.StatementCount())
}
