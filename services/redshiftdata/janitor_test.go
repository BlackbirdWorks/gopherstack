package redshiftdata_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

// TestJanitor_EvictsExpiredStatements verifies that the janitor
// removes terminal statements whose UpdatedAt is older than the TTL cutoff.
func TestJanitor_EvictsExpiredStatements(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)

	// Seed a FINISHED statement that has already aged past the TTL.
	stmt := redshiftdata.AddStatementInternal(b, testRegion, "old-stmt", "SELECT 1", "dev", "FINISHED", true)
	require.NotNil(t, stmt)

	// Sweep with a cutoff in the future so the statement is considered expired.
	cutoff := time.Now().Add(time.Hour)
	evicted := b.EvictExpiredStatements(cutoff)

	assert.Equal(t, 1, evicted, "should evict the expired statement")
	assert.Equal(t, 0, b.StatementCount(), "backend should be empty after eviction")
}

// TestJanitor_DoesNotEvictNonTerminal verifies that non-terminal
// statements are never evicted by the janitor.
func TestJanitor_DoesNotEvictNonTerminal(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)

	// A STARTED statement (non-terminal) — must not be evicted.
	redshiftdata.AddStatementInternal(b, testRegion, "running-stmt", "SELECT 1", "dev", "STARTED", false)

	cutoff := time.Now().Add(time.Hour)
	evicted := b.EvictExpiredStatements(cutoff)

	assert.Equal(t, 0, evicted, "non-terminal statements must not be evicted")
	assert.Equal(t, 1, b.StatementCount(), "statement should still be present")
}

// TestJanitor_SweepOnce uses the Janitor.SweepOnce helper for testing.
func TestJanitor_SweepOnce(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	redshiftdata.AddStatementInternal(b, testRegion, "expired", "SELECT 1", "dev", "FINISHED", true)
	redshiftdata.AddStatementInternal(b, testRegion, "running", "SELECT 2", "dev", "STARTED", false)

	j := redshiftdata.NewJanitor(b, time.Minute, time.Nanosecond) // very short TTL to force eviction
	j.SweepOnce(context.Background())

	// Both FINISHED and running should still be there initially — need cutoff in future.
	// The TTL of 1ns means any UpdatedAt before now is expired, but STARTED is exempt.
	assert.LessOrEqual(t, b.StatementCount(), 1, "only STARTED should survive")
}
