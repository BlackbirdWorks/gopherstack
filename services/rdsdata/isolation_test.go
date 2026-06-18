package rdsdata //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rdsdataCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestRDSDataRegionIsolation proves that transactions and executed statements created
// in two different regions are fully isolated: each region sees only its own data,
// and committing in one region leaves the other untouched.
func TestRDSDataRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := rdsdataCtxRegion("us-east-1")
	ctxWest := rdsdataCtxRegion("us-west-2")

	// 1. Create an east transaction. West must see zero transactions.
	eastTxID, err := backend.BeginTransaction(
		ctxEast,
		"arn:aws:rds:us-east-1:000000000000:cluster:shared",
	)
	require.NoError(t, err)

	westBeforeCreate := backend.ListTransactions(ctxWest)
	assert.Empty(t, westBeforeCreate, "west must see no transactions before any west TX is created")

	// 2. Create a west transaction. East must still see exactly its own one.
	westTxID, err := backend.BeginTransaction(
		ctxWest,
		"arn:aws:rds:us-west-2:000000000000:cluster:shared",
	)
	require.NoError(t, err)

	eastTxns := backend.ListTransactions(ctxEast)
	require.Len(t, eastTxns, 1, "east must see exactly its own transaction")
	assert.Contains(t, eastTxns, eastTxID)

	westTxns := backend.ListTransactions(ctxWest)
	require.Len(t, westTxns, 1, "west must see exactly its own transaction")
	assert.Contains(t, westTxns, westTxID)

	// 3. Execute a statement in us-east-1. The west region must not see it.
	_, _, _, err = backend.ExecuteStatement(
		ctxEast,
		"arn:aws:rds:us-east-1:000000000000:cluster:shared",
		"SELECT 1",
		eastTxID,
	)
	require.NoError(t, err)

	eastStmts := backend.ListExecutedStatements(ctxEast)
	require.Len(t, eastStmts, 1)
	assert.Equal(t, "SELECT 1", eastStmts[0].SQL)

	westStmts := backend.ListExecutedStatements(ctxWest)
	assert.Empty(t, westStmts)

	// 4. Commit the east transaction must not affect the west transaction.
	status, err := backend.CommitTransaction(ctxEast, eastTxID)
	require.NoError(t, err)
	assert.Equal(t, "Transaction committed", status)

	// East transaction is gone.
	eastTxnsAfter := backend.ListTransactions(ctxEast)
	assert.Empty(t, eastTxnsAfter)

	// West transaction is untouched.
	westTxnsAfter := backend.ListTransactions(ctxWest)
	require.Len(t, westTxnsAfter, 1)
	assert.Contains(t, westTxnsAfter, westTxID)

	// 5. Using a transaction ID that only exists in west must fail from east.
	// Create a second west TX to get a unique ID (txn-000002) that east never had.
	westTxID2, err := backend.BeginTransaction(ctxWest, "arn")
	require.NoError(t, err)

	_, err = backend.CommitTransaction(ctxEast, westTxID2)
	require.Error(t, err, "west-only transaction ID must not be resolvable from the east region")

	// 6. Rollback the west transactions leaves east region unaffected.
	_, err = backend.RollbackTransaction(ctxWest, westTxID)
	require.NoError(t, err)
	_, err = backend.RollbackTransaction(ctxWest, westTxID2)
	require.NoError(t, err)

	westFinal := backend.ListTransactions(ctxWest)
	assert.Empty(t, westFinal)
}

// TestRDSDataDefaultRegionFallback verifies that a context without a region falls
// back to the backend's configured default region.
func TestRDSDataDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "eu-central-1")

	// No region in context → default region.
	txID, err := backend.BeginTransaction(context.Background(), "arn")
	require.NoError(t, err)

	// Reading via the explicit default region sees the transaction.
	txns := backend.ListTransactions(rdsdataCtxRegion("eu-central-1"))
	require.Len(t, txns, 1)
	assert.Contains(t, txns, txID)

	// A different region sees nothing.
	other := backend.ListTransactions(rdsdataCtxRegion("ap-south-1"))
	assert.Empty(t, other)
}
