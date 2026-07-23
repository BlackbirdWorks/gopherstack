package rdsdata_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rdsdata"
)

func TestBackend_Region(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "eu-west-1")
	assert.Equal(t, "eu-west-1", b.Region())
}

// TestBackend_AccountID verifies AccountID is exposed correctly.
func TestBackend_AccountID(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("123456789012", "eu-west-1")
	assert.Equal(t, "123456789012", b.AccountID())
}

// TestBackend_Reset verifies Reset clears all state.
func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.BeginTransaction(
		context.Background(),
		"arn:aws:rds:us-east-1:000000000000:cluster:test",
	)
	require.NoError(t, err)

	_, _, _, _, err = b.ExecuteStatement(
		context.Background(),
		"arn:aws:rds:us-east-1:000000000000:cluster:test",
		"SELECT 1",
		"",
	)
	require.NoError(t, err)

	b.Reset()

	assert.Equal(t, 0, rdsdata.TransactionCount(b))
	assert.Equal(t, 0, rdsdata.ExecutedStatementCount(b))
}

// TestHandler_Reset verifies Handler.Reset() delegates to the backend.
func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	h := rdsdata.NewHandler(b)

	_, err := b.BeginTransaction(
		context.Background(),
		"arn:aws:rds:us-east-1:000000000000:cluster:test",
	)
	require.NoError(t, err)

	h.Reset()

	assert.Equal(t, 0, rdsdata.TransactionCount(b))
}

// TestBackend_AddTransactionInternal verifies the seed helper works.
func TestBackend_AddTransactionInternal(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddTransactionInternal("txn-seeded")

	txns := b.ListTransactions(context.Background())
	assert.Contains(t, txns, "txn-seeded")
}

// TestBackend_ExecutedStatementCount verifies the export helper.
func TestBackend_ExecutedStatementCount(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, rdsdata.ExecutedStatementCount(b))

	_, _, _, _, err := b.ExecuteStatement(context.Background(), "arn", "SELECT 1", "")
	require.NoError(t, err)
	assert.Equal(t, 1, rdsdata.ExecutedStatementCount(b))
}

// TestBackend_TransactionCount verifies the export helper.
func TestBackend_TransactionCount(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, rdsdata.TransactionCount(b))

	_, err := b.BeginTransaction(context.Background(), "arn")
	require.NoError(t, err)
	assert.Equal(t, 1, rdsdata.TransactionCount(b))
}

// TestBackend_MultipleTransactions verifies multiple concurrent transactions.
func TestBackend_MultipleTransactions(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")

	tx1, err := b.BeginTransaction(context.Background(), "arn1")
	require.NoError(t, err)
	tx2, err := b.BeginTransaction(context.Background(), "arn1")
	require.NoError(t, err)

	assert.NotEqual(t, tx1, tx2)
	assert.Equal(t, 2, rdsdata.TransactionCount(b))

	_, err = b.CommitTransaction(context.Background(), tx1)
	require.NoError(t, err)
	assert.Equal(t, 1, rdsdata.TransactionCount(b))

	_, err = b.RollbackTransaction(context.Background(), tx2)
	require.NoError(t, err)
	assert.Equal(t, 0, rdsdata.TransactionCount(b))
}

// TestBackend_StorageBackendInterface verifies that Handler accepts a StorageBackend.
func TestBackend_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	var b rdsdata.StorageBackend = rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	h := rdsdata.NewHandler(b)
	assert.NotNil(t, h)
}

// TestBackend_SnapshotRestore verifies full round-trip of Snapshot/Restore.
func TestBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.BeginTransaction(
		context.Background(),
		"arn:aws:rds:us-east-1:000000000000:cluster:test",
	)
	require.NoError(t, err)

	_, _, _, _, err = b.ExecuteStatement(
		context.Background(),
		"arn:aws:rds:us-east-1:000000000000:cluster:test",
		"SELECT 42",
		"",
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := rdsdata.NewInMemoryBackend("", "")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, rdsdata.TransactionCount(b2))
	assert.Equal(t, 1, rdsdata.ExecutedStatementCount(b2))

	stmts := b2.ListExecutedStatements(context.Background())
	require.Len(t, stmts, 1)
	assert.Equal(t, "SELECT 42", stmts[0].SQL)
}

// TestBackend_RestoreInvalidJSON verifies Restore returns an error on invalid JSON.
func TestBackend_RestoreInvalidJSON(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-json"))
	require.Error(t, err)
}

// TestBackend_SnapshotEmpty verifies Snapshot works on an empty backend.
func TestBackend_SnapshotEmpty(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := rdsdata.NewInMemoryBackend("", "")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 0, rdsdata.TransactionCount(b2))
	assert.Equal(t, 0, rdsdata.ExecutedStatementCount(b2))
}

// TestBackend_SnapshotPreservesCounter verifies txCounter is preserved in snapshot.
func TestBackend_SnapshotPreservesCounter(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")

	// Create 3 transactions so counter is at 3.
	for range 3 {
		_, err := b.BeginTransaction(context.Background(), "arn")
		require.NoError(t, err)
	}

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := rdsdata.NewInMemoryBackend("", "")
	require.NoError(t, b2.Restore(t.Context(), snap))

	// After restore, the next transaction ID should continue from 4.
	txID, err := b2.BeginTransaction(context.Background(), "arn")
	require.NoError(t, err)
	assert.Equal(t, "txn-000004", txID)
}

// TestBackend_SnapshotRegionAccountID verifies AccountID and Region survive snapshot round-trip.
func TestBackend_SnapshotRegionAccountID(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("111111111111", "ap-southeast-1")
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := rdsdata.NewInMemoryBackend("", "")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, "111111111111", b2.AccountID())
	assert.Equal(t, "ap-southeast-1", b2.Region())
}
