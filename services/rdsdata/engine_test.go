package rdsdata_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rdsdata"
)

const engineARN = "arn:aws:rds:us-east-1:000000000000:cluster:engine-test"

func newEngineBackend() *rdsdata.InMemoryBackend {
	return rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
}

func int64Ptr(v int64) *int64 { return new(v) }

// TestEngine_CreateInsertSelectRoundTrip verifies that a table created and
// populated through the Data API can be read back with real values.
func TestEngine_CreateInsertSelectRoundTrip(t *testing.T) {
	t.Parallel()

	b := newEngineBackend()
	ctx := context.Background()

	_, _, _, err := b.ExecuteStatement(ctx, engineARN, "CREATE TABLE users (id INTEGER, name TEXT)", "")
	require.NoError(t, err)

	_, _, updated, err := b.ExecuteStatement(ctx, engineARN, "INSERT INTO users (id, name) VALUES (1, 'ada')", "")
	require.NoError(t, err)
	assert.Equal(t, int64(1), updated)

	records, columns, _, err := b.ExecuteStatement(
		ctx, engineARN, "SELECT id, name FROM users ORDER BY id", "")
	require.NoError(t, err)

	require.Len(t, records, 1)
	require.Len(t, records[0], 2)
	require.NotNil(t, records[0][0].LongValue)
	assert.Equal(t, int64(1), *records[0][0].LongValue)
	require.NotNil(t, records[0][1].StringValue)
	assert.Equal(t, "ada", *records[0][1].StringValue)
	assert.Len(t, columns, 2)
	assert.Equal(t, "id", columns[0].Name)
}

// TestEngine_SelectLiteral verifies a parameterless scalar query returns a row.
func TestEngine_SelectLiteral(t *testing.T) {
	t.Parallel()

	b := newEngineBackend()

	records, _, _, err := b.ExecuteStatement(context.Background(), engineARN, "SELECT 42", "")
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.NotNil(t, records[0][0].LongValue)
	assert.Equal(t, int64(42), *records[0][0].LongValue)
}

// TestEngine_LenientFallbackOnError verifies that statements the engine rejects
// degrade to the historical empty-success envelope instead of erroring.
func TestEngine_LenientFallbackOnError(t *testing.T) {
	t.Parallel()

	b := newEngineBackend()

	records, columns, updated, err := b.ExecuteStatement(
		context.Background(), engineARN, "INSERT INTO missing_table VALUES (1)", "")
	require.NoError(t, err)
	assert.Empty(t, records)
	assert.Empty(t, columns)
	assert.Zero(t, updated)
}

// TestEngine_ResourceIsolation verifies separate resource ARNs get separate DBs.
func TestEngine_ResourceIsolation(t *testing.T) {
	t.Parallel()

	b := newEngineBackend()
	ctx := context.Background()
	const otherARN = "arn:aws:rds:us-east-1:000000000000:cluster:other"

	_, _, _, err := b.ExecuteStatement(ctx, engineARN, "CREATE TABLE only_here (id INTEGER)", "")
	require.NoError(t, err)

	// The table does not exist on a different resource, so the read falls back
	// to the empty envelope rather than returning rows.
	records, _, _, err := b.ExecuteStatement(ctx, otherARN, "SELECT * FROM only_here", "")
	require.NoError(t, err)
	assert.Empty(t, records)
}

// TestEngine_TransactionCommit verifies that work inside a committed
// transaction is visible afterwards.
func TestEngine_TransactionCommit(t *testing.T) {
	t.Parallel()

	b := newEngineBackend()
	ctx := context.Background()

	_, _, _, err := b.ExecuteStatement(ctx, engineARN, "CREATE TABLE acct (n INTEGER)", "")
	require.NoError(t, err)

	txID, err := b.BeginTransaction(ctx, engineARN)
	require.NoError(t, err)

	_, _, _, err = b.ExecuteStatement(ctx, engineARN, "INSERT INTO acct (n) VALUES (7)", txID)
	require.NoError(t, err)

	_, err = b.CommitTransaction(ctx, txID)
	require.NoError(t, err)

	records, _, _, err := b.ExecuteStatement(ctx, engineARN, "SELECT n FROM acct", "")
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, int64(7), *records[0][0].LongValue)
}

// TestEngine_TransactionRollback verifies that rolled-back work is discarded.
func TestEngine_TransactionRollback(t *testing.T) {
	t.Parallel()

	b := newEngineBackend()
	ctx := context.Background()

	_, _, _, err := b.ExecuteStatement(ctx, engineARN, "CREATE TABLE roll (n INTEGER)", "")
	require.NoError(t, err)

	txID, err := b.BeginTransaction(ctx, engineARN)
	require.NoError(t, err)

	_, _, _, err = b.ExecuteStatement(ctx, engineARN, "INSERT INTO roll (n) VALUES (9)", txID)
	require.NoError(t, err)

	_, err = b.RollbackTransaction(ctx, txID)
	require.NoError(t, err)

	records, _, _, err := b.ExecuteStatement(ctx, engineARN, "SELECT n FROM roll", "")
	require.NoError(t, err)
	assert.Empty(t, records)
}

// TestEngine_ExecuteStatementWithNamedParameters verifies that bound parameters
// are substituted into a single ExecuteStatement on both the write and read path.
func TestEngine_ExecuteStatementWithNamedParameters(t *testing.T) {
	t.Parallel()

	b := newEngineBackend()
	ctx := context.Background()

	_, _, _, err := b.ExecuteStatement(ctx, engineARN, "CREATE TABLE people (id INTEGER, name TEXT)", "")
	require.NoError(t, err)

	strVal := "grace"
	_, _, updated, err := b.ExecuteStatement(
		ctx, engineARN, "INSERT INTO people (id, name) VALUES (:id, :name)", "",
		rdsdata.SQLParameter{Name: "id", Value: rdsdata.Field{LongValue: int64Ptr(10)}},
		rdsdata.SQLParameter{Name: "name", Value: rdsdata.Field{StringValue: &strVal}},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(1), updated)

	records, _, _, err := b.ExecuteStatement(
		ctx, engineARN, "SELECT name FROM people WHERE id = :id", "",
		rdsdata.SQLParameter{Name: "id", Value: rdsdata.Field{LongValue: int64Ptr(10)}},
	)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.NotNil(t, records[0][0].StringValue)
	assert.Equal(t, "grace", *records[0][0].StringValue)
}

// TestEngine_BatchExecuteInsertsRows verifies that BatchExecuteStatement applies
// every parameter set against the engine.
func TestEngine_BatchExecuteInsertsRows(t *testing.T) {
	t.Parallel()

	b := newEngineBackend()
	ctx := context.Background()

	_, _, _, err := b.ExecuteStatement(ctx, engineARN, "CREATE TABLE nums (id INTEGER)", "")
	require.NoError(t, err)

	paramSets := [][]rdsdata.SQLParameter{
		{{Name: "id", Value: rdsdata.Field{LongValue: int64Ptr(1)}}},
		{{Name: "id", Value: rdsdata.Field{LongValue: int64Ptr(2)}}},
		{{Name: "id", Value: rdsdata.Field{LongValue: int64Ptr(3)}}},
	}

	results, err := b.BatchExecuteStatement(ctx, engineARN, "INSERT INTO nums (id) VALUES (:id)", "", paramSets)
	require.NoError(t, err)
	assert.Len(t, results, 3)

	records, _, _, err := b.ExecuteStatement(ctx, engineARN, "SELECT COUNT(*) FROM nums", "")
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, int64(3), *records[0][0].LongValue)
}

// TestEngine_SnapshotRestoreReplaysState verifies that table state is rebuilt
// from the recorded statement log on restore.
func TestEngine_SnapshotRestoreReplaysState(t *testing.T) {
	t.Parallel()

	b := newEngineBackend()
	ctx := context.Background()

	_, _, _, err := b.ExecuteStatement(ctx, engineARN, "CREATE TABLE keep (id INTEGER)", "")
	require.NoError(t, err)
	_, _, _, err = b.ExecuteStatement(ctx, engineARN, "INSERT INTO keep (id) VALUES (5)", "")
	require.NoError(t, err)

	snap := b.Snapshot(ctx)
	require.NotNil(t, snap)

	restored := newEngineBackend()
	require.NoError(t, restored.Restore(ctx, snap))

	records, _, _, err := restored.ExecuteStatement(ctx, engineARN, "SELECT id FROM keep", "")
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, int64(5), *records[0][0].LongValue)
}

// TestEngine_ResetClearsTables verifies Reset drops engine state.
func TestEngine_ResetClearsTables(t *testing.T) {
	t.Parallel()

	b := newEngineBackend()
	ctx := context.Background()

	_, _, _, err := b.ExecuteStatement(ctx, engineARN, "CREATE TABLE gone (id INTEGER)", "")
	require.NoError(t, err)
	_, _, _, err = b.ExecuteStatement(ctx, engineARN, "INSERT INTO gone (id) VALUES (1)", "")
	require.NoError(t, err)

	b.Reset()

	records, _, _, err := b.ExecuteStatement(ctx, engineARN, "SELECT id FROM gone", "")
	require.NoError(t, err)
	assert.Empty(t, records)
}

// TestEngine_ColumnMetadata_TypeAffinity verifies that ColumnMetadata.Type,
// IsSigned, and IsCaseSensitive are derived from each column's declared
// SQLite type affinity (see sqliteAffinity), not left at zero values.
func TestEngine_ColumnMetadata_TypeAffinity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantType      int32
		wantSigned    bool
		wantCaseSense bool
		wantNullable  int32
	}{
		{name: "n INTEGER", wantType: 4, wantSigned: true, wantNullable: 1},
		{name: "s TEXT", wantType: 12, wantCaseSense: true, wantNullable: 1},
		{name: "d REAL", wantType: 8, wantSigned: true, wantNullable: 1},
		{name: "m NUMERIC", wantType: 3, wantSigned: true, wantNullable: 1},
		{name: "b BLOB", wantType: 2004, wantNullable: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newEngineBackend()
			ctx := context.Background()
			arn := "arn:aws:rds:us-east-1:000000000000:cluster:coltype-" + tt.name

			_, _, _, err := b.ExecuteStatement(ctx, arn, "CREATE TABLE t ("+tt.name+")", "")
			require.NoError(t, err)

			_, columns, _, err := b.ExecuteStatement(ctx, arn, "SELECT * FROM t", "")
			require.NoError(t, err)
			require.Len(t, columns, 1)

			col := columns[0]
			assert.Equal(t, tt.wantType, col.Type, "Type")
			assert.Equal(t, tt.wantSigned, col.IsSigned, "IsSigned")
			assert.Equal(t, tt.wantCaseSense, col.IsCaseSensitive, "IsCaseSensitive")
			assert.Equal(t, tt.wantNullable, col.Nullable, "Nullable")
		})
	}
}

// TestEngine_ExecuteSQLUpdatesCount verifies the deprecated ExecuteSql path
// reports the real update count.
func TestEngine_ExecuteSQLUpdatesCount(t *testing.T) {
	t.Parallel()

	b := newEngineBackend()
	ctx := context.Background()

	_, err := b.ExecuteSQL(ctx, engineARN, "CREATE TABLE legacy (id INTEGER)")
	require.NoError(t, err)

	results, err := b.ExecuteSQL(ctx, engineARN, "INSERT INTO legacy (id) VALUES (1)")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, int64(1), results[0].NumberOfRecordsUpdated)
}
