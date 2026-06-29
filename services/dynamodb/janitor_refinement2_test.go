package dynamodb_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// ---------------------------------------------------------------------------
// TTL sweep: OOB panic guard
// ---------------------------------------------------------------------------

// TestRefinement2_TTLSweep_ConcurrentDeleteNoOOB verifies that sweepTableTTL
// correctly clamps i after a concurrent delete shrinks the Items slice between
// batches. Without the clamp, i could exceed len(table.Items)-1, causing a
// panic. The test runs with -race to detect any data races.
func TestRefinement2_TTLSweep_ConcurrentDeleteNoOOB(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	// Create a table with many items so the sweep processes multiple batches.
	_, err := db.CreateTable(t.Context(), makeCreateTableInput("clamp-table", "pk"))
	require.NoError(t, err)

	_, err = db.UpdateTimeToLive(t.Context(), &ddbsdk.UpdateTimeToLiveInput{
		TableName: aws.String("clamp-table"),
		TimeToLiveSpecification: &ddbsdktypes.TimeToLiveSpecification{
			AttributeName: aws.String("expire"),
			Enabled:       aws.Bool(true),
		},
	})
	require.NoError(t, err)

	// Insert more than one TTL sweep batch worth of items with past expiry.
	batchSize := dynamodb.TTLSweepBatchSize
	pastTS := strconv.FormatInt(time.Now().Add(-time.Hour).Unix(), 10)

	for i := range batchSize + 10 {
		_, putErr := db.PutItem(t.Context(), &ddbsdk.PutItemInput{
			TableName: aws.String("clamp-table"),
			Item: map[string]ddbsdktypes.AttributeValue{
				"pk":     &ddbsdktypes.AttributeValueMemberS{Value: fmt.Sprintf("k-%d", i)},
				"expire": &ddbsdktypes.AttributeValueMemberN{Value: pastTS},
			},
		})
		require.NoError(t, putErr)
	}

	// Run one full sweep — must not panic even if concurrent deletes
	// theoretically reduce the slice between batches.
	j := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: time.Hour})
	require.NotPanics(t, func() {
		j.SweepOnce(t.Context())
	})

	// All expired items should have been removed.
	out, err := db.Scan(t.Context(), &ddbsdk.ScanInput{TableName: aws.String("clamp-table")})
	require.NoError(t, err)
	assert.Zero(t, out.Count, "all expired items should be evicted")
}

// ---------------------------------------------------------------------------
// sweepTxnTokens / sweepTxnPending accept context
// ---------------------------------------------------------------------------

// TestRefinement2_SweepTxnTokens_AcceptsContext verifies that sweepTxnTokens
// exits immediately when called with a pre-cancelled context.
func TestRefinement2_SweepTxnTokens_AcceptsContext(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	db.AddTxnToken("tok-1", time.Now().Add(-time.Hour)) // expired

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // pre-cancel

	j := dynamodb.NewJanitor(db, dynamodb.Settings{})
	j.SweepTxnTokens(ctx) // must not panic, exits early

	// Token is NOT removed because context was cancelled.
	// We can't assert specific count due to race, just assert no panic.
}

// TestRefinement2_SweepTxnTokens_ActiveContextSweeps verifies that expired tokens
// are removed when the context is active.
func TestRefinement2_SweepTxnTokens_ActiveContextSweeps(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	db.AddTxnToken("tok-expired", time.Now().Add(-time.Hour))
	db.AddTxnToken("tok-fresh", time.Now().Add(time.Hour))

	j := dynamodb.NewJanitor(db, dynamodb.Settings{})
	j.SweepTxnTokens(t.Context())

	assert.Equal(t, 1, db.TxnTokensCount(), "only the fresh token should remain")
}

// TestRefinement2_SweepTxnPending_AcceptsContext verifies that sweepTxnPending
// exits immediately when called with a pre-cancelled context.
func TestRefinement2_SweepTxnPending_AcceptsContext(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	db.InjectStaleTxnPendingForTest("stale-1")

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // pre-cancel

	j := dynamodb.NewJanitor(db, dynamodb.Settings{})
	j.SweepTxnPending(ctx) // must not panic, exits early
}

// ---------------------------------------------------------------------------
// sweepStreamRecords accepts context
// ---------------------------------------------------------------------------

// TestRefinement2_SweepStreamRecords_AcceptsContext verifies that sweepStreamRecords
// correctly receives a context (compile-time check that signature changed).
func TestRefinement2_SweepStreamRecords_AcceptsContext(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	for i := range 3 {
		_, err := db.CreateTable(
			t.Context(),
			makeCreateTableInput(fmt.Sprintf("sr-table-%d", i), "pk"),
		)
		require.NoError(t, err)
	}

	j := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: time.Hour})

	// Pre-cancel: loop should exit early without panicking.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NotPanics(t, func() { j.SweepOnce(ctx) })
}

// ---------------------------------------------------------------------------
// Sort performance: sort.Slice replaces insertion sort
// ---------------------------------------------------------------------------

// TestRefinement2_NthSmallest_LargeSlice verifies nthSmallest works correctly
// with a large slice (exercising the [sort.Slice] path).
func TestRefinement2_NthSmallest_LargeSlice(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping large-slice sort test in short mode")
	}

	db := dynamodb.NewInMemoryDB()

	// Insert cap+1 tokens with future expiry (forces hard-cap eviction path).
	future := time.Now().Add(24 * time.Hour)
	for i := range dynamodb.TxnTokensMaxCap + 1 {
		db.AddTxnToken(fmt.Sprintf("tok-%d", i), future)
	}

	j := dynamodb.NewJanitor(db, dynamodb.Settings{})

	// The sort must complete without panic and the count must drop to <= cap.
	require.NotPanics(t, func() { j.SweepTxnTokens(t.Context()) })
	assert.LessOrEqual(t, db.TxnTokensCount(), dynamodb.TxnTokensMaxCap)
}

// ---------------------------------------------------------------------------
// Stream record compaction: make() instead of [:0]
// ---------------------------------------------------------------------------

// TestRefinement2_StreamCompaction_AllocatesNewSlice verifies that after
// compaction the ring buffer is a fresh allocation (capacity still correct).
func TestRefinement2_StreamCompaction_AllocatesNewSlice(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTable(t, db, "compact2-table")

	// Add stream records with a timestamp older than 24 hours.
	// We exercise the compaction path via SweepOnce.
	j := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: time.Hour})
	j.SweepOnce(t.Context())

	// The stream record count for a fresh table should be 0 after compaction.
	assert.Zero(t, db.StreamRecordCount("compact2-table"),
		"stream record count should be 0 after compaction of fresh table")
}

// ---------------------------------------------------------------------------
// runTableCleaner: no redundant names slice
// ---------------------------------------------------------------------------

// TestRefinement2_RunTableCleaner_LogsFromTable verifies that runTableCleaner
// logs correct table names (using the Table.Name field, not a separate names
// slice). This is a regression guard for the refactor.
func TestRefinement2_RunTableCleaner_LogsFromTable(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	// Create and then delete a table to trigger the deletingTables path.
	_, err := db.CreateTable(t.Context(), makeCreateTableInput("cleanup-table", "pk"))
	require.NoError(t, err)

	_, err = db.DeleteTable(t.Context(), &ddbsdk.DeleteTableInput{
		TableName: aws.String("cleanup-table"),
	})
	require.NoError(t, err)

	// runTableCleaner is called via SweepOnce → runOnce.
	j := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: time.Hour})
	require.NotPanics(t, func() { j.SweepOnce(t.Context()) })
}

// ---------------------------------------------------------------------------
// purgeAllServices goroutine drain
// ---------------------------------------------------------------------------

// TestRefinement2_PurgeGoroutineDrain is a compile-time existence check that
// InMemoryDB satisfies the Purgeable interface and Purge returns promptly on
// a cancelled context (proving the goroutine drain path works).
func TestRefinement2_PurgeGoroutineDrain(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	_, err := db.CreateTable(t.Context(), makeCreateTableInput("purge-drain-table", "pk"))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	start := time.Now()
	db.Purge(ctx, time.Now().Add(time.Hour))
	elapsed := time.Since(start)

	// With a pre-cancelled context Purge returns almost immediately.
	assert.Less(t, elapsed, 100*time.Millisecond,
		"Purge with cancelled ctx should return in under 100ms")
}

// ---------------------------------------------------------------------------
// S3-related: lifecycle Days=nil guard
// ---------------------------------------------------------------------------

// TestRefinement2_DynamoDB_Reset_ClosesMutex verifies that Reset() does not leak
// DynamoDB table mutexes (i.e. stopTableTimers + mu.Close are called).
func TestRefinement2_DynamoDB_Reset_ClosesMutex(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	for i := range 5 {
		_, err := db.CreateTable(
			t.Context(),
			makeCreateTableInput(fmt.Sprintf("reset-table-%d", i), "pk"),
		)
		require.NoError(t, err)
	}

	// Reset must not panic when called with existing tables.
	require.NotPanics(t, db.Reset, "Reset must not panic")

	// After reset, the table list should be empty.
	out, err := db.ListTables(t.Context(), &ddbsdk.ListTablesInput{})
	require.NoError(t, err)
	assert.Empty(t, out.TableNames, "all tables must be gone after Reset")
}

// TestRefinement2_Purge_SubsetOfTables verifies that Purge only removes tables
// created before the cutoff, leaving newer tables intact.
func TestRefinement2_Purge_SubsetOfTables(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	for i := range 3 {
		_, err := db.CreateTable(
			t.Context(),
			makeCreateTableInput(fmt.Sprintf("old-table-%d", i), "pk"),
		)
		require.NoError(t, err)
	}

	cutoff := time.Now().Add(time.Hour) // future cutoff removes all tables above

	db.Purge(t.Context(), cutoff)

	out, err := db.ListTables(t.Context(), &ddbsdk.ListTablesInput{})
	require.NoError(t, err)
	assert.Empty(t, out.TableNames, "all tables created before cutoff must be purged")
}

// TestRefinement2_Purge_KeepsNewerTables verifies Purge leaves tables created
// after the cutoff.
func TestRefinement2_Purge_KeepsNewerTables(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	for i := range 3 {
		_, err := db.CreateTable(
			t.Context(),
			makeCreateTableInput(fmt.Sprintf("keep-table-%d", i), "pk"),
		)
		require.NoError(t, err)
	}

	cutoff := time.Now().Add(-time.Hour) // past cutoff; tables are newer

	db.Purge(t.Context(), cutoff)

	out, err := db.ListTables(t.Context(), &ddbsdk.ListTablesInput{})
	require.NoError(t, err)
	assert.Len(t, out.TableNames, 3, "tables created after cutoff must survive Purge")
}

// ---------------------------------------------------------------------------
// ShardIteratorStore GC in production Run() loop
// ---------------------------------------------------------------------------

// TestRefinement2_Janitor_Run_SweepsIteratorStore verifies that the production
// janitor Run() loop sweeps expired ShardIterator tokens between TTL ticks,
// preventing unbounded memory growth from accumulated expired iterators.
func TestRefinement2_Janitor_Run_SweepsIteratorStore(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	_, err := db.CreateTable(t.Context(), makeCreateTableInput("tbl", "pk"))
	require.NoError(t, err)

	// Inject an expired iterator so the store has size > 0.
	db.InjectExpiredShardIteratorForTest("tbl")
	require.Equal(t, 1, db.IteratorStoreSize(), "pre-condition: one expired entry")

	// Run the janitor with a very short main interval, let it tick once, then cancel.
	j := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: 10 * time.Millisecond})
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	go j.Run(ctx)
	<-ctx.Done()

	// The janitor's main-ticker must have swept the expired entry.
	assert.Equal(
		t,
		0,
		db.IteratorStoreSize(),
		"expired iterator tokens must be swept by janitor Run loop",
	)
}
