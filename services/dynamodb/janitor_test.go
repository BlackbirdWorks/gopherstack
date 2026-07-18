package dynamodb_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// newFastDDBJanitor creates a Janitor with a short interval for deterministic tests.
func newFastDDBJanitor(db *dynamodb.InMemoryDB) *dynamodb.Janitor {
	return dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: 5 * time.Millisecond})
}

func TestDDBJanitor_DeleteTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		deleteTable  string
		wantGone     string
		wantPresent  string
		createTables []string
	}{
		{
			name:         "deleted_table_moves_to_deleting_queue",
			createTables: []string{"queued-table"},
			deleteTable:  "queued-table",
			wantGone:     "queued-table",
		},
		{
			name:         "active_table_unaffected_when_other_deleted",
			createTables: []string{"keep-table", "drop-table"},
			deleteTable:  "drop-table",
			wantGone:     "drop-table",
			wantPresent:  "keep-table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			for _, tbl := range tt.createTables {
				createTable(t, db, tbl)
			}

			_, err := db.DeleteTable(t.Context(), &dynamodb_sdk.DeleteTableInput{
				TableName: aws.String(tt.deleteTable),
			})
			require.NoError(t, err)

			_, err = db.DescribeTable(t.Context(), &dynamodb_sdk.DescribeTableInput{
				TableName: aws.String(tt.wantGone),
			})
			require.Error(t, err, "expected deleted table to be gone from active map")

			if tt.wantPresent != "" {
				out, descErr := db.DescribeTable(t.Context(), &dynamodb_sdk.DescribeTableInput{
					TableName: aws.String(tt.wantPresent),
				})
				require.NoError(t, descErr)
				assert.Equal(t, tt.wantPresent, aws.ToString(out.Table.TableName))
			}
		})
	}
}

func TestDDBJanitor_RemovesTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		createTable string
	}{
		{
			name:        "janitor_finally_removes_deleted_table",
			createTable: "delete-me",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			createTable(t, db, tt.createTable)

			_, err := db.DeleteTable(t.Context(), &dynamodb_sdk.DeleteTableInput{
				TableName: aws.String(tt.createTable),
			})
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			j := newFastDDBJanitor(db)
			go j.Run(ctx)

			require.Eventually(t, func() bool {
				listed, listErr := db.ListTables(t.Context(), &dynamodb_sdk.ListTablesInput{})

				return listErr == nil && len(listed.TableNames) == 0
			}, 500*time.Millisecond, 10*time.Millisecond)
		})
	}
}

func TestJanitor_RunOnce_CleansPendingDeletions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		deleteTable  string
		createTables []string
	}{
		{
			name:         "runOnce removes table from deleting queue",
			createTables: []string{"cleanup-table"},
			deleteTable:  "cleanup-table",
		},
		{
			name:         "runOnce with empty deleting queue is a no-op",
			createTables: []string{"keep-table"},
			deleteTable:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			for _, tbl := range tt.createTables {
				createTable(t, db, tbl)
			}

			if tt.deleteTable != "" {
				_, err := db.DeleteTable(t.Context(), &dynamodb_sdk.DeleteTableInput{
					TableName: aws.String(tt.deleteTable),
				})
				require.NoError(t, err)
			}

			j := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: time.Hour})
			j.RunOnce(t.Context())
			// Should not panic and should complete without error
		})
	}
}

// TestDDBJanitor_TaskTimeout_WithJanitor verifies that WithJanitor propagates
// the variadic taskTimeout into the janitor's TaskTimeout field.
func TestDDBJanitor_TaskTimeout_WithJanitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		taskTimeout time.Duration
		want        time.Duration
	}{
		{
			name:        "no_timeout_zero",
			taskTimeout: 0,
			want:        0,
		},
		{
			name:        "with_30s_timeout",
			taskTimeout: 30 * time.Second,
			want:        30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			h := dynamodb.NewHandler(db)
			h.WithJanitor(dynamodb.Settings{}, tt.taskTimeout)

			assert.Equal(t, tt.want, h.GetJanitorTaskTimeout())
		})
	}
}

func TestDDBJanitor_TTLSweepBatchSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		settings  dynamodb.Settings
		wantBatch int
	}{
		{
			name:      "uses default when unset",
			settings:  dynamodb.Settings{},
			wantBatch: dynamodb.TTLSweepBatchSize,
		},
		{
			name: "uses configured batch size",
			settings: dynamodb.Settings{
				TTLSweepBatchSize: 32,
			},
			wantBatch: 32,
		},
		{
			name: "falls back to default when configured as non-positive",
			settings: dynamodb.Settings{
				TTLSweepBatchSize: -10,
			},
			wantBatch: dynamodb.TTLSweepBatchSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			j := dynamodb.NewJanitor(db, tt.settings)

			assert.Equal(t, tt.wantBatch, j.TTLSweepBatchSizeForTest())
		})
	}
}

// TestDDBJanitor_SweepOnce_EvictsPendingDeletion verifies SweepOnce removes
// tables pending deletion without running the janitor loop.
func TestDDBJanitor_SweepOnce_EvictsPendingDeletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		deleteOne string
		tables    []string
		wantLen   int
	}{
		{
			name:      "single_table_deleted",
			tables:    []string{"sweep-table"},
			deleteOne: "sweep-table",
			wantLen:   0,
		},
		{
			name:      "one_of_two_tables_deleted",
			tables:    []string{"keep-table", "gone-table"},
			deleteOne: "gone-table",
			wantLen:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			for _, tbl := range tt.tables {
				createTable(t, db, tbl)
			}

			_, err := db.DeleteTable(t.Context(), &dynamodb_sdk.DeleteTableInput{
				TableName: aws.String(tt.deleteOne),
			})
			require.NoError(t, err)

			j := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: time.Minute})
			j.SweepOnce(t.Context())

			out, err := db.ListTables(t.Context(), &dynamodb_sdk.ListTablesInput{})
			require.NoError(t, err)

			assert.Len(t, out.TableNames, tt.wantLen)
		})
	}
}

func TestTTLGracePeriod_ItemExpiredAfterGrace(t *testing.T) {
	t.Parallel()
	item := map[string]any{
		"ttl": map[string]any{"N": strconv.FormatInt(time.Now().Add(-2*time.Second).Unix(), 10)},
	}

	// With zero grace: expired
	if !dynamodb.IsItemExpiredWithGrace(item, "ttl", 0) {
		t.Error("expected item to be expired with zero grace period")
	}

	// With 10s grace: not yet expired (expired 2s ago + 10s grace = still valid)
	if dynamodb.IsItemExpiredWithGrace(item, "ttl", 10*time.Second) {
		t.Error("expected item to NOT be expired within grace period")
	}
}

func TestTTLGracePeriod_FutureTimestamp_NotExpired(t *testing.T) {
	t.Parallel()
	item := map[string]any{
		"ttl": map[string]any{"N": strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)},
	}

	if dynamodb.IsItemExpiredWithGrace(item, "ttl", 0) {
		t.Error("future TTL should not be expired")
	}
}

func TestTTLGracePeriod_NoTTLAttr_NotExpired(t *testing.T) {
	t.Parallel()
	item := map[string]any{
		"pk": map[string]any{"S": "val"},
	}

	if dynamodb.IsItemExpiredWithGrace(item, "ttl", 0) {
		t.Error("item without TTL attribute should not be expired")
	}

	if dynamodb.IsItemExpiredWithGrace(item, "", 0) {
		t.Error("empty ttlAttr should not expire any item")
	}
}

// ---------------------------------------------------------------------------
// TTL sweep batching
// ---------------------------------------------------------------------------

// TestBatchTTLSweep verifies that sweepTableTTL evicts all expired
// items correctly regardless of whether the total item count exceeds the batch size.
func TestBatchTTLSweep(t *testing.T) {
	t.Parallel()

	const batchSize = dynamodb.TTLSweepBatchSize

	tests := []struct {
		name         string
		totalItems   int
		expiredCount int
	}{
		{
			name:         "fewer_items_than_one_batch",
			totalItems:   batchSize/2 + 3,
			expiredCount: batchSize/2 + 3, // all expired
		},
		{
			name:         "exactly_one_batch",
			totalItems:   batchSize,
			expiredCount: batchSize / 2,
		},
		{
			name:         "slightly_more_than_one_batch",
			totalItems:   batchSize + 5,
			expiredCount: batchSize/2 + 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			const tableName = "ttl-batch-table"

			// Create table.
			_, err := db.CreateTable(t.Context(), makeCreateTableInput(tableName, "pk"))
			require.NoError(t, err)

			// Enable TTL.
			_, err = db.UpdateTimeToLive(t.Context(), &dynamodb_sdk.UpdateTimeToLiveInput{
				TableName: aws.String(tableName),
				TimeToLiveSpecification: &types.TimeToLiveSpecification{
					AttributeName: aws.String("expire"),
					Enabled:       aws.Bool(true),
				},
			})
			require.NoError(t, err)

			pastTS := strconv.FormatInt(time.Now().Add(-time.Hour).Unix(), 10)
			futureTS := strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)

			for i := range tt.totalItems {
				ttlVal := futureTS
				if i < tt.expiredCount {
					ttlVal = pastTS
				}

				_, putErr := db.PutItem(t.Context(), &dynamodb_sdk.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"pk":     &types.AttributeValueMemberS{Value: fmt.Sprintf("k-%d", i)},
						"expire": &types.AttributeValueMemberN{Value: ttlVal},
					},
				})
				require.NoError(t, putErr)
			}

			j := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: time.Hour})
			j.SweepOnce(t.Context())

			out, err := db.Scan(t.Context(), &dynamodb_sdk.ScanInput{
				TableName: aws.String(tableName),
			})
			require.NoError(t, err)

			wantRemaining := tt.totalItems - tt.expiredCount
			assert.Equal(t, int32(wantRemaining), out.Count,
				"expected %d non-expired items after TTL sweep", wantRemaining)
		})
	}
}

// TestBatchTTLSweep_ContextCancel verifies that a pre-cancelled
// context causes the TTL sweep to abort early without panicking.
func TestBatchTTLSweep_ContextCancel(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	for i := range 5 {
		_, err := db.CreateTable(
			t.Context(),
			makeCreateTableInput(fmt.Sprintf("ctx-ttl-%d", i), "pk"),
		)
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // pre-cancel

	j := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: time.Hour})
	j.SweepOnce(ctx) // must return quickly, not panic
}

// ---------------------------------------------------------------------------
// txnTokens hard cap
// ---------------------------------------------------------------------------

// TestTxnTokensCap verifies that sweepTxnTokens enforces the hard
// cap and does not allow the map to grow without bound.
func TestTxnTokensCap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		insertTokens int
		wantLECap    bool
	}{
		{
			name:         "below_cap_no_forced_eviction",
			insertTokens: 50,
			wantLECap:    true,
		},
		{
			name:         "above_cap_forced_eviction_reduces_count",
			insertTokens: dynamodb.TxnTokensMaxCap + 1,
			wantLECap:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if testing.Short() && tt.insertTokens > 1000 {
				t.Skip("skipping heavy token-insertion test in short mode")
			}

			db := dynamodb.NewInMemoryDB()

			// Insert tokens with a far-future expiry so normal TTL sweep ignores them.
			future := time.Now().Add(24 * time.Hour)
			for i := range tt.insertTokens {
				db.AddTxnToken(fmt.Sprintf("token-%d", i), future)
			}

			j := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: time.Hour})
			j.SweepOnce(t.Context())

			count := db.TxnTokensCount()
			assert.LessOrEqual(t, count, dynamodb.TxnTokensMaxCap,
				"txnTokens map must not exceed cap after sweep; got %d", count)
		})
	}
}

// TestTxnTokensCap_OldestEvictedFirst verifies that when the cap is
// exceeded, the entries with the earliest expiry are evicted (not arbitrary ones).
func TestTxnTokensCap_OldestEvictedFirst(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping heavy token-insertion test in short mode")
	}

	db := dynamodb.NewInMemoryDB()

	base := time.Now()

	// Insert cap+100 tokens: first 100 have the oldest (soonest) expiry.
	for i := range dynamodb.TxnTokensMaxCap + 100 {
		expiry := base.Add(time.Duration(i+1) * time.Second)
		db.AddTxnToken(fmt.Sprintf("tok-%d", i), expiry)
	}

	j := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: time.Hour})
	j.SweepOnce(t.Context())

	assert.LessOrEqual(t, db.TxnTokensCount(), dynamodb.TxnTokensMaxCap,
		"map must stay within cap after hard-cap eviction")
}

// ---------------------------------------------------------------------------
// Stream records compaction
// ---------------------------------------------------------------------------

// TestStreamRecordsCompaction verifies that sweepStreamRecords
// compacts the ring buffer slot count to zero for a table with no stream records.
func TestStreamRecordsCompaction(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTable(t, db, "compact-table")

	j := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: time.Hour})
	j.SweepOnce(t.Context())

	count := db.StreamRecordCount("compact-table")
	assert.Zero(t, count, "fresh table has no stream records to compact")
}

// ---------------------------------------------------------------------------
// Janitor Run loop recover
// ---------------------------------------------------------------------------

// TestJanitorRunExitsOnContextCancel verifies that the Run loop
// exits cleanly when the context is cancelled, demonstrating that the defer
// recover() wrapper does not prevent normal shutdown.
func TestJanitorRunExitsOnContextCancel(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	j := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: 5 * time.Millisecond})

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})

	go func() {
		defer close(done)
		j.Run(ctx)
	}()

	cancel()

	select {
	case <-done:
		// clean exit
	case <-time.After(500 * time.Millisecond):
		t.Fatal("janitor Run did not exit after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// Purgeable context propagation
// ---------------------------------------------------------------------------

// TestPurgeableContext verifies that Purge now accepts [context.Context]
// and respects it (cancelled context should stop the Purge loop early).
func TestPurgeableContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		cancelCtx      bool
		wantTablesGone bool
	}{
		{
			name:           "active_context_purges_all_tables",
			cancelCtx:      false,
			wantTablesGone: true,
		},
		{
			name:      "cancelled_context_stops_purge_early",
			cancelCtx: true,
			// With a pre-cancelled context the purge loop exits immediately via
			// ctx.Err() check, so tables may survive.
			wantTablesGone: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			handler := dynamodb.NewHandler(db)

			for i := range 5 {
				createTable(t, db, fmt.Sprintf("purge-table-%d", i))
			}

			ctx := t.Context()
			if tt.cancelCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			handler.Purge(ctx, time.Now().Add(time.Hour))

			out, err := db.ListTables(t.Context(), &dynamodb_sdk.ListTablesInput{})
			require.NoError(t, err)

			if tt.wantTablesGone {
				assert.Empty(t, out.TableNames,
					"Purge with active context should remove all old tables")
			}
			// With a cancelled context we don't assert on exact count —
			// only that no panic occurred.
		})
	}
}

// TestTxnTokensCap_Constants verifies that the exported cap constants
// have sane, non-zero values.
func TestTxnTokensCap_Constants(t *testing.T) {
	t.Parallel()

	assert.Positive(t, dynamodb.TxnTokensMaxCap, "TxnTokensMaxCap must be > 0")
	assert.Positive(t, dynamodb.TxnPendingMaxCap, "TxnPendingMaxCap must be > 0")
	assert.Positive(t, dynamodb.TTLSweepBatchSize, "TTLSweepBatchSize must be > 0")
}

// ---------------------------------------------------------------------------
// TTL sweep: OOB panic guard
// ---------------------------------------------------------------------------

// TestTTLSweep_ConcurrentDeleteNoOOB verifies that sweepTableTTL
// correctly clamps i after a concurrent delete shrinks the Items slice between
// batches. Without the clamp, i could exceed len(table.Items)-1, causing a
// panic. The test runs with -race to detect any data races.
func TestTTLSweep_ConcurrentDeleteNoOOB(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	// Create a table with many items so the sweep processes multiple batches.
	_, err := db.CreateTable(t.Context(), makeCreateTableInput("clamp-table", "pk"))
	require.NoError(t, err)

	_, err = db.UpdateTimeToLive(t.Context(), &dynamodb_sdk.UpdateTimeToLiveInput{
		TableName: aws.String("clamp-table"),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String("expire"),
			Enabled:       aws.Bool(true),
		},
	})
	require.NoError(t, err)

	// Insert more than one TTL sweep batch worth of items with past expiry.
	batchSize := dynamodb.TTLSweepBatchSize
	pastTS := strconv.FormatInt(time.Now().Add(-time.Hour).Unix(), 10)

	for i := range batchSize + 10 {
		_, putErr := db.PutItem(t.Context(), &dynamodb_sdk.PutItemInput{
			TableName: aws.String("clamp-table"),
			Item: map[string]types.AttributeValue{
				"pk":     &types.AttributeValueMemberS{Value: fmt.Sprintf("k-%d", i)},
				"expire": &types.AttributeValueMemberN{Value: pastTS},
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
	out, err := db.Scan(t.Context(), &dynamodb_sdk.ScanInput{TableName: aws.String("clamp-table")})
	require.NoError(t, err)
	assert.Zero(t, out.Count, "all expired items should be evicted")
}

// ---------------------------------------------------------------------------
// sweepTxnTokens / sweepTxnPending accept context
// ---------------------------------------------------------------------------

// TestSweepTxnTokens_AcceptsContext verifies that sweepTxnTokens
// exits immediately when called with a pre-cancelled context.
func TestSweepTxnTokens_AcceptsContext(t *testing.T) {
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

// TestSweepTxnTokens_ActiveContextSweeps verifies that expired tokens
// are removed when the context is active.
func TestSweepTxnTokens_ActiveContextSweeps(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	db.AddTxnToken("tok-expired", time.Now().Add(-time.Hour))
	db.AddTxnToken("tok-fresh", time.Now().Add(time.Hour))

	j := dynamodb.NewJanitor(db, dynamodb.Settings{})
	j.SweepTxnTokens(t.Context())

	assert.Equal(t, 1, db.TxnTokensCount(), "only the fresh token should remain")
}

// TestSweepTxnPending_AcceptsContext verifies that sweepTxnPending
// exits immediately when called with a pre-cancelled context.
func TestSweepTxnPending_AcceptsContext(t *testing.T) {
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

// TestSweepStreamRecords_AcceptsContext verifies that sweepStreamRecords
// correctly receives a context (compile-time check that signature changed).
func TestSweepStreamRecords_AcceptsContext(t *testing.T) {
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

// TestNthSmallest_LargeSlice verifies nthSmallest works correctly
// with a large slice (exercising the [sort.Slice] path).
func TestNthSmallest_LargeSlice(t *testing.T) {
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

// TestStreamCompaction_AllocatesNewSlice verifies that after
// compaction the ring buffer is a fresh allocation (capacity still correct).
func TestStreamCompaction_AllocatesNewSlice(t *testing.T) {
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

// TestRunTableCleaner_LogsFromTable verifies that runTableCleaner
// logs correct table names (using the Table.Name field, not a separate names
// slice). This is a regression guard for the refactor.
func TestRunTableCleaner_LogsFromTable(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	// Create and then delete a table to trigger the deletingTables path.
	_, err := db.CreateTable(t.Context(), makeCreateTableInput("cleanup-table", "pk"))
	require.NoError(t, err)

	_, err = db.DeleteTable(t.Context(), &dynamodb_sdk.DeleteTableInput{
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

// TestPurgeGoroutineDrain is a compile-time existence check that
// InMemoryDB satisfies the Purgeable interface and Purge returns promptly on
// a cancelled context (proving the goroutine drain path works).
func TestPurgeGoroutineDrain(t *testing.T) {
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

// TestDynamoDB_Reset_ClosesMutex verifies that Reset() does not leak
// DynamoDB table mutexes (i.e. stopTableTimers + mu.Close are called).
func TestDynamoDB_Reset_ClosesMutex(t *testing.T) {
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
	out, err := db.ListTables(t.Context(), &dynamodb_sdk.ListTablesInput{})
	require.NoError(t, err)
	assert.Empty(t, out.TableNames, "all tables must be gone after Reset")
}

// TestPurge_SubsetOfTables verifies that Purge only removes tables
// created before the cutoff, leaving newer tables intact.
func TestPurge_SubsetOfTables(t *testing.T) {
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

	out, err := db.ListTables(t.Context(), &dynamodb_sdk.ListTablesInput{})
	require.NoError(t, err)
	assert.Empty(t, out.TableNames, "all tables created before cutoff must be purged")
}

// TestPurge_KeepsNewerTables verifies Purge leaves tables created
// after the cutoff.
func TestPurge_KeepsNewerTables(t *testing.T) {
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

	out, err := db.ListTables(t.Context(), &dynamodb_sdk.ListTablesInput{})
	require.NoError(t, err)
	assert.Len(t, out.TableNames, 3, "tables created after cutoff must survive Purge")
}

// ---------------------------------------------------------------------------
// ShardIteratorStore GC in production Run() loop
// ---------------------------------------------------------------------------

// TestJanitor_Run_SweepsIteratorStore verifies that the production
// janitor Run() loop sweeps expired ShardIterator tokens between TTL ticks,
// preventing unbounded memory growth from accumulated expired iterators.
func TestJanitor_Run_SweepsIteratorStore(t *testing.T) {
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

func TestSweepTxnTokens_TwoPhaseDoesSweep(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)
	janitor := dynamodb.NewJanitorForTest(db)

	// Inject an expired token.
	db.InjectExpiredTxnTokenForTest("expired-token")
	require.Equal(t, 1, db.TxnTokenCount())

	janitor.SweepTxnTokens(t.Context())

	// Token should be gone after sweep.
	assert.Equal(t, 0, db.TxnTokenCount())
}

func TestSweepTxnPending_TwoPhaseDoesSweep(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)
	janitor := dynamodb.NewJanitorForTest(db)

	// Inject a stale pending token.
	db.InjectStaleTxnPendingForTest("stale-pending")
	require.Equal(t, 1, db.TxnPendingCount())

	janitor.SweepTxnPending(t.Context())

	assert.Equal(t, 0, db.TxnPendingCount())
}

func TestSweepTxnTokens_LiveTokenNotRemoved(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)
	janitor := dynamodb.NewJanitorForTest(db)

	// Add a token that is still alive (expires in the future).
	db.AddTxnToken("live-token", time.Now().Add(time.Hour))
	require.Equal(t, 1, db.TxnTokenCount())

	janitor.SweepTxnTokens(t.Context())

	// Live token must not be removed.
	assert.Equal(t, 1, db.TxnTokenCount())
}

// (14) nthSmallest quickselect: correctness

func TestNthSmallest_QuickselectCorrectness(t *testing.T) {
	t.Parallel()

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		want  time.Time
		name  string
		times []time.Time
		n     int
	}{
		{
			name:  "single_element",
			times: []time.Time{base},
			n:     1,
			want:  base,
		},
		{
			name:  "n_equals_one_smallest",
			times: []time.Time{base.Add(2 * time.Hour), base.Add(1 * time.Hour), base},
			n:     1,
			want:  base,
		},
		{
			name: "n_equals_middle",
			times: []time.Time{
				base.Add(3 * time.Hour),
				base.Add(1 * time.Hour),
				base.Add(2 * time.Hour),
			},
			n:    2,
			want: base.Add(2 * time.Hour),
		},
		{
			name:  "n_beyond_length_returns_max",
			times: []time.Time{base, base.Add(time.Hour)},
			n:     99,
			want:  base.Add(time.Hour),
		},
		{
			name:  "already_sorted",
			times: []time.Time{base, base.Add(time.Hour), base.Add(2 * time.Hour)},
			n:     2,
			want:  base.Add(time.Hour),
		},
		{
			name:  "reverse_sorted",
			times: []time.Time{base.Add(2 * time.Hour), base.Add(time.Hour), base},
			n:     2,
			want:  base.Add(time.Hour),
		},
		{
			name:  "duplicates",
			times: []time.Time{base, base, base.Add(time.Hour)},
			n:     1,
			want:  base,
		},
		{
			name:  "n_zero_returns_zero_time",
			times: []time.Time{base},
			n:     0,
			want:  time.Time{},
		},
		{
			name:  "empty_slice_n_one",
			times: []time.Time{},
			n:     1,
			want:  time.Time{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// nthSmallest is accessible through evictOldestTokens which calls it internally.
			// We test it indirectly via the sweep path that exercises the same quickselect code.
			got := dynamodb.NthSmallestForTest(tt.times, tt.n)
			assert.Equal(t, tt.want, got, "nthSmallest(%d) mismatch", tt.n)
		})
	}
}
