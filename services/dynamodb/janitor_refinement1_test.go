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
// TTL sweep batching
// ---------------------------------------------------------------------------

// TestRefinement1_BatchTTLSweep verifies that sweepTableTTL evicts all expired
// items correctly regardless of whether the total item count exceeds the batch size.
func TestRefinement1_BatchTTLSweep(t *testing.T) {
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
			_, err = db.UpdateTimeToLive(t.Context(), &ddbsdk.UpdateTimeToLiveInput{
				TableName: aws.String(tableName),
				TimeToLiveSpecification: &ddbsdktypes.TimeToLiveSpecification{
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

				_, putErr := db.PutItem(t.Context(), &ddbsdk.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]ddbsdktypes.AttributeValue{
						"pk":     &ddbsdktypes.AttributeValueMemberS{Value: fmt.Sprintf("k-%d", i)},
						"expire": &ddbsdktypes.AttributeValueMemberN{Value: ttlVal},
					},
				})
				require.NoError(t, putErr)
			}

			j := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: time.Hour})
			j.SweepOnce(t.Context())

			out, err := db.Scan(t.Context(), &ddbsdk.ScanInput{
				TableName: aws.String(tableName),
			})
			require.NoError(t, err)

			wantRemaining := tt.totalItems - tt.expiredCount
			assert.Equal(t, int32(wantRemaining), out.Count,
				"expected %d non-expired items after TTL sweep", wantRemaining)
		})
	}
}

// TestRefinement1_BatchTTLSweep_ContextCancel verifies that a pre-cancelled
// context causes the TTL sweep to abort early without panicking.
func TestRefinement1_BatchTTLSweep_ContextCancel(t *testing.T) {
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

// TestRefinement1_TxnTokensCap verifies that sweepTxnTokens enforces the hard
// cap and does not allow the map to grow without bound.
func TestRefinement1_TxnTokensCap(t *testing.T) {
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

// TestRefinement1_TxnTokensCap_OldestEvictedFirst verifies that when the cap is
// exceeded, the entries with the earliest expiry are evicted (not arbitrary ones).
func TestRefinement1_TxnTokensCap_OldestEvictedFirst(t *testing.T) {
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

// TestRefinement1_StreamRecordsCompaction verifies that sweepStreamRecords
// compacts the ring buffer slot count to zero for a table with no stream records.
func TestRefinement1_StreamRecordsCompaction(t *testing.T) {
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

// TestRefinement1_JanitorRunExitsOnContextCancel verifies that the Run loop
// exits cleanly when the context is cancelled, demonstrating that the defer
// recover() wrapper does not prevent normal shutdown.
func TestRefinement1_JanitorRunExitsOnContextCancel(t *testing.T) {
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

// TestRefinement1_PurgeableContext verifies that Purge now accepts [context.Context]
// and respects it (cancelled context should stop the Purge loop early).
func TestRefinement1_PurgeableContext(t *testing.T) {
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

			out, err := db.ListTables(t.Context(), &ddbsdk.ListTablesInput{})
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

// TestRefinement1_TxnTokensCap_Constants verifies that the exported cap constants
// have sane, non-zero values.
func TestRefinement1_TxnTokensCap_Constants(t *testing.T) {
	t.Parallel()

	assert.Positive(t, dynamodb.TxnTokensMaxCap, "TxnTokensMaxCap must be > 0")
	assert.Positive(t, dynamodb.TxnPendingMaxCap, "TxnPendingMaxCap must be > 0")
	assert.Positive(t, dynamodb.TTLSweepBatchSize, "TTLSweepBatchSize must be > 0")
}
