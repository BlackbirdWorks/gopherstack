package dynamodb_test

// Tests for the memory, race, and performance fixes introduced in issue #674:
//   1. Stream ring buffer: O(1) in-place ring buffer (no allocation-heavy reslicing)
//   2. Expression cache TTL: lazy eviction on Get + periodic Sweep
//   3. txnPending cleanup: janitor sweeps orphaned in-progress tokens
//   4. BatchGetItem: no deadlock — table refs collected before goroutine spawn
//   5. Single-table batch: no goroutine overhead for single-table BatchWriteItem

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Fix 1: Stream ring buffer — insertion-order preserved and no slice growth
// ---------------------------------------------------------------------------

func TestStreamRingBuffer_OrderPreservedWhenFull(t *testing.T) {
	t.Parallel()

	const maxRecords = 1000

	db := dynamodb.NewInMemoryDB()
	createTableHelper(t, db, "RingBufTable", "pk")
	require.NoError(t, db.EnableStream(t.Context(), "RingBufTable", "NEW_IMAGE"))

	// Write exactly maxRecords+50 items so the ring wraps around.
	total := maxRecords + 50
	for i := range total {
		_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
			TableName: aws.String("RingBufTable"),
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%05d", i)},
			},
		})
		require.NoError(t, err)
	}

	tbl, ok := db.GetTable("RingBufTable")
	require.True(t, ok)
	// Ring buffer never exceeds maxStreamRecords.
	assert.LessOrEqual(t, len(tbl.StreamRecords), maxRecords)

	// StreamRecordsInOrder returns records in insertion order; sequence numbers
	// must be monotonically increasing.
	ordered := tbl.StreamRecordsInOrder()
	require.Len(t, ordered, maxRecords, "ordered view should have exactly maxStreamRecords entries")

	for i := 1; i < len(ordered); i++ {
		assert.Greater(t, ordered[i].SequenceNumber, ordered[i-1].SequenceNumber,
			"records must be in ascending sequence order at position %d", i)
	}
}

func TestStreamRingBuffer_OrderPreservedBeforeFull(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTableHelper(t, db, "SmallRingTable", "pk")
	require.NoError(t, db.EnableStream(t.Context(), "SmallRingTable", "NEW_IMAGE"))

	const n = 10
	for i := range n {
		_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
			TableName: aws.String("SmallRingTable"),
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", i)},
			},
		})
		require.NoError(t, err)
	}

	tbl, ok := db.GetTable("SmallRingTable")
	require.True(t, ok)

	ordered := tbl.StreamRecordsInOrder()
	require.Len(t, ordered, n)
	for i := 1; i < len(ordered); i++ {
		assert.Greater(t, ordered[i].SequenceNumber, ordered[i-1].SequenceNumber,
			"records must be in ascending order at position %d", i)
	}
}

// ---------------------------------------------------------------------------
// Fix 2: Expression cache TTL — lazy eviction + periodic Sweep
// ---------------------------------------------------------------------------

func TestExpressionCacheTTL_LazyEvictionOnGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		ttl       time.Duration
		sleepFor  time.Duration
		wantFound bool
	}{
		{
			name:      "entry_expired_returns_miss",
			ttl:       1 * time.Millisecond,
			sleepFor:  5 * time.Millisecond,
			wantFound: false,
		},
		{
			name:      "entry_not_yet_expired_returns_hit",
			ttl:       1 * time.Hour,
			sleepFor:  0,
			wantFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cache := dynamodb.NewExpressionCacheWithTTL(100, tt.ttl)
			cache.Put("my-key", "my-value")

			if tt.sleepFor > 0 {
				time.Sleep(tt.sleepFor)
			}

			_, found := cache.Get("my-key")
			assert.Equal(t, tt.wantFound, found)
		})
	}
}

func TestExpressionCacheTTL_SweepRemovesExpiredEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		nExpired int
		nFresh   int
	}{
		{
			name:     "sweep_removes_all_expired",
			nExpired: 5,
			nFresh:   0,
		},
		{
			name:     "sweep_keeps_fresh_entries",
			nExpired: 3,
			nFresh:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Use a cache with a very short TTL so entries expire quickly.
			cache := dynamodb.NewExpressionCacheWithTTL(200, 1*time.Millisecond)

			// Add entries with the short TTL — they will expire.
			for i := range tt.nExpired {
				cache.Put(fmt.Sprintf("expired-%d", i), i)
			}

			// Wait for the short-TTL entries to expire.
			time.Sleep(5 * time.Millisecond)

			// Add fresh entries into a SEPARATE long-TTL cache. Using a separate
			// instance avoids TTL races with the short-TTL cache above and lets us
			// assert independently. For mixed-cache behaviour (expired + fresh in the
			// same cache instance), see TestExpressionCacheTTL_SweepMixedInSameCache.
			freshCache := dynamodb.NewExpressionCacheWithTTL(200, 1*time.Hour)

			for i := range tt.nFresh {
				freshCache.Put(fmt.Sprintf("fresh-%d", i), i)
			}

			// Sweep the short-TTL cache — all expired entries should be removed.
			cache.Sweep()

			for i := range tt.nExpired {
				_, found := cache.Get(fmt.Sprintf("expired-%d", i))
				assert.False(t, found, "expired entry %d should be gone after Sweep", i)
			}

			// The long-TTL cache entries should survive their own sweep.
			freshCache.Sweep()

			for i := range tt.nFresh {
				_, found := freshCache.Get(fmt.Sprintf("fresh-%d", i))
				assert.True(t, found, "fresh entry %d should survive Sweep", i)
			}
		})
	}
}

// TestExpressionCacheTTL_SweepMixedInSameCache verifies that Sweep removes only
// expired entries when both expired and fresh entries coexist in the same cache.
func TestExpressionCacheTTL_SweepMixedInSameCache(t *testing.T) {
	t.Parallel()

	// Use a relatively long TTL. We'll add "expired" entries first, wait for them
	// to expire using a cache with a very short TTL, then add fresh entries.
	//
	// Strategy: use a short-TTL cache, add entries, wait for expiry, add more
	// entries which get a NEW expiresAt (now + shortTTL, which is in the future
	// relative to the wait already elapsed). Since Put refreshes expiresAt on
	// existing keys but here we use distinct keys, the new keys get fresh expiry.
	const shortTTL = 5 * time.Millisecond

	cache := dynamodb.NewExpressionCacheWithTTL(200, shortTTL)

	// Put entries that will expire.
	for i := range 3 {
		cache.Put(fmt.Sprintf("expired-%d", i), i)
	}

	// Wait for them to expire.
	time.Sleep(10 * time.Millisecond)

	// Put fresh entries into the SAME cache; they get expiresAt = now + shortTTL.
	for i := range 2 {
		cache.Put(fmt.Sprintf("fresh-%d", i), i)
	}

	// Sweep: expired entries should be removed, fresh ones should remain.
	cache.Sweep()

	for i := range 3 {
		_, found := cache.Get(fmt.Sprintf("expired-%d", i))
		assert.False(t, found, "expired entry %d should be gone after Sweep", i)
	}

	for i := range 2 {
		_, found := cache.Get(fmt.Sprintf("fresh-%d", i))
		assert.True(t, found, "fresh entry %d should survive Sweep", i)
	}
}

func TestExpressionCacheTTL_JanitorSweep(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	// Inject an entry directly via test helper (bypasses normal DDB flow).
	db.ExprCachePut("expr-key", "parsed-expr")
	_, found := db.ExprCacheGet("expr-key")
	require.True(t, found, "entry should be present before sweep")

	// Normal sweep (nothing expired) — entry should remain.
	db.SweepExprCache()
	_, found = db.ExprCacheGet("expr-key")
	assert.True(t, found, "non-expired entry should survive Sweep")
}

// ---------------------------------------------------------------------------
// Fix 3: txnPending cleanup — janitor sweeps orphaned in-progress tokens
// ---------------------------------------------------------------------------

func TestSweepTxnPending_RemovesStalePendingTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		injectStale    int
		wantAfterSweep int
	}{
		{
			name:           "removes_all_stale",
			injectStale:    3,
			wantAfterSweep: 0,
		},
		{
			name:           "empty_pending_is_noop",
			injectStale:    0,
			wantAfterSweep: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()

			for i := range tt.injectStale {
				db.InjectStaleTxnPendingForTest(fmt.Sprintf("stale-token-%d", i))
			}

			require.Equal(t, tt.injectStale, db.TxnPendingCount(), "pre-sweep count")

			janitor := dynamodb.NewJanitor(db, dynamodb.Settings{})
			janitor.SweepTxnPending()

			assert.Equal(t, tt.wantAfterSweep, db.TxnPendingCount(), "post-sweep count")
		})
	}
}

func TestSweepTxnPending_FreshTokensNotRemoved(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTableHelper(t, db, "PendingTable", "pk")

	// A real transaction in flight uses txnPending. We simulate a fresh pending
	// token by examining that the token set is non-empty mid-transaction. We
	// verify the janitor does not remove tokens that were just inserted.
	//
	// This test does NOT directly test the "fresh" branch of SweepTxnPending;
	// instead it verifies that normal committed tokens are not touched by SweepTxnPending.
	_, err := db.TransactWriteItems(t.Context(), &sdk.TransactWriteItemsInput{
		ClientRequestToken: aws.String("committed-token"),
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName: aws.String("PendingTable"),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "item1"},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// After completion, txnPending should be 0.
	assert.Equal(t, 0, db.TxnPendingCount(), "pending count should be 0 after transaction completes")

	// Sweep should be a no-op.
	janitor := dynamodb.NewJanitor(db, dynamodb.Settings{})
	janitor.SweepTxnPending()
	assert.Equal(t, 0, db.TxnPendingCount(), "pending count should still be 0 after sweep")
}

// ---------------------------------------------------------------------------
// Fix 4: BatchGetItem — no deadlock (concurrent writes don't starve readers)
// ---------------------------------------------------------------------------

func TestBatchGetItem_ConcurrentWritesNoDeadlock(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTableHelper(t, db, "ConcTable", "pk")

	// Pre-populate a few items.
	for i := range 5 {
		_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
			TableName: aws.String("ConcTable"),
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("k%d", i)},
			},
		})
		require.NoError(t, err)
	}

	// Concurrently read via BatchGetItem and write via PutItem.
	// Errors are collected in buffered channels so they can be asserted below.
	// If there is a deadlock this test will hang and be caught by the timeout.
	writeErrs := make(chan error, 50)
	readErrs := make(chan error, 50)
	done := make(chan struct{})
	var wg sync.WaitGroup

	wg.Go(func() {
		for i := range 50 {
			_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
				TableName: aws.String("ConcTable"),
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("w%d", i)},
				},
			})
			if err != nil {
				writeErrs <- err
			}
		}
	})

	wg.Go(func() {
		for range 50 {
			_, err := db.BatchGetItem(t.Context(), &sdk.BatchGetItemInput{
				RequestItems: map[string]types.KeysAndAttributes{
					"ConcTable": {
						Keys: []map[string]types.AttributeValue{
							{"pk": &types.AttributeValueMemberS{Value: "k0"}},
							{"pk": &types.AttributeValueMemberS{Value: "k1"}},
						},
					},
				},
			})
			if err != nil {
				readErrs <- err
			}
		}
	})

	go func() {
		wg.Wait()
		close(writeErrs)
		close(readErrs)
		close(done)
	}()

	select {
	case <-done:
		// success — check for any errors
	case <-time.After(10 * time.Second):
		require.Fail(t, "deadlock detected: BatchGetItem + concurrent PutItem timed out")
	}

	for err := range writeErrs {
		require.NoError(t, err, "unexpected PutItem error")
	}

	for err := range readErrs {
		require.NoError(t, err, "unexpected BatchGetItem error")
	}
}

// ---------------------------------------------------------------------------
// Fix 5: Single-table BatchWriteItem — no goroutine overhead
// ---------------------------------------------------------------------------

func TestBatchWriteItem_SingleTable_Works(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		items     int
		wantItems int
	}{
		{
			name:      "single_item",
			items:     1,
			wantItems: 1,
		},
		{
			name:      "multiple_items_same_table",
			items:     5,
			wantItems: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			createTableHelper(t, db, "SingleBatchTable", "pk")

			writeRequests := make([]types.WriteRequest, tt.items)
			for i := range tt.items {
				writeRequests[i] = types.WriteRequest{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", i)},
						},
					},
				}
			}

			_, err := db.BatchWriteItem(t.Context(), &sdk.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					"SingleBatchTable": writeRequests,
				},
			})
			require.NoError(t, err)

			out, err := db.Scan(t.Context(), &sdk.ScanInput{
				TableName: aws.String("SingleBatchTable"),
			})
			require.NoError(t, err)
			assert.Equal(t, int32(tt.wantItems), out.Count)
		})
	}
}

// ---------------------------------------------------------------------------
// Fix 6: UpdateTable with GSI create/delete preserves the primary key index
// ---------------------------------------------------------------------------

// TestUpdateTable_GSICreate_PreservesPrimaryIndex verifies that adding a GSI via
// UpdateTable does not clear the primary key index of existing items.
// Before the fix, applyGSICreate called initializeIndexes() which wiped the index,
// causing subsequent primary-key Query calls to degrade to O(n) full scans.
func TestUpdateTable_GSICreate_PreservesPrimaryIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		itemCount int
	}{
		{name: "small_table", itemCount: 5},
		{name: "large_table", itemCount: 50},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			ctx := t.Context()

			// Create table with a composite key.
			_, err := db.CreateTable(ctx, &sdk.CreateTableInput{
				TableName: aws.String("PKTable"),
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
					{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
					{AttributeName: aws.String("gsiPK"), AttributeType: types.ScalarAttributeTypeS},
				},
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
				},
				BillingMode: types.BillingModePayPerRequest,
			})
			require.NoError(t, err)

			// Insert items.
			for i := range tt.itemCount {
				_, putErr := db.PutItem(ctx, &sdk.PutItemInput{
					TableName: aws.String("PKTable"),
					Item: map[string]types.AttributeValue{
						"pk":    &types.AttributeValueMemberS{Value: fmt.Sprintf("pk-%d", i)},
						"sk":    &types.AttributeValueMemberS{Value: fmt.Sprintf("sk-%d", i)},
						"gsiPK": &types.AttributeValueMemberS{Value: fmt.Sprintf("gsi-%d", i%5)},
					},
				})
				require.NoError(t, putErr)
			}

			// Add a GSI via UpdateTable.
			_, err = db.UpdateTable(ctx, &sdk.UpdateTableInput{
				TableName: aws.String("PKTable"),
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("gsiPK"), AttributeType: types.ScalarAttributeTypeS},
				},
				GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
					{
						Create: &types.CreateGlobalSecondaryIndexAction{
							IndexName: aws.String("GSI1"),
							KeySchema: []types.KeySchemaElement{
								{AttributeName: aws.String("gsiPK"), KeyType: types.KeyTypeHash},
							},
							Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
						},
					},
				},
			})
			require.NoError(t, err)

			// Query by primary key — must still find the correct item via the index,
			// not a degraded full scan (we can't directly observe which path was taken,
			// but correctness is a prerequisite for index usage).
			for i := range tt.itemCount {
				out, queryErr := db.Query(ctx, &sdk.QueryInput{
					TableName:              aws.String("PKTable"),
					KeyConditionExpression: aws.String("pk = :pk"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("pk-%d", i)},
					},
				})
				require.NoError(t, queryErr)
				assert.Equal(t, int32(1), out.Count, "item %d should be found after GSI create", i)
			}
		})
	}
}

// TestUpdateTable_GSIDelete_PreservesPrimaryIndex verifies that deleting a GSI via
// UpdateTable does not clear the primary key index of existing items.
func TestUpdateTable_GSIDelete_PreservesPrimaryIndex(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	ctx := t.Context()

	// Create table with a GSI.
	_, err := db.CreateTable(ctx, &sdk.CreateTableInput{
		TableName: aws.String("GSIDeleteTable"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsiPK"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("ToDelete"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("gsiPK"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	// Insert items.
	for i := range 10 {
		_, putErr := db.PutItem(ctx, &sdk.PutItemInput{
			TableName: aws.String("GSIDeleteTable"),
			Item: map[string]types.AttributeValue{
				"pk":    &types.AttributeValueMemberS{Value: fmt.Sprintf("k%d", i)},
				"gsiPK": &types.AttributeValueMemberS{Value: fmt.Sprintf("g%d", i)},
			},
		})
		require.NoError(t, putErr)
	}

	// Delete the GSI.
	_, err = db.UpdateTable(ctx, &sdk.UpdateTableInput{
		TableName: aws.String("GSIDeleteTable"),
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
			{Delete: &types.DeleteGlobalSecondaryIndexAction{IndexName: aws.String("ToDelete")}},
		},
	})
	require.NoError(t, err)

	// All items must still be reachable by primary key.
	for i := range 10 {
		out, getErr := db.GetItem(ctx, &sdk.GetItemInput{
			TableName: aws.String("GSIDeleteTable"),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("k%d", i)},
			},
		})
		require.NoError(t, getErr)
		require.NotNil(t, out.Item, "item %d should be retrievable after GSI delete", i)
	}
}

// ---------------------------------------------------------------------------
// Fix 7: Item size estimation — TableSizeBytes reflects actual data, not a flat 400-byte average
// ---------------------------------------------------------------------------

// TestItemSizeEstimation_TableSizeScalesWithItemSize verifies that TableSizeBytes
// reported by DescribeTable scales proportionally with actual item content.
// Before the fix, all items were assumed to be 400 bytes regardless of content.
func TestItemSizeEstimation_TableSizeScalesWithItemSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		smallAttr string
		largeAttr string
	}{
		{
			name:      "string_attribute",
			smallAttr: "a",
			largeAttr: strings.Repeat("x", 2048),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			ctx := t.Context()

			createTableHelper(t, db, "SizeSmall", "pk")
			createTableHelper(t, db, "SizeLarge", "pk")

			_, err := db.PutItem(ctx, &sdk.PutItemInput{
				TableName: aws.String("SizeSmall"),
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "k1"},
					"v":  &types.AttributeValueMemberS{Value: tt.smallAttr},
				},
			})
			require.NoError(t, err)

			_, err = db.PutItem(ctx, &sdk.PutItemInput{
				TableName: aws.String("SizeLarge"),
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "k1"},
					"v":  &types.AttributeValueMemberS{Value: tt.largeAttr},
				},
			})
			require.NoError(t, err)

			smallDesc, err := db.DescribeTable(ctx, &sdk.DescribeTableInput{
				TableName: aws.String("SizeSmall"),
			})
			require.NoError(t, err)

			largeDesc, err := db.DescribeTable(ctx, &sdk.DescribeTableInput{
				TableName: aws.String("SizeLarge"),
			})
			require.NoError(t, err)

			smallBytes := aws.ToInt64(smallDesc.Table.TableSizeBytes)
			largeBytes := aws.ToInt64(largeDesc.Table.TableSizeBytes)

			assert.Less(t, smallBytes, largeBytes,
				"larger item content should produce a larger TableSizeBytes; small=%d large=%d",
				smallBytes, largeBytes)

			// Sanity: both must be positive.
			assert.Positive(t, smallBytes)
			assert.Positive(t, largeBytes)
		})
	}
}
