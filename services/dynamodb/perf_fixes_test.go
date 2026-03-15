package dynamodb_test

// Tests for the performance fixes introduced in issue #621:
//   1. deepCopyItem uses recursive map copy instead of JSON round-trip
//   2. Scan no longer deep-copies items that are filtered out by Limit
//   3. TransactWriteItems tokens expire after TTL (sweepTxnTokens)
//   4. Query copies only the relevant PK's index entries
//   5. findTableByStreamARN uses O(1) reverse index (streamARNIndex)

import (
	"fmt"
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
// Fix 1: deepCopyItem — recursive copy correctness
// ---------------------------------------------------------------------------

func TestDeepCopyItem_NestedMutationIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		item   map[string]any
		mutate func(copied map[string]any)
		verify func(t *testing.T, original map[string]any)
		name   string
	}{
		{
			name: "string_attribute_top_level_replace",
			item: map[string]any{
				"pk": map[string]any{"S": "key1"},
			},
			mutate: func(copied map[string]any) {
				copied["pk"] = map[string]any{"S": "mutated"}
			},
			verify: func(t *testing.T, orig map[string]any) {
				t.Helper()
				assert.Equal(t, map[string]any{"S": "key1"}, orig["pk"])
			},
		},
		{
			name: "nested_map_inner_value",
			item: map[string]any{
				"pk": map[string]any{"S": "key1"},
				"m": map[string]any{
					"M": map[string]any{
						"inner": map[string]any{"S": "original"},
					},
				},
			},
			mutate: func(copied map[string]any) {
				// Mutate a value deep inside the nested map.
				copied["m"].(map[string]any)["M"].(map[string]any)["inner"] = map[string]any{"S": "mutated"}
			},
			verify: func(t *testing.T, orig map[string]any) {
				t.Helper()
				inner := orig["m"].(map[string]any)["M"].(map[string]any)["inner"]
				assert.Equal(t, map[string]any{"S": "original"}, inner, "nested map value should be unchanged")
			},
		},
		{
			name: "list_element_replace",
			item: map[string]any{
				"pk": map[string]any{"S": "key1"},
				"l": map[string]any{
					"L": []any{
						map[string]any{"S": "a"},
						map[string]any{"S": "b"},
					},
				},
			},
			mutate: func(copied map[string]any) {
				// Replace the first element inside the L list.
				copied["l"].(map[string]any)["L"].([]any)[0] = map[string]any{"S": "mutated"}
			},
			verify: func(t *testing.T, orig map[string]any) {
				t.Helper()
				list := orig["l"].(map[string]any)["L"].([]any)
				assert.Equal(t, map[string]any{"S": "a"}, list[0], "original list[0] should be unchanged")
			},
		},
		{
			name: "ss_set_element_mutation",
			item: map[string]any{
				"pk": map[string]any{"S": "key1"},
				"ss": map[string]any{"SS": []string{"apple", "banana"}},
			},
			mutate: func(copied map[string]any) {
				// Mutate the copied []string backing array in-place.
				s := copied["ss"].(map[string]any)["SS"].([]string)
				s[0] = "mutated"
			},
			verify: func(t *testing.T, orig map[string]any) {
				t.Helper()
				ss := orig["ss"].(map[string]any)["SS"].([]string)
				assert.Equal(t, "apple", ss[0], "original SS[0] should be unchanged after mutating the copy")
			},
		},
		{
			name: "ns_set_element_mutation",
			item: map[string]any{
				"pk": map[string]any{"S": "key1"},
				"ns": map[string]any{"NS": []string{"1", "2", "3"}},
			},
			mutate: func(copied map[string]any) {
				s := copied["ns"].(map[string]any)["NS"].([]string)
				s[0] = "999"
			},
			verify: func(t *testing.T, orig map[string]any) {
				t.Helper()
				ns := orig["ns"].(map[string]any)["NS"].([]string)
				assert.Equal(t, "1", ns[0], "original NS[0] should be unchanged after mutating the copy")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			copied := dynamodb.DeepCopyItem(tt.item)

			tt.mutate(copied)
			tt.verify(t, tt.item)
		})
	}
}

func TestDeepCopyItem_ListElementMutation(t *testing.T) {
	t.Parallel()

	original := map[string]any{
		"pk": map[string]any{"S": "k"},
		"l": map[string]any{
			"L": []any{
				map[string]any{"S": "a"},
			},
		},
	}

	copied := dynamodb.DeepCopyItem(original)

	// Mutate a nested list element in the copy.
	list := copied["l"].(map[string]any)["L"].([]any)
	list[0] = map[string]any{"S": "mutated"}

	origList := original["l"].(map[string]any)["L"].([]any)
	assert.Equal(t, map[string]any{"S": "a"}, origList[0], "original list element should be unchanged")
}

// ---------------------------------------------------------------------------
// Fix 3: TransactWriteItems token TTL
// ---------------------------------------------------------------------------

func TestSweepTxnTokens_RemovesExpiredTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		injectExpired  int
		injectFresh    int
		wantAfterSweep int
	}{
		{
			name:           "removes_only_expired",
			injectExpired:  3,
			injectFresh:    2,
			wantAfterSweep: 2,
		},
		{
			name:           "removes_all_expired",
			injectExpired:  5,
			injectFresh:    0,
			wantAfterSweep: 0,
		},
		{
			name:           "nothing_to_remove",
			injectExpired:  0,
			injectFresh:    3,
			wantAfterSweep: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()

			// Inject expired tokens via test helper.
			for i := range tt.injectExpired {
				db.InjectExpiredTxnTokenForTest(fmt.Sprintf("expired-token-%d", i))
			}

			// Inject fresh tokens by committing real transactions.
			if tt.injectFresh > 0 {
				createTableHelper(t, db, "TxnTTLTable", "pk")
			}

			for i := range tt.injectFresh {
				_, err := db.TransactWriteItems(t.Context(), &sdk.TransactWriteItemsInput{
					ClientRequestToken: aws.String(fmt.Sprintf("fresh-token-%d", i)),
					TransactItems: []types.TransactWriteItem{
						{
							Put: &types.Put{
								TableName: aws.String("TxnTTLTable"),
								Item: map[string]types.AttributeValue{
									"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("pk-%d", i)},
								},
							},
						},
					},
				})
				require.NoError(t, err)
			}

			assert.Equal(t, tt.injectExpired+tt.injectFresh, db.TxnTokenCount(), "total before sweep")

			janitor := dynamodb.NewJanitor(db, dynamodb.Settings{})
			janitor.SweepTxnTokens()

			assert.Equal(t, tt.wantAfterSweep, db.TxnTokenCount(), "total after sweep")
		})
	}
}

func TestTransactWriteItems_ExpiredTokenAllowsReuse(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTableHelper(t, db, "TxnReplayTable", "pk")

	token := "reusable-token"

	// First commit — should succeed.
	_, err := db.TransactWriteItems(t.Context(), &sdk.TransactWriteItemsInput{
		ClientRequestToken: aws.String(token),
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName: aws.String("TxnReplayTable"),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "item1"},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, db.TxnTokenCount())

	// Expire the token via test helper.
	db.InjectExpiredTxnTokenForTest(token)

	// Sweep — token should be removed.
	janitor := dynamodb.NewJanitor(db, dynamodb.Settings{})
	janitor.SweepTxnTokens()
	require.Equal(t, 0, db.TxnTokenCount())

	// Second commit with the same token — should succeed (not treated as duplicate).
	_, err = db.TransactWriteItems(t.Context(), &sdk.TransactWriteItemsInput{
		ClientRequestToken: aws.String(token),
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName: aws.String("TxnReplayTable"),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "item2"},
					},
				},
			},
		},
	})
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Fix 5: streamARNIndex — O(1) reverse lookup
// ---------------------------------------------------------------------------

func TestStreamARNIndex_CreateTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		streamEnabled bool
		wantIndexSize int
	}{
		{
			name:          "table_with_stream_adds_to_index",
			streamEnabled: true,
			wantIndexSize: 1,
		},
		{
			name:          "table_without_stream_not_in_index",
			streamEnabled: false,
			wantIndexSize: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()

			input := &sdk.CreateTableInput{
				TableName: aws.String("StreamTable"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				},
			}

			if tt.streamEnabled {
				input.StreamSpecification = &types.StreamSpecification{
					StreamEnabled:  aws.Bool(true),
					StreamViewType: types.StreamViewTypeNewImage,
				}
			}

			_, err := db.CreateTable(t.Context(), input)
			require.NoError(t, err)

			assert.Equal(t, tt.wantIndexSize, db.StreamARNIndexSize())

			if tt.streamEnabled {
				tbl, ok := db.GetTable("StreamTable")
				require.True(t, ok)
				_, found := db.LookupStreamARNIndex(tbl.StreamARN)
				assert.True(t, found, "table should be findable via streamARNIndex")
			}
		})
	}
}

func TestStreamARNIndex_EnableDisableStream(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTableHelper(t, db, "IndexedStreamTable", "pk")
	require.Equal(t, 0, db.StreamARNIndexSize())

	// Enable stream — index should be populated.
	require.NoError(t, db.EnableStream(t.Context(), "IndexedStreamTable", "NEW_IMAGE"))
	assert.Equal(t, 1, db.StreamARNIndexSize())

	tbl, ok := db.GetTable("IndexedStreamTable")
	require.True(t, ok)
	assert.Equal(t, "NEW_IMAGE", tbl.StreamViewType, "StreamViewType should be set after EnableStream")

	_, found := db.LookupStreamARNIndex(tbl.StreamARN)
	assert.True(t, found, "stream should be in index after EnableStream")

	// Disable stream — index entry should be removed and StreamViewType cleared.
	require.NoError(t, db.DisableStream(t.Context(), "IndexedStreamTable"))
	assert.Equal(t, 0, db.StreamARNIndexSize())

	tbl, ok = db.GetTable("IndexedStreamTable")
	require.True(t, ok)
	assert.Empty(t, tbl.StreamARN, "StreamARN should be empty after DisableStream")
	assert.Empty(t, tbl.StreamViewType, "StreamViewType should be cleared after DisableStream")
}

func TestStreamARNIndex_DeleteTable(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	_, err := db.CreateTable(t.Context(), &sdk.CreateTableInput{
		TableName: aws.String("DeletedStreamTable"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewImage,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, db.StreamARNIndexSize())

	_, err = db.DeleteTable(t.Context(), &sdk.DeleteTableInput{
		TableName: aws.String("DeletedStreamTable"),
	})
	require.NoError(t, err)
	assert.Equal(t, 0, db.StreamARNIndexSize(), "stream ARN should be removed from index on delete")
}

func TestStreamARNIndex_UpdateTable_EnableAndDisable(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTableHelper(t, db, "UpdateStreamTable", "pk")
	require.Equal(t, 0, db.StreamARNIndexSize())

	// Enable streams via UpdateTable.
	_, err := db.UpdateTable(t.Context(), &sdk.UpdateTableInput{
		TableName: aws.String("UpdateStreamTable"),
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewImage,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, db.StreamARNIndexSize())

	tbl, ok := db.GetTable("UpdateStreamTable")
	require.True(t, ok)
	streamARN := tbl.StreamARN
	_, found := db.LookupStreamARNIndex(streamARN)
	assert.True(t, found)

	// Disable streams via UpdateTable.
	_, err = db.UpdateTable(t.Context(), &sdk.UpdateTableInput{
		TableName: aws.String("UpdateStreamTable"),
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled: aws.Bool(false),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 0, db.StreamARNIndexSize())
}

// ---------------------------------------------------------------------------
// Fix 2 & 4: Scan/Query correctness after shallow-copy / targeted index copy
// ---------------------------------------------------------------------------

func TestScanWithLimit_CorrectResultsAfterShallowCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numItems  int
		limit     int
		wantCount int
	}{
		{
			name:      "limit_1_of_100",
			numItems:  100,
			limit:     1,
			wantCount: 1,
		},
		{
			name:      "limit_10_of_100",
			numItems:  100,
			limit:     10,
			wantCount: 10,
		},
		{
			name:      "no_limit",
			numItems:  50,
			limit:     0,
			wantCount: 50,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			createTableHelper(t, db, "ScanLimitTable", "pk")

			for i := range tt.numItems {
				_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
					TableName: aws.String("ScanLimitTable"),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%04d", i)},
					},
				})
				require.NoError(t, err)
			}

			inp := &sdk.ScanInput{TableName: aws.String("ScanLimitTable")}
			if tt.limit > 0 {
				inp.Limit = aws.Int32(int32(tt.limit))
			}

			out, err := db.Scan(t.Context(), inp)
			require.NoError(t, err)
			assert.Equal(t, int32(tt.wantCount), out.Count)
		})
	}
}

func TestQueryTargetedIndexCopy_CorrectResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queryPK   string
		numItems  int
		wantCount int
	}{
		{
			name:      "query_existing_pk",
			numItems:  1000,
			queryPK:   "item-0500",
			wantCount: 1,
		},
		{
			name:      "query_missing_pk",
			numItems:  10,
			queryPK:   "nonexistent",
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			createTableHelper(t, db, "QueryIndexTable", "pk")

			for i := range tt.numItems {
				_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
					TableName: aws.String("QueryIndexTable"),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%04d", i)},
					},
				})
				require.NoError(t, err)
			}

			out, err := db.Query(t.Context(), &sdk.QueryInput{
				TableName:              aws.String("QueryIndexTable"),
				KeyConditionExpression: aws.String("pk = :pk"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":pk": &types.AttributeValueMemberS{Value: tt.queryPK},
				},
			})
			require.NoError(t, err)
			assert.Equal(t, int32(tt.wantCount), out.Count)
		})
	}
}

// TestScanPerformance_LimitVsFullTable ensures that scanning with a small
// Limit on a large table completes well within the expected time budget,
// validating that we no longer deep-copy the entire table.
// Skipped under -short to avoid flakiness on slow/contended CI runners.
func TestScanPerformance_LimitVsFullTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping wall-clock performance test in short mode")
	}

	t.Parallel()

	const (
		numItems = 5000
		limit    = 5
		// Allow generous time for -race detector overhead.
		maxDuration = 10 * time.Second
	)

	db := dynamodb.NewInMemoryDB()
	createTableHelper(t, db, "PerfScanTable", "pk")

	for i := range numItems {
		_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
			TableName: aws.String("PerfScanTable"),
			Item: map[string]types.AttributeValue{
				"pk":   &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%05d", i)},
				"data": &types.AttributeValueMemberS{Value: "padding-to-simulate-real-item-size"},
			},
		})
		require.NoError(t, err)
	}

	start := time.Now()

	for range 100 {
		out, err := db.Scan(t.Context(), &sdk.ScanInput{
			TableName: aws.String("PerfScanTable"),
			Limit:     aws.Int32(limit),
		})
		require.NoError(t, err)
		require.Equal(t, int32(limit), out.Count)
	}

	elapsed := time.Since(start)
	assert.Less(t, elapsed, maxDuration, "100 limited scans on %d items should complete quickly", numItems)
}

// BenchmarkScanWithLimit measures the performance of Scan with a small Limit
// on a large table to track regressions in the shallow-copy optimization.
func BenchmarkScanWithLimit(b *testing.B) {
	const (
		numItems = 5000
		limit    = 5
	)

	db := dynamodb.NewInMemoryDB()

	_, err := db.CreateTable(b.Context(), &sdk.CreateTableInput{
		TableName: aws.String("BenchScanTable"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
	})
	if err != nil {
		b.Fatal(err)
	}

	for i := range numItems {
		_, err = db.PutItem(b.Context(), &sdk.PutItemInput{
			TableName: aws.String("BenchScanTable"),
			Item: map[string]types.AttributeValue{
				"pk":   &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%05d", i)},
				"data": &types.AttributeValueMemberS{Value: "padding-to-simulate-real-item-size"},
			},
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	for range b.N {
		var out *sdk.ScanOutput
		out, err = db.Scan(b.Context(), &sdk.ScanInput{
			TableName: aws.String("BenchScanTable"),
			Limit:     aws.Int32(limit),
		})
		if err != nil {
			b.Fatal(err)
		}

		if out.Count != limit {
			b.Fatalf("expected %d items, got %d", limit, out.Count)
		}
	}
}
