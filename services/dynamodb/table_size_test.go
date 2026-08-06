package dynamodb_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeTable_BatchWriteItem_TableSizeBytesMatchesItems guards
// table.itemSizes/table.totalItemSizeBytes staying in sync with table.Items
// after BatchWriteItem (handleBatchPutWithIndex/applyBatchDeletes in
// item_ops_batch.go), not just the single-item PutItem/UpdateItem/DeleteItem
// paths -- a length mismatch is a latent index-out-of-range panic risk for
// later writes and for global-table replica cloning, which assumes
// len(itemSizes) == len(Items).
func TestDescribeTable_BatchWriteItem_TableSizeBytesMatchesItems(t *testing.T) {
	t.Parallel()

	const tableName = "BatchSizeTable"
	const numItems = 40

	db := dynamodb.NewInMemoryDB()
	ctx := t.Context()

	createTableHelper(t, db, tableName, "pk")

	var expectedTotal int64

	items := make([]map[string]types.AttributeValue, numItems)
	for i := range numItems {
		// Vary payload size per item so a naive "N * flat size" bug would not
		// accidentally satisfy the assertions below.
		payload := strings.Repeat("x", 10*(i+1))
		item := map[string]types.AttributeValue{
			"pk":      &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%03d", i)},
			"payload": &types.AttributeValueMemberS{Value: payload},
		}
		items[i] = item

		size, err := dynamodb.CalculateItemSize(models.FromSDKItem(item))
		require.NoError(t, err)
		expectedTotal += int64(size)
	}

	// BatchWriteItem allows at most 25 WriteRequests per call, so split into two
	// batches of 20 to also exercise multiple batch-write calls accumulating size.
	const batchSize = 20
	for start := 0; start < numItems; start += batchSize {
		end := min(start+batchSize, numItems)

		reqs := make([]types.WriteRequest, 0, end-start)
		for _, item := range items[start:end] {
			reqs = append(reqs, types.WriteRequest{
				PutRequest: &types.PutRequest{Item: item},
			})
		}

		_, err := db.BatchWriteItem(ctx, &sdk.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tableName: reqs,
			},
		})
		require.NoError(t, err)
	}

	desc, err := db.DescribeTable(ctx, &sdk.DescribeTableInput{TableName: aws.String(tableName)})
	require.NoError(t, err)
	require.NotNil(t, desc.Table.ItemCount)
	require.NotNil(t, desc.Table.TableSizeBytes)

	assert.EqualValues(t, numItems, *desc.Table.ItemCount)
	assert.Positive(t, *desc.Table.TableSizeBytes)
	assert.Equal(t, expectedTotal, *desc.Table.TableSizeBytes,
		"TableSizeBytes should equal the sum of CalculateItemSize over all batch-written items")

	// Delete a subset of items via BatchWriteItem and confirm both ItemCount and
	// TableSizeBytes shrink correctly. This covers applyBatchDeletes, which must
	// keep table.itemSizes in lockstep with table.Items (swap-with-last +
	// truncate) on delete, not just on insert.
	const numDeleted = 5

	delReqs := make([]types.WriteRequest, 0, numDeleted)

	var deletedSize int64

	for i := range numDeleted {
		delReqs = append(delReqs, types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%03d", i)},
				},
			},
		})

		size, sizeErr := dynamodb.CalculateItemSize(models.FromSDKItem(items[i]))
		require.NoError(t, sizeErr)
		deletedSize += int64(size)
	}

	_, err = db.BatchWriteItem(ctx, &sdk.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tableName: delReqs,
		},
	})
	require.NoError(t, err)

	desc2, err := db.DescribeTable(ctx, &sdk.DescribeTableInput{TableName: aws.String(tableName)})
	require.NoError(t, err)
	require.NotNil(t, desc2.Table.ItemCount)
	require.NotNil(t, desc2.Table.TableSizeBytes)

	assert.EqualValues(t, numItems-numDeleted, *desc2.Table.ItemCount)
	assert.Equal(t, expectedTotal-deletedSize, *desc2.Table.TableSizeBytes)
}
