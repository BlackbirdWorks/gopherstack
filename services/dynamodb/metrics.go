package dynamodb

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

// AWS/DynamoDB metrics per docs.aws.amazon.com/amazondynamodb/latest/developerguide/metrics-dimensions.html.
const (
	ddbMetricNamespace = "AWS/DynamoDB"
	ddbUnitCount       = "Count"
	ddbUnitMillis      = "Milliseconds"
)

// SetMetricEmitter sets the emitter that publishes AWS/DynamoDB metrics to CloudWatch.
func (db *InMemoryDB) SetMetricEmitter(e cwmetric.Emitter) { db.metrics.Set(e) }

func ddbTableDim(table string) cwmetric.Dimension {
	return cwmetric.Dimension{Name: wireTableName, Value: table}
}

func (db *InMemoryDB) emitRCU(region, table string, units float64) {
	db.metrics.Put(region, ddbMetricNamespace, "ConsumedReadCapacityUnits", ddbUnitCount, units, ddbTableDim(table))
}

func (db *InMemoryDB) emitWCU(region, table string, units float64) {
	db.metrics.Put(region, ddbMetricNamespace, "ConsumedWriteCapacityUnits", ddbUnitCount, units, ddbTableDim(table))
}

// observeOp publishes latency and error metrics for one finished table operation; items < 0 skips ReturnedItemCount.
func (db *InMemoryDB) observeOp(ctx context.Context, op, table string, start time.Time, err error, items int) {
	region := getRegionFromContext(ctx, db)
	dims := []cwmetric.Dimension{ddbTableDim(table), {Name: "Operation", Value: op}}

	if err == nil {
		db.metrics.Put(region, ddbMetricNamespace, "SuccessfulRequestLatency", ddbUnitMillis,
			float64(time.Since(start))/float64(time.Millisecond), dims...)

		if items >= 0 {
			db.metrics.Put(region, ddbMetricNamespace, "ReturnedItemCount", ddbUnitCount, float64(items), dims...)
		}

		return
	}

	wireErr, ok := errors.AsType[*Error](err)
	if !ok || wireErr.Type == errInternalServerErrorType {
		return
	}

	switch {
	case strings.HasSuffix(wireErr.Type, "#ProvisionedThroughputExceededException"):
		db.metrics.Put(region, ddbMetricNamespace, "ThrottledRequests", ddbUnitCount, 1, dims...)

		return
	case strings.HasSuffix(wireErr.Type, "#ConditionalCheckFailedException"):
		db.metrics.Put(region, ddbMetricNamespace, "ConditionalCheckFailedRequests", ddbUnitCount, 1, dims[0])
	}

	db.metrics.Put(region, ddbMetricNamespace, "UserErrors", ddbUnitCount, 1)
}

// PutItem writes an item and publishes its metrics.
func (db *InMemoryDB) PutItem(ctx context.Context, input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	if !db.metrics.Enabled() {
		return db.putItemOp(ctx, input)
	}

	start := time.Now()
	out, err := db.putItemOp(ctx, input)
	db.observeOp(ctx, opPutItem, aws.ToString(input.TableName), start, err, -1)

	return out, err
}

// GetItem reads an item and publishes its metrics.
func (db *InMemoryDB) GetItem(ctx context.Context, input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	if !db.metrics.Enabled() {
		return db.getItemOp(ctx, input)
	}

	start := time.Now()
	out, err := db.getItemOp(ctx, input)
	db.observeOp(ctx, opGetItem, aws.ToString(input.TableName), start, err, -1)

	return out, err
}

// DeleteItem deletes an item and publishes its metrics.
func (db *InMemoryDB) DeleteItem(
	ctx context.Context, input *dynamodb.DeleteItemInput,
) (*dynamodb.DeleteItemOutput, error) {
	if !db.metrics.Enabled() {
		return db.deleteItemOp(ctx, input)
	}

	start := time.Now()
	out, err := db.deleteItemOp(ctx, input)
	db.observeOp(ctx, opDeleteItem, aws.ToString(input.TableName), start, err, -1)

	return out, err
}

// UpdateItem updates an item and publishes its metrics.
func (db *InMemoryDB) UpdateItem(
	ctx context.Context, input *dynamodb.UpdateItemInput,
) (*dynamodb.UpdateItemOutput, error) {
	if !db.metrics.Enabled() {
		return db.updateItemOp(ctx, input)
	}

	start := time.Now()
	out, err := db.updateItemOp(ctx, input)
	db.observeOp(ctx, opUpdateItem, aws.ToString(input.TableName), start, err, -1)

	return out, err
}

func (db *InMemoryDB) observedQuery(ctx context.Context, input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	if !db.metrics.Enabled() {
		return db.QueryWithContext(ctx, input)
	}

	start := time.Now()
	out, err := db.QueryWithContext(ctx, input)

	items := -1
	if out != nil {
		items = int(out.Count)
	}

	db.observeOp(ctx, opQuery, aws.ToString(input.TableName), start, err, items)

	return out, err
}

func (db *InMemoryDB) observedScan(ctx context.Context, input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	if !db.metrics.Enabled() {
		return db.ScanWithContext(ctx, input)
	}

	start := time.Now()
	out, err := db.ScanWithContext(ctx, input)

	items := -1
	if out != nil {
		items = int(out.Count)
	}

	db.observeOp(ctx, opScan, aws.ToString(input.TableName), start, err, items)

	return out, err
}

// BatchGetItem reads items from one or more tables and publishes per-table metrics.
func (db *InMemoryDB) BatchGetItem(
	ctx context.Context, input *dynamodb.BatchGetItemInput,
) (*dynamodb.BatchGetItemOutput, error) {
	if !db.metrics.Enabled() {
		return db.batchGetItemOp(ctx, input)
	}

	start := time.Now()
	out, err := db.batchGetItemOp(ctx, input)

	for table := range input.RequestItems {
		db.observeOp(ctx, opBatchGetItem, table, start, err, -1)
	}

	return out, err
}

// BatchWriteItem writes items to one or more tables and publishes per-table metrics.
func (db *InMemoryDB) BatchWriteItem(
	ctx context.Context, input *dynamodb.BatchWriteItemInput,
) (*dynamodb.BatchWriteItemOutput, error) {
	if !db.metrics.Enabled() {
		return db.batchWriteItemOp(ctx, input)
	}

	start := time.Now()
	out, err := db.batchWriteItemOp(ctx, input)

	for table := range input.RequestItems {
		db.observeOp(ctx, opBatchWriteItem, table, start, err, -1)
	}

	return out, err
}
