package dynamodb

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

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

func ddbIndexDim(index string) cwmetric.Dimension {
	return cwmetric.Dimension{Name: "GlobalSecondaryIndexName", Value: index}
}

// emitIndexRCU publishes read capacity under the TableName+GlobalSecondaryIndexName dimensions when index is a GSI.
func (db *InMemoryDB) emitIndexRCU(region, tableName string, table *Table, index string, units float64) {
	if index == "" || !db.metrics.Enabled() || !isIndexGSI(table, index) {
		return
	}

	db.metrics.Put(region, ddbMetricNamespace, "ConsumedReadCapacityUnits", ddbUnitCount, units,
		ddbTableDim(tableName), ddbIndexDim(index))
}

// emitIndexWCU publishes write capacity for every GSI the items populate. Caller holds table.mu.
func (db *InMemoryDB) emitIndexWCU(region string, table *Table, units float64, items ...map[string]any) {
	if !db.metrics.Enabled() {
		return
	}

	for index, wcu := range calculateGSIWriteBreakdowns(table, units, items...) {
		db.metrics.Put(region, ddbMetricNamespace, "ConsumedWriteCapacityUnits", ddbUnitCount, wcu,
			ddbTableDim(table.Name), ddbIndexDim(index))
	}
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
		db.metrics.Put(region, ddbMetricNamespace, "SystemErrors", ddbUnitCount, 1, dims...)

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

func transactWriteTables(items []types.TransactWriteItem) []string {
	names := make([]string, 0, len(items))

	for _, ti := range items {
		switch {
		case ti.Put != nil:
			names = append(names, aws.ToString(ti.Put.TableName))
		case ti.Delete != nil:
			names = append(names, aws.ToString(ti.Delete.TableName))
		case ti.Update != nil:
			names = append(names, aws.ToString(ti.Update.TableName))
		case ti.ConditionCheck != nil:
			names = append(names, aws.ToString(ti.ConditionCheck.TableName))
		}
	}

	return names
}

func transactGetTables(items []types.TransactGetItem) []string {
	names := make([]string, 0, len(items))

	for _, ti := range items {
		if ti.Get != nil {
			names = append(names, aws.ToString(ti.Get.TableName))
		}
	}

	return names
}

func transactStatementTables(statements []types.ParameterizedStatement) []string {
	names := make([]string, 0, len(statements))

	for _, s := range statements {
		if name := extractPartiQLTableName(aws.ToString(s.Statement)); name != "" {
			names = append(names, name)
		}
	}

	return names
}

func batchStatementTables(statements []types.BatchStatementRequest) []string {
	names := make([]string, 0, len(statements))

	for _, s := range statements {
		if name := extractPartiQLTableName(aws.ToString(s.Statement)); name != "" {
			names = append(names, name)
		}
	}

	return names
}

// observeTables publishes one operation's latency/error metrics once per distinct table.
func (db *InMemoryDB) observeTables(ctx context.Context, op string, tables []string, start time.Time, err error) {
	seen := make(map[string]struct{}, len(tables))

	for _, table := range tables {
		if _, dup := seen[table]; dup || table == "" {
			continue
		}

		seen[table] = struct{}{}
		db.observeOp(ctx, op, table, start, err, -1)
	}
}

// TransactWriteItems executes the write actions atomically and publishes per-table metrics.
func (db *InMemoryDB) TransactWriteItems(
	ctx context.Context, input *dynamodb.TransactWriteItemsInput,
) (*dynamodb.TransactWriteItemsOutput, error) {
	if !db.metrics.Enabled() {
		return db.transactWriteItemsOp(ctx, input)
	}

	start := time.Now()
	out, err := db.transactWriteItemsOp(ctx, input)
	db.observeTables(ctx, opTransactWriteItems, transactWriteTables(input.TransactItems), start, err)

	return out, err
}

// TransactGetItems reads the items atomically and publishes per-table metrics.
func (db *InMemoryDB) TransactGetItems(
	ctx context.Context, input *dynamodb.TransactGetItemsInput,
) (*dynamodb.TransactGetItemsOutput, error) {
	if !db.metrics.Enabled() {
		return db.transactGetItemsOp(ctx, input)
	}

	start := time.Now()
	out, err := db.transactGetItemsOp(ctx, input)
	db.observeTables(ctx, opTransactGetItems, transactGetTables(input.TransactItems), start, err)

	return out, err
}

// ExecuteTransaction runs the PartiQL statements atomically and publishes per-table metrics.
func (db *InMemoryDB) ExecuteTransaction(
	ctx context.Context, input *dynamodb.ExecuteTransactionInput,
) (*dynamodb.ExecuteTransactionOutput, error) {
	if !db.metrics.Enabled() {
		return db.executeTransactionOp(ctx, input)
	}

	start := time.Now()
	out, err := db.executeTransactionOp(ctx, input)
	db.observeTables(ctx, "ExecuteTransaction", transactStatementTables(input.TransactStatements), start, err)

	return out, err
}

// BatchExecuteStatement runs the PartiQL statements and publishes per-table metrics.
func (db *InMemoryDB) BatchExecuteStatement(
	ctx context.Context, input *dynamodb.BatchExecuteStatementInput,
) (*dynamodb.BatchExecuteStatementOutput, error) {
	if !db.metrics.Enabled() || input == nil {
		return db.batchExecuteStatementOp(ctx, input)
	}

	start := time.Now()
	out, err := db.batchExecuteStatementOp(ctx, input)
	db.observeTables(ctx, "BatchExecuteStatement", batchStatementTables(input.Statements), start, err)

	return out, err
}
