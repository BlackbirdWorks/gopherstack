package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/dynamodb/models"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// batchWriteResponseLimit is the simulated 16 MB response size limit for BatchWriteItem.
const batchWriteResponseLimit = 16 * 1024 * 1024

// eventuallyConsistentRCU is the RCU cost per read for eventually-consistent reads (0.5 per 4KB).
const eventuallyConsistentRCU = 0.5

func (db *InMemoryDB) BatchGetItem(
	ctx context.Context,
	input *dynamodb.BatchGetItemInput,
) (*dynamodb.BatchGetItemOutput, error) {
	db.mu.RLock("BatchGetItem")
	defer db.mu.RUnlock()

	if err := db.validateBatchGetInput(ctx, input); err != nil {
		return nil, err
	}

	responses := make(map[string][]map[string]types.AttributeValue)
	mu := lockmetrics.New("dynamodb.batch.get")
	defer mu.Close()
	var wg sync.WaitGroup

	for tableName, keysAndAttrs := range input.RequestItems {
		wg.Add(1)
		go func(tblName string, attrs types.KeysAndAttributes) {
			defer wg.Done()

			table, exists := db.getTableRLock(ctx, tblName)
			if !exists {
				return
			}

			table.mu.RLock("BatchGetItem")
			results := db.processBatchGetTableNoLock(ctx, table, attrs)
			table.mu.RUnlock()

			if len(results) > 0 {
				mu.Lock("BatchGetItem")
				responses[tblName] = results
				mu.Unlock()
			}
		}(tableName, keysAndAttrs)
	}

	wg.Wait()

	out := &dynamodb.BatchGetItemOutput{
		Responses:        responses,
		UnprocessedKeys:  make(map[string]types.KeysAndAttributes),
		ConsumedCapacity: batchGetConsumedCapacity(input.ReturnConsumedCapacity, responses),
	}

	return out, nil
}

func batchGetConsumedCapacity(
	req types.ReturnConsumedCapacity,
	responses map[string][]map[string]types.AttributeValue,
) []types.ConsumedCapacity {
	if req == "" || req == types.ReturnConsumedCapacityNone {
		return nil
	}

	caps := make([]types.ConsumedCapacity, 0, len(responses))
	for tableName, items := range responses {
		cu := float64(len(items)) * eventuallyConsistentRCU
		if cu < eventuallyConsistentRCU {
			cu = eventuallyConsistentRCU
		}
		caps = append(caps, types.ConsumedCapacity{
			TableName:         aws.String(tableName),
			CapacityUnits:     aws.Float64(cu),
			ReadCapacityUnits: aws.Float64(cu),
		})
	}

	return caps
}

func (db *InMemoryDB) validateBatchGetInput(ctx context.Context, input *dynamodb.BatchGetItemInput) error {
	const batchSizeLimit = 100

	totalItems := 0
	for _, keysAndAttrs := range input.RequestItems {
		totalItems += len(keysAndAttrs.Keys)
	}
	if totalItems > batchSizeLimit {
		return NewValidationException(
			fmt.Sprintf("Batch size limit exceeded: Max %d items per request", batchSizeLimit),
		)
	}

	region := getRegionFromContext(ctx, db)
	regionTables, exists := db.Tables[region]
	if !exists {
		// No tables in this region
		if len(input.RequestItems) > 0 {
			// Check if any table is requested
			for tableName := range input.RequestItems {
				return NewResourceNotFoundException(fmt.Sprintf("Table not found: %s", tableName))
			}
		}

		return nil
	}

	for tableName := range input.RequestItems {
		if _, tableExists := regionTables[tableName]; !tableExists {
			return NewResourceNotFoundException(fmt.Sprintf("Table not found: %s", tableName))
		}
	}

	return nil
}

func (db *InMemoryDB) processBatchGetTableNoLock(
	ctx context.Context,
	table *Table,
	keysAndAttrs types.KeysAndAttributes,
) []map[string]types.AttributeValue {
	pkDef, skDef := getPKAndSK(table.KeySchema)
	var results []map[string]types.AttributeValue

	proj := aws.ToString(keysAndAttrs.ProjectionExpression)
	if proj != "" {
		log := logger.Load(ctx)
		log.DebugContext(ctx, "Evaluating BatchGetItem ProjectionExpression",
			"tableName", table.Name,
			"expression", proj,
			"attributeNames", keysAndAttrs.ExpressionAttributeNames)
	}

	for _, sdkKey := range keysAndAttrs.Keys {
		wireKey := models.FromSDKItem(sdkKey)
		item := db.lookupItem(table, wireKey, pkDef.AttributeName, skDef.AttributeName)
		if item == nil {
			continue
		}

		result := item
		if proj != "" {
			result = projectItem(item, proj, keysAndAttrs.ExpressionAttributeNames)
		}

		sdkResult, _ := models.ToSDKItem(result)
		results = append(results, sdkResult)
	}

	return results
}

func (db *InMemoryDB) BatchWriteItem(
	ctx context.Context,
	input *dynamodb.BatchWriteItemInput,
) (*dynamodb.BatchWriteItemOutput, error) {
	if len(input.RequestItems) == 0 {
		return nil, NewValidationException("The batch write request cannot be empty")
	}

	const batchWriteLimit = 25
	totalItems := 0
	for _, requests := range input.RequestItems {
		totalItems += len(requests)
	}
	if totalItems > batchWriteLimit {
		return nil, NewValidationException("Batch size limit exceeded: Max 25 items per request")
	}

	region := getRegionFromContext(ctx, db)

	// Get table references with read lock
	db.mu.RLock("BatchWriteItem")
	tables, err := db.getRequestTables(region, input.RequestItems)
	db.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	// Split requests per table by size limit before processing.
	toProcess := make(map[string][]types.WriteRequest, len(input.RequestItems))
	unprocessedItems := make(map[string][]types.WriteRequest)

	for tableName, requests := range input.RequestItems {
		process, unprocessed := splitWriteRequestsBySize(requests, batchWriteResponseLimit)
		toProcess[tableName] = process
		if len(unprocessed) > 0 {
			unprocessedItems[tableName] = unprocessed
		}
	}

	// Process tables in sorted order (deadlock prevention)
	tableNames := make([]string, 0, len(tables))
	for name := range tables {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	// Parallelize table processing with error collection
	var wg sync.WaitGroup
	mu := lockmetrics.New("dynamodb.batch.write")
	defer mu.Close()
	var firstErr error

	for _, tableName := range tableNames {
		wg.Add(1)
		go func(tblName string) {
			defer wg.Done()
			if e := db.processTableWriteRequests(tables[tblName], toProcess[tblName]); e != nil {
				mu.Lock("BatchWriteItem")
				if firstErr == nil {
					firstErr = e
				}
				mu.Unlock()
			}
		}(tableName)
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	out := &dynamodb.BatchWriteItemOutput{
		UnprocessedItems: unprocessedItems,
		ConsumedCapacity: batchWriteConsumedCapacity(input.ReturnConsumedCapacity, toProcess),
	}

	return out, nil
}

func batchWriteConsumedCapacity(
	req types.ReturnConsumedCapacity,
	processed map[string][]types.WriteRequest,
) []types.ConsumedCapacity {
	if req == "" || req == types.ReturnConsumedCapacityNone {
		return nil
	}

	caps := make([]types.ConsumedCapacity, 0, len(processed))
	for tableName, reqs := range processed {
		cu := float64(len(reqs))
		caps = append(caps, types.ConsumedCapacity{
			TableName:          aws.String(tableName),
			CapacityUnits:      aws.Float64(cu),
			WriteCapacityUnits: aws.Float64(cu),
		})
	}

	return caps
}

// splitWriteRequestsBySize splits write requests into those whose cumulative estimated JSON size
// fits within sizeLimit bytes and those that exceed it. Only PutRequests contribute to size.
func splitWriteRequestsBySize(
	requests []types.WriteRequest,
	sizeLimit int,
) ([]types.WriteRequest, []types.WriteRequest) {
	accumulated := 0
	var process, unprocessed []types.WriteRequest

	for _, req := range requests {
		if req.PutRequest != nil {
			data, err := json.Marshal(req.PutRequest.Item)
			if err != nil {
				// Cannot estimate size; process conservatively without counting toward limit.
				process = append(process, req)

				continue
			}

			if accumulated+len(data) > sizeLimit {
				unprocessed = append(unprocessed, req)

				continue
			}

			accumulated += len(data)
		}

		process = append(process, req)
	}

	return process, unprocessed
}

// getRequestTables retrieves all requested tables from the specified region.
// The caller must hold db.mu locked for reading.
func (db *InMemoryDB) getRequestTables(
	region string,
	requestItems map[string][]types.WriteRequest,
) (map[string]*Table, error) {
	tables := make(map[string]*Table, len(requestItems))
	regionTables, regionExists := db.Tables[region]

	for tableName := range requestItems {
		var table *Table
		if regionExists {
			if t, tableExists := regionTables[tableName]; tableExists {
				table = t
			}
		}

		if table == nil {
			return nil, NewResourceNotFoundException(fmt.Sprintf("Table not found: %s", tableName))
		}
		tables[tableName] = table
	}

	return tables, nil
}

func (db *InMemoryDB) processTableWriteRequests(table *Table, requests []types.WriteRequest) error {
	table.mu.Lock("BatchWriteItem")
	defer table.mu.Unlock()

	modifiedIndices := db.processBatchPutRequests(table, requests)
	deletedIndices := db.processBatchDeleteRequests(table, requests)

	if len(deletedIndices) > 0 {
		db.applyBatchDeletes(table, deletedIndices)
		table.rebuildIndexes()
	} else if len(modifiedIndices) > 0 {
		db.updateBatchIndexes(table, modifiedIndices)
	}

	return nil
}

func (db *InMemoryDB) processBatchPutRequests(
	table *Table,
	requests []types.WriteRequest,
) map[int]bool {
	modifiedIndices := make(map[int]bool)

	for _, req := range requests {
		if req.PutRequest != nil {
			wireItem := models.FromSDKItem(req.PutRequest.Item)
			idx := db.handleBatchPutWithIndex(table, wireItem)
			if idx >= 0 {
				modifiedIndices[idx] = true
			}
		}
	}

	return modifiedIndices
}

func (db *InMemoryDB) processBatchDeleteRequests(table *Table, requests []types.WriteRequest) map[int]bool {
	deletedIndices := make(map[int]bool)

	for _, req := range requests {
		if req.DeleteRequest != nil {
			wireKey := models.FromSDKItem(req.DeleteRequest.Key)
			_, matchIndex := db.findMatchForPut(table, wireKey)
			if matchIndex != -1 {
				deletedIndices[matchIndex] = true
			}
		}
	}

	return deletedIndices
}

func (db *InMemoryDB) applyBatchDeletes(table *Table, deletedIndices map[int]bool) {
	if len(deletedIndices) == 0 {
		return
	}

	// Optimize: single-pass compaction instead of O(M*N)
	newItems := make([]map[string]any, 0, len(table.Items)-len(deletedIndices))
	for i, item := range table.Items {
		if !deletedIndices[i] {
			newItems = append(newItems, item)
		}
	}
	table.Items = newItems
}

func (db *InMemoryDB) updateBatchIndexes(
	table *Table,
	modifiedIndices map[int]bool,
) {
	if len(modifiedIndices) == 0 {
		return
	}

	// Incremental update: only rebuild indices for modified items (O(K) instead of O(N))
	pkDef, skDef := getPKAndSK(table.KeySchema)
	for idx := range modifiedIndices {
		if idx >= 0 && idx < len(table.Items) {
			db.updateItemIndex(table, idx, pkDef, skDef)
		}
	}
}

func (db *InMemoryDB) updateItemIndex(
	table *Table,
	idx int,
	pkDef models.KeySchemaElement,
	skDef models.KeySchemaElement,
) {
	item := table.Items[idx]
	pkVal := BuildKeyString(item, pkDef.AttributeName)

	if skDef.AttributeName != "" {
		skVal := BuildKeyString(item, skDef.AttributeName)
		if table.pkskIndex[pkVal] == nil {
			table.pkskIndex[pkVal] = make(map[string]int)
		}
		table.pkskIndex[pkVal][skVal] = idx
	} else {
		table.pkIndex[pkVal] = idx
	}
}
func (db *InMemoryDB) handleBatchPutWithIndex(table *Table, item map[string]any) int {
	_, matchIndex := db.findMatchForPut(table, item)
	if matchIndex != -1 {
		table.Items[matchIndex] = item

		return matchIndex
	}
	idx := len(table.Items)
	table.Items = append(table.Items, item)

	return idx
}
