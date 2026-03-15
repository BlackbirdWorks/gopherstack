package dynamodb

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

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
	// Validate size limit (no lock needed — only inspects input).
	const batchSizeLimit = 100
	totalItems := 0
	for _, keysAndAttrs := range input.RequestItems {
		totalItems += len(keysAndAttrs.Keys)
	}
	if totalItems > batchSizeLimit {
		return nil, NewValidationException(
			fmt.Sprintf("Batch size limit exceeded: Max %d items per request", batchSizeLimit),
		)
	}

	// Collect table references under db.mu.RLock.
	tableRefs, tableErr := db.batchGetTableRefs(ctx, input.RequestItems)
	if tableErr != nil {
		return nil, tableErr
	}

	// Sort table names for deterministic processing (AWS also tends toward this).
	tableNames := make([]string, 0, len(input.RequestItems))
	for name := range input.RequestItems {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	return db.batchGetResponses(input, tableNames, tableRefs)
}

// batchGetResponses collects items across tables enforcing the 16MB response limit.
// Size is computed on the projected item so projection reduces the counted bytes.
func (db *InMemoryDB) batchGetResponses(
	input *dynamodb.BatchGetItemInput,
	tableNames []string,
	tableRefs map[string]*Table,
) (*dynamodb.BatchGetItemOutput, error) {
	const responseSizeLimit = 16 * 1024 * 1024
	currentSize := 0
	responses := make(map[string][]map[string]types.AttributeValue)
	unprocessedKeys := make(map[string]types.KeysAndAttributes)

	for _, tableName := range tableNames {
		keysAndAttrs := input.RequestItems[tableName]
		table := tableRefs[tableName]

		truncated, tableResults := db.batchGetTable(
			table,
			keysAndAttrs,
			tableName,
			&currentSize,
			responseSizeLimit,
			unprocessedKeys,
		)
		if len(tableResults) > 0 {
			responses[tableName] = tableResults
		}

		if truncated {
			for j := sort.SearchStrings(tableNames, tableName) + 1; j < len(tableNames); j++ {
				nextTable := tableNames[j]
				unprocessedKeys[nextTable] = input.RequestItems[nextTable]
			}

			break
		}
	}

	return &dynamodb.BatchGetItemOutput{
		Responses:        responses,
		UnprocessedKeys:  unprocessedKeys,
		ConsumedCapacity: batchGetConsumedCapacity(input.ReturnConsumedCapacity, input.RequestItems),
	}, nil
}

// batchGetTable reads items for a single table, enforcing the cumulative size limit.
// Per AWS semantics, the ProjectionExpression is applied before measuring item size —
// this means only the projected bytes count toward the 16MB response limit, not the
// full raw item. Returns (truncated, results) where truncated means the 16MB limit was reached.
func (db *InMemoryDB) batchGetTable(
	table *Table,
	keysAndAttrs types.KeysAndAttributes,
	tableName string,
	currentSize *int,
	responseSizeLimit int,
	unprocessedKeys map[string]types.KeysAndAttributes,
) (bool, []map[string]types.AttributeValue) {
	pkDef, skDef := getPKAndSK(table.KeySchema)
	proj := aws.ToString(keysAndAttrs.ProjectionExpression)
	projector, _ := ParseProjector(proj, keysAndAttrs.ExpressionAttributeNames)

	var tableResults []map[string]types.AttributeValue

	table.mu.RLock("BatchGetItem")
	defer table.mu.RUnlock()

	for i, sdkKey := range keysAndAttrs.Keys {
		wireKey := models.FromSDKItem(sdkKey)
		item := db.lookupItem(table, wireKey, pkDef.AttributeName, skDef.AttributeName)

		if item == nil {
			continue
		}

		// Project first, then measure size of the projected result (per AWS semantics).
		result := projector.Project(item)
		itemSize, _ := CalculateItemSize(result)

		if *currentSize+itemSize > responseSizeLimit && len(tableResults) > 0 {
			unprocessedKeys[tableName] = types.KeysAndAttributes{
				Keys:                     keysAndAttrs.Keys[i:],
				AttributesToGet:          keysAndAttrs.AttributesToGet,
				ConsistentRead:           keysAndAttrs.ConsistentRead,
				ExpressionAttributeNames: keysAndAttrs.ExpressionAttributeNames,
				ProjectionExpression:     keysAndAttrs.ProjectionExpression,
			}

			return true, tableResults
		}

		*currentSize += itemSize

		sdkResult, _ := models.ToSDKItem(result)
		tableResults = append(tableResults, sdkResult)
	}

	return false, tableResults
}

// batchGetTableRefs collects table references under db.mu.RLock.
func (db *InMemoryDB) batchGetTableRefs(
	ctx context.Context,
	requestItems map[string]types.KeysAndAttributes,
) (map[string]*Table, error) {
	db.mu.RLock("BatchGetItem")
	defer db.mu.RUnlock()

	region := getRegionFromContext(ctx, db)
	regionTables, ok := db.Tables[region]
	if !ok {
		// Region might not have tables yet
		return nil, NewResourceNotFoundException("No tables found in region")
	}
	tableRefs := make(map[string]*Table, len(requestItems))

	for tableName := range requestItems {
		t, exists := regionTables[tableName]
		if !exists {
			return nil, NewResourceNotFoundException(fmt.Sprintf("Table not found: %s", tableName))
		}

		tableRefs[tableName] = t
	}

	return tableRefs, nil
}

func batchGetConsumedCapacity(
	req types.ReturnConsumedCapacity,
	requestItems map[string]types.KeysAndAttributes,
) []types.ConsumedCapacity {
	if req == "" || req == types.ReturnConsumedCapacityNone {
		return nil
	}

	// Capacity is charged per requested key, not per returned item (missing items still consume RCU).
	caps := make([]types.ConsumedCapacity, 0, len(requestItems))
	for tableName, keysAndAttrs := range requestItems {
		cu := float64(len(keysAndAttrs.Keys)) * eventuallyConsistentRCU
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

	// Sequential processing for simplicity and deadlock prevention
	for _, tableName := range tableNames {
		if err = db.processTableWriteRequests(tables[tableName], toProcess[tableName]); err != nil {
			return nil, err
		}
	}

	return &dynamodb.BatchWriteItemOutput{
		UnprocessedItems: unprocessedItems,
		ConsumedCapacity: batchWriteConsumedCapacity(input.ReturnConsumedCapacity, toProcess),
	}, nil
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

// splitWriteRequestsBySize splits write requests into those whose cumulative estimated size
// fits within sizeLimit bytes and those that exceed it. Only PutRequests contribute to size.
func splitWriteRequestsBySize(
	requests []types.WriteRequest,
	sizeLimit int,
) ([]types.WriteRequest, []types.WriteRequest) {
	accumulated := 0
	var process, unprocessed []types.WriteRequest

	for _, req := range requests {
		if req.PutRequest != nil {
			wireItem := models.FromSDKItem(req.PutRequest.Item)
			itemSize, err := CalculateItemSize(wireItem)
			if err != nil {
				// Process conservatively if size calculation fails
				process = append(process, req)

				continue
			}

			if accumulated+itemSize > sizeLimit {
				unprocessed = append(unprocessed, req)

				continue
			}

			accumulated += itemSize
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
		indices := make([]int, 0, len(deletedIndices))
		for idx := range deletedIndices {
			indices = append(indices, idx)
		}
		db.applyBatchDeletes(table, indices)
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

func (db *InMemoryDB) applyBatchDeletes(table *Table, indices []int) {
	if len(indices) == 0 {
		return
	}

	// Sort indices in descending order to delete in-place without shifting issues
	sort.Ints(indices)
	for i := len(indices) - 1; i >= 0; i-- {
		idx := indices[i]
		if idx < 0 || idx >= len(table.Items) {
			continue
		}
		// Capture stream record (REMOVE)
		table.appendStreamRecord(streamEventRemove, deepCopyItem(table.Items[idx]), nil)

		// Delete by swapping with last and truncating
		table.Items[idx] = table.Items[len(table.Items)-1]
		table.Items = table.Items[:len(table.Items)-1]
	}

	table.rebuildIndexes()
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
