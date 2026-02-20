package dynamodb

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/blackbirdworks/gopherstack/dynamodb/models"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (db *InMemoryDB) BatchGetItem(
	ctx context.Context,
	input *dynamodb.BatchGetItemInput,
) (*dynamodb.BatchGetItemOutput, error) {
	db.mu.RLock("BatchGetItem")
	defer db.mu.RUnlock()

	if err := db.validateBatchGetInput(input); err != nil {
		return nil, err
	}

	responses := make(map[string][]map[string]types.AttributeValue)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for tableName, keysAndAttrs := range input.RequestItems {
		wg.Add(1)
		go func(tblName string, attrs types.KeysAndAttributes) {
			defer wg.Done()

			table, exists := db.getTableNoLock(tblName)
			if !exists {
				return
			}

			table.mu.RLock("BatchGetItem")
			results := db.processBatchGetTableNoLock(ctx, table, attrs)
			table.mu.RUnlock()

			if len(results) > 0 {
				mu.Lock()
				responses[tblName] = results
				mu.Unlock()
			}
		}(tableName, keysAndAttrs)
	}

	wg.Wait()

	return &dynamodb.BatchGetItemOutput{
		Responses:       responses,
		UnprocessedKeys: make(map[string]types.KeysAndAttributes),
	}, nil
}

func (db *InMemoryDB) validateBatchGetInput(input *dynamodb.BatchGetItemInput) error {
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

	for tableName := range input.RequestItems {
		if _, exists := db.Tables[tableName]; !exists {
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
	_ context.Context,
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

	// Get table references with read lock
	db.mu.RLock("BatchWriteItem")
	tables := make(map[string]*Table, len(input.RequestItems))
	for tableName := range input.RequestItems {
		if table, exists := db.Tables[tableName]; exists {
			tables[tableName] = table
		} else {
			db.mu.RUnlock()

			return nil, NewResourceNotFoundException(fmt.Sprintf("Table not found: %s", tableName))
		}
	}
	db.mu.RUnlock()

	// Process tables in sorted order (deadlock prevention)
	tableNames := make([]string, 0, len(tables))
	for name := range tables {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	// Parallelize table processing with error collection
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for _, tableName := range tableNames {
		wg.Add(1)
		go func(tblName string) {
			defer wg.Done()
			if err := db.processTableWriteRequests(tables[tblName], input.RequestItems[tblName]); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(tableName)
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return &dynamodb.BatchWriteItemOutput{
		UnprocessedItems: make(map[string][]types.WriteRequest),
	}, nil
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
