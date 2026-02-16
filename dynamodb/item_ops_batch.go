package dynamodb

import (
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (db *InMemoryDB) BatchGetItem(input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	responses := make(map[string][]map[string]types.AttributeValue)

	// Validate batch size (max 100 items for BatchGetItem, though 25 for BatchWriteItem.
	// SDK docs say BatchGetItem up to 100 items. Gopherstack had 25 limit constant?)
	// I'll stick to Gopherstack's existing limit logic if I can find it,
	// otherwise I'll just use a reasonable default. The old code used 'batchSizeLimit' constant.
	// I should probably export/import that constant or redefine it.
	// It was defined in `item_ops.go` which I saw earlier but it was just helper file.
	// I'll assume it's available or define locally.
	const batchSizeLimit = 100 // DynamoDB limit for BatchGetItem is 100.

	totalItems := 0
	for _, keysAndAttrs := range input.RequestItems {
		totalItems += len(keysAndAttrs.Keys)
	}
	if totalItems > batchSizeLimit {
		return nil, NewValidationException(
			fmt.Sprintf("Batch size limit exceeded: Max %d items per request", batchSizeLimit),
		)
	}

	for tableName, keysAndAttrs := range input.RequestItems {
		table, exists := db.Tables[tableName]
		if !exists {
			return nil, NewResourceNotFoundException(fmt.Sprintf("Table not found: %s", tableName))
		}

		pkDef, skDef := getPKAndSK(table.KeySchema)
		var tableResults []map[string]types.AttributeValue

		proj := aws.ToString(keysAndAttrs.ProjectionExpression)

		for _, sdkKey := range keysAndAttrs.Keys {
			wireKey := FromSDKItem(sdkKey)
			item := db.lookupItem(table, wireKey, pkDef.AttributeName, skDef.AttributeName)
			if item != nil {
				result := item
				if proj != "" {
					result = projectItem(item, proj, keysAndAttrs.ExpressionAttributeNames)
				}

				sdkResult, _ := ToSDKItem(result)
				tableResults = append(tableResults, sdkResult)
			}
		}

		if len(tableResults) > 0 {
			responses[tableName] = tableResults
		}
	}

	return &dynamodb.BatchGetItemOutput{
		Responses:       responses,
		UnprocessedKeys: make(map[string]types.KeysAndAttributes),
	}, nil
}

func (db *InMemoryDB) BatchWriteItem(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
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
	db.mu.RLock()
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

	for _, tableName := range tableNames {
		if err := db.processTableWriteRequests(tables[tableName], input.RequestItems[tableName]); err != nil {
			return nil, err
		}
	}

	return &dynamodb.BatchWriteItemOutput{
		UnprocessedItems: make(map[string][]types.WriteRequest),
	}, nil
}

func (db *InMemoryDB) processTableWriteRequests(table *Table, requests []types.WriteRequest) error {
	table.mu.Lock()
	defer table.mu.Unlock()

	for _, req := range requests {
		if req.PutRequest != nil {
			wireItem := FromSDKItem(req.PutRequest.Item)
			db.handleBatchPut(table, wireItem)
		} else if req.DeleteRequest != nil {
			wireKey := FromSDKItem(req.DeleteRequest.Key)
			db.handleBatchDelete(table, wireKey)
		}
	}

	// Rebuild indexes (or rely on handleBatch* to update them incrementally?
	// Original code rebuilt once efficiently?
	// Original code: `table.rebuildIndexes()`
	// I need to make sure `rebuildIndexes` is available or implement it.
	// It is likely in `store.go`.
	// But `handleBatchPut/Delete` in original were separate methods modifying Items slice.
	// I'll stick to original pattern.

	// Wait, original `handleBatchPut` appended to slice. `rebuildIndexes` corrected the map.
	// Efficient for bulk.
	table.rebuildIndexes()

	return nil
}

func (db *InMemoryDB) handleBatchPut(table *Table, item map[string]any) {
	_, matchIndex := db.findMatchForPut(table, item)
	if matchIndex != -1 {
		table.Items[matchIndex] = item
	} else {
		table.Items = append(table.Items, item)
	}
}

func (db *InMemoryDB) handleBatchDelete(table *Table, key map[string]any) {
	_, matchIndex := db.findMatchForPut(table, key)
	if matchIndex != -1 {
		// Just removing from slice, indexes rebuilt later
		table.Items = append(table.Items[:matchIndex], table.Items[matchIndex+1:]...)
	}
}
