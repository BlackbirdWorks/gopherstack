package dynamodb

import (
	"encoding/json"
	"fmt"
	"sort"
)

func (db *InMemoryDB) BatchGetItem(body []byte) (any, error) {
	var input BatchGetItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	responses := make(map[string][]map[string]any)
	// Validate batch size (max 25 items)
	totalItems := 0
	for _, requests := range input.RequestItems {
		totalItems += len(requests.Keys)
	}
	if totalItems > batchSizeLimit {
		return nil, NewValidationException("Batch size limit exceeded: Max 25 items per request")
	}

	for tableName, keysAndAttrs := range input.RequestItems {
		table, exists := db.Tables[tableName]
		if !exists {
			return nil, NewResourceNotFoundException(fmt.Sprintf("Table not found: %s", tableName))
		}

		pkDef, skDef := getPKAndSK(table.KeySchema)
		tableResults := []map[string]any{}
		for _, keyMap := range keysAndAttrs.Keys {
			item := db.lookupItem(table, keyMap, pkDef.AttributeName, skDef.AttributeName)
			if item != nil {
				result := item
				if keysAndAttrs.ProjectionExpression != "" {
					result = projectItem(item, keysAndAttrs.ProjectionExpression, keysAndAttrs.ExpressionAttributeNames)
				}

				tableResults = append(tableResults, result)
			}
		}

		responses[tableName] = tableResults
	}

	return BatchGetItemOutput{Responses: responses}, nil
}

func (db *InMemoryDB) BatchWriteItem(body []byte) (any, error) {
	var input BatchWriteItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	if len(input.RequestItems) == 0 {
		return nil, NewValidationException("The batch write request cannot be empty")
	}

	// Validate batch size (max 25 items)
	totalItems := 0
	for _, requests := range input.RequestItems {
		totalItems += len(requests)
	}
	if totalItems > batchSizeLimit {
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

	// Process tables in sorted order to prevent deadlock when concurrent
	// BatchWriteItem calls lock overlapping table sets in different orders.
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

	return BatchWriteItemOutput{UnprocessedItems: make(map[string][]WriteRequest)}, nil
}

func (db *InMemoryDB) processTableWriteRequests(table *Table, requests []WriteRequest) error {
	table.mu.Lock()
	defer table.mu.Unlock()

	for _, req := range requests {
		if req.PutRequest != nil {
			db.handleBatchPut(table, req.PutRequest)
		} else if req.DeleteRequest != nil {
			db.handleBatchDelete(table, req.DeleteRequest)
		}
	}

	// Rebuild indexes once after all operations
	table.rebuildIndexes()

	return nil
}

func (db *InMemoryDB) handleBatchPut(table *Table, req *PutRequest) {
	_, matchIndex := db.findMatchForPut(table, req.Item)
	if matchIndex != -1 {
		table.Items[matchIndex] = req.Item
	} else {
		table.Items = append(table.Items, req.Item)
	}
}

func (db *InMemoryDB) handleBatchDelete(table *Table, req *DeleteRequest) {
	_, matchIndex := db.findMatchForPut(table, req.Key)
	if matchIndex != -1 {
		db.deleteItemAtIndex(table, matchIndex)
	}
}
