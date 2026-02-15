package dynamodb

import (
	"encoding/json"
	"fmt"
	"sort"
)

const txCancelPrefix = "Transaction cancelled, please refer cancellation reasons for specific reasons"

// TransactWriteItems executes up to 100 write actions atomically.
// All conditions are checked first; if any fail the whole transaction is cancelled.
func (db *InMemoryDB) TransactWriteItems(body []byte) (any, error) {
	var input TransactWriteItemsInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	if len(input.TransactItems) == 0 {
		return TransactWriteItemsOutput{}, nil
	}

	// Collect unique table names and lock in sorted order to prevent deadlocks.
	tableNames := db.transactTableNames(input.TransactItems)

	tables, lockErr := db.lockTablesWrite(tableNames)
	if lockErr != nil {
		return nil, lockErr
	}

	defer func() {
		for _, t := range tables {
			t.mu.Unlock()
		}
	}()

	// Phase 1: validate all conditions without mutating.
	for i, ti := range input.TransactItems {
		condErr := db.checkTransactWriteCondition(tables, ti, i)
		if condErr != nil {
			return nil, condErr
		}
	}

	// Phase 2: apply all writes.
	for _, ti := range input.TransactItems {
		applyErr := db.applyTransactWrite(tables, ti)
		if applyErr != nil {
			return nil, applyErr
		}
	}

	return TransactWriteItemsOutput{}, nil
}

// TransactGetItems reads up to 100 items atomically (snapshot consistency).
func (db *InMemoryDB) TransactGetItems(body []byte) (any, error) {
	var input TransactGetItemsInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	// Collect unique table names and lock in sorted order.
	tableNames := make([]string, 0)
	seen := make(map[string]bool)

	for _, ti := range input.TransactItems {
		if ti.Get != nil && !seen[ti.Get.TableName] {
			tableNames = append(tableNames, ti.Get.TableName)
			seen[ti.Get.TableName] = true
		}
	}

	sort.Strings(tableNames)

	tables, lockErr := db.lockTablesRead(tableNames)
	if lockErr != nil {
		return nil, lockErr
	}

	defer func() {
		for _, t := range tables {
			t.mu.RUnlock()
		}
	}()

	responses := make([]ItemResponse, 0, len(input.TransactItems))

	for _, ti := range input.TransactItems {
		if ti.Get == nil {
			responses = append(responses, ItemResponse{})

			continue
		}

		table, ok := tables[ti.Get.TableName]
		if !ok {
			return nil, NewResourceNotFoundException(
				fmt.Sprintf("Requested resource not found: Table: %s not found", ti.Get.TableName),
			)
		}

		pkDef, skDef := getPKAndSK(table.KeySchema)
		item := db.lookupItem(table, ti.Get.Key, pkDef.AttributeName, skDef.AttributeName)

		if item == nil || isItemExpired(item, table.TTLAttribute) {
			responses = append(responses, ItemResponse{})

			continue
		}

		result := item
		if ti.Get.ProjectionExpression != "" {
			result = projectItem(item, ti.Get.ProjectionExpression, ti.Get.ExpressionAttributeNames)
		}

		responses = append(responses, ItemResponse{Item: result})
	}

	return TransactGetItemsOutput{Responses: responses}, nil
}

func (db *InMemoryDB) transactTableNames(items []TransactWriteItem) []string {
	seen := make(map[string]bool)

	for _, ti := range items {
		switch {
		case ti.Put != nil:
			seen[ti.Put.TableName] = true
		case ti.Delete != nil:
			seen[ti.Delete.TableName] = true
		case ti.Update != nil:
			seen[ti.Update.TableName] = true
		case ti.ConditionCheck != nil:
			seen[ti.ConditionCheck.TableName] = true
		}
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

func (db *InMemoryDB) lockTablesWrite(tableNames []string) (map[string]*Table, error) {
	tables := make(map[string]*Table, len(tableNames))

	db.mu.RLock()
	for _, name := range tableNames {
		t, ok := db.Tables[name]
		if !ok {
			db.mu.RUnlock()

			return nil, NewResourceNotFoundException(
				fmt.Sprintf("Requested resource not found: Table: %s not found", name),
			)
		}

		tables[name] = t
	}
	db.mu.RUnlock()

	for _, name := range tableNames {
		tables[name].mu.Lock()
	}

	return tables, nil
}

func (db *InMemoryDB) lockTablesRead(tableNames []string) (map[string]*Table, error) {
	tables := make(map[string]*Table, len(tableNames))

	db.mu.RLock()
	for _, name := range tableNames {
		t, ok := db.Tables[name]
		if !ok {
			db.mu.RUnlock()

			return nil, NewResourceNotFoundException(
				fmt.Sprintf("Requested resource not found: Table: %s not found", name),
			)
		}

		tables[name] = t
	}
	db.mu.RUnlock()

	for _, name := range tableNames {
		tables[name].mu.RLock()
	}

	return tables, nil
}

func (db *InMemoryDB) checkTransactWriteCondition(
	tables map[string]*Table,
	ti TransactWriteItem,
	idx int,
) error {
	switch {
	case ti.Put != nil:
		return db.checkTransactPut(tables, ti.Put, idx)
	case ti.Delete != nil:
		return db.checkTransactCondExpr(
			tables[ti.Delete.TableName], ti.Delete.Key,
			ti.Delete.ConditionExpression,
			ti.Delete.ExpressionAttributeValues,
			ti.Delete.ExpressionAttributeNames,
			idx,
		)
	case ti.Update != nil:
		return db.checkTransactCondExpr(
			tables[ti.Update.TableName], ti.Update.Key,
			ti.Update.ConditionExpression,
			ti.Update.ExpressionAttributeValues,
			ti.Update.ExpressionAttributeNames,
			idx,
		)
	case ti.ConditionCheck != nil:
		return db.checkTransactCondExpr(
			tables[ti.ConditionCheck.TableName], ti.ConditionCheck.Key,
			ti.ConditionCheck.ConditionExpression,
			ti.ConditionCheck.ExpressionAttributeValues,
			ti.ConditionCheck.ExpressionAttributeNames,
			idx,
		)
	}

	return nil
}

func (db *InMemoryDB) checkTransactPut(tables map[string]*Table, input *PutItemInput, idx int) error {
	table := tables[input.TableName]
	oldItem, _ := db.findMatchForPut(table, input.Item)

	if err := db.checkPutCondition(input, oldItem); err != nil {
		return NewTransactionCanceledException(
			fmt.Sprintf("%s [%d]: %s", txCancelPrefix, idx, err),
		)
	}

	return nil
}

func (db *InMemoryDB) checkTransactCondExpr(
	table *Table,
	key map[string]any,
	condExpr string,
	eavs map[string]any,
	eans map[string]string,
	idx int,
) error {
	if condExpr == "" {
		return nil
	}

	oldItem, _ := db.findMatchForPut(table, key)

	match, err := evaluateExpression(condExpr, oldItem, eavs, eans)
	if err != nil {
		return err
	}

	if !match {
		return NewTransactionCanceledException(
			fmt.Sprintf("%s [%d]: ConditionalCheckFailed", txCancelPrefix, idx),
		)
	}

	return nil
}

func (db *InMemoryDB) applyTransactWrite(tables map[string]*Table, ti TransactWriteItem) error {
	switch {
	case ti.Put != nil:
		table := tables[ti.Put.TableName]
		if err := db.validateItem(ti.Put.Item, table); err != nil {
			return err
		}

		_, matchIndex := db.findMatchForPut(table, ti.Put.Item)
		db.doPut(table, ti.Put, matchIndex)

	case ti.Delete != nil:
		table := tables[ti.Delete.TableName]
		_, matchIndex := db.findMatchForPut(table, ti.Delete.Key)

		if matchIndex != -1 {
			db.deleteItemAtIndex(table, matchIndex)
		}

	case ti.Update != nil:
		table := tables[ti.Update.TableName]
		oldItem, matchIndex := db.findMatchForPut(table, ti.Update.Key)

		if _, err := db.doUpdate(table, ti.Update, oldItem, matchIndex); err != nil {
			return err
		}
	}

	return nil
}
