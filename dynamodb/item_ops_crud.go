package dynamodb

import (
	"encoding/json"
)

func (db *InMemoryDB) PutItem(body []byte) (any, error) {
	var input PutItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	if input.TableName == "" {
		return nil, NewValidationException("Table name is required")
	}

	table, err := db.getTable(input.TableName)
	if err != nil {
		return nil, err
	}

	table.mu.Lock()
	defer table.mu.Unlock()

	if err := db.validateItem(input.Item, table); err != nil {
		return nil, err
	}

	oldItem, matchIndex := db.findMatchForPut(table, input.Item)
	if err := db.checkPutCondition(&input, oldItem); err != nil {
		return nil, err
	}

	db.doPut(table, &input, matchIndex)

	return db.populatePutItemOutput(&input, table, oldItem), nil
}

func (db *InMemoryDB) findMatchForPut(table *Table, item map[string]any) (map[string]any, int) {
	pkDef, skDef := getPKAndSK(table.KeySchema)
	pkVal := BuildKeyString(item, pkDef.AttributeName)

	if skDef.AttributeName != "" {
		skVal := BuildKeyString(item, skDef.AttributeName)
		if skMap, ok := table.pkskIndex[pkVal]; ok {
			if idx, okIdx := skMap[skVal]; okIdx {
				return table.Items[idx], idx
			}
		}
	} else if idx, ok := table.pkIndex[pkVal]; ok {
		return table.Items[idx], idx
	}

	return nil, -1
}

func (db *InMemoryDB) checkPutCondition(input *PutItemInput, oldItem map[string]any) error {
	if input.ConditionExpression == "" {
		return nil
	}

	match, err := evaluateExpression(
		input.ConditionExpression,
		oldItem,
		input.ExpressionAttributeValues,
		input.ExpressionAttributeNames,
	)
	if err != nil {
		return err
	}
	if !match {
		return NewConditionalCheckFailedException("The conditional request failed")
	}

	return nil
}

func (db *InMemoryDB) doPut(table *Table, input *PutItemInput, matchIndex int) {
	if matchIndex != -1 {
		table.Items[matchIndex] = input.Item
		db.updateIndexes(table, input.Item, matchIndex)
	} else {
		idx := len(table.Items)
		table.Items = append(table.Items, input.Item)
		db.updateIndexes(table, input.Item, idx)
	}
}

func (db *InMemoryDB) validateItem(item map[string]any, table *Table) error {
	if err := ValidateDataTypes(item); err != nil {
		return err
	}
	if err := ValidateItemSize(item); err != nil {
		return err
	}

	return validateKeySchema(item, table.KeySchema)
}

func (db *InMemoryDB) populatePutItemOutput(input *PutItemInput, table *Table, oldItem map[string]any) PutItemOutput {
	out := PutItemOutput{}
	if input.ReturnValues == ReturnValuesAllOld && oldItem != nil {
		out.Attributes = oldItem
	}

	if input.ReturnConsumedCapacity != "" {
		out.ConsumedCapacity = &ConsumedCapacity{
			TableName:          table.Name,
			CapacityUnits:      WriteCapacityUnits(input.Item),
			WriteCapacityUnits: WriteCapacityUnits(input.Item),
		}
	}

	if input.ReturnItemCollectionMetrics == "SIZE" {
		out.ItemCollectionMetrics = &ItemCollectionMetrics{
			ItemCollectionKey: extractKey(input.Item, table.KeySchema),
			// For in-memory simplification, we use a single hardcoded value
			SizeEstimateRangeGB: []float64{0.0, 1.0},
		}
	}

	return out
}

func (db *InMemoryDB) GetItem(body []byte) (any, error) {
	var input GetItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	table, err := db.getTable(input.TableName)
	if err != nil {
		return nil, err
	}

	table.mu.RLock()
	defer table.mu.RUnlock()

	pkDef, skDef := getPKAndSK(table.KeySchema)
	item := db.lookupItem(table, input.Key, pkDef.AttributeName, skDef.AttributeName)

	if item == nil || isItemExpired(item, table.TTLAttribute) {
		return GetItemOutput{}, nil
	}

	result := item
	if input.ProjectionExpression != "" {
		result = projectItem(item, input.ProjectionExpression, input.ExpressionAttributeNames)
	}

	return GetItemOutput{Item: result}, nil
}

func (db *InMemoryDB) DeleteItem(body []byte) (any, error) {
	var input DeleteItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	table, err := db.getTable(input.TableName)
	if err != nil {
		return nil, err
	}

	table.mu.Lock()
	defer table.mu.Unlock()

	pkDef, skDef := getPKAndSK(table.KeySchema)
	oldItem := db.lookupItem(table, input.Key, pkDef.AttributeName, skDef.AttributeName)

	// Check condition
	if input.ConditionExpression != "" {
		match, err := evaluateExpression(
			input.ConditionExpression,
			oldItem,
			input.ExpressionAttributeValues,
			input.ExpressionAttributeNames,
		)
		if err != nil {
			return nil, err
		}
		if !match {
			return nil, NewConditionalCheckFailedException("The conditional request failed")
		}
	}

	if oldItem != nil {
		_, matchIndex := db.findMatchForPut(table, oldItem)
		if matchIndex != -1 {
			db.deleteItemAtIndex(table, matchIndex)
		}
	}

	return DeleteItemOutput{}, nil
}

func (db *InMemoryDB) UpdateItem(body []byte) (any, error) {
	var input UpdateItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	table, err := db.getTable(input.TableName)
	if err != nil {
		return nil, err
	}

	table.mu.Lock()
	defer table.mu.Unlock()

	existing, matchIndex := db.findMatchForPut(table, input.Key)

	if err := db.checkUpdateCondition(&input, existing); err != nil {
		return nil, err
	}

	updated, err := db.doUpdate(table, &input, existing, matchIndex)
	if err != nil {
		return nil, err
	}

	return db.populateUpdateOutput(&input, table, existing, updated), nil
}

func (db *InMemoryDB) checkUpdateCondition(input *UpdateItemInput, item map[string]any) error {
	if input.ConditionExpression == "" {
		return nil
	}

	match, err := evaluateExpression(
		input.ConditionExpression,
		item,
		input.ExpressionAttributeValues,
		input.ExpressionAttributeNames,
	)
	if err != nil {
		return err
	}
	if !match {
		return NewConditionalCheckFailedException("The conditional request failed")
	}

	return nil
}

func (db *InMemoryDB) doUpdate(
	table *Table,
	input *UpdateItemInput,
	existing map[string]any,
	matchIndex int,
) (map[string]any, error) {
	updated := make(map[string]any)
	if existing != nil {
		for k, v := range existing {
			updated[k] = v
		}
	} else {
		// Create new item from key
		for k, v := range input.Key {
			updated[k] = v
		}
	}

	if input.UpdateExpression != "" {
		if err := applyUpdate(
			updated,
			input.UpdateExpression,
			input.ExpressionAttributeNames,
			input.ExpressionAttributeValues,
		); err != nil {
			return nil, err
		}
	}

	if err := db.validateItem(updated, table); err != nil {
		return nil, err
	}

	if matchIndex != -1 {
		table.Items[matchIndex] = updated
		db.updateIndexes(table, updated, matchIndex)
	} else {
		newIdx := len(table.Items)
		table.Items = append(table.Items, updated)
		db.updateIndexes(table, updated, newIdx)
	}

	return updated, nil
}

func (db *InMemoryDB) updateIndexes(table *Table, item map[string]any, index int) {
	pkDef, skDef := getPKAndSK(table.KeySchema)
	pkVal := BuildKeyString(item, pkDef.AttributeName)

	if skDef.AttributeName != "" {
		if table.pkskIndex[pkVal] == nil {
			table.pkskIndex[pkVal] = make(map[string]int)
		}
		table.pkskIndex[pkVal][BuildKeyString(item, skDef.AttributeName)] = index
	} else {
		table.pkIndex[pkVal] = index
	}
}

func (db *InMemoryDB) populateUpdateOutput(
	input *UpdateItemInput,
	table *Table,
	oldItem, newItem map[string]any,
) UpdateItemOutput {
	out := UpdateItemOutput{}
	if input.ReturnValues == ReturnValuesAllOld && oldItem != nil {
		out.Attributes = oldItem
	} else if input.ReturnValues == ReturnValuesAllNew {
		out.Attributes = newItem
	}

	if input.ReturnConsumedCapacity != "" {
		out.ConsumedCapacity = &ConsumedCapacity{
			TableName:          table.Name,
			CapacityUnits:      WriteCapacityUnits(newItem),
			WriteCapacityUnits: WriteCapacityUnits(newItem),
		}
	}

	return out
}

func (db *InMemoryDB) deleteItemAtIndex(table *Table, matchIndex int) {
	item := table.Items[matchIndex]
	pkDef, skDef := getPKAndSK(table.KeySchema)
	pkVal := BuildKeyString(item, pkDef.AttributeName)

	if skDef.AttributeName != "" {
		deleteFromPKSKIndex(table, pkVal, BuildKeyString(item, skDef.AttributeName), matchIndex)
	} else {
		deleteFromPKIndex(table, pkVal, matchIndex)
	}

	table.Items = append(table.Items[:matchIndex], table.Items[matchIndex+1:]...)
}

func deleteFromPKSKIndex(table *Table, pkVal, skVal string, matchIndex int) {
	if skMap, ok := table.pkskIndex[pkVal]; ok {
		delete(skMap, skVal)
		if len(skMap) == 0 {
			delete(table.pkskIndex, pkVal)
		}
	}

	for _, skMap := range table.pkskIndex {
		for sk, idx := range skMap {
			if idx > matchIndex {
				skMap[sk] = idx - 1
			}
		}
	}
}

func deleteFromPKIndex(table *Table, pkVal string, matchIndex int) {
	delete(table.pkIndex, pkVal)

	for pk, idx := range table.pkIndex {
		if idx > matchIndex {
			table.pkIndex[pk] = idx - 1
		}
	}
}
