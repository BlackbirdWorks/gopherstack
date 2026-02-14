package dynamodb

import (
	"encoding/json"
	"fmt"
	"strings"
)

func (db *InMemoryDB) PutItem(body []byte) (interface{}, error) {
	var input PutItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	table, exists := db.Tables[input.TableName]
	if !exists {
		return nil, NewResourceNotFoundException(fmt.Sprintf("Requested resource not found: Table: %s not found", input.TableName))
	}

	// Validation
	if err := validateKeySchema(input.Item, table.KeySchema); err != nil {
		return nil, err
	}
	if err := validateItemSize(input.Item); err != nil {
		return nil, err
	}
	if err := validateDataTypes(input.Item); err != nil {
		return nil, err
	}

	// Condition Expression Stub
	// Check ConditionExpression
	if input.ConditionExpression != "" {
		// Verify if item exists and matches condition
		// Find existing item by Key (from input.Item)
		// Naive scan for now as generic helper not yet extracted
		var existingItem map[string]interface{}

		pkName := ""
		for _, k := range table.KeySchema {
			if k.KeyType == "HASH" {
				pkName = k.AttributeName
				break
			}
		}

		if pkVal, ok := input.Item[pkName]; ok {
			for _, it := range table.Items {
				if p, ok := it[pkName]; ok {
					b1, _ := json.Marshal(p)
					b2, _ := json.Marshal(pkVal)
					if string(b1) == string(b2) {
						existingItem = it
						break
					}
				}
			}
		}

		match, err := evaluateExpression(input.ConditionExpression, existingItem, input.ExpressionAttributeValues, input.ExpressionAttributeNames)
		if err != nil {
			return nil, fmt.Errorf("Invalid ConditionExpression: %v", err)
		}
		if !match {
			return nil, NewConditionalCheckFailedException("The conditional request failed")
		}
	}

	// Check if item exists with same key
	idx := -1
	for i, item := range table.Items {
		if itemsMatchKey(item, input.Item, table.KeySchema) {
			idx = i
			break
		}
	}

	var oldItem map[string]interface{}
	if idx != -1 {
		oldItem = table.Items[idx]
		// Overwrite
		table.Items[idx] = input.Item
	} else {
		// Append
		table.Items = append(table.Items, input.Item)
	}

	output := PutItemOutput{}
	if input.ReturnValues == "ALL_OLD" && oldItem != nil {
		output.Attributes = oldItem
	}

	if input.ReturnConsumedCapacity == "TOTAL" || input.ReturnConsumedCapacity == "INDEXES" {
		output.ConsumedCapacity = &ConsumedCapacity{
			TableName:          input.TableName,
			CapacityUnits:      1.0, // Mocked value
			ReadCapacityUnits:  0.5,
			WriteCapacityUnits: 0.5,
		}
	}

	if input.ReturnItemCollectionMetrics == "SIZE" {
		output.ItemCollectionMetrics = &ItemCollectionMetrics{
			ItemCollectionKey:   map[string]interface{}{"pk": input.Item["pk"]},
			SizeEstimateRangeGB: []float64{0.0, 1.0},
		}
	}

	return output, nil
}

func (db *InMemoryDB) GetItem(body []byte) (interface{}, error) {
	var input GetItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	table, exists := db.Tables[input.TableName]
	if !exists {
		return nil, NewResourceNotFoundException(fmt.Sprintf("Requested resource not found: Table: %s not found", input.TableName))
	}

	var foundItem map[string]interface{}

	for _, item := range table.Items {
		if itemsMatchKey(item, input.Key, table.KeySchema) {
			foundItem = item
			break
		}
	}

	if foundItem != nil && input.ProjectionExpression != "" {
		foundItem = projectItem(foundItem, input.ProjectionExpression, input.ExpressionAttributeNames)
	}

	return GetItemOutput{
		Item: foundItem,
	}, nil
}

func (db *InMemoryDB) DeleteItem(body []byte) (interface{}, error) {
	var input DeleteItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	table, exists := db.Tables[input.TableName]
	if !exists {
		return nil, NewResourceNotFoundException(fmt.Sprintf("Requested resource not found: Table: %s not found", input.TableName))
	}

	idx := -1
	for i, item := range table.Items {
		if itemsMatchKey(item, input.Key, table.KeySchema) {
			idx = i
			break
		}
	}

	if input.ConditionExpression != "" {
		var existingItem map[string]interface{}
		if idx != -1 {
			existingItem = table.Items[idx]
		}

		match, err := evaluateExpression(input.ConditionExpression, existingItem, input.ExpressionAttributeValues, input.ExpressionAttributeNames)
		if err != nil {
			return nil, fmt.Errorf("Invalid ConditionExpression: %v", err)
		}
		if !match {
			return nil, NewConditionalCheckFailedException("The conditional request failed")
		}
	}

	if idx != -1 {
		// Remove item
		table.Items = append(table.Items[:idx], table.Items[idx+1:]...)
	}

	return DeleteItemOutput{}, nil
}

func (db *InMemoryDB) Scan(body []byte) (interface{}, error) {
	var input ScanInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	table, exists := db.Tables[input.TableName]
	if !exists {
		return nil, NewResourceNotFoundException(fmt.Sprintf("Requested resource not found: Table: %s not found", input.TableName))
	}

	var items []map[string]interface{}

	// Check if IndexName is provided
	var projection *Projection
	var gsiSchema []KeySchemaElement
	if input.IndexName != "" {
		found := false
		for _, gsi := range table.GlobalSecondaryIndexes {
			if gsi.IndexName == input.IndexName {
				projection = &gsi.Projection
				gsiSchema = gsi.KeySchema
				found = true
				break
			}
		}
		if !found {
			return nil, NewResourceNotFoundException(fmt.Sprintf("Requested resource not found: Index: %s not found", input.IndexName))
		}

		// Scan GSI: Only items that contain the GSI Keys (Sparse Index)
		for _, item := range table.Items {
			hasKeys := true
			for _, k := range gsiSchema {
				if _, ok := item[k.AttributeName]; !ok {
					hasKeys = false
					break
				}
			}
			if hasKeys {
				// Apply GSI projection
				projected := applyGSIProjection(item, *projection, table.KeySchema, gsiSchema)
				items = append(items, projected)
			}
		}
	} else {
		items = table.Items
	}

	// Apply FilterExpression
	if input.FilterExpression != "" {
		var filteredItems []map[string]interface{}
		for _, item := range items {
			match, err := evaluateExpression(input.FilterExpression, item, input.ExpressionAttributeValues, input.ExpressionAttributeNames)
			if err == nil && match {
				filteredItems = append(filteredItems, item)
			}
		}
		items = filteredItems
	}

	// Apply ProjectionExpression (Client requested projection)
	// Apply AFTER GSI projection (which is conceptually the "source" table for the scan)
	if input.ProjectionExpression != "" {
		projectedItems := make([]map[string]interface{}, len(items))
		for i, item := range items {
			projectedItems[i] = projectItem(item, input.ProjectionExpression, input.ExpressionAttributeNames)
		}
		items = projectedItems
	}

	return ScanOutput{
		Items: items,
		Count: len(items),
		ScannedCount: func() int {
			if input.IndexName != "" {
				return len(items) // In AWS this is the count of items in the index
			} else {
				return len(table.Items)
			}
		}(),
	}, nil
}

func (db *InMemoryDB) UpdateItem(body []byte) (interface{}, error) {
	var input UpdateItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	table, exists := db.Tables[input.TableName]
	if !exists {
		return nil, NewResourceNotFoundException(fmt.Sprintf("Requested resource not found: Table: %s not found", input.TableName))
	}

	// Check ConditionExpression
	// We have the index `idx` from the loop below? No, we need to find it first.
	// Reuse loop
	idx := -1
	for i, item := range table.Items {
		if itemsMatchKey(item, input.Key, table.KeySchema) {
			idx = i
			break
		}
	}

	var existingItem map[string]interface{}
	if idx != -1 {
		existingItem = table.Items[idx]
	}

	if input.ConditionExpression != "" {
		match, err := evaluateExpression(input.ConditionExpression, existingItem, input.ExpressionAttributeValues, input.ExpressionAttributeNames)
		if err != nil {
			return nil, fmt.Errorf("Invalid ConditionExpression: %v", err)
		}
		if !match {
			return nil, NewConditionalCheckFailedException("The conditional request failed")
		}
	}

	var item map[string]interface{}
	isNew := false

	if idx != -1 {
		item = table.Items[idx]
	} else {
		// New item, start with Key
		item = make(map[string]interface{})
		for k, v := range input.Key {
			item[k] = v
		}
		isNew = true
	}

	// Create a deep copy for OLD values if needed
	var oldItem map[string]interface{}
	if input.ReturnValues == "ALL_OLD" || input.ReturnValues == "UPDATED_OLD" {
		oldItem = make(map[string]interface{})
		for k, v := range item {
			oldItem[k] = v
		}
	}

	// Parse UpdateExpression (Very basic SET support)
	// Example: "SET #val = :v"
	if input.UpdateExpression != "" {
		parts := strings.Split(input.UpdateExpression, "SET ")
		if len(parts) > 1 {
			assignments := strings.Split(parts[1], ",")
			for _, assignment := range assignments {
				kv := strings.Split(assignment, "=")
				if len(kv) == 2 {
					key := strings.TrimSpace(kv[0])
					valPlaceholder := strings.TrimSpace(kv[1])

					if val, ok := input.ExpressionAttributeValues[valPlaceholder]; ok {
						item[key] = val
					}
				}
			}
		}
	}

	if isNew {
		// Validate new item
		if err := validateItemSize(item); err != nil {
			return nil, err
		}
		if err := validateDataTypes(item); err != nil {
			return nil, err
		}
		table.Items = append(table.Items, item)
	} else {
		// Validate updated item
		if err := validateItemSize(item); err != nil {
			return nil, err
		}
		// optimize: validateDataTypes only on updated fields? Or full item for simplicity
		if err := validateDataTypes(item); err != nil {
			return nil, err
		}
		table.Items[idx] = item
	}

	output := UpdateItemOutput{}

	if input.ReturnValues == "ALL_OLD" {
		output.Attributes = oldItem
	} else if input.ReturnValues == "ALL_NEW" {
		output.Attributes = item
	} else if input.ReturnValues == "UPDATED_NEW" {
		output.Attributes = item
	}

	if input.ReturnConsumedCapacity == "TOTAL" || input.ReturnConsumedCapacity == "INDEXES" {
		output.ConsumedCapacity = &ConsumedCapacity{
			TableName:          input.TableName,
			CapacityUnits:      1.0,
			ReadCapacityUnits:  0.5,
			WriteCapacityUnits: 0.5,
		}
	}

	if input.ReturnItemCollectionMetrics == "SIZE" {
		output.ItemCollectionMetrics = &ItemCollectionMetrics{
			ItemCollectionKey:   map[string]interface{}{"pk": input.Key["pk"]},
			SizeEstimateRangeGB: []float64{0.0, 1.0},
		}
	}

	return output, nil
}

func (db *InMemoryDB) Query(body []byte) (interface{}, error) {
	var input QueryInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	table, exists := db.Tables[input.TableName]
	if !exists {
		return nil, NewResourceNotFoundException(fmt.Sprintf("Requested resource not found: Table: %s not found", input.TableName))
	}

	// 1. Determine Key Schema (Table or GSI)
	var keySchema []KeySchemaElement
	var projection *Projection

	if input.IndexName != "" {
		// Find GSI
		found := false
		for _, gsi := range table.GlobalSecondaryIndexes {
			if gsi.IndexName == input.IndexName {
				keySchema = gsi.KeySchema
				projection = &gsi.Projection
				found = true
				break
			}
		}
		if !found {
			// Find LSI
			for _, lsi := range table.LocalSecondaryIndexes {
				if lsi.IndexName == input.IndexName {
					keySchema = lsi.KeySchema
					projection = &lsi.Projection
					found = true
					break
				}
			}
		}

		if !found {
			return nil, NewResourceNotFoundException(fmt.Sprintf("Requested resource not found: Index: %s not found", input.IndexName))
		}
	} else {
		keySchema = table.KeySchema
	}

	// 2. Identify PK and SK definitions
	var pkDef, skDef KeySchemaElement
	for _, k := range keySchema {
		if k.KeyType == "HASH" {
			pkDef = k
		} else if k.KeyType == "RANGE" {
			skDef = k
		}
	}

	// 3. Parse KeyConditionExpression
	// We need to extract PK value and potentially SK condition.
	// Simple Parser for V1:
	// Assumes "PK = :pkval [AND SK op :skval]" structure.

	// A robust parser is complex. We will implement a "split by AND" approach.
	// Part 1 must be PK equality.
	// Part 2 (optional) is SK condition.

	exprParts := strings.Split(input.KeyConditionExpression, " AND ")
	if len(exprParts) == 0 {
		return nil, fmt.Errorf("Invalid KeyConditionExpression")
	}

	// Parse PK Condition (Must be equality)
	pkExpr := strings.TrimSpace(exprParts[0])
	// Expect "pkName = :v"
	// We can use evaluateExpression helper logic effectively by checking against item?
	// But we need to EXTRACT the value to index lookup efficiently (scan optimization).
	// For InMemoryDB, we scan all items, so evaluating per item is actually fine!

	// WAIT! We need candidates to sort.
	// Filter Loop:
	var candidates []map[string]interface{}

	for _, item := range table.Items {
		// 3.a Check PK Match
		// We use `evaluateExpression` on the single PK part?
		// Or manually parse. Manual is safer for strict "EQ" requirement.
		// Let's rely on evaluateExpression for flexibility if it supports EQ.
		// But AWS requires PK to be EQ.

		matchPK, err := evaluateExpression(pkExpr, item, input.ExpressionAttributeValues, input.ExpressionAttributeNames)
		if err != nil {
			return nil, err
		}
		if !matchPK {
			continue
		}

		// 3.b Check SK Match (if exists)
		matchSK := true
		if len(exprParts) > 1 {
			skExpr := strings.Join(exprParts[1:], " AND ") // Rejoin rest
			matchSK, err = evaluateExpression(skExpr, item, input.ExpressionAttributeValues, input.ExpressionAttributeNames)
			if err != nil {
				return nil, err
			}
		}

		if matchSK {
			// Item matches Key Condition

			// Apply GSI Projection immediately if GSI
			candidate := item
			if input.IndexName != "" && projection != nil {
				candidate = applyGSIProjection(item, *projection, table.KeySchema, keySchema)
			}

			candidates = append(candidates, candidate)
		}
	}

	// 4. Sort Items by Sort Key (if present)
	if skDef.AttributeName != "" {
		// We need to know the type of SK to sort correctly.
		// In Validation we didn't store types. We have to infer or look at AttributeDefinitions?
		// We have table.AttributeDefinitions.
		skType := "S" // Default
		for _, ad := range table.AttributeDefinitions {
			if ad.AttributeName == skDef.AttributeName {
				skType = ad.AttributeType
				break
			}
		}

		sortCandidates(candidates, skDef.AttributeName, skType)
	}

	// 5. Apply ScanIndexForward (Reverse if false)
	if input.ScanIndexForward != nil && !*input.ScanIndexForward {
		for i, j := 0, len(candidates)-1; i < j; i, j = i+1, j-1 {
			candidates[i], candidates[j] = candidates[j], candidates[i]
		}
	}

	// 6. Pagination: ExclusiveStartKey
	startIndex := 0
	if input.ExclusiveStartKey != nil {
		// Find the index of the StartKey
		// AWS: StartKey is the *last evaluated key* from previous page. We start *after* it.
		// We match PK and SK.
		for i, item := range candidates {
			// Check if item keys match ExclusiveStartKey
			matchesStartKey := true

			// Check PK
			if !compareAttributeValues(item[pkDef.AttributeName], input.ExclusiveStartKey[pkDef.AttributeName]) {
				matchesStartKey = false
			}
			// Check SK if exists
			if matchesStartKey && skDef.AttributeName != "" {
				if !compareAttributeValues(item[skDef.AttributeName], input.ExclusiveStartKey[skDef.AttributeName]) {
					matchesStartKey = false
				}
			}

			if matchesStartKey {
				startIndex = i + 1
				break
			}
		}
	}

	// 7. Apply Limit and FilterExpression
	var items []map[string]interface{}
	var lastEvaluatedKey map[string]interface{}

	limit := int(input.Limit)
	count := 0

	for i := startIndex; i < len(candidates); i++ {
		// Check Limit (Items Scanned/Evaluated limit in strict AWS, but Items Returned limit often for simple mocks)
		// Implementing "Items Returned" limit for utility.
		if limit > 0 && count >= limit {
			// We reached limit. The *previous* item was the last one.
			// Set LastEvaluatedKey to the PREVIOUS item's key.
			lastEvaluatedKey = extractKey(items[len(items)-1], keySchema)
			break
		}

		item := candidates[i]

		// Apply FilterExpression
		include := true
		if input.FilterExpression != "" {
			match, err := evaluateExpression(input.FilterExpression, item, input.ExpressionAttributeValues, input.ExpressionAttributeNames)
			if err == nil && !match {
				include = false
			}
		}

		if include {
			items = append(items, item)
			count++
		}

		// If we are at the very end of loop and hit limit exactly?
		if limit > 0 && count == limit {
			// We just added the last allowed item.
			lastEvaluatedKey = extractKey(item, keySchema)
			// We can stop scanning?
			// If we stop here, next loop check catches it?
			// We need to return.
			break
		}
	}

	// 8. Apply ProjectionExpression (Final reduction)
	if input.ProjectionExpression != "" {
		projectedItems := make([]map[string]interface{}, len(items))
		for i, item := range items {
			projectedItems[i] = projectItem(item, input.ProjectionExpression, input.ExpressionAttributeNames)
		}
		items = projectedItems
	}

	return QueryOutput{
		Items:            items,
		Count:            len(items),
		ScannedCount:     len(candidates), // Approximation of scanned (post-key-condition)
		LastEvaluatedKey: lastEvaluatedKey,
	}, nil
}

// Helpers

func extractKey(item map[string]interface{}, schema []KeySchemaElement) map[string]interface{} {
	key := make(map[string]interface{})
	for _, k := range schema {
		if val, ok := item[k.AttributeName]; ok {
			key[k.AttributeName] = val
		}
	}
	return key
}

func compareAttributeValues(v1, v2 interface{}) bool {
	// Deep equality check for map[string]interface{} (AttributeValue)
	b1, _ := json.Marshal(v1)
	b2, _ := json.Marshal(v2)
	return string(b1) == string(b2)
}

func sortCandidates(items []map[string]interface{}, sortKey string, sortType string) {
	// Bubble sort or Slice sort. Slice sort is easy.
	// Since we are inside `item_ops.go`, we need to import "sort" if not present.
	// The file only imports "encoding/json", "fmt", "strings".
	// We'll bubble sort for minimal import changes or add import.
	// Bubble sort is fine for in-memory small datasets.

	n := len(items)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			v1 := items[j][sortKey]
			v2 := items[j+1][sortKey]

			swap := false
			if sortType == "N" {
				// Parse numbers
				n1 := parseNum(v1)
				n2 := parseNum(v2)
				if n1 > n2 {
					swap = true
				}
			} else {
				// String compare (works for S and generic)
				s1 := parseStr(v1)
				s2 := parseStr(v2)
				if s1 > s2 {
					swap = true
				}
			}

			if swap {
				items[j], items[j+1] = items[j+1], items[j]
			}
		}
	}
}

func parseStr(v interface{}) string {
	// v is map[string]interface{} {"S": "val"}
	if m, ok := v.(map[string]interface{}); ok {
		if s, ok := m["S"]; ok {
			return fmt.Sprint(s)
		}
		if n, ok := m["N"]; ok {
			return fmt.Sprint(n)
		}
	}
	return ""
}

func parseNum(v interface{}) float64 {
	// v is map[string]interface{} {"N": "123"}
	s := parseStr(v)
	var f float64
	fmt.Sscanf(s, "%f", &f)
	return f
}

func (db *InMemoryDB) BatchGetItem(body []byte) (interface{}, error) {
	var input BatchGetItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	responses := make(map[string][]map[string]interface{})
	unprocessed := make(map[string]KeysAndAttributes)

	for tableName, keysAndAttrs := range input.RequestItems {
		table, exists := db.Tables[tableName]
		if !exists {
			// AWS behavior: invalid table in batch might return error or unprocessed?
			// Usually ResourceNotFound for the whole request if a table is missing?
			// Let's assume ResourceNotFound for simplicity for now.
			return nil, NewResourceNotFoundException(fmt.Sprintf("Requested resource not found: Table: %s not found", tableName))
		}

		// Find PK name
		pkName := ""
		for _, k := range table.KeySchema {
			if k.KeyType == "HASH" {
				pkName = k.AttributeName
				break
			}
		}

		var items []map[string]interface{}
		for _, keyMap := range keysAndAttrs.Keys {
			// Inefficient O(N) scan for each key, but allowed for InMemoryDB
			// Ideally we should have an index.
			// Reusing simple scan logic per key.
			found := false
			for _, item := range table.Items {
				// Check PK match
				if pkVal, ok := keyMap[pkName]; ok {
					if itemPk, ok2 := item[pkName]; ok2 {
						// Compare
						pkJSON, _ := json.Marshal(pkVal)
						itemPkJSON, _ := json.Marshal(itemPk)
						if string(pkJSON) == string(itemPkJSON) {
							// Found (assuming no Sort Key for now or exact match ignored SK)
							// If SK exists in keyMap, we must match it too.
							// Let's do a full key match check.
							match := true
							for k, v := range keyMap {
								if iv, ok3 := item[k]; !ok3 {
									match = false
									break
								} else {
									vJSON, _ := json.Marshal(v)
									ivJSON, _ := json.Marshal(iv)
									if string(vJSON) != string(ivJSON) {
										match = false
										break
									}
								}
							}
							if match {
								items = append(items, item)
								found = true
								break // Found the item for this key
							}
						}
					}
				}
			}
			if !found {
				// Item not found, just ignored in Response (AWS behavior)
			}
		}

		// Apply ProjectionExpression (if present in KeysAndAttributes)
		if keysAndAttrs.ProjectionExpression != "" {
			projectedItems := make([]map[string]interface{}, len(items))
			for i, item := range items {
				projectedItems[i] = projectItem(item, keysAndAttrs.ProjectionExpression, keysAndAttrs.ExpressionAttributeNames)
			}
			items = projectedItems
		}

		if len(items) > 0 {
			responses[tableName] = items
		}
	}

	return BatchGetItemOutput{
		Responses:       responses,
		UnprocessedKeys: unprocessed,
	}, nil
}

func (db *InMemoryDB) BatchWriteItem(body []byte) (interface{}, error) {
	var input BatchWriteItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Validation phase (ensure all tables exist)
	for tableName := range input.RequestItems {
		if _, exists := db.Tables[tableName]; !exists {
			return nil, NewResourceNotFoundException(fmt.Sprintf("Requested resource not found: Table: %s not found", tableName))
		}
	}

	// Execution phase
	// Note: We are already holding the lock! We cannot call db.PutItem/DeleteItem if they also lock.
	// We need internal helpers or duplicate logic.
	// Duplicating simple logic for now or refactoring later.
	// Since PutItem/DeleteItem logic is small (update list), we can inline relevant parts or call unlocked helpers.
	// BUT `item_ops.go` methods like `PutItem` take a lock.
	// We must NOT call them.
	// Let's implement inline for now.

	for tableName, requests := range input.RequestItems {
		table := db.Tables[tableName]
		// Find PK name again
		pkName := ""
		for _, k := range table.KeySchema {
			if k.KeyType == "HASH" {
				pkName = k.AttributeName
				break
			}
		}

		for _, req := range requests {
			if req.PutRequest != nil {
				// Put Logic
				item := req.PutRequest.Item
				// Remove existing if any (based on PK)
				// Naive: loop and replace or append
				// This is O(N) per item.
				// Better: remove matching index

				// Identify Key
				key := make(map[string]interface{})
				if pkVal, ok := item[pkName]; ok {
					key[pkName] = pkVal
				}
				// SK?
				// For now assume PK only or simple match

				// Remove old
				// Use the same matching logic as BatchGet but modify slice
				// It's safer to rebuild the slice or swap remove

				// Let's just append for now and filter duplicates? No, that's bad.
				// Correct approach: Find index of existing item
				matchIndex := -1
				for i, existing := range table.Items {
					// Match keys
					match := true
					for k, v := range key {
						if iv, ok := existing[k]; !ok {
							match = false
							break
						} else {
							vJSON, _ := json.Marshal(v)
							ivJSON, _ := json.Marshal(iv)
							if string(vJSON) != string(ivJSON) {
								match = false
								break
							}
						}
					}
					if match {
						matchIndex = i
						break
					}
				}

				if matchIndex != -1 {
					// Replace
					table.Items[matchIndex] = item
				} else {
					// Append
					table.Items = append(table.Items, item)
				}

			} else if req.DeleteRequest != nil {
				// Delete Logic
				key := req.DeleteRequest.Key
				matchIndex := -1
				for i, existing := range table.Items {
					match := true
					for k, v := range key {
						if iv, ok := existing[k]; !ok {
							match = false
							break
						} else {
							vJSON, _ := json.Marshal(v)
							ivJSON, _ := json.Marshal(iv)
							if string(vJSON) != string(ivJSON) {
								match = false
								break
							}
						}
					}
					if match {
						matchIndex = i
						break
					}
				}

				if matchIndex != -1 {
					// Remove
					table.Items = append(table.Items[:matchIndex], table.Items[matchIndex+1:]...)
				}
			}
		}
		db.Tables[tableName] = table // Update map entry struct (since Table is a struct, not pointer in map? Wait, Table is struct)
		// type Table struct { Items ... }
		// db.Tables is map[string]Table
		// So we MUST assign back to map.
	}

	return BatchWriteItemOutput{
		UnprocessedItems: make(map[string][]WriteRequest),
	}, nil
}

// applyGSIProjection filters the item attributes based on GSI projection definition
func applyGSIProjection(item map[string]interface{}, projection Projection, tableSchema, gsiSchema []KeySchemaElement) map[string]interface{} {
	if projection.ProjectionType == "ALL" {
		return item
	}

	newItem := make(map[string]interface{})

	// 1. Always include Table Keys
	for _, k := range tableSchema {
		if val, ok := item[k.AttributeName]; ok {
			newItem[k.AttributeName] = val
		}
	}

	// 2. Always include GSI Keys
	for _, k := range gsiSchema {
		if val, ok := item[k.AttributeName]; ok {
			newItem[k.AttributeName] = val
		}
	}

	// 3. Include NonKeyAttributes if INCLUDE
	if projection.ProjectionType == "INCLUDE" {
		for _, attr := range projection.NonKeyAttributes {
			if val, ok := item[attr]; ok {
				newItem[attr] = val
			}
		}
	}

	return newItem
}
