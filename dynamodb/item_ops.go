package dynamodb

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	estimatedMatchRateDivisor = 2
	minScanAllocationSize     = 10
	batchSizeLimit            = 25
	estimatedMatchRateGSI     = 10
	expectedPKParts           = 2
)

// isItemExpired returns true when the table has a TTL attribute configured and
// the item's TTL value is in the past. Items without the attribute are never expired.
func isItemExpired(item map[string]any, ttlAttr string) bool {
	if ttlAttr == "" {
		return false
	}

	raw, ok := item[ttlAttr]
	if !ok {
		return false
	}

	m, ok := raw.(map[string]any)
	if !ok {
		return false
	}

	nStr, ok := m["N"].(string)
	if !ok {
		return false
	}

	epoch, err := strconv.ParseInt(nStr, 10, 64)
	if err != nil {
		return false
	}

	return time.Unix(epoch, 0).Before(time.Now())
}

func (db *InMemoryDB) PutItem(body []byte) (any, error) {
	var input PutItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	table, err := db.getTable(input.TableName)
	if err != nil {
		return nil, err
	}

	table.mu.Lock()
	defer table.mu.Unlock()

	if errVal := db.validateItem(input.Item, table); errVal != nil {
		return nil, errVal
	}

	oldItem, matchIndex := db.findMatchForPut(table, input.Item)

	if errVal := db.checkPutCondition(&input, oldItem); errVal != nil {
		return nil, errVal
	}

	db.doPut(table, &input, matchIndex)

	return db.populatePutItemOutput(&input, table, oldItem), nil
}

func (db *InMemoryDB) getTable(name string) (*Table, error) {
	db.mu.RLock()
	table, exists := db.Tables[name]
	db.mu.RUnlock()

	if !exists {
		return nil, NewResourceNotFoundException(
			fmt.Sprintf("Requested resource not found: Table: %s not found", name),
		)
	}

	return table, nil
}

func (db *InMemoryDB) findMatchForPut(table *Table, item map[string]any) (map[string]any, int) {
	pkDef, skDef := getPKAndSK(table.KeySchema)
	pkVal := BuildKeyString(item, pkDef.AttributeName)

	if skDef.AttributeName == "" {
		if idx, ok := table.pkIndex[pkVal]; ok {
			return table.Items[idx], idx
		}

		return nil, -1
	}

	skVal := BuildKeyString(item, skDef.AttributeName)
	skMap, okVal := table.pkskIndex[pkVal]
	if !okVal {
		return nil, -1
	}

	if idx, okIdx := skMap[skVal]; okIdx {
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
	} else {
		table.Items = append(table.Items, input.Item)
		matchIndex = len(table.Items) - 1
	}

	pkDef, skDef := getPKAndSK(table.KeySchema)
	pkVal := BuildKeyString(input.Item, pkDef.AttributeName)
	if skDef.AttributeName != "" {
		skVal := BuildKeyString(input.Item, skDef.AttributeName)
		if table.pkskIndex[pkVal] == nil {
			table.pkskIndex[pkVal] = make(map[string]int)
		}
		table.pkskIndex[pkVal][skVal] = matchIndex
	} else {
		table.pkIndex[pkVal] = matchIndex
	}
}

func (db *InMemoryDB) validateItem(item map[string]any, table *Table) error {
	if err := validateKeySchema(item, table.KeySchema); err != nil {
		return err
	}

	if err := ValidateItemSize(item); err != nil {
		return err
	}

	return ValidateDataTypes(item)
}

func (db *InMemoryDB) populatePutItemOutput(input *PutItemInput, table *Table, oldItem map[string]any) PutItemOutput {
	output := PutItemOutput{}
	if input.ReturnValues == ReturnValuesAllOld && oldItem != nil {
		output.Attributes = oldItem
	}

	if input.ReturnConsumedCapacity == "TOTAL" || input.ReturnConsumedCapacity == "INDEXES" {
		wcu := WriteCapacityUnits(input.Item)
		output.ConsumedCapacity = &ConsumedCapacity{
			TableName:          input.TableName,
			CapacityUnits:      wcu,
			WriteCapacityUnits: wcu,
		}
	}

	if input.ReturnItemCollectionMetrics == "SIZE" {
		pkName := getHashKeyName(table.KeySchema)
		output.ItemCollectionMetrics = &ItemCollectionMetrics{
			ItemCollectionKey:   map[string]any{pkName: input.Item[pkName]},
			SizeEstimateRangeGB: []float64{0.0, 1.0},
		}
	}

	return output
}

func (db *InMemoryDB) GetItem(body []byte) (any, error) {
	var input GetItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	db.mu.RLock()
	table, exists := db.Tables[input.TableName]
	db.mu.RUnlock()

	if !exists {
		return nil, NewResourceNotFoundException(
			fmt.Sprintf("Requested resource not found: Table: %s not found", input.TableName),
		)
	}

	table.mu.RLock()
	defer table.mu.RUnlock()

	// Fast index-based lookup
	pkDef, skDef := getPKAndSK(table.KeySchema)
	foundItem := db.lookupItem(table, input.Key, pkDef.AttributeName, skDef.AttributeName)

	if foundItem == nil || isItemExpired(foundItem, table.TTLAttribute) {
		return GetItemOutput{}, nil
	}

	result := foundItem
	if input.ProjectionExpression != "" {
		result = projectItem(foundItem, input.ProjectionExpression, input.ExpressionAttributeNames)
	}

	return GetItemOutput{Item: result}, nil
}

func (db *InMemoryDB) DeleteItem(body []byte) (any, error) {
	var input DeleteItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	db.mu.RLock()
	table, exists := db.Tables[input.TableName]
	db.mu.RUnlock()

	if !exists {
		return nil, NewResourceNotFoundException(
			fmt.Sprintf("Requested resource not found: Table: %s not found", input.TableName),
		)
	}

	table.mu.Lock()
	defer table.mu.Unlock()

	oldItem, matchIndex := db.findMatchForPut(table, input.Key)

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

	if matchIndex != -1 {
		db.deleteItemAtIndex(table, matchIndex)
	}

	return DeleteItemOutput{}, nil
}

func (db *InMemoryDB) Scan(body []byte) (any, error) {
	var input ScanInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	table, err := db.getTable(input.TableName)
	if err != nil {
		return nil, err
	}

	table.mu.RLock()
	defer table.mu.RUnlock()

	items := table.Items
	pkDef, skDef, err := db.getScanKeySchema(table, &input)
	if err != nil {
		return nil, err
	}

	results := db.doScan(items, &input, pkDef, skDef, table.TTLAttribute)

	return ScanOutput{
		Items:        results,
		Count:        len(results),
		ScannedCount: len(items),
	}, nil
}

func (db *InMemoryDB) getScanKeySchema(table *Table, input *ScanInput) (KeySchemaElement, KeySchemaElement, error) {
	if input.IndexName == "" {
		return KeySchemaElement{}, KeySchemaElement{}, nil
	}
	keySchema, _, err := db.extractKeySchema(table, input.IndexName)
	if err != nil {
		return KeySchemaElement{}, KeySchemaElement{}, err
	}

	pk, sk := getPKAndSK(keySchema)

	return pk, sk, nil
}

func (db *InMemoryDB) doScan(
	items []map[string]any,
	input *ScanInput,
	pkDef, skDef KeySchemaElement,
	ttlAttr string,
) []map[string]any {
	estimatedResults := max(len(items)/estimatedMatchRateDivisor, minScanAllocationSize)
	results := make([]map[string]any, 0, estimatedResults)

	for _, item := range items {
		if isItemExpired(item, ttlAttr) {
			continue
		}

		if !db.shouldIncludeInScan(item, input, pkDef, skDef) {
			continue
		}

		res := make(map[string]any, len(item))
		maps.Copy(res, item)

		if input.ProjectionExpression != "" {
			res = projectItem(res, input.ProjectionExpression, input.ExpressionAttributeNames)
		}

		results = append(results, res)

		if input.Limit != nil && len(results) >= int(*input.Limit) {
			break
		}
	}

	return results
}

func (db *InMemoryDB) shouldIncludeInScan(item map[string]any, input *ScanInput, pkDef, skDef KeySchemaElement) bool {
	if input.IndexName != "" {
		if _, hasPK := item[pkDef.AttributeName]; !hasPK {
			return false
		}
		if skDef.AttributeName != "" {
			if _, hasSK := item[skDef.AttributeName]; !hasSK {
				return false
			}
		}
	}

	if input.FilterExpression != "" {
		match, err := evaluateExpression(
			input.FilterExpression,
			item,
			input.ExpressionAttributeValues,
			input.ExpressionAttributeNames,
		)
		if err == nil && !match {
			return false
		}
	}

	return true
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

	existingItem, matchIndex := db.findMatchForPut(table, input.Key)

	if errVal := db.checkUpdateCondition(&input, existingItem); errVal != nil {
		return nil, errVal
	}

	newItem, err := db.doUpdate(table, &input, existingItem, matchIndex)
	if err != nil {
		return nil, err
	}

	return db.populateUpdateOutput(&input, table, existingItem, newItem), nil
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
	newItem := make(map[string]any)
	if existing != nil {
		maps.Copy(newItem, existing)
	} else {
		maps.Copy(newItem, input.Key)
	}

	if input.UpdateExpression != "" {
		err := applyUpdate(
			newItem,
			input.UpdateExpression,
			input.ExpressionAttributeNames,
			input.ExpressionAttributeValues,
		)
		if err != nil {
			return nil, err
		}
	}

	if existing == nil {
		table.Items = append(table.Items, newItem)
		matchIndex = len(table.Items) - 1
	} else {
		table.Items[matchIndex] = newItem
	}

	db.updateIndexes(table, newItem, matchIndex)

	return newItem, nil
}

func (db *InMemoryDB) updateIndexes(table *Table, item map[string]any, index int) {
	pkDef, skDef := getPKAndSK(table.KeySchema)
	pkVal := BuildKeyString(item, pkDef.AttributeName)
	if skDef.AttributeName != "" {
		skVal := BuildKeyString(item, skDef.AttributeName)
		if table.pkskIndex[pkVal] == nil {
			table.pkskIndex[pkVal] = make(map[string]int)
		}
		table.pkskIndex[pkVal][skVal] = index
	} else {
		table.pkIndex[pkVal] = index
	}
}

func (db *InMemoryDB) populateUpdateOutput(
	input *UpdateItemInput,
	table *Table,
	oldItem, newItem map[string]any,
) UpdateItemOutput {
	output := UpdateItemOutput{}
	if input.ReturnValues == ReturnValuesAllOld && oldItem != nil {
		output.Attributes = oldItem
	} else if input.ReturnValues == ReturnValuesAllNew {
		output.Attributes = newItem
	}

	if input.ReturnConsumedCapacity == "TOTAL" || input.ReturnConsumedCapacity == "INDEXES" {
		item := newItem
		if item == nil {
			item = oldItem
		}

		wcu := WriteCapacityUnits(item)
		output.ConsumedCapacity = &ConsumedCapacity{
			TableName:          input.TableName,
			CapacityUnits:      wcu,
			WriteCapacityUnits: wcu,
		}
	}

	if input.ReturnItemCollectionMetrics == "SIZE" {
		pkName := getHashKeyName(table.KeySchema)
		output.ItemCollectionMetrics = &ItemCollectionMetrics{
			ItemCollectionKey:   map[string]any{pkName: input.Key[pkName]},
			SizeEstimateRangeGB: []float64{0.0, 1.0},
		}
	}

	return output
}

func (db *InMemoryDB) Query(body []byte) (any, error) {
	var input QueryInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	db.mu.RLock()
	table, exists := db.Tables[input.TableName]
	db.mu.RUnlock()

	if !exists {
		return nil, NewResourceNotFoundException(
			fmt.Sprintf("Requested resource not found: Table: %s not found", input.TableName),
		)
	}

	table.mu.RLock()
	defer table.mu.RUnlock()

	keySchema, projection, err := db.extractKeySchema(table, input.IndexName)
	if err != nil {
		return nil, err
	}

	_, skDef := getPKAndSK(keySchema)

	candidates, err := db.filterCandidatesForKeyCondition(table, &input, projection, keySchema)
	if err != nil {
		return nil, err
	}

	if skDef.AttributeName != "" {
		db.sortCandidates(candidates, skDef, table, input.ScanIndexForward)
	}

	return db.processQueryResults(candidates, &input, keySchema, table.TTLAttribute), nil
}

func (db *InMemoryDB) extractKeySchema(table *Table, indexName string) ([]KeySchemaElement, *Projection, error) {
	if indexName == "" {
		return table.KeySchema, nil, nil
	}

	for _, gsi := range table.GlobalSecondaryIndexes {
		if gsi.IndexName == indexName {
			return gsi.KeySchema, &gsi.Projection, nil
		}
	}

	for _, lsi := range table.LocalSecondaryIndexes {
		if lsi.IndexName == indexName {
			return lsi.KeySchema, &lsi.Projection, nil
		}
	}

	return nil, nil, NewResourceNotFoundException(
		fmt.Sprintf("Requested resource not found: Index: %s not found", indexName),
	)
}

func getPKAndSK(keySchema []KeySchemaElement) (KeySchemaElement, KeySchemaElement) {
	var pkDef, skDef KeySchemaElement
	for _, k := range keySchema {
		switch k.KeyType {
		case KeyTypeHash:
			pkDef = k
		case KeyTypeRange:
			skDef = k
		}
	}

	return pkDef, skDef
}

func (db *InMemoryDB) filterCandidatesForKeyCondition(
	table *Table,
	input *QueryInput,
	projection *Projection,
	keySchema []KeySchemaElement,
) ([]map[string]any, error) {
	exprParts := strings.Split(input.KeyConditionExpression, " AND ")
	if len(exprParts) == 0 {
		return nil, NewValidationException("invalid KeyConditionExpression")
	}

	pkExpr := strings.TrimSpace(exprParts[0])
	pkDef, skDef := getPKAndSK(keySchema)

	// Try to use index for primary table queries (not GSI/LSI)
	if input.IndexName == "" {
		candidates, ok := db.tryFilterUsingAuthoritativeIndex(
			table,
			input,
			projection,
			keySchema,
			pkExpr,
			pkDef,
			skDef,
			exprParts,
		)
		if ok {
			return candidates, nil
		}
	}

	// Fallback to full scan for GSI/LSI or complex expressions
	return db.filterCandidatesScan(table, input, projection, keySchema, exprParts)
}

func (db *InMemoryDB) tryFilterUsingAuthoritativeIndex(
	table *Table,
	input *QueryInput,
	projection *Projection,
	keySchema []KeySchemaElement,
	pkExpr string,
	pkDef KeySchemaElement,
	skDef KeySchemaElement,
	exprParts []string,
) ([]map[string]any, bool) {
	pkVal := extractPKValueFromExpression(
		pkExpr,
		input.ExpressionAttributeValues,
		input.ExpressionAttributeNames,
		pkDef.AttributeName,
	)

	if pkVal == "" {
		return nil, false
	}

	// Check composite index
	if skDef.AttributeName != "" && len(table.pkskIndex) > 0 {
		if skMap, ok := table.pkskIndex[pkVal]; ok {
			indices := make([]int, 0, len(skMap))
			for _, idx := range skMap {
				indices = append(indices, idx)
			}

			res, _ := db.filterUsingIndices(table, input, projection, keySchema, indices, exprParts)

			return res, true
		}

		return []map[string]any{}, true
	}

	// Check simple index
	if skDef.AttributeName == "" && len(table.pkIndex) > 0 {
		if idx, ok := table.pkIndex[pkVal]; ok {
			res, _ := db.filterUsingIndices(table, input, projection, keySchema, []int{idx}, exprParts)

			return res, true
		}

		return []map[string]any{}, true
	}

	return nil, false
}

func (db *InMemoryDB) filterUsingIndices(
	table *Table,
	input *QueryInput,
	projection *Projection,
	keySchema []KeySchemaElement,
	indices []int,
	exprParts []string,
) ([]map[string]any, error) {
	candidates := make([]map[string]any, 0, len(indices))
	for _, idx := range indices {
		if idx < 0 || idx >= len(table.Items) {
			continue
		}
		item := table.Items[idx]

		matchSK := true
		if len(exprParts) > 1 {
			skExpr := strings.Join(exprParts[1:], " AND ")
			var err error
			matchSK, err = evaluateExpression(
				skExpr,
				item,
				input.ExpressionAttributeValues,
				input.ExpressionAttributeNames,
			)
			if err != nil {
				return nil, err
			}
		}

		if matchSK {
			candidate := item
			if input.IndexName != "" && projection != nil {
				candidate = applyGSIProjection(item, *projection, table.KeySchema, keySchema)
			}
			candidates = append(candidates, candidate)
		}
	}

	return candidates, nil
}

func extractPKValueFromExpression(
	expression string,
	attrValues map[string]any,
	attrNames map[string]string,
	pkName string,
) string {
	parts := strings.Split(expression, "=")
	if len(parts) != expectedPKParts {
		return ""
	}

	left := strings.TrimSpace(parts[0])
	right := strings.TrimSpace(parts[1])

	valToken := dbResolvePKTarget(left, right, attrNames, pkName)
	if valToken == "" {
		return ""
	}

	return dbExtractValueFromToken(valToken, attrValues)
}

func dbResolvePKTarget(left, right string, attrNames map[string]string, pkName string) string {
	resolvedLeft := resolveAttrName(left, attrNames)
	if resolvedLeft == pkName {
		return right
	}

	resolvedRight := resolveAttrName(right, attrNames)
	if resolvedRight == pkName {
		return left
	}

	return ""
}

func resolveAttrName(name string, attrNames map[string]string) string {
	if strings.HasPrefix(name, "#") {
		if resolved, ok := attrNames[name]; ok {
			return resolved
		}
	}

	return name
}

func dbExtractValueFromToken(token string, attrValues map[string]any) string {
	if !strings.HasPrefix(token, ":") {
		return ""
	}

	val, ok := attrValues[token]
	if !ok {
		return ""
	}

	if m, okM := val.(map[string]any); okM {
		if s, okS := m["S"].(string); okS {
			return s
		}

		if n, okN := m["N"].(string); okN {
			return n
		}

		if b, okB := m["B"].(string); okB {
			return b
		}

		for _, v := range m {
			return toString(v)
		}
	}

	return toString(val)
}

// filterCandidatesScan is the fallback full-scan method.
func (db *InMemoryDB) filterCandidatesScan(
	table *Table,
	input *QueryInput,
	projection *Projection,
	keySchema []KeySchemaElement,
	exprParts []string,
) ([]map[string]any, error) {
	pkExpr := strings.TrimSpace(exprParts[0])

	// Pre-allocate with reasonable capacity (estimate 10% match rate)
	estimatedSize := max(len(table.Items)/estimatedMatchRateGSI, minScanAllocationSize)
	candidates := make([]map[string]any, 0, estimatedSize)

	for _, item := range table.Items {
		matchPK, err := evaluateExpression(
			pkExpr,
			item,
			input.ExpressionAttributeValues,
			input.ExpressionAttributeNames,
		)
		if err != nil {
			return nil, err
		}

		if !matchPK {
			continue
		}

		matchSK := true
		if len(exprParts) > 1 {
			skExpr := strings.Join(exprParts[1:], " AND ")
			matchSK, err = evaluateExpression(
				skExpr,
				item,
				input.ExpressionAttributeValues,
				input.ExpressionAttributeNames,
			)
			if err != nil {
				return nil, err
			}
		}

		if matchSK {
			candidate := item
			if input.IndexName != "" && projection != nil {
				candidate = applyGSIProjection(item, *projection, table.KeySchema, keySchema)
			}

			candidates = append(candidates, candidate)
		}
	}

	return candidates, nil
}

func (db *InMemoryDB) sortCandidates(
	candidates []map[string]any,
	skDef KeySchemaElement,
	table *Table,
	scanIndexForward *bool,
) {
	// Try to get type from AttributeDefinitions first
	skType := getAttributeType(table.AttributeDefinitions, skDef.AttributeName, "")

	// If not found, infer from first candidate's actual value
	if skType == "" {
		skType = inferSKType(candidates, skDef.AttributeName)
	}
	if skType == "" {
		skType = "S" // final fallback
	}

	sort.Slice(candidates, func(i, j int) bool {
		v1 := unwrapAttributeValue(candidates[i][skDef.AttributeName])
		v2 := unwrapAttributeValue(candidates[j][skDef.AttributeName])
		res := compareAny(v1, v2, skType)
		if scanIndexForward != nil && !*scanIndexForward {
			return res > 0
		}

		return res < 0
	})
}

func (db *InMemoryDB) processQueryResults(
	candidates []map[string]any,
	input *QueryInput,
	keySchema []KeySchemaElement,
	ttlAttr string,
) QueryOutput {
	startIndex := findExclusiveStartIndex(candidates, input.ExclusiveStartKey, keySchema)

	capacity := int(input.Limit)
	if capacity == 0 || capacity > 100 {
		capacity = 100
	}
	items := make([]map[string]any, 0, capacity)

	var lastEvaluatedKey map[string]any
	limit := int(input.Limit)
	count := 0

	for i := startIndex; i < len(candidates); i++ {
		if limit > 0 && count >= limit {
			lastEvaluatedKey = extractKey(items[len(items)-1], keySchema)

			break
		}

		item := candidates[i]
		if isItemExpired(item, ttlAttr) || !db.shouldIncludeInQuery(item, input) {
			continue
		}

		processedItem := item
		if input.ProjectionExpression != "" {
			processedItem = projectItem(item, input.ProjectionExpression, input.ExpressionAttributeNames)
		}

		items = append(items, processedItem)
		count++
	}

	return QueryOutput{
		Items:            items,
		Count:            len(items),
		ScannedCount:     len(candidates),
		LastEvaluatedKey: lastEvaluatedKey,
	}
}

func (db *InMemoryDB) shouldIncludeInQuery(item map[string]any, input *QueryInput) bool {
	if input.FilterExpression == "" {
		return true
	}
	match, err := evaluateExpression(
		input.FilterExpression,
		item,
		input.ExpressionAttributeValues,
		input.ExpressionAttributeNames,
	)

	return err == nil && match
}

// deleteItemAtIndex removes the item at matchIndex from the table and performs an
// incremental index update — O(k) where k is the number of indexed keys — rather
// than a full O(n) rebuildIndexes call.
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

func extractKey(item map[string]any, schema []KeySchemaElement) map[string]any {
	key := make(map[string]any)
	for _, k := range schema {
		if val, ok := item[k.AttributeName]; ok {
			key[k.AttributeName] = val
		}
	}

	return key
}

// compareAttributeValues compares two DynamoDB attribute values without reflection.
// Values are always map[string]any with a single type key (e.g. {"S": "foo"}).
func compareAttributeValues(v1, v2 any) bool {
	m1, ok1 := v1.(map[string]any)
	m2, ok2 := v2.(map[string]any)

	if !ok1 || !ok2 {
		// Fallback for bare Go primitives (shouldn't occur in normal operation).
		return fmt.Sprintf("%v", v1) == fmt.Sprintf("%v", v2)
	}

	for typeKey, val1 := range m1 {
		val2, exists := m2[typeKey]
		if !exists {
			return false
		}

		s1, isStr1 := val1.(string)
		s2, isStr2 := val2.(string)

		if isStr1 && isStr2 {
			return s1 == s2
		}

		// Nested map (e.g. M, L types) — fall back to string representation.
		return fmt.Sprintf("%v", val1) == fmt.Sprintf("%v", val2)
	}

	return len(m2) == 0
}

func applyGSIProjection(
	item map[string]any,
	projection Projection,
	tableSchema []KeySchemaElement,
	gsiSchema []KeySchemaElement,
) map[string]any {
	if projection.ProjectionType == "ALL" {
		return item
	}

	newItem := make(map[string]any)
	for _, k := range tableSchema {
		if val, ok := item[k.AttributeName]; ok {
			newItem[k.AttributeName] = val
		}
	}

	for _, k := range gsiSchema {
		if val, ok := item[k.AttributeName]; ok {
			newItem[k.AttributeName] = val
		}
	}

	if projection.ProjectionType == "INCLUDE" {
		for _, attr := range projection.NonKeyAttributes {
			if val, ok := item[attr]; ok {
				newItem[attr] = val
			}
		}
	}

	return newItem
}

func compareAny(v1, v2 any, typ string) int {
	if v1 == nil || v2 == nil {
		return 0
	}

	if typ == "N" {
		f1 := parseNumber(v1)
		f2 := parseNumber(v2)
		if f1 < f2 {
			return -1
		}

		if f1 > f2 {
			return 1
		}

		return 0
	}

	s1 := fmt.Sprintf("%v", v1)
	s2 := fmt.Sprintf("%v", v2)
	if s1 < s2 {
		return -1
	}

	if s1 > s2 {
		return 1
	}

	return 0
}

func parseNumber(v any) float64 {
	s := parseStr(v)
	f, _ := strconv.ParseFloat(s, 64)

	return f
}

func parseStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}

	if m, ok := v.(map[string]any); ok {
		if s, ok2 := m["S"]; ok2 {
			return fmt.Sprint(s)
		}

		if n, ok3 := m["N"]; ok3 {
			return fmt.Sprint(n)
		}
	}

	return fmt.Sprintf("%v", v)
}

// getHashKeyName returns the attribute name of the hash key from the key schema.
func getHashKeyName(keySchema []KeySchemaElement) string {
	for _, k := range keySchema {
		if k.KeyType == KeyTypeHash {
			return k.AttributeName
		}
	}

	return ""
}

// getAttributeType returns the attribute type for a given attribute name, or defaultType if not found.
func getAttributeType(attrDefs []AttributeDefinition, attrName string, defaultType string) string {
	for _, ad := range attrDefs {
		if ad.AttributeName == attrName {
			return ad.AttributeType
		}
	}

	return defaultType
}

// findExclusiveStartIndex finds the index after the ExclusiveStartKey in the candidates list.
// Returns 0 if ExclusiveStartKey is nil or not found.
func findExclusiveStartIndex(
	candidates []map[string]any,
	exclusiveStartKey map[string]any,
	keySchema []KeySchemaElement,
) int {
	if exclusiveStartKey == nil {
		return 0
	}

	pkDef, skDef := getPKAndSK(keySchema)

	for i, item := range candidates {
		matches := compareAttributeValues(item[pkDef.AttributeName], exclusiveStartKey[pkDef.AttributeName])
		if matches && skDef.AttributeName != "" {
			matches = compareAttributeValues(
				item[skDef.AttributeName],
				exclusiveStartKey[skDef.AttributeName],
			)
		}

		if matches {
			return i + 1
		}
	}

	return 0
}
func (db *InMemoryDB) lookupItem(
	table *Table,
	key map[string]any,
	pkName, skName string,
) map[string]any {
	pkVal := BuildKeyString(key, pkName)
	if skName != "" {
		skVal := BuildKeyString(key, skName)
		if skMap, hasPK := table.pkskIndex[pkVal]; hasPK {
			if itemIdx, hasSK := skMap[skVal]; hasSK {
				return table.Items[itemIdx]
			}
		}

		return nil
	}

	if itemIdx, found := table.pkIndex[pkVal]; found {
		return table.Items[itemIdx]
	}

	return nil
}

func inferSKType(candidates []map[string]any, skName string) string {
	if len(candidates) == 0 {
		return ""
	}

	val, okVal := candidates[0][skName]
	if !okVal {
		return ""
	}

	m, okM := val.(map[string]any)
	if !okM {
		return ""
	}

	for _, t := range []string{"N", "S", "B"} {
		if _, has := m[t]; has {
			return t
		}
	}

	return "S" // default
}
