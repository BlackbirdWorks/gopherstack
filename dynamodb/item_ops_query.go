package dynamodb

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

func (db *InMemoryDB) Query(body []byte) (any, error) {
	var input QueryInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	table, err := db.getTable(input.TableName)
	if err != nil {
		return nil, err
	}

	table.mu.RLock()
	defer table.mu.RUnlock()

	keySchema, projection, err := db.extractKeySchema(table, input.IndexName)
	if err != nil {
		return nil, err
	}

	candidates, err := db.filterCandidatesForKeyCondition(table, &input, projection, keySchema)
	if err != nil {
		return nil, err
	}

	_, skDef := getPKAndSK(keySchema)
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

	return nil, nil, NewResourceNotFoundException(fmt.Sprintf("Index: %s not found", indexName))
}

func (db *InMemoryDB) filterCandidatesForKeyCondition(
	table *Table,
	input *QueryInput,
	projection *Projection,
	keySchema []KeySchemaElement,
) ([]map[string]any, error) {
	exprParts := splitANDConditions(input.KeyConditionExpression)
	if len(exprParts) == 0 {
		return nil, NewValidationException("invalid KeyConditionExpression")
	}

	pkExpr := strings.TrimSpace(exprParts[0])
	for strings.HasPrefix(pkExpr, "(") && strings.HasSuffix(pkExpr, ")") {
		pkExpr = strings.TrimSpace(pkExpr[1 : len(pkExpr)-1])
	}

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

	return db.filterCandidatesScan(table, input, projection, keySchema, exprParts)
}

func (db *InMemoryDB) tryFilterUsingAuthoritativeIndex(
	table *Table,
	input *QueryInput,
	projection *Projection,
	_ []KeySchemaElement,
	pkExpr string,
	_ KeySchemaElement,
	skDef KeySchemaElement,
	exprParts []string,
) ([]map[string]any, bool) {
	pkValue := extractPKValueFromExpression(pkExpr, input.ExpressionAttributeValues, input.ExpressionAttributeNames)
	if pkValue == "" {
		return nil, false
	}

	if skDef.AttributeName != "" {
		if skMap, ok := table.pkskIndex[pkValue]; ok {
			indices := make([]int, 0, len(skMap))
			for _, idx := range skMap {
				indices = append(indices, idx)
			}

			candidates := db.filterUsingIndices(table, input, projection, indices, exprParts)

			return candidates, true
		}
	} else if idx, ok := table.pkIndex[pkValue]; ok {
		candidates := db.filterUsingIndices(table, input, projection, []int{idx}, exprParts)

		return candidates, true
	}

	return nil, true // PK exists in schema but no items match it
}

func (db *InMemoryDB) filterUsingIndices(
	table *Table,
	input *QueryInput,
	_ *Projection,
	indices []int,
	exprParts []string,
) []map[string]any {
	candidates := make([]map[string]any, 0, len(indices))
	for _, idx := range indices {
		item := table.Items[idx]
		match := true
		for _, part := range exprParts {
			m, err := evaluateExpression(part, item, input.ExpressionAttributeValues, input.ExpressionAttributeNames)
			if err != nil || !m {
				match = false

				break
			}
		}

		if match {
			candidates = append(candidates, item)
		}
	}

	return candidates
}

func extractPKValueFromExpression(
	expression string,
	attrValues map[string]any,
	attrNames map[string]string,
) string {
	parts := strings.Split(expression, " = ")
	if len(parts) != expectedPKParts {
		return ""
	}

	return dbResolvePKTarget(parts[0], parts[1], attrNames, attrValues)
}

func dbResolvePKTarget(left, right string, attrNames map[string]string, attrValues map[string]any) string {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)

	lName := resolveAttrName(left, attrNames)
	rName := resolveAttrName(right, attrNames)

	if lName == "" && rName == "" {
		return "" // Not related to PK
	}

	// Figure out which side is the value token
	var valueToken string
	if strings.HasPrefix(left, ":") {
		valueToken = left
	} else if strings.HasPrefix(right, ":") {
		valueToken = right
	}

	if valueToken == "" {
		return ""
	}

	return dbExtractValueFromToken(valueToken, attrValues)
}

func (db *InMemoryDB) filterCandidatesScan(
	table *Table,
	input *QueryInput,
	projection *Projection,
	keySchema []KeySchemaElement,
	exprParts []string,
) ([]map[string]any, error) {
	pkDef, skDef := getPKAndSK(keySchema)
	candidates := make([]map[string]any, 0, len(table.Items)/estimatedMatchRateDivisor)

	for _, item := range table.Items {
		match := true
		for _, part := range exprParts {
			m, err := evaluateExpression(part, item, input.ExpressionAttributeValues, input.ExpressionAttributeNames)
			if err != nil || !m {
				match = false

				break
			}
		}

		if match {
			if input.IndexName != "" {
				candidates = append(candidates, applyGSIProjection(item, *projection, table.KeySchema, keySchema))
			} else {
				candidates = append(candidates, item)
			}
		}
	}

	// Filter and sort for GSI if needed (already handled by SortCandidates if sk exists)
	_ = pkDef
	_ = skDef

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
