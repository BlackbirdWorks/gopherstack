package dynamodb

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"

	"Gopherstack/dynamodb/models"
	"Gopherstack/pkgs/dynamoattr"
	"Gopherstack/pkgs/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (db *InMemoryDB) Query(
	ctx context.Context,
	input *dynamodb.QueryInput,
) (*dynamodb.QueryOutput, error) {
	return db.QueryWithContext(ctx, input)
}

func (db *InMemoryDB) QueryWithContext(
	ctx context.Context,
	input *dynamodb.QueryInput,
) (*dynamodb.QueryOutput, error) {
	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("query cancelled: %w", ctx.Err())
	default:
	}

	tableName := aws.ToString(input.TableName)
	table, err := db.getTable(tableName)
	if err != nil {
		return nil, err
	}

	// Snapshot table metadata and items under lock
	table.mu.RLock()
	itemsCopy := make([]map[string]any, len(table.Items))
	copy(itemsCopy, table.Items)
	keySchemaOrig := make([]models.KeySchemaElement, len(table.KeySchema))
	copy(keySchemaOrig, table.KeySchema)
	gsiList := make([]models.GlobalSecondaryIndex, len(table.GlobalSecondaryIndexes))
	copy(gsiList, table.GlobalSecondaryIndexes)
	lsiList := make([]models.LocalSecondaryIndex, len(table.LocalSecondaryIndexes))
	copy(lsiList, table.LocalSecondaryIndexes)
	attrDefs := make([]models.AttributeDefinition, len(table.AttributeDefinitions))
	copy(attrDefs, table.AttributeDefinitions)
	ttlAttr := table.TTLAttribute
	// Copy pk/sk indices for efficient lookups
	pkIndexCopy := make(map[string]int, len(table.pkIndex))
	maps.Copy(pkIndexCopy, table.pkIndex)
	pkskIndexCopy := make(map[string]map[string]int, len(table.pkskIndex))
	for k, m := range table.pkskIndex {
		m2 := make(map[string]int, len(m))
		maps.Copy(m2, m)
		pkskIndexCopy[k] = m2
	}
	table.mu.RUnlock()

	// Reconstruct snapshot table for querying
	snapshotTable := &Table{
		Items:                  itemsCopy,
		KeySchema:              keySchemaOrig,
		GlobalSecondaryIndexes: gsiList,
		LocalSecondaryIndexes:  lsiList,
		AttributeDefinitions:   attrDefs,
		TTLAttribute:           ttlAttr,
		pkIndex:                pkIndexCopy,
		pkskIndex:              pkskIndexCopy,
	}

	idxName := aws.ToString(input.IndexName)
	keySchema, projection, err := db.extractKeySchema(snapshotTable, idxName)
	if err != nil {
		return nil, err
	}

	candidates, err := db.filterCandidatesForKeyCondition(ctx, snapshotTable, input, projection, keySchema)
	if err != nil {
		return nil, err
	}

	_, skDef := getPKAndSK(keySchema)
	sortForward := true
	if input.ScanIndexForward != nil {
		sortForward = *input.ScanIndexForward
	}

	if skDef.AttributeName != "" {
		db.sortCandidates(candidates, skDef, snapshotTable, sortForward)
	}

	return db.processQueryResults(ctx, candidates, input, keySchema, ttlAttr), nil
}

func (db *InMemoryDB) extractKeySchema(
	table *Table,
	indexName string,
) ([]models.KeySchemaElement, *models.Projection, error) {
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
	ctx context.Context,
	table *Table,
	input *dynamodb.QueryInput,
	projection *models.Projection,
	keySchema []models.KeySchemaElement,
) ([]map[string]any, error) {
	cond := aws.ToString(input.KeyConditionExpression)
	if cond != "" {
		log := logger.Load(ctx)
		log.DebugContext(ctx, "Evaluating Query KeyConditionExpression",
			"expression", cond,
			"attributeNames", input.ExpressionAttributeNames,
			"attributeValues", input.ExpressionAttributeValues)
	}
	exprParts := dynamoattr.SplitANDConditions(cond)
	if len(exprParts) == 0 {
		return nil, NewValidationException("invalid KeyConditionExpression")
	}

	pkExpr := strings.TrimSpace(exprParts[0])
	for strings.HasPrefix(pkExpr, "(") && strings.HasSuffix(pkExpr, ")") {
		pkExpr = strings.TrimSpace(pkExpr[1 : len(pkExpr)-1])
	}

	pkDef, skDef := getPKAndSK(keySchema)
	idxName := aws.ToString(input.IndexName)

	eav := models.FromSDKItem(input.ExpressionAttributeValues)

	// Try to use index for primary table queries (not GSI/LSI)
	if idxName == "" {
		candidates, ok := db.tryFilterUsingAuthoritativeIndex(
			table,
			input,
			projection,
			keySchema,
			pkExpr,
			pkDef,
			skDef,
			exprParts,
			eav,
		)
		if ok {
			return candidates, nil
		}
	}

	return db.filterCandidatesScan(table, input, projection, keySchema, exprParts, eav)
}

func (db *InMemoryDB) tryFilterUsingAuthoritativeIndex(
	table *Table,
	input *dynamodb.QueryInput,
	projection *models.Projection,
	_ []models.KeySchemaElement,
	pkExpr string,
	_ models.KeySchemaElement,
	skDef models.KeySchemaElement,
	exprParts []string,
	eav map[string]any,
) ([]map[string]any, bool) {
	pkValue := extractPKValueFromExpression(pkExpr, eav, input.ExpressionAttributeNames)
	if pkValue == "" {
		return nil, false
	}

	if skDef.AttributeName != "" {
		if skMap, ok := table.pkskIndex[pkValue]; ok {
			indices := make([]int, 0, len(skMap))
			for _, idx := range skMap {
				indices = append(indices, idx)
			}

			candidates := db.filterUsingIndices(table, input, projection, indices, exprParts, eav)

			return candidates, true
		}
	} else if idx, ok := table.pkIndex[pkValue]; ok {
		indices := []int{idx}
		candidates := db.filterUsingIndices(table, input, projection, indices, exprParts, eav)

		return candidates, true
	}

	return nil, true // PK exists in schema but no items match it
}

func (db *InMemoryDB) filterUsingIndices(
	table *Table,
	input *dynamodb.QueryInput,
	_ *models.Projection,
	indices []int,
	exprParts []string,
	eav map[string]any,
) []map[string]any {
	candidates := make([]map[string]any, 0, len(indices))
	for _, idx := range indices {
		item := table.Items[idx]
		match := true
		for _, part := range exprParts {
			m, err := evaluateExpression(part, item, eav, input.ExpressionAttributeNames)
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
	parts := strings.Split(expression, "=")
	if len(parts) != expectedPKParts {
		return ""
	}

	return dbResolvePKTarget(parts[0], parts[1], attrNames, attrValues)
}

func dbResolvePKTarget(
	left, right string,
	attrNames map[string]string,
	attrValues map[string]any,
) string {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)

	lName := resolveAttrName(left, attrNames)
	rName := resolveAttrName(right, attrNames)

	if lName == "" && rName == "" {
		return ""
	}

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
	input *dynamodb.QueryInput,
	projection *models.Projection,
	keySchema []models.KeySchemaElement,
	exprParts []string,
	eav map[string]any,
) ([]map[string]any, error) {
	// naive scan filtering
	candidates := make([]map[string]any, 0, len(table.Items)/estimatedMatchRateDivisor)

	idxName := aws.ToString(input.IndexName)

	for _, item := range table.Items {
		match := true
		for _, part := range exprParts {
			m, err := evaluateExpression(part, item, eav, input.ExpressionAttributeNames)
			if err != nil || !m {
				match = false

				break
			}
		}

		if match {
			if idxName != "" {
				candidates = append(
					candidates,
					applyGSIProjection(item, *projection, table.KeySchema, keySchema),
				)
			} else {
				candidates = append(candidates, item)
			}
		}
	}

	return candidates, nil
}

func (db *InMemoryDB) sortCandidates(
	candidates []map[string]any,
	skDef models.KeySchemaElement,
	table *Table,
	scanIndexForward bool,
) {
	skType := getAttributeType(table.AttributeDefinitions, skDef.AttributeName, "")
	if skType == "" {
		skType = inferSKType(candidates, skDef.AttributeName)
	}
	if skType == "" {
		skType = "S"
	}

	sort.Slice(candidates, func(i, j int) bool {
		v1 := dynamoattr.UnwrapAttributeValue(candidates[i][skDef.AttributeName])
		v2 := dynamoattr.UnwrapAttributeValue(candidates[j][skDef.AttributeName])
		res := compareAny(v1, v2, skType)
		if !scanIndexForward {
			return res > 0
		}

		return res < 0
	})
}

func (db *InMemoryDB) processQueryResults(
	ctx context.Context,
	candidates []map[string]any,
	input *dynamodb.QueryInput,
	keySchema []models.KeySchemaElement,
	ttlAttr string,
) *dynamodb.QueryOutput {
	eav := models.FromSDKItem(input.ExpressionAttributeValues)
	exclusiveStartKey := models.FromSDKItem(input.ExclusiveStartKey)

	startIndex := findExclusiveStartIndex(candidates, exclusiveStartKey, keySchema)

	capacity := int(aws.ToInt32(input.Limit))
	if capacity == 0 || capacity > 100 {
		capacity = 100 // default or max page size for safety
	}
	items := make([]map[string]any, 0, capacity)

	var lastEvaluatedKey map[string]any
	limit := int(aws.ToInt32(input.Limit))
	count := 0

	for i := startIndex; i < len(candidates); i++ {
		if limit > 0 && count >= limit {
			lastEvaluatedKey = extractKey(items[len(items)-1], keySchema)

			break
		}

		item := candidates[i]
		if isItemExpired(item, ttlAttr) || !db.shouldIncludeInQuery(ctx, item, input, eav) {
			continue
		}

		processedItem := item
		proj := aws.ToString(input.ProjectionExpression)
		if proj != "" {
			processedItem = projectItem(item, proj, input.ExpressionAttributeNames)
		}

		items = append(items, processedItem)
		count++
	}

	// Prepare output
	outItems := make([]map[string]types.AttributeValue, len(items))
	for i, it := range items {
		sdkIt, _ := models.ToSDKItem(it)
		outItems[i] = sdkIt
	}

	out := &dynamodb.QueryOutput{
		Items:        outItems,
		Count:        int32(len(items)),      // #nosec G115
		ScannedCount: int32(len(candidates)), // #nosec G115
	}

	if lastEvaluatedKey != nil {
		out.LastEvaluatedKey, _ = models.ToSDKItem(lastEvaluatedKey)
	}

	return out
}

func (db *InMemoryDB) shouldIncludeInQuery(
	ctx context.Context,
	item map[string]any,
	input *dynamodb.QueryInput,
	eav map[string]any,
) bool {
	filter := aws.ToString(input.FilterExpression)
	if filter == "" {
		return true
	}

	log := logger.Load(ctx)
	log.DebugContext(ctx, "Evaluating Query FilterExpression",
		"expression", filter,
		"attributeNames", input.ExpressionAttributeNames,
		"attributeValues", input.ExpressionAttributeValues)

	match, err := evaluateExpression(
		filter,
		item,
		eav,
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

	return "S"
}
