package dynamodb

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// consumedCapacityForQuery returns a populated ConsumedCapacity when the caller
// has requested capacity reporting. Returns nil when reporting is disabled.
func consumedCapacityForQuery(tableName string, req types.ReturnConsumedCapacity, scanned int) *types.ConsumedCapacity {
	if req == "" || req == types.ReturnConsumedCapacityNone {
		return nil
	}
	const halfRCU = 0.5
	cu := float64(scanned) * halfRCU
	if cu < halfRCU {
		cu = halfRCU
	}

	return &types.ConsumedCapacity{
		TableName:         aws.String(tableName),
		CapacityUnits:     aws.Float64(cu),
		ReadCapacityUnits: aws.Float64(cu),
	}
}

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
	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	idxName := aws.ToString(input.IndexName)

	// For primary-table queries, pre-parse the PK value from the expression
	// before taking the lock so we can do a targeted single-PK index copy
	// instead of copying the entire index (which may have hundreds of thousands of entries).
	precomputedPKValue := preParseQueryPKValue(input, idxName)

	// Snapshot table metadata and items under lock.
	// Items are shallow-copied (pointers only): writes always replace table.Items[i] with a
	// new map rather than mutating the old one in place, so our references remain safe.
	table.mu.RLock("Query")
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

	// Copy only the index entries we actually need:
	// - GSI/LSI queries never use the primary index, so skip it entirely.
	// - Primary-table queries with a known PK copy only that PK's entries.
	// - Primary-table queries with an unknown PK fall back to copying the full index.
	pkIndexCopy, pkskIndexCopy := db.snapshotIndexForQuery(
		table, idxName, precomputedPKValue,
	)
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

	keySchema, projection, err := db.extractKeySchema(snapshotTable, idxName)
	if err != nil {
		return nil, err
	}

	candidates, err := db.filterCandidatesForKeyCondition(
		ctx,
		snapshotTable,
		input,
		projection,
		keySchema,
	)
	if err != nil {
		return nil, err
	}

	// Enforce throughput: charge 0.5 RCU per scanned candidate (eventually-consistent).
	region := getRegionFromContext(ctx, db)
	if err = db.throttler.ConsumeRead(throttleKey(region, tableName), rcuForCount(len(candidates))); err != nil {
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

	if err := validateQueryKeyValues(exprParts, keySchema, eav, input.ExpressionAttributeNames); err != nil {
		return nil, err
	}

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
		if allExprPartsMatch(exprParts, item, eav, input.ExpressionAttributeNames) {
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
		if !allExprPartsMatch(exprParts, item, eav, input.ExpressionAttributeNames) {
			continue
		}

		if idxName != "" {
			candidates = append(
				candidates,
				applyGSIProjection(item, *projection, table.KeySchema, keySchema),
			)
		} else {
			candidates = append(candidates, item)
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

	items, lastEvaluatedKey := db.collectQueryPage(ctx, candidates, input, keySchema, ttlAttr, startIndex, eav)

	outItems := make([]map[string]types.AttributeValue, len(items))
	for i, it := range items {
		sdkIt, _ := models.ToSDKItem(it)
		outItems[i] = sdkIt
	}

	out := &dynamodb.QueryOutput{
		Items:        outItems,
		Count:        int32(len(items)),      // #nosec G115
		ScannedCount: int32(len(candidates)), // #nosec G115
		ConsumedCapacity: consumedCapacityForQuery(
			aws.ToString(input.TableName), input.ReturnConsumedCapacity, len(candidates),
		),
	}

	if lastEvaluatedKey != nil {
		out.LastEvaluatedKey, _ = models.ToSDKItem(lastEvaluatedKey)
	}

	return out
}

// collectQueryPage iterates candidates from startIndex, collecting items up to
// the input's Limit. Returns the collected items and the last-evaluated key for
// pagination if the limit was reached.
func (db *InMemoryDB) collectQueryPage(
	ctx context.Context,
	candidates []map[string]any,
	input *dynamodb.QueryInput,
	keySchema []models.KeySchemaElement,
	ttlAttr string,
	startIndex int,
	eav map[string]any,
) ([]map[string]any, map[string]any) {
	limit := int(aws.ToInt32(input.Limit))
	capacity := limit
	if capacity == 0 || capacity > 100 {
		capacity = 100 // default or max page size for safety
	}
	items := make([]map[string]any, 0, capacity)
	count := 0

	for i := startIndex; i < len(candidates); i++ {
		if limit > 0 && count >= limit {
			return items, extractKey(items[len(items)-1], keySchema)
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

	return items, nil
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

// allExprPartsMatch reports whether all expression parts evaluate to true for the given item.
func allExprPartsMatch(exprParts []string, item, eav map[string]any, exprAttrNames map[string]string) bool {
	for _, part := range exprParts {
		m, err := evaluateExpression(part, item, eav, exprAttrNames)
		if err != nil || !m {
			return false
		}
	}

	return true
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

// preParseQueryPKValue extracts the partition key value from a QueryInput's
// KeyConditionExpression before taking any lock. Returns "" when the PK value
// cannot be determined (unknown index, unparseable expression, etc.).
// Only operates on primary-table queries (idxName == "") because GSI/LSI
// queries do not use the primary index.
func preParseQueryPKValue(input *dynamodb.QueryInput, idxName string) string {
	if idxName != "" {
		return ""
	}

	eav := models.FromSDKItem(input.ExpressionAttributeValues)
	exprParts := dynamoattr.SplitANDConditions(aws.ToString(input.KeyConditionExpression))

	if len(exprParts) == 0 {
		return ""
	}

	pkExpr := strings.TrimSpace(exprParts[0])
	for strings.HasPrefix(pkExpr, "(") && strings.HasSuffix(pkExpr, ")") {
		pkExpr = strings.TrimSpace(pkExpr[1 : len(pkExpr)-1])
	}

	return extractPKValueFromExpression(pkExpr, eav, input.ExpressionAttributeNames)
}
