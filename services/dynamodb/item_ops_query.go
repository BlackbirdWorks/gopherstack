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
func consumedCapacityForQuery(
	tableName string,
	req types.ReturnConsumedCapacity,
	scanned int,
	consistentRead bool,
) *types.ConsumedCapacity {
	if req == "" || req == types.ReturnConsumedCapacityNone {
		return nil
	}
	const halfRCU = 0.5
	cu := float64(scanned) * halfRCU
	if cu < halfRCU {
		cu = halfRCU
	}
	// Strongly-consistent reads cost twice as much as eventually-consistent ones.
	cu = applyConsistentReadMultiplier(cu, consistentRead)

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

	if err := validatePositiveLimit(input.Limit); err != nil {
		return nil, err
	}

	tableName := aws.ToString(input.TableName)
	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	idxName := aws.ToString(input.IndexName)

	// Pre-parse PK value before locking so we can do a targeted index copy.
	precomputedPKValue := preParseQueryPKValue(input, idxName)
	snapshotTable, billingMode, ttlAttr := db.snapshotTableForQuery(
		table, idxName, precomputedPKValue,
	)

	keySchema, projection, err := db.extractKeySchema(
		snapshotTable,
		idxName,
		aws.ToBool(input.ConsistentRead),
	)
	if err != nil {
		return nil, err
	}

	if verr := validateSelectConstraints(
		input.Select, idxName, projection,
		aws.ToString(input.ProjectionExpression), input.AttributesToGet,
	); verr != nil {
		return nil, verr
	}

	candidates, err := db.filterCandidatesForKeyCondition(
		ctx, snapshotTable, input, projection, keySchema,
	)
	if err != nil {
		return nil, err
	}

	// Enforce throughput: charge RCU per scanned candidate.
	region := getRegionFromContext(ctx, db)
	consistentRead := aws.ToBool(input.ConsistentRead)
	rcuUnits := applyConsistentReadMultiplier(rcuForCount(len(candidates)), consistentRead)

	if !isOnDemandTable(billingMode) {
		if err = db.throttler.ConsumeRead(throttleKey(region, tableName), rcuUnits); err != nil {
			return nil, err
		}
	}

	_, skDef := getPKAndSK(keySchema)
	sortForward := input.ScanIndexForward == nil || *input.ScanIndexForward

	if skDef.AttributeName != "" {
		db.sortCandidates(candidates, skDef, snapshotTable, sortForward)
	}

	return db.processQueryResults(
		ctx, candidates, input, keySchema, snapshotTable.KeySchema, ttlAttr,
	), nil
}

// snapshotTableForQuery snapshots table metadata and items under lock, releasing
// the lock before returning. Returns the snapshot Table, billing mode, and TTL attr.
func (db *InMemoryDB) snapshotTableForQuery(
	table *Table,
	idxName, precomputedPKValue string,
) (*Table, string, string) {
	// Items are shallow-copied (pointers only): writes always replace table.Items[i]
	// with a new map rather than mutating the old one in place.
	table.mu.RLock("Query")

	keySchemaOrig := make([]models.KeySchemaElement, len(table.KeySchema))
	copy(keySchemaOrig, table.KeySchema)
	gsiList := make([]models.GlobalSecondaryIndex, len(table.GlobalSecondaryIndexes))
	copy(gsiList, table.GlobalSecondaryIndexes)
	lsiList := make([]models.LocalSecondaryIndex, len(table.LocalSecondaryIndexes))
	copy(lsiList, table.LocalSecondaryIndexes)
	attrDefs := make([]models.AttributeDefinition, len(table.AttributeDefinitions))
	copy(attrDefs, table.AttributeDefinitions)
	ttlAttr := table.TTLAttribute
	billingMode := table.BillingMode

	// Copy only the index entries we actually need (#57).
	pkIndexCopy, pkskIndexCopy := db.snapshotIndexForQuery(table, idxName, precomputedPKValue)

	var itemsCopy []map[string]any
	var itemsByOffset map[int]map[string]any

	if idxName == "" && precomputedPKValue != "" {
		itemsByOffset = snapshotItemsByOffset(table, pkIndexCopy, pkskIndexCopy)
	} else {
		itemsCopy = make([]map[string]any, len(table.Items))
		copy(itemsCopy, table.Items)
	}

	table.mu.RUnlock()

	snapshotTable := &Table{
		Items:                  itemsCopy,
		itemsByOffset:          itemsByOffset,
		KeySchema:              keySchemaOrig,
		GlobalSecondaryIndexes: gsiList,
		LocalSecondaryIndexes:  lsiList,
		AttributeDefinitions:   attrDefs,
		TTLAttribute:           ttlAttr,
		pkIndex:                pkIndexCopy,
		pkskIndex:              pkskIndexCopy,
	}

	return snapshotTable, billingMode, ttlAttr
}

func (db *InMemoryDB) extractKeySchema(
	table *Table,
	indexName string,
	consistentRead bool,
) ([]models.KeySchemaElement, *models.Projection, error) {
	if indexName == "" {
		return table.KeySchema, nil, nil
	}

	for _, gsi := range table.GlobalSecondaryIndexes {
		if gsi.IndexName == indexName {
			// A GSI is eventually consistent; AWS rejects ConsistentRead=true on a GSI.
			if consistentRead {
				return nil, nil, NewValidationException(
					"Consistent reads are not supported on global secondary indexes",
				)
			}

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

	// Parse all condition parts once
	parsedParts := make([]*ParsedCondition, 0, len(exprParts))
	for _, part := range exprParts {
		pc, err := ParseConditionStr(part)
		if err != nil {
			return nil, err
		}
		parsedParts = append(parsedParts, pc)
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
			parsedParts,
			eav,
		)
		if ok {
			return candidates, nil
		}
	}

	return db.filterCandidatesScan(table, input, projection, keySchema, parsedParts, eav)
}

func (db *InMemoryDB) tryFilterUsingAuthoritativeIndex(
	table *Table,
	input *dynamodb.QueryInput,
	projection *models.Projection,
	_ []models.KeySchemaElement,
	pkExpr string,
	_ models.KeySchemaElement,
	skDef models.KeySchemaElement,
	exprParts []*ParsedCondition,
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
	exprParts []*ParsedCondition,
	eav map[string]any,
) []map[string]any {
	candidates := make([]map[string]any, 0, len(indices))
	for _, idx := range indices {
		// #57: prefer itemsByOffset snapshot (set for known-PK queries) to avoid
		// touching the full Items slice.
		var item map[string]any
		if table.itemsByOffset != nil {
			item = table.itemsByOffset[idx]
		} else {
			item = table.Items[idx]
		}

		if item == nil {
			continue
		}

		if allParsedExprPartsMatch(exprParts, item, eav, input.ExpressionAttributeNames) {
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
	exprParts []*ParsedCondition,
	eav map[string]any,
) ([]map[string]any, error) {
	// naive scan filtering
	candidates := make([]map[string]any, 0, len(table.Items)/estimatedMatchRateDivisor)

	idxName := aws.ToString(input.IndexName)

	for _, item := range table.Items {
		if !allParsedExprPartsMatch(exprParts, item, eav, input.ExpressionAttributeNames) {
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
	tableKeySchema []models.KeySchemaElement,
	ttlAttr string,
) *dynamodb.QueryOutput {
	eav := models.FromSDKItem(input.ExpressionAttributeValues)
	exclusiveStartKey := models.FromSDKItem(input.ExclusiveStartKey)

	startIndex := findExclusiveStartIndex(candidates, exclusiveStartKey, keySchema, tableKeySchema)

	items, lastEvaluatedKey, scannedCount := db.collectQueryPage(
		ctx,
		candidates,
		input,
		keySchema,
		tableKeySchema,
		ttlAttr,
		startIndex,
		eav,
	)

	// AWS omits Items entirely when Select=COUNT: "Returns the number of matching
	// items, rather than the matching items themselves." Count/ScannedCount still
	// reflect the matched/scanned totals.
	var outItems []map[string]types.AttributeValue
	if input.Select != types.SelectCount {
		outItems = make([]map[string]types.AttributeValue, len(items))
		for i, it := range items {
			sdkIt, _ := models.ToSDKItem(it)
			outItems[i] = sdkIt
		}
	}

	out := &dynamodb.QueryOutput{
		Items:        outItems,
		Count:        int32(len(items)),   // #nosec G115
		ScannedCount: int32(scannedCount), // #nosec G115
		ConsumedCapacity: consumedCapacityForQuery(
			aws.ToString(input.TableName), input.ReturnConsumedCapacity, scannedCount,
			aws.ToBool(input.ConsistentRead),
		),
	}

	if lastEvaluatedKey != nil {
		out.LastEvaluatedKey, _ = models.ToSDKItem(lastEvaluatedKey)
	}

	return out
}

// collectQueryPage iterates candidates from startIndex, collecting items up to
// the input's Limit or 1MB size limit. Returns the collected items, the
// last-evaluated key for pagination, and the total number of items scanned.
// tableKeySchema is the base table's primary key schema; when querying a
// GSI/LSI it is used to include the table PK in LastEvaluatedKey so that
// pagination tokens are unambiguous (matching AWS behaviour).
func (db *InMemoryDB) collectQueryPage(
	_ context.Context,
	candidates []map[string]any,
	input *dynamodb.QueryInput,
	keySchema []models.KeySchemaElement,
	tableKeySchema []models.KeySchemaElement,
	ttlAttr string,
	startIndex int,
	eav map[string]any,
) ([]map[string]any, map[string]any, int) {
	limit := int(aws.ToInt32(input.Limit))

	projector, _ := ParseProjector(
		aws.ToString(input.ProjectionExpression),
		input.ExpressionAttributeNames,
	)

	// Pre-parse the filter expression once to avoid per-item re-lexing overhead.
	parsedFilter, _ := ParseConditionStr(aws.ToString(input.FilterExpression))

	const maxResponseSize = 1024 * 1024 // 1MB
	items := make([]map[string]any, 0)
	totalScannedSize := 0
	scannedCount := 0

	for i := startIndex; i < len(candidates); i++ {
		scannedCount++
		item := candidates[i]
		itemSize, _ := CalculateItemSize(item)

		if totalScannedSize+itemSize > maxResponseSize && len(items) > 0 {
			prevItem := candidates[i-1]

			return items, extractKeyWithBase(prevItem, keySchema, tableKeySchema), scannedCount - 1
		}
		totalScannedSize += itemSize

		if !isItemExpired(item, ttlAttr) &&
			parsedFilter.Evaluate(item, eav, input.ExpressionAttributeNames) {
			items = append(items, projector.Project(item))
		}

		if limit > 0 && scannedCount >= limit {
			return items, extractKeyWithBase(item, keySchema, tableKeySchema), scannedCount
		}

		if totalScannedSize >= maxResponseSize {
			return items, extractKeyWithBase(item, keySchema, tableKeySchema), scannedCount
		}
	}

	return items, nil, scannedCount
}

// allExprPartsMatch reports whether all expression parts evaluate to true for the given item.
// allParsedExprPartsMatch reports whether all pre-parsed expression parts evaluate to true.
func allParsedExprPartsMatch(
	exprParts []*ParsedCondition,
	item, eav map[string]any,
	exprAttrNames map[string]string,
) bool {
	for _, part := range exprParts {
		if !part.Evaluate(item, eav, exprAttrNames) {
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
