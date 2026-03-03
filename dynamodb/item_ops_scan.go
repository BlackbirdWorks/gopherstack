package dynamodb

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/blackbirdworks/gopherstack/dynamodb/models"
	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// consumedCapacityForScan returns a populated ConsumedCapacity when the caller
// has requested capacity reporting. Returns nil when reporting is disabled.
func consumedCapacityForScan(tableName string, req types.ReturnConsumedCapacity, n int) *types.ConsumedCapacity {
	if req == "" || req == types.ReturnConsumedCapacityNone {
		return nil
	}
	const halfRCU = 0.5 // each 4 KB read costs 0.5 RCU for eventually-consistent reads
	cu := float64(n) * halfRCU
	if cu < halfRCU {
		cu = halfRCU
	}

	return &types.ConsumedCapacity{
		TableName:         aws.String(tableName),
		CapacityUnits:     aws.Float64(cu),
		ReadCapacityUnits: aws.Float64(cu),
	}
}

func (db *InMemoryDB) Scan(
	ctx context.Context,
	input *dynamodb.ScanInput,
) (*dynamodb.ScanOutput, error) {
	return db.ScanWithContext(ctx, input)
}

func (db *InMemoryDB) ScanWithContext(
	ctx context.Context,
	input *dynamodb.ScanInput,
) (*dynamodb.ScanOutput, error) {
	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("scan cancelled: %w", ctx.Err())
	default:
	}

	tableName := aws.ToString(input.TableName)
	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	// Snapshot items and metadata under lock, release immediately
	table.mu.RLock("Scan")
	itemsCopy := make([]map[string]any, len(table.Items))
	for i, it := range table.Items {
		itemsCopy[i] = deepCopyItem(it)
	}
	ttlAttr := table.TTLAttribute
	keySchema := make([]models.KeySchemaElement, len(table.KeySchema))
	copy(keySchema, table.KeySchema)
	gsiList := make([]models.GlobalSecondaryIndex, len(table.GlobalSecondaryIndexes))
	copy(gsiList, table.GlobalSecondaryIndexes)
	lsiList := make([]models.LocalSecondaryIndex, len(table.LocalSecondaryIndexes))
	copy(lsiList, table.LocalSecondaryIndexes)
	attrDefs := make([]models.AttributeDefinition, len(table.AttributeDefinitions))
	copy(attrDefs, table.AttributeDefinitions)
	table.mu.RUnlock()

	// Get key schema definitions (reconstruct the table temporarily for getScanKeySchema)
	snapshotTable := &Table{
		KeySchema:              keySchema,
		GlobalSecondaryIndexes: gsiList,
		LocalSecondaryIndexes:  lsiList,
		AttributeDefinitions:   attrDefs,
	}

	pkDef, skDef, err := db.getScanKeySchema(snapshotTable, input)
	if err != nil {
		return nil, err
	}

	// Process scan outside the lock
	items, lastKey, scannedCount := db.doScan(ctx, itemsCopy, ttlAttr, snapshotTable, input, pkDef, skDef)

	outItems := make([]map[string]types.AttributeValue, len(items))
	for i, it := range items {
		sdkIt, _ := models.ToSDKItem(it)
		outItems[i] = sdkIt
	}

	out := &dynamodb.ScanOutput{
		Items:            outItems,
		Count:            int32(len(items)), // #nosec G115
		ScannedCount:     scannedCount,
		ConsumedCapacity: consumedCapacityForScan(tableName, input.ReturnConsumedCapacity, int(scannedCount)),
	}

	if lastKey != nil {
		sdkKey, _ := models.ToSDKItem(lastKey)
		out.LastEvaluatedKey = sdkKey
	}

	return out, nil
}

func (db *InMemoryDB) getScanKeySchema(
	table *Table,
	input *dynamodb.ScanInput,
) (models.KeySchemaElement, models.KeySchemaElement, error) {
	indexName := aws.ToString(input.IndexName)
	if indexName == "" {
		pk, sk := getPKAndSK(table.KeySchema)

		return pk, sk, nil
	}

	for _, gsi := range table.GlobalSecondaryIndexes {
		if gsi.IndexName == indexName {
			pk, sk := getPKAndSK(gsi.KeySchema)

			return pk, sk, nil
		}
	}

	for _, lsi := range table.LocalSecondaryIndexes {
		if lsi.IndexName == indexName {
			pk, sk := getPKAndSK(lsi.KeySchema)

			return pk, sk, nil
		}
	}

	return models.KeySchemaElement{}, models.KeySchemaElement{}, NewResourceNotFoundException(
		fmt.Sprintf("Index: %s not found", indexName),
	)
}

func (db *InMemoryDB) doScan(
	ctx context.Context,
	items []map[string]any,
	ttlAttr string,
	table *Table,
	input *dynamodb.ScanInput,
	pkDef, skDef models.KeySchemaElement,
) ([]map[string]any, map[string]any, int32) {
	eav := models.FromSDKItem(input.ExpressionAttributeValues)
	limit := int(aws.ToInt32(input.Limit))
	proj := aws.ToString(input.ProjectionExpression)

	// Collect all non-expired items that are in the target index (ignore FilterExpression here).
	candidate := make([]map[string]any, 0, minScanAllocationSize)

	for _, item := range items {
		if isItemExpired(item, ttlAttr) {
			continue
		}

		if isItemInIndex(item, input, pkDef, skDef) {
			candidate = append(candidate, item)
		}
	}

	// Sort candidate set by PK then SK (deterministic ordering for pagination).
	sortScanResults(candidate, pkDef, skDef, table)

	// Apply parallel-scan segment filter (Segment / TotalSegments).
	candidate = applySegmentFilter(candidate, input, pkDef)

	// Apply ExclusiveStartKey: skip items up to and including the start-key item.
	candidate = applyExclusiveStartKey(candidate, input.ExclusiveStartKey, pkDef, skDef)

	// Apply limit before FilterExpression (matches real DynamoDB semantics).
	// ScannedCount = items examined in this page; Count = items passing FilterExpression.
	candidate, lastKey := applyScanLimit(candidate, limit, pkDef, skDef)
	scannedCount := int32(len(candidate)) // #nosec G115

	// Apply FilterExpression to the scanned set.
	candidate = applyScanFilter(ctx, candidate, input, eav)

	// Apply projection after filtering so we still have key attrs for LastEvaluatedKey.
	if proj != "" {
		for i, item := range candidate {
			candidate[i] = projectItem(item, proj, input.ExpressionAttributeNames)
		}
	}

	return candidate, lastKey, scannedCount
}

// applySegmentFilter partitions items by parallel scan segment using FNV hash on PK.
func applySegmentFilter(
	candidate []map[string]any,
	input *dynamodb.ScanInput,
	pkDef models.KeySchemaElement,
) []map[string]any {
	totalSegments := int(aws.ToInt32(input.TotalSegments))
	if totalSegments <= 1 {
		return candidate
	}

	segment := int(aws.ToInt32(input.Segment))
	filtered := candidate[:0]

	for _, item := range candidate {
		pkVal := fmt.Sprintf("%v", dynamoattr.UnwrapAttributeValue(item[pkDef.AttributeName]))
		h := fnv.New32a()
		_, _ = h.Write([]byte(pkVal))
		if int(h.Sum32())%totalSegments == segment {
			filtered = append(filtered, item)
		}
	}

	return filtered
}

// applyScanLimit truncates the candidate set to limit items and returns the
// last-evaluated key if more items exist beyond the page boundary.
func applyScanLimit(
	candidate []map[string]any,
	limit int,
	pkDef, skDef models.KeySchemaElement,
) ([]map[string]any, map[string]any) {
	if limit <= 0 || len(candidate) <= limit {
		return candidate, nil
	}

	lastItem := candidate[limit-1]
	lastKey := map[string]any{pkDef.AttributeName: lastItem[pkDef.AttributeName]}
	if skDef.AttributeName != "" {
		lastKey[skDef.AttributeName] = lastItem[skDef.AttributeName]
	}

	return candidate[:limit], lastKey
}

// applyScanFilter applies the FilterExpression to the candidate set and returns only matching items.
func applyScanFilter(
	ctx context.Context,
	candidate []map[string]any,
	input *dynamodb.ScanInput,
	eav map[string]any,
) []map[string]any {
	filter := aws.ToString(input.FilterExpression)
	if filter == "" {
		return candidate
	}

	log := logger.Load(ctx)
	log.DebugContext(ctx, "Evaluating Scan FilterExpression",
		"expression", filter,
		"attributeNames", input.ExpressionAttributeNames,
		"attributeValues", input.ExpressionAttributeValues)

	retained := candidate[:0]
	for _, item := range candidate {
		match, err := evaluateExpression(filter, item, eav, input.ExpressionAttributeNames)
		if err == nil && match {
			retained = append(retained, item)
		}
	}

	return retained
}

func sortScanResults(
	items []map[string]any,
	pkDef, skDef models.KeySchemaElement,
	table *Table,
) {
	pkType := getAttributeType(table.AttributeDefinitions, pkDef.AttributeName, "S")
	var skType string
	if skDef.AttributeName != "" {
		skType = getAttributeType(table.AttributeDefinitions, skDef.AttributeName, "S")
	}

	sort.Slice(items, func(i, j int) bool {
		v1pk := dynamoattr.UnwrapAttributeValue(items[i][pkDef.AttributeName])
		v2pk := dynamoattr.UnwrapAttributeValue(items[j][pkDef.AttributeName])
		pkRes := compareAny(v1pk, v2pk, pkType)
		if pkRes != 0 {
			return pkRes < 0
		}

		if skDef.AttributeName != "" {
			v1sk := dynamoattr.UnwrapAttributeValue(items[i][skDef.AttributeName])
			v2sk := dynamoattr.UnwrapAttributeValue(items[j][skDef.AttributeName])
			skRes := compareAny(v1sk, v2sk, skType)

			return skRes < 0
		}

		return false
	})
}

// isItemInIndex reports whether item should be included in the scan based solely
// on index membership (i.e. whether the item has the required index keys).
// FilterExpression is intentionally NOT evaluated here so that Limit applies
// before filtering, matching real DynamoDB semantics.
func isItemInIndex(
	item map[string]any,
	input *dynamodb.ScanInput,
	pkDef, skDef models.KeySchemaElement,
) bool {
	indexName := aws.ToString(input.IndexName)

	// If it's a GSI scan, item MUST have the GSI's PK (and SK if defined)
	if indexName != "" {
		if _, ok := item[pkDef.AttributeName]; !ok {
			return false
		}
		if skDef.AttributeName != "" {
			if _, ok := item[skDef.AttributeName]; !ok {
				return false
			}
		}
	}

	return true
}

func applyExclusiveStartKey(
	candidate []map[string]any,
	exclusiveStartKey map[string]types.AttributeValue,
	pkDef, skDef models.KeySchemaElement,
) []map[string]any {
	if len(exclusiveStartKey) == 0 {
		return candidate
	}

	startKey := models.FromSDKItem(exclusiveStartKey)
	pkName := pkDef.AttributeName
	skName := skDef.AttributeName

	for i, item := range candidate {
		if !compareAttributeValues(item[pkName], startKey[pkName]) {
			continue
		}

		if skName == "" {
			return candidate[i+1:]
		}

		if compareAttributeValues(item[skName], startKey[skName]) {
			return candidate[i+1:]
		}
	}

	return candidate
}
