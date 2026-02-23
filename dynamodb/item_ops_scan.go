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
	scannedCount := int32(len(table.Items)) // #nosec G115
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
	items, lastKey := db.doScan(ctx, itemsCopy, ttlAttr, snapshotTable, input, pkDef, skDef)

	outItems := make([]map[string]types.AttributeValue, len(items))
	for i, it := range items {
		sdkIt, _ := models.ToSDKItem(it)
		outItems[i] = sdkIt
	}

	out := &dynamodb.ScanOutput{
		Items:        outItems,
		Count:        int32(len(items)), // #nosec G115
		ScannedCount: scannedCount,
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
) ([]map[string]any, map[string]any) {
	eav := models.FromSDKItem(input.ExpressionAttributeValues)
	limit := int(aws.ToInt32(input.Limit))
	proj := aws.ToString(input.ProjectionExpression)

	// Collect all matching, non-expired items.
	candidate := make([]map[string]any, 0, minScanAllocationSize)

	for _, item := range items {
		if isItemExpired(item, ttlAttr) {
			continue
		}

		if db.shouldIncludeInScan(ctx, item, input, pkDef, skDef, eav) {
			candidate = append(candidate, item)
		}
	}

	// Sort candidate set by PK then SK (deterministic ordering for pagination).
	sortScanResults(candidate, pkDef, skDef, table)

	// Apply parallel-scan segment filter (Segment / TotalSegments).
	// Assign each item to a segment by hashing its PK string representation.
	totalSegments := int(aws.ToInt32(input.TotalSegments))
	segment := int(aws.ToInt32(input.Segment))

	if totalSegments > 1 {
		filtered := candidate[:0]
		for _, item := range candidate {
			pkVal := fmt.Sprintf("%v", dynamoattr.UnwrapAttributeValue(item[pkDef.AttributeName]))
			h := fnv.New32a()
			_, _ = h.Write([]byte(pkVal))
			if int(h.Sum32())%totalSegments == segment {
				filtered = append(filtered, item)
			}
		}
		candidate = filtered
	}

	// Apply ExclusiveStartKey: skip items up to and including the start-key item.
	candidate = applyExclusiveStartKey(candidate, input.ExclusiveStartKey, pkDef, skDef)

	// Apply limit and track LastEvaluatedKey.
	var lastKey map[string]any

	if limit > 0 && len(candidate) > limit {
		lastItem := candidate[limit-1]
		// Build LastEvaluatedKey from the last returned item's primary key attributes.
		lastKey = map[string]any{
			pkDef.AttributeName: lastItem[pkDef.AttributeName],
		}
		if skDef.AttributeName != "" {
			lastKey[skDef.AttributeName] = lastItem[skDef.AttributeName]
		}

		candidate = candidate[:limit]
	}

	// Apply projection after limit so we still have key attrs for LastEvaluatedKey.
	if proj != "" {
		for i, item := range candidate {
			candidate[i] = projectItem(item, proj, input.ExpressionAttributeNames)
		}
	}

	return candidate, lastKey
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

func (db *InMemoryDB) shouldIncludeInScan(
	ctx context.Context,
	item map[string]any,
	input *dynamodb.ScanInput,
	pkDef, skDef models.KeySchemaElement,
	eav map[string]any,
) bool {
	indexName := aws.ToString(input.IndexName)

	// If it's a GSI scan, item MUST have the GSI's PK (and SK if defined)
	// Actually, doScan iterates table.Items (all items).
	// If scanning a GSI, effectively we are filtering for items that have the GSI keys.
	// But local GSI implementation might store GSI data differently?
	// The current InMemoryDB implementation seems to store "Items" as the main table items.
	// Index lookups use `pkIndex` / `pkskIndex`.
	// For Scan, if we scan a GSI, we should ideally scan the GSI index or filter main items.
	// The existing logic checks if item has the keys.

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

	filter := aws.ToString(input.FilterExpression)
	if filter != "" {
		log := logger.Load(ctx)
		log.DebugContext(ctx, "Evaluating Scan FilterExpression",
			"expression", filter,
			"attributeNames", input.ExpressionAttributeNames,
			"attributeValues", input.ExpressionAttributeValues)

		match, err := evaluateExpression(
			filter,
			item,
			eav,
			input.ExpressionAttributeNames,
		)
		if err != nil || !match {
			return false
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

	startPK := fmt.Sprintf("%v", dynamoattr.UnwrapAttributeValue(startKey[pkName]))

	var startSK string
	if skName != "" {
		startSK = fmt.Sprintf("%v", dynamoattr.UnwrapAttributeValue(startKey[skName]))
	}

	skipUntil := -1

	for i, item := range candidate {
		itemPK := fmt.Sprintf("%v", dynamoattr.UnwrapAttributeValue(item[pkName]))
		if itemPK != startPK {
			continue
		}

		if skName == "" {
			skipUntil = i

			break
		}

		itemSK := fmt.Sprintf("%v", dynamoattr.UnwrapAttributeValue(item[skName]))
		if itemSK == startSK {
			skipUntil = i

			break
		}
	}

	if skipUntil >= 0 {
		return candidate[skipUntil+1:]
	}

	return candidate
}
