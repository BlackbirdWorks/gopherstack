package dynamodb

import (
	"context"
	"fmt"

	"Gopherstack/dynamodb/models"
	"Gopherstack/pkgs/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (db *InMemoryDB) Scan(ctx context.Context, input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	return db.ScanWithContext(ctx, input)
}

func (db *InMemoryDB) ScanWithContext(ctx context.Context, input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("scan cancelled: %w", ctx.Err())
	default:
	}

	tableName := aws.ToString(input.TableName)
	table, err := db.getTable(tableName)
	if err != nil {
		return nil, err
	}

	table.mu.RLock()
	defer table.mu.RUnlock()

	pkDef, skDef, err := db.getScanKeySchema(table, input)
	if err != nil {
		return nil, err
	}

	items := db.doScan(ctx, table.Items, input, pkDef, skDef, table.TTLAttribute)

	outItems := make([]map[string]types.AttributeValue, len(items))
	for i, it := range items {
		sdkIt, _ := models.ToSDKItem(it)
		outItems[i] = sdkIt
	}

	return &dynamodb.ScanOutput{
		Items:        outItems,
		Count:        int32(len(items)),       // #nosec G115
		ScannedCount: int32(len(table.Items)), // #nosec G115
	}, nil
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
	input *dynamodb.ScanInput,
	pkDef, skDef models.KeySchemaElement,
	ttlAttr string,
) []map[string]any {
	result := make([]map[string]any, 0, minScanAllocationSize)

	eav := models.FromSDKItem(input.ExpressionAttributeValues)
	limit := int(aws.ToInt32(input.Limit))

	proj := aws.ToString(input.ProjectionExpression)

	for _, item := range items {
		if isItemExpired(item, ttlAttr) {
			continue
		}

		if db.shouldIncludeInScan(ctx, item, input, pkDef, skDef, eav) {
			processedItem := item
			if proj != "" {
				processedItem = projectItem(item, proj, input.ExpressionAttributeNames)
			}
			result = append(result, processedItem)
		}

		if limit > 0 && len(result) >= limit {
			break
		}
	}

	return result
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
