package dynamodb

import (
	"encoding/json"
	"fmt"
)

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

	pkDef, skDef, err := db.getScanKeySchema(table, &input)
	if err != nil {
		return nil, err
	}

	items := db.doScan(table.Items, &input, pkDef, skDef, table.TTLAttribute)

	return ScanOutput{
		Items:        items,
		Count:        len(items),
		ScannedCount: len(table.Items),
	}, nil
}

func (db *InMemoryDB) getScanKeySchema(table *Table, input *ScanInput) (KeySchemaElement, KeySchemaElement, error) {
	if input.IndexName == "" {
		pk, sk := getPKAndSK(table.KeySchema)

		return pk, sk, nil
	}

	for _, gsi := range table.GlobalSecondaryIndexes {
		if gsi.IndexName == input.IndexName {
			pk, sk := getPKAndSK(gsi.KeySchema)

			return pk, sk, nil
		}
	}

	return KeySchemaElement{}, KeySchemaElement{}, NewResourceNotFoundException(fmt.Sprintf("Index: %s not found", input.IndexName))
}

func (db *InMemoryDB) doScan(
	items []map[string]any,
	input *ScanInput,
	pkDef, skDef KeySchemaElement,
	ttlAttr string,
) []map[string]any {
	result := make([]map[string]any, 0, minScanAllocationSize)
	for _, item := range items {
		if isItemExpired(item, ttlAttr) {
			continue
		}

		if db.shouldIncludeInScan(item, input, pkDef, skDef) {
			processedItem := item
			if input.ProjectionExpression != "" {
				processedItem = projectItem(item, input.ProjectionExpression, input.ExpressionAttributeNames)
			}
			result = append(result, processedItem)
		}

		if input.Limit != nil && int(*input.Limit) > 0 && len(result) >= int(*input.Limit) {
			break
		}
	}

	return result
}

func (db *InMemoryDB) shouldIncludeInScan(item map[string]any, input *ScanInput, pkDef, skDef KeySchemaElement) bool {
	// If it's a GSI scan, item MUST have the GSI's PK (and SK if defined)
	if input.IndexName != "" {
		if _, ok := item[pkDef.AttributeName]; !ok {
			return false
		}
		if skDef.AttributeName != "" {
			if _, ok := item[skDef.AttributeName]; !ok {
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
		if err != nil || !match {
			return false
		}
	}

	return true
}
