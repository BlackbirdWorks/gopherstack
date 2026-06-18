package dynamodb

import (
	"context"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (db *InMemoryDB) PutItem(
	ctx context.Context,
	input *dynamodb.PutItemInput,
) (*dynamodb.PutItemOutput, error) {
	tableName := aws.ToString(input.TableName)
	if tableName == "" {
		return nil, NewValidationException("Table name is required")
	}

	if err := validatePutDeleteReturnValues(input.ReturnValues); err != nil {
		return nil, err
	}

	if err := validateExpressionAttributeNames(input.ExpressionAttributeNames); err != nil {
		return nil, err
	}

	condExpr := aws.ToString(input.ConditionExpression)
	if err := checkUnusedExpressionAttributeNames(input.ExpressionAttributeNames, condExpr); err != nil {
		return nil, err
	}

	wireEAV := models.FromSDKItem(input.ExpressionAttributeValues)
	if err := checkUnusedExpressionAttributeValues(wireEAV, condExpr); err != nil {
		return nil, err
	}

	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	// Convert SDK Item to Wire Item once; reused for validation and WCU calculation.
	wireItem := models.FromSDKItem(input.Item)

	table.mu.Lock("PutItem")

	// Validate item before charging capacity so that validation errors do not
	// consume tokens (matches real DynamoDB behaviour).
	err = db.validateItem(wireItem, table)
	if err != nil {
		table.mu.Unlock()

		return nil, err
	}

	// Enforce throughput after validation, before mutating state.
	// PAY_PER_REQUEST tables bypass throttling.
	wcu := WriteCapacityUnits(wireItem)
	region := getRegionFromContext(ctx, db)

	if !isOnDemandTable(table.BillingMode) {
		if throttleErr := db.throttler.ConsumeWrite(throttleKey(region, tableName), wcu); throttleErr != nil {
			table.mu.Unlock()

			return nil, throttleErr
		}
	}

	oldItem, matchIndex := db.findMatchForPut(table, wireItem)
	err = db.checkPutCondition(ctx, input, oldItem)
	if err != nil {
		table.mu.Unlock()

		return nil, err
	}

	// Enforce LSI 10 GB per-collection limit before mutating state.
	lsiCollectionBytes, lsiErr := db.checkLSICollectionSize(table, wireItem, matchIndex)
	if lsiErr != nil {
		table.mu.Unlock()

		return nil, lsiErr
	}

	db.doPut(table, wireItem, matchIndex)

	// Capture stream event
	if matchIndex != -1 {
		table.appendStreamRecord(streamEventModify, oldItem, deepCopyItem(wireItem))
	} else {
		table.appendStreamRecord(streamEventInsert, nil, deepCopyItem(wireItem))
	}

	globalTableName := table.GlobalTableName
	out := db.populatePutItemOutput(input, table, oldItem, lsiCollectionBytes)

	table.mu.Unlock()

	// Propagate to global table replicas after releasing the primary lock.
	if globalTableName != "" {
		db.replicateItemMutation(tableName, globalTableName, region, deepCopyItem(wireItem), "PUT")
	}

	return out, nil
}

func (db *InMemoryDB) findMatchForPut(table *Table, item map[string]any) (map[string]any, int) {
	pkDef, skDef := getPKAndSK(table.KeySchema)
	pkVal := BuildKeyString(item, pkDef.AttributeName)

	if skDef.AttributeName != "" {
		skVal := BuildKeyString(item, skDef.AttributeName)
		if skMap, ok := table.pkskIndex[pkVal]; ok {
			if idx, okIdx := skMap[skVal]; okIdx {
				return table.Items[idx], idx
			}
		}
	} else if idx, ok := table.pkIndex[pkVal]; ok {
		return table.Items[idx], idx
	}

	return nil, -1
}

func (db *InMemoryDB) checkPutCondition(
	ctx context.Context,
	input *dynamodb.PutItemInput,
	oldItem map[string]any,
) error {
	condition := aws.ToString(input.ConditionExpression)
	if condition == "" {
		return nil
	}

	log := logger.Load(ctx)
	log.DebugContext(ctx, "Evaluating PutItem condition",
		"expression", condition,
		"attributeNames", input.ExpressionAttributeNames,
		"attributeValues", input.ExpressionAttributeValues)

	// Convert EAV to Wire format for evaluator
	eav := models.FromSDKItem(input.ExpressionAttributeValues)

	match, err := evaluateExpression(
		condition,
		oldItem,
		eav,
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

func (db *InMemoryDB) doPut(table *Table, item map[string]any, matchIndex int) {
	if matchIndex != -1 {
		table.Items[matchIndex] = item
		db.updateIndexes(table, item, matchIndex)
	} else {
		idx := len(table.Items)
		table.Items = append(table.Items, item)
		db.updateIndexes(table, item, idx)
	}
}

// bytesPerGB is the number of bytes in one gibibyte.
const bytesPerGB = 1024 * 1024 * 1024

// lsiMaxCollectionBytes is the AWS-imposed 10 GB per-collection limit for LSI tables.
const lsiMaxCollectionBytes = 10 * bytesPerGB

// checkLSICollectionSize enforces the 10 GB per-collection limit for tables with LSIs.
// Returns (collectionBytes, nil) when the limit is not exceeded, (-1, nil) for non-LSI
// tables, and (-1, error) when the limit would be exceeded. Must be called under table.mu.
func (db *InMemoryDB) checkLSICollectionSize(table *Table, newItem map[string]any, oldMatchIndex int) (int64, error) {
	if len(table.LocalSecondaryIndexes) == 0 {
		return -1, nil
	}

	pkDef, _ := getPKAndSK(table.KeySchema)
	pkVal := BuildKeyString(newItem, pkDef.AttributeName)
	size := computeLSICollectionSize(table, pkVal, newItem, oldMatchIndex)

	if size > lsiMaxCollectionBytes {
		return -1, NewItemCollectionSizeLimitExceededException(
			"Item collection size limit exceeded; largest item collection has size " +
				formatGB(size) + " GB")
	}

	return size, nil
}

// computeLSICollectionSize returns the projected total byte size of all items sharing
// pkVal as their partition key, as if newItem replaces the item at oldMatchIndex (or
// is appended when oldMatchIndex == -1). Must be called under table.mu held.
func computeLSICollectionSize(table *Table, pkVal string, newItem map[string]any, oldMatchIndex int) int64 {
	var total int64

	if skMap, ok := table.pkskIndex[pkVal]; ok {
		for _, offset := range skMap {
			sz, _ := CalculateItemSize(table.Items[offset])
			total += int64(sz)
		}
	} else if offset, ok2 := table.pkIndex[pkVal]; ok2 {
		sz, _ := CalculateItemSize(table.Items[offset])
		total += int64(sz)
	}

	// Subtract old item (it will be replaced).
	if oldMatchIndex != -1 {
		sz, _ := CalculateItemSize(table.Items[oldMatchIndex])
		total -= int64(sz)
	}

	// Add new item.
	sz, _ := CalculateItemSize(newItem)
	total += int64(sz)

	return total
}

// collectionBytesToGB converts a byte count to GB, returning 0 for negative values.
func collectionBytesToGB(bytes int64) float64 {
	if bytes <= 0 {
		return 0
	}

	return float64(bytes) / bytesPerGB
}

// formatGB formats a byte count as a GB string with two decimal places.
func formatGB(bytes int64) string {
	return fmt.Sprintf("%.2f", collectionBytesToGB(bytes))
}

func (db *InMemoryDB) validateItem(item map[string]any, table *Table) error {
	if err := validateAttributeNames(item); err != nil {
		return err
	}

	if err := ValidateDataTypes(item); err != nil {
		return err
	}

	if err := ValidateItemSize(item); err != nil {
		return err
	}

	return validateKeySchema(item, table.KeySchema)
}

func (db *InMemoryDB) populatePutItemOutput(
	input *dynamodb.PutItemInput,
	table *Table,
	oldItem map[string]any,
	lsiCollectionBytes int64,
) *dynamodb.PutItemOutput {
	out := &dynamodb.PutItemOutput{}

	// Simplify ReturnValues: supporting ALL_OLD mostly
	if input.ReturnValues == models.ReturnValuesAllOld && oldItem != nil {
		out.Attributes, _ = models.ToSDKItem(oldItem)
	}

	// Handle ConsumedCapacity. WCU on a write is ceil(item-size / 1 KB),
	// minimum 1 — same formula AWS uses, computed from the wire-item we just
	// put.
	if input.ReturnConsumedCapacity != "" &&
		input.ReturnConsumedCapacity != types.ReturnConsumedCapacityNone {
		writeUnits := WriteCapacityUnits(models.FromSDKItem(input.Item))
		out.ConsumedCapacity = &types.ConsumedCapacity{
			TableName:          aws.String(table.Name),
			CapacityUnits:      aws.Float64(writeUnits),
			WriteCapacityUnits: aws.Float64(writeUnits),
		}
	}

	// ItemCollectionMetrics: only for tables with LSI and when requested.
	// ItemCollectionKey contains only the partition key attribute (not the full item).
	if input.ReturnItemCollectionMetrics != "" &&
		input.ReturnItemCollectionMetrics != types.ReturnItemCollectionMetricsNone {
		pkDef, _ := getPKAndSK(table.KeySchema)
		pkKey := map[string]types.AttributeValue{
			pkDef.AttributeName: input.Item[pkDef.AttributeName],
		}
		sizeGB := collectionBytesToGB(lsiCollectionBytes)
		out.ItemCollectionMetrics = &types.ItemCollectionMetrics{
			ItemCollectionKey:   pkKey,
			SizeEstimateRangeGB: []float64{sizeGB, sizeGB},
		}
	}

	return out
}

func (db *InMemoryDB) GetItem(
	ctx context.Context,
	input *dynamodb.GetItemInput,
) (*dynamodb.GetItemOutput, error) {
	tableName := aws.ToString(input.TableName)
	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	// Validate projection params before taking any lock.
	projExpr := aws.ToString(input.ProjectionExpression)
	if projErr := validateProjectionParams(projExpr, input.AttributesToGet); projErr != nil {
		return nil, projErr
	}

	table.mu.RLock("GetItem")
	defer table.mu.RUnlock()

	wireKey := models.FromSDKItem(input.Key)
	err = validateKeySchema(wireKey, table.KeySchema)
	if err != nil {
		return nil, err
	}

	// Enforce throughput after key validation so that invalid requests do not
	// consume tokens. Double RCU for strongly-consistent reads.
	consistentRead := aws.ToBool(input.ConsistentRead)
	rcu := applyConsistentReadMultiplier(models.ConsumedReadUnit, consistentRead)
	region := getRegionFromContext(ctx, db)

	if !isOnDemandTable(table.BillingMode) {
		if throttleErr := db.throttler.ConsumeRead(
			throttleKey(region, tableName),
			rcu,
		); throttleErr != nil {
			return nil, throttleErr
		}
	}

	pkDef, skDef := getPKAndSK(table.KeySchema)
	item := db.lookupItem(table, wireKey, pkDef.AttributeName, skDef.AttributeName)

	if item == nil || isItemExpired(item, table.TTLAttribute) {
		return &dynamodb.GetItemOutput{}, nil
	}

	// Resolve effective projection (fallback to AttributesToGet).
	effectiveProj := resolveProjection(projExpr, input.AttributesToGet)
	result := item

	if effectiveProj != "" {
		result = projectItem(item, effectiveProj, input.ExpressionAttributeNames)
	}

	sdkItem, err := models.ToSDKItem(result)
	if err != nil {
		return nil, err
	}

	out := &dynamodb.GetItemOutput{Item: sdkItem}
	if input.ReturnConsumedCapacity != "" &&
		input.ReturnConsumedCapacity != types.ReturnConsumedCapacityNone {
		// RCU on a read is ceil(item-size / 4 KB) * 0.5 (eventually consistent)
		// or doubled when ConsistentRead=true. Matches the real AWS formula.
		readUnits := applyConsistentReadMultiplier(ReadCapacityUnits(item), consistentRead)
		out.ConsumedCapacity = &types.ConsumedCapacity{
			TableName:         aws.String(table.Name),
			CapacityUnits:     aws.Float64(readUnits),
			ReadCapacityUnits: aws.Float64(readUnits),
		}
	}

	return out, nil
}

func (db *InMemoryDB) DeleteItem(
	ctx context.Context,
	input *dynamodb.DeleteItemInput,
) (*dynamodb.DeleteItemOutput, error) {
	tableName := aws.ToString(input.TableName)

	if err := validatePutDeleteReturnValues(input.ReturnValues); err != nil {
		return nil, err
	}

	if err := validateExpressionAttributeNames(input.ExpressionAttributeNames); err != nil {
		return nil, err
	}

	condExpr := aws.ToString(input.ConditionExpression)
	if err := checkUnusedExpressionAttributeNames(input.ExpressionAttributeNames, condExpr); err != nil {
		return nil, err
	}

	wireEAV := models.FromSDKItem(input.ExpressionAttributeValues)
	if err := checkUnusedExpressionAttributeValues(wireEAV, condExpr); err != nil {
		return nil, err
	}

	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	table.mu.Lock("DeleteItem")

	wireKey := models.FromSDKItem(input.Key)
	err = validateKeySchema(wireKey, table.KeySchema)
	if err != nil {
		table.mu.Unlock()

		return nil, err
	}

	region := getRegionFromContext(ctx, db)

	pkDef, skDef := getPKAndSK(table.KeySchema)

	// Get item and index in one lookup (avoids duplicate index lookup)
	oldItem, matchIndex := db.lookupItemWithIndex(
		table,
		wireKey,
		pkDef.AttributeName,
		skDef.AttributeName,
	)

	// Enforce throughput after key validation so that invalid requests do not
	// consume tokens. PAY_PER_REQUEST tables bypass throttling. DeleteItem
	// consumes WCUs proportional to the size of the deleted item (min 1).
	if !isOnDemandTable(table.BillingMode) {
		if throttleErr := db.throttler.ConsumeWrite(
			throttleKey(region, tableName), WriteCapacityUnits(oldItem),
		); throttleErr != nil {
			table.mu.Unlock()

			return nil, throttleErr
		}
	}

	if err = db.checkDeleteCondition(ctx, input, oldItem); err != nil {
		table.mu.Unlock()

		return nil, err
	}

	if oldItem != nil && matchIndex != -1 {
		db.deleteItemAtIndex(table, matchIndex)
		// Capture stream REMOVE event
		table.appendStreamRecord(streamEventRemove, deepCopyItem(oldItem), nil)
	}

	out := db.buildDeleteItemOutput(input, table, oldItem)
	globalTableName := table.GlobalTableName

	table.mu.Unlock()

	// Propagate deletion to global table replicas after releasing the primary lock.
	if globalTableName != "" && oldItem != nil {
		db.replicateItemMutation(tableName, globalTableName, region, deepCopyItem(wireKey), "DELETE")
	}

	return out, nil
}

func (db *InMemoryDB) checkDeleteCondition(
	ctx context.Context,
	input *dynamodb.DeleteItemInput,
	oldItem map[string]any,
) error {
	condition := aws.ToString(input.ConditionExpression)
	if condition == "" {
		return nil
	}

	log := logger.Load(ctx)
	log.DebugContext(ctx, "Evaluating DeleteItem condition",
		"expression", condition,
		"attributeNames", input.ExpressionAttributeNames,
		"attributeValues", input.ExpressionAttributeValues)

	eav := models.FromSDKItem(input.ExpressionAttributeValues)

	match, err := evaluateExpression(
		condition,
		oldItem,
		eav,
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

func (db *InMemoryDB) buildDeleteItemOutput(
	input *dynamodb.DeleteItemInput,
	table *Table,
	oldItem map[string]any,
) *dynamodb.DeleteItemOutput {
	out := &dynamodb.DeleteItemOutput{}

	if input.ReturnValues == models.ReturnValuesAllOld && oldItem != nil {
		out.Attributes, _ = models.ToSDKItem(oldItem)
	}

	if input.ReturnConsumedCapacity != "" &&
		input.ReturnConsumedCapacity != types.ReturnConsumedCapacityNone {
		// WCU on a delete is ceil(deleted-item-size / 1 KB), minimum 1. If the
		// row didn't exist, AWS still bills the 1-WCU floor for the tombstone
		// write — matching that here keeps cost projections accurate.
		writeUnits := 1.0
		if oldItem != nil {
			writeUnits = WriteCapacityUnits(oldItem)
		}
		out.ConsumedCapacity = &types.ConsumedCapacity{
			TableName:          aws.String(table.Name),
			CapacityUnits:      aws.Float64(writeUnits),
			WriteCapacityUnits: aws.Float64(writeUnits),
		}
	}

	if input.ReturnItemCollectionMetrics != "" &&
		input.ReturnItemCollectionMetrics != types.ReturnItemCollectionMetricsNone {
		out.ItemCollectionMetrics = &types.ItemCollectionMetrics{
			ItemCollectionKey:   input.Key,
			SizeEstimateRangeGB: []float64{0.0, 1.0},
		}
	}

	return out
}

func (db *InMemoryDB) UpdateItem(
	ctx context.Context,
	input *dynamodb.UpdateItemInput,
) (*dynamodb.UpdateItemOutput, error) {
	tableName := aws.ToString(input.TableName)
	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	if err = validateExpressionAttributeNames(input.ExpressionAttributeNames); err != nil {
		return nil, err
	}

	updateExpr := aws.ToString(input.UpdateExpression)
	condExpr := aws.ToString(input.ConditionExpression)
	allExprs := []string{updateExpr, condExpr}

	if err = checkUnusedExpressionAttributeNames(input.ExpressionAttributeNames, allExprs...); err != nil {
		return nil, err
	}

	wireEAV := models.FromSDKItem(input.ExpressionAttributeValues)
	if err = checkUnusedExpressionAttributeValues(wireEAV, allExprs...); err != nil {
		return nil, err
	}

	if err = validateUpdateDoesNotModifyKeys(updateExpr, input.ExpressionAttributeNames, table.KeySchema); err != nil {
		return nil, err
	}

	table.mu.Lock("UpdateItem")

	wireKey := models.FromSDKItem(input.Key)
	err = validateKeySchema(wireKey, table.KeySchema)
	if err != nil {
		table.mu.Unlock()

		return nil, err
	}

	region := getRegionFromContext(ctx, db)

	existing, matchIndex := db.findMatchForPut(table, wireKey)

	// Enforce throughput after key validation so that invalid requests do not
	// consume tokens. PAY_PER_REQUEST tables bypass throttling. UpdateItem
	// consumes WCUs proportional to the (pre-update) item size, min 1.
	if !isOnDemandTable(table.BillingMode) {
		if throttleErr := db.throttler.ConsumeWrite(
			throttleKey(region, tableName), WriteCapacityUnits(existing),
		); throttleErr != nil {
			table.mu.Unlock()

			return nil, throttleErr
		}
	}

	err = db.checkUpdateCondition(ctx, input, existing)
	if err != nil {
		table.mu.Unlock()

		return nil, err
	}

	updated, updatedPaths, err := db.doUpdate(ctx, table, input, existing, matchIndex)
	if err != nil {
		table.mu.Unlock()

		return nil, err
	}

	// Capture stream event for UpdateItem
	if matchIndex != -1 {
		table.appendStreamRecord(streamEventModify, deepCopyItem(existing), deepCopyItem(updated))
	} else {
		table.appendStreamRecord(streamEventInsert, nil, deepCopyItem(updated))
	}

	globalTableName := table.GlobalTableName
	out, outErr := db.populateUpdateOutput(input, table, existing, updated, updatedPaths)

	table.mu.Unlock()

	// Propagate the final item state to all global table replicas.
	if globalTableName != "" {
		db.replicateItemMutation(tableName, globalTableName, region, deepCopyItem(updated), "PUT")
	}

	return out, outErr
}

func (db *InMemoryDB) checkUpdateCondition(
	ctx context.Context,
	input *dynamodb.UpdateItemInput,
	item map[string]any,
) error {
	condition := aws.ToString(input.ConditionExpression)
	if condition == "" {
		return nil
	}

	log := logger.Load(ctx)
	log.DebugContext(ctx, "Evaluating UpdateItem condition",
		"expression", condition,
		"attributeNames", input.ExpressionAttributeNames,
		"attributeValues", input.ExpressionAttributeValues)

	eav := models.FromSDKItem(input.ExpressionAttributeValues)
	match, err := evaluateExpression(
		condition,
		item,
		eav,
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
	ctx context.Context,
	table *Table,
	input *dynamodb.UpdateItemInput,
	existing map[string]any,
	matchIndex int,
) (map[string]any, map[string]struct{}, error) {
	updated := make(map[string]any)
	wireKey := models.FromSDKItem(input.Key)

	if existing != nil {
		maps.Copy(updated, deepCopyItem(existing))
	} else {
		// Create new item from key
		maps.Copy(updated, wireKey)
	}

	var updatedPaths map[string]struct{}

	updateExpr := aws.ToString(input.UpdateExpression)
	if updateExpr != "" {
		log := logger.Load(ctx)
		log.DebugContext(ctx, "Applying UpdateItem expression",
			"expression", updateExpr,
			"attributeNames", input.ExpressionAttributeNames,
			"attributeValues", input.ExpressionAttributeValues)

		eav := models.FromSDKItem(input.ExpressionAttributeValues)
		var err error
		updatedPaths, err = applyUpdate(
			updated,
			updateExpr,
			input.ExpressionAttributeNames,
			eav,
		)
		if err != nil {
			return nil, nil, err
		}
	}

	if err := db.validateItem(updated, table); err != nil {
		return nil, nil, err
	}

	if matchIndex != -1 {
		table.Items[matchIndex] = updated
		db.updateIndexes(table, updated, matchIndex)
	} else {
		newIdx := len(table.Items)
		table.Items = append(table.Items, updated)
		db.updateIndexes(table, updated, newIdx)
	}

	return updated, updatedPaths, nil
}

func (db *InMemoryDB) updateIndexes(table *Table, item map[string]any, index int) {
	pkDef, skDef := getPKAndSK(table.KeySchema)
	pkVal := BuildKeyString(item, pkDef.AttributeName)

	if skDef.AttributeName != "" {
		if table.pkskIndex[pkVal] == nil {
			table.pkskIndex[pkVal] = make(map[string]int)
		}
		table.pkskIndex[pkVal][BuildKeyString(item, skDef.AttributeName)] = index
	} else {
		table.pkIndex[pkVal] = index
	}
}

func resolveReturnValues(
	rv types.ReturnValue,
	oldItem, newItem map[string]any,
	updatedPaths map[string]struct{},
) (map[string]types.AttributeValue, error) {
	switch rv {
	case types.ReturnValueAllOld:
		if oldItem != nil {
			return models.ToSDKItem(oldItem)
		}
	case types.ReturnValueAllNew:
		return models.ToSDKItem(newItem)
	case types.ReturnValueUpdatedOld:
		if oldItem != nil {
			filtered := pickPaths(oldItem, updatedPaths)
			if len(filtered) > 0 {
				return models.ToSDKItem(filtered)
			}
		}
	case types.ReturnValueUpdatedNew:
		filtered := pickPaths(newItem, updatedPaths)
		if len(filtered) > 0 {
			return models.ToSDKItem(filtered)
		}
	case types.ReturnValueNone:
		// Do nothing
	}

	return nil, nil //nolint:nilnil // nil attributes is valid when ReturnValues is NONE or item doesn't exist
}

// pickPaths returns a new item containing only the attributes whose keys are in paths.
// For a new item (no existing item before update), all attributes are returned.
func pickPaths(item map[string]any, paths map[string]struct{}) map[string]any {
	if len(paths) == 0 {
		return item
	}

	result := make(map[string]any, len(paths))
	for k, v := range item {
		if _, ok := paths[k]; ok {
			result[k] = v
		}
	}

	return result
}

func (db *InMemoryDB) populateUpdateOutput(
	input *dynamodb.UpdateItemInput,
	table *Table,
	oldItem, newItem map[string]any,
	updatedPaths map[string]struct{},
) (*dynamodb.UpdateItemOutput, error) {
	out := &dynamodb.UpdateItemOutput{}

	attrs, err := resolveReturnValues(input.ReturnValues, oldItem, newItem, updatedPaths)
	if err != nil {
		return nil, err
	}

	out.Attributes = attrs

	// Handle ConsumedCapacity. AWS bills UpdateItem at the larger of the
	// pre- and post-update item sizes (ceil to 1 KB units). Computing that
	// off the actual sizes makes ConsumedCapacity round-trip realistic for
	// cost-projection tests.
	if input.ReturnConsumedCapacity != "" &&
		input.ReturnConsumedCapacity != types.ReturnConsumedCapacityNone {
		writeUnits := WriteCapacityUnits(newItem)
		if oldItem != nil {
			if w := WriteCapacityUnits(oldItem); w > writeUnits {
				writeUnits = w
			}
		}
		out.ConsumedCapacity = &types.ConsumedCapacity{
			TableName:          aws.String(table.Name),
			CapacityUnits:      aws.Float64(writeUnits),
			WriteCapacityUnits: aws.Float64(writeUnits),
		}
	}

	// Handle ItemCollectionMetrics
	if input.ReturnItemCollectionMetrics != "" &&
		input.ReturnItemCollectionMetrics != types.ReturnItemCollectionMetricsNone {
		out.ItemCollectionMetrics = &types.ItemCollectionMetrics{
			ItemCollectionKey:   input.Key,
			SizeEstimateRangeGB: []float64{0.0, 1.0},
		}
	}

	return out, nil
}

func (db *InMemoryDB) deleteItemAtIndex(table *Table, matchIndex int) {
	item := table.Items[matchIndex]
	pkDef, skDef := getPKAndSK(table.KeySchema)
	pkVal := BuildKeyString(item, pkDef.AttributeName)

	if skDef.AttributeName != "" {
		skVal := BuildKeyString(item, skDef.AttributeName)
		if skMap, ok := table.pkskIndex[pkVal]; ok {
			delete(skMap, skVal)
			if len(skMap) == 0 {
				delete(table.pkskIndex, pkVal)
			}
		}
	} else {
		delete(table.pkIndex, pkVal)
	}

	// Swap with last strategy for O(1) deletion
	lastIdx := len(table.Items) - 1
	if matchIndex != lastIdx {
		// Move last item to deleted spot
		lastItem := table.Items[lastIdx]
		table.Items[matchIndex] = lastItem

		// Update index for the moved item
		db.updateIndexes(table, lastItem, matchIndex)
	}

	// Shrink slice
	table.Items = table.Items[:lastIdx]
}

// deepCopyItem returns a deep copy of a wire-format item so that mutations
// to nested map/list structures in the copy do not affect the original.
// It uses a recursive approach rather than JSON round-trip for better performance.
func deepCopyItem(item map[string]any) map[string]any {
	return deepCopyMap(item)
}

// deepCopyMap recursively copies a map[string]any.
func deepCopyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}

	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = deepCopyAny(v)
	}

	return out
}

// deepCopyAny recursively copies any DynamoDB wire-format value.
// Scalars (string, float64, bool, nil) are immutable value types and are returned as-is.
// Maps and slices are deep-copied to prevent shared mutation.
//
// Wire-format set types:
//   - SS and NS are stored as map[string]any{"SS": []string{...}} / {"NS": []string{...}}
//   - BS is stored as map[string]any{"BS": []any{...}} (base64-encoded strings in []any)
//
// The []string case must be deep-copied; leaving the original backing array shared would
// allow in-place modifications in the copy to silently corrupt the original.
func deepCopyAny(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return deepCopyMap(t)
	case []any:
		out := make([]any, len(t))
		for i, elem := range t {
			out[i] = deepCopyAny(elem)
		}

		return out
	case []string:
		// SS / NS sets are stored as []string; copy so the backing array is not shared.
		out := make([]string, len(t))
		copy(out, t)

		return out
	default:
		// string, float64, bool, nil — immutable or value types; safe to share.
		return v
	}
}

// estimateTableSizeBytes computes the total estimated size of all items in the table.
func estimateTableSizeBytes(items []map[string]any) int64 {
	var total int64
	for _, item := range items {
		size, _ := CalculateItemSize(item)
		total += int64(size)
	}

	return total
}
