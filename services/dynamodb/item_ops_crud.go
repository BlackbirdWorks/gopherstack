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

	if err := applyLegacyPutParams(input); err != nil {
		return nil, err
	}

	condExpr := aws.ToString(input.ConditionExpression)
	if len(input.ExpressionAttributeNames) > 0 {
		if err := checkUnusedExpressionAttributeNames(input.ExpressionAttributeNames, condExpr); err != nil {
			return nil, err
		}
	}

	if len(input.ExpressionAttributeValues) > 0 {
		wireEAV := models.FromSDKItem(input.ExpressionAttributeValues)
		if err := checkUnusedExpressionAttributeValues(wireEAV, condExpr); err != nil {
			return nil, err
		}
	}

	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	// Convert SDK Item to Wire Item once; reused for validation and WCU calculation.
	wireItem := models.FromSDKItem(input.Item)
	itemSize, err := CalculateItemSize(wireItem)
	if err != nil {
		return nil, err
	}

	out, globalTableName, region, putErr := db.putItemLocked(ctx, tableName, table, input, wireItem, itemSize)
	if putErr != nil {
		return nil, putErr
	}

	// Propagate to global table replicas after releasing the primary lock.
	if globalTableName != "" {
		db.replicateItemMutation(tableName, globalTableName, region, deepCopyItem(wireItem), "PUT")
	}

	return out, nil
}

// putItemLocked performs the table.mu-guarded portion of PutItem. Using a
// single defer (rather than a manual table.mu.Unlock() call at every early
// return, as this used to do) means a panic partway through -- e.g. from a
// caller elsewhere racing an unguarded read against this table -- can never
// leave table.mu permanently locked, which would otherwise hang every future
// operation on this table forever.
func (db *InMemoryDB) putItemLocked(
	ctx context.Context,
	tableName string,
	table *Table,
	input *dynamodb.PutItemInput,
	wireItem map[string]any,
	itemSize int,
) (*dynamodb.PutItemOutput, string, string, error) {
	table.mu.Lock("PutItem")
	defer table.mu.Unlock()

	// Validate item before charging capacity so that validation errors do not
	// consume tokens (matches real DynamoDB behaviour).
	if err := db.validateItem(wireItem, table); err != nil {
		return nil, "", "", err
	}

	// Enforce throughput after validation, before mutating state.
	// PAY_PER_REQUEST tables bypass throttling.
	wcu := WriteCapacityUnitsFromSize(itemSize)
	region := getRegionFromContext(ctx, db)

	if !isOnDemandTable(table.BillingMode) {
		if throttleErr := db.throttler.ConsumeWrite(throttleKey(region, tableName), wcu); throttleErr != nil {
			return nil, "", "", throttleErr
		}
	}

	oldItem, matchIndex := db.findMatchForPut(table, wireItem)
	if condErr := db.checkPutCondition(ctx, input, oldItem); condErr != nil {
		return nil, "", "", condErr
	}

	// Enforce LSI 10 GB per-collection limit before mutating state.
	lsiCollectionBytes, lsiErr := db.checkLSICollectionSize(table, wireItem, matchIndex)
	if lsiErr != nil {
		return nil, "", "", lsiErr
	}

	db.doPutWithSize(table, wireItem, matchIndex, itemSize)

	// Capture stream event
	if matchIndex != -1 {
		table.appendStreamRecord(streamEventModify, oldItem, wireItem, "", "")
	} else {
		table.appendStreamRecord(streamEventInsert, nil, wireItem, "", "")
	}

	globalTableName := table.GlobalTableName
	out := db.populatePutItemOutput(input, table, oldItem, wireItem, wcu, lsiCollectionBytes)

	return out, globalTableName, region, nil
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

// conditionalCheckFailed builds a ConditionalCheckFailedException, attaching the
// existing item when the caller requested ReturnValuesOnConditionCheckFailure=ALL_OLD.
// This mirrors AWS, which returns the current item in the error body so clients doing
// optimistic locking can inspect it without issuing a follow-up read.
func conditionalCheckFailed(
	rv types.ReturnValuesOnConditionCheckFailure,
	oldItem map[string]any,
) *Error {
	if rv == types.ReturnValuesOnConditionCheckFailureAllOld && oldItem != nil {
		// oldItem is already in DynamoDB wire form (e.g. {"pk":{"S":"a"}}), which is
		// exactly the shape AWS returns in the ConditionalCheckFailedException body.
		return NewConditionalCheckFailedExceptionWithItem("The conditional request failed", oldItem)
	}

	return NewConditionalCheckFailedException("The conditional request failed")
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
		return conditionalCheckFailed(input.ReturnValuesOnConditionCheckFailure, oldItem)
	}

	return nil
}

func (db *InMemoryDB) doPut(table *Table, item map[string]any, matchIndex int) {
	itemSize, _ := CalculateItemSize(item)
	db.doPutWithSize(table, item, matchIndex, itemSize)
}

func (db *InMemoryDB) doPutWithSize(table *Table, item map[string]any, matchIndex int, itemSize int) {
	if matchIndex != -1 {
		oldItem := table.Items[matchIndex]
		table.totalItemSizeBytes += int64(itemSize) - int64(table.itemSizes[matchIndex])
		table.itemSizes[matchIndex] = itemSize
		table.Items[matchIndex] = item
		db.updateIndexes(table, item, matchIndex)
		table.updateSecondaryIndexes(oldItem, matchIndex, item, matchIndex)
	} else {
		idx := len(table.Items)
		table.Items = append(table.Items, item)
		table.itemSizes = append(table.itemSizes, itemSize)
		table.totalItemSizeBytes += int64(itemSize)
		db.updateIndexes(table, item, idx)
		table.updateSecondaryIndexes(nil, 0, item, idx)
	}
}

// bytesPerGB is the number of bytes in one gibibyte.
const bytesPerGB = 1024 * 1024 * 1024

// lsiMaxCollectionBytes is the AWS-imposed 10 GB per-collection limit for LSI tables.
const lsiMaxCollectionBytes = 10 * bytesPerGB

// checkLSICollectionSize enforces the 10 GB per-collection limit for tables with LSIs.
// Returns (collectionBytes, nil) when the limit is not exceeded, (-1, nil) for non-LSI
// tables, and (-1, error) when the limit would be exceeded. Must be called under table.mu.
func (db *InMemoryDB) checkLSICollectionSize(
	table *Table,
	newItem map[string]any,
	oldMatchIndex int,
) (int64, error) {
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

// currentLSICollectionBytes returns the total byte size of all items currently
// stored under pkVal as their partition key (the item collection). Must be called
// under table.mu.
func currentLSICollectionBytes(table *Table, pkVal string) int64 {
	var total int64

	if skMap, ok := table.pkskIndex[pkVal]; ok {
		for _, offset := range skMap {
			total += int64(table.itemSizes[offset])
		}
	} else if offset, ok2 := table.pkIndex[pkVal]; ok2 {
		total += int64(table.itemSizes[offset])
	}

	return total
}

// computeLSICollectionSize returns the projected total byte size of all items sharing
// pkVal as their partition key, as if newItem replaces the item at oldMatchIndex (or
// is appended when oldMatchIndex == -1). Must be called under table.mu held.
func computeLSICollectionSize(
	table *Table,
	pkVal string,
	newItem map[string]any,
	oldMatchIndex int,
) int64 {
	total := currentLSICollectionBytes(table, pkVal)

	// Subtract old item (it will be replaced).
	if oldMatchIndex != -1 {
		total -= int64(table.itemSizes[oldMatchIndex])
	}

	// Add new item.
	sz, _ := CalculateItemSize(newItem)
	total += int64(sz)

	return total
}

// buildItemCollectionMetrics builds the ItemCollectionMetrics for a write, or nil.
// AWS only returns metrics for tables with at least one local secondary index; the
// ItemCollectionKey is the partition-key attribute only, and SizeEstimateRangeGB
// brackets the projected collection size. Must be called under table.mu.
func buildItemCollectionMetrics(
	table *Table,
	rim types.ReturnItemCollectionMetrics,
	pkKey map[string]types.AttributeValue,
	collectionBytes int64,
) *types.ItemCollectionMetrics {
	if rim == "" || rim == types.ReturnItemCollectionMetricsNone {
		return nil
	}
	if len(table.LocalSecondaryIndexes) == 0 {
		return nil
	}

	sizeGB := collectionBytesToGB(collectionBytes)

	return &types.ItemCollectionMetrics{
		ItemCollectionKey:   pkKey,
		SizeEstimateRangeGB: []float64{sizeGB, sizeGB},
	}
}

// pkOnlyKey extracts the partition-key attribute (only) from a full SDK key/item.
func pkOnlyKey(table *Table, src map[string]types.AttributeValue) map[string]types.AttributeValue {
	pkDef, _ := getPKAndSK(table.KeySchema)

	return map[string]types.AttributeValue{pkDef.AttributeName: src[pkDef.AttributeName]}
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
	oldItem, wireItem map[string]any,
	wcu float64,
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
		gsiWCU, lsiWCU := calculateWriteIndexBreakdowns(table, wcu, wireItem)
		out.ConsumedCapacity = buildConsumedCapacityWithIndexes(
			table.Name,
			input.ReturnConsumedCapacity,
			0, wcu,
			nil, gsiWCU,
			nil, lsiWCU,
		)
	}

	// ItemCollectionMetrics: only for tables with an LSI and when requested.
	// ItemCollectionKey contains only the partition key attribute (not the full item).
	out.ItemCollectionMetrics = buildItemCollectionMetrics(
		table,
		input.ReturnItemCollectionMetrics,
		pkOnlyKey(table, input.Item),
		lsiCollectionBytes,
	)

	return out
}

func resolveGetItemKeys(
	key map[string]types.AttributeValue,
	keySchema []models.KeySchemaElement,
) (string, string, error) {
	pkDef, skDef := getPKAndSK(keySchema)
	pkVal := BuildKeyStringFromSDK(key, pkDef.AttributeName)
	if pkVal == "" {
		wireKey := models.FromSDKItem(key)

		return "", "", validateKeySchema(wireKey, keySchema)
	}

	var skVal string
	if skDef.AttributeName != "" {
		skVal = BuildKeyStringFromSDK(key, skDef.AttributeName)
		if skVal == "" {
			wireKey := models.FromSDKItem(key)

			return "", "", validateKeySchema(wireKey, keySchema)
		}
	}

	return pkVal, skVal, nil
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

	consistentRead := aws.ToBool(input.ConsistentRead)
	rcu := applyConsistentReadMultiplier(models.ConsumedReadUnit, consistentRead)
	region := getRegionFromContext(ctx, db)

	var (
		item     map[string]any
		ttlAttr  string
		tableErr error
	)

	func() {
		table.mu.RLock("GetItem")
		defer table.mu.RUnlock()

		pkVal, skVal, keyErr := resolveGetItemKeys(input.Key, table.KeySchema)
		if keyErr != nil {
			tableErr = keyErr

			return
		}

		if !isOnDemandTable(table.BillingMode) {
			if throttleErr := db.throttler.ConsumeRead(
				throttleKey(region, tableName),
				rcu,
			); throttleErr != nil {
				tableErr = throttleErr

				return
			}
		}

		item = db.lookupItemByKeys(table, pkVal, skVal)
		ttlAttr = table.TTLAttribute
	}()

	if tableErr != nil {
		return nil, tableErr
	}

	if item == nil || isItemExpired(item, ttlAttr) {
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
		out.ConsumedCapacity = buildConsumedCapacityWithIndexes(
			aws.ToString(input.TableName),
			input.ReturnConsumedCapacity,
			readUnits, 0,
			nil, nil,
			nil, nil,
		)
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

	if err := applyLegacyDeleteParams(input); err != nil {
		return nil, err
	}

	condExpr := aws.ToString(input.ConditionExpression)
	if len(input.ExpressionAttributeNames) > 0 {
		if err := checkUnusedExpressionAttributeNames(input.ExpressionAttributeNames, condExpr); err != nil {
			return nil, err
		}
	}

	if len(input.ExpressionAttributeValues) > 0 {
		wireEAV := models.FromSDKItem(input.ExpressionAttributeValues)
		if err := checkUnusedExpressionAttributeValues(wireEAV, condExpr); err != nil {
			return nil, err
		}
	}

	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	wireKey := models.FromSDKItem(input.Key)

	out, globalTableName, region, oldItem, delErr := db.deleteItemLocked(ctx, tableName, table, input, wireKey)
	if delErr != nil {
		return nil, delErr
	}

	// Propagate deletion to global table replicas after releasing the primary lock.
	if globalTableName != "" && oldItem != nil {
		db.replicateItemMutation(
			tableName,
			globalTableName,
			region,
			deepCopyItem(wireKey),
			"DELETE",
		)
	}

	return out, nil
}

// deleteItemLocked performs the table.mu-guarded portion of DeleteItem behind
// a single defer, so a panic partway through can never leave table.mu
// permanently locked (see putItemLocked's doc for why that matters).
func (db *InMemoryDB) deleteItemLocked(
	ctx context.Context,
	tableName string,
	table *Table,
	input *dynamodb.DeleteItemInput,
	wireKey map[string]any,
) (*dynamodb.DeleteItemOutput, string, string, map[string]any, error) {
	table.mu.Lock("DeleteItem")
	defer table.mu.Unlock()

	if err := validateKeySchema(wireKey, table.KeySchema); err != nil {
		return nil, "", "", nil, err
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

	var oldItemSize int
	if matchIndex != -1 && matchIndex < len(table.itemSizes) {
		oldItemSize = table.itemSizes[matchIndex]
	}

	wcu := WriteCapacityUnitsFromSize(oldItemSize)

	// Enforce throughput after key validation so that invalid requests do not
	// consume tokens. PAY_PER_REQUEST tables bypass throttling. DeleteItem
	// consumes WCUs proportional to the size of the deleted item (min 1).
	if !isOnDemandTable(table.BillingMode) {
		if throttleErr := db.throttler.ConsumeWrite(
			throttleKey(region, tableName), wcu,
		); throttleErr != nil {
			return nil, "", "", nil, throttleErr
		}
	}

	if err := db.checkDeleteCondition(ctx, input, oldItem); err != nil {
		return nil, "", "", nil, err
	}

	if oldItem != nil && matchIndex != -1 {
		db.deleteItemAtIndex(table, matchIndex)
		// Capture stream REMOVE event
		table.appendStreamRecord(streamEventRemove, oldItem, nil, "", "")
	}

	out := db.buildDeleteItemOutput(input, table, oldItem, wcu)
	globalTableName := table.GlobalTableName

	return out, globalTableName, region, oldItem, nil
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
		return conditionalCheckFailed(input.ReturnValuesOnConditionCheckFailure, oldItem)
	}

	return nil
}

func (db *InMemoryDB) buildDeleteItemOutput(
	input *dynamodb.DeleteItemInput,
	table *Table,
	oldItem map[string]any,
	wcu float64,
) *dynamodb.DeleteItemOutput {
	out := &dynamodb.DeleteItemOutput{}

	if input.ReturnValues == models.ReturnValuesAllOld && oldItem != nil {
		out.Attributes, _ = models.ToSDKItem(oldItem)
	}

	if input.ReturnConsumedCapacity != "" &&
		input.ReturnConsumedCapacity != types.ReturnConsumedCapacityNone {
		gsiWCU, lsiWCU := calculateWriteIndexBreakdowns(table, wcu, oldItem)
		out.ConsumedCapacity = buildConsumedCapacityWithIndexes(
			table.Name,
			input.ReturnConsumedCapacity,
			0, wcu,
			nil, gsiWCU,
			nil, lsiWCU,
		)
	}

	// ItemCollectionMetrics reflect the collection remaining after the delete.
	pkDef, _ := getPKAndSK(table.KeySchema)
	pkVal := BuildKeyStringFromSDK(input.Key, pkDef.AttributeName)
	out.ItemCollectionMetrics = buildItemCollectionMetrics(
		table,
		input.ReturnItemCollectionMetrics,
		pkOnlyKey(table, input.Key),
		currentLSICollectionBytes(table, pkVal),
	)

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

	if err = applyLegacyUpdateParams(input); err != nil {
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

	wireKey := models.FromSDKItem(input.Key)

	out, globalTableName, region, updated, outErr := db.updateItemLocked(ctx, tableName, table, input, wireKey)
	if outErr != nil {
		return nil, outErr
	}

	// Propagate the final item state to all global table replicas.
	if globalTableName != "" {
		db.replicateItemMutation(tableName, globalTableName, region, deepCopyItem(updated), "PUT")
	}

	return out, nil
}

// updateItemLocked performs the table.mu-guarded portion of UpdateItem behind
// a single defer, so a panic partway through can never leave table.mu
// permanently locked (see putItemLocked's doc for why that matters).
func (db *InMemoryDB) updateItemLocked(
	ctx context.Context,
	tableName string,
	table *Table,
	input *dynamodb.UpdateItemInput,
	wireKey map[string]any,
) (*dynamodb.UpdateItemOutput, string, string, map[string]any, error) {
	table.mu.Lock("UpdateItem")
	defer table.mu.Unlock()

	if err := validateKeySchema(wireKey, table.KeySchema); err != nil {
		return nil, "", "", nil, err
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
			return nil, "", "", nil, throttleErr
		}
	}

	if err := db.checkUpdateCondition(ctx, input, existing); err != nil {
		return nil, "", "", nil, err
	}

	updated, updatedPaths, err := db.doUpdate(ctx, table, input, existing, matchIndex)
	if err != nil {
		return nil, "", "", nil, err
	}

	// Capture stream event for UpdateItem
	if matchIndex != -1 {
		table.appendStreamRecord(streamEventModify, existing, updated, "", "")
	} else {
		table.appendStreamRecord(streamEventInsert, nil, updated, "", "")
	}

	globalTableName := table.GlobalTableName
	out, outErr := db.populateUpdateOutput(input, table, existing, updated, updatedPaths)

	return out, globalTableName, region, updated, outErr
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
		return conditionalCheckFailed(input.ReturnValuesOnConditionCheckFailure, item)
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

	updatedSize, _ := CalculateItemSize(updated)

	if matchIndex != -1 {
		table.totalItemSizeBytes += int64(updatedSize) - int64(table.itemSizes[matchIndex])
		table.itemSizes[matchIndex] = updatedSize
		table.Items[matchIndex] = updated
		db.updateIndexes(table, updated, matchIndex)
		table.updateSecondaryIndexes(existing, matchIndex, updated, matchIndex)
	} else {
		newIdx := len(table.Items)
		table.Items = append(table.Items, updated)
		table.itemSizes = append(table.itemSizes, updatedSize)
		table.totalItemSizeBytes += int64(updatedSize)
		db.updateIndexes(table, updated, newIdx)
		table.updateSecondaryIndexes(nil, 0, updated, newIdx)
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
		gsiWCU, lsiWCU := calculateWriteIndexBreakdowns(table, writeUnits, oldItem, newItem)
		out.ConsumedCapacity = buildConsumedCapacityWithIndexes(
			table.Name,
			input.ReturnConsumedCapacity,
			0, writeUnits,
			nil, gsiWCU,
			nil, lsiWCU,
		)
	}

	// ItemCollectionMetrics reflect the collection after the update is applied.
	pkDef, _ := getPKAndSK(table.KeySchema)
	pkVal := BuildKeyStringFromSDK(input.Key, pkDef.AttributeName)
	out.ItemCollectionMetrics = buildItemCollectionMetrics(
		table,
		input.ReturnItemCollectionMetrics,
		pkOnlyKey(table, input.Key),
		currentLSICollectionBytes(table, pkVal),
	)

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

	table.updateSecondaryIndexes(item, matchIndex, nil, 0)

	// Swap with last strategy for O(1) deletion
	lastIdx := len(table.Items) - 1
	deletedSize := table.itemSizes[matchIndex]

	if matchIndex != lastIdx {
		// Move last item to deleted spot
		lastItem := table.Items[lastIdx]
		table.Items[matchIndex] = lastItem
		table.itemSizes[matchIndex] = table.itemSizes[lastIdx]

		// Update index for the moved item
		db.updateIndexes(table, lastItem, matchIndex)
		table.updateSecondaryIndexes(lastItem, lastIdx, lastItem, matchIndex)
	}

	// Shrink slice
	table.Items = table.Items[:lastIdx]
	table.itemSizes = table.itemSizes[:lastIdx]
	table.totalItemSizeBytes -= int64(deletedSize)
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

// estimateTableSizeBytes returns the cached total estimated size of all items.
func estimateTableSizeBytes(table *Table) int64 {
	return table.totalItemSizeBytes
}
