package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// errConditionalCheckFailed is a sentinel used internally to signal that a
// ConditionExpression did not match during a TransactWriteItems condition check.
var errConditionalCheckFailed = errors.New("conditional check failed")

type tableStateSnapshot struct {
	pkIndex   map[string]int
	pkskIndex map[string]map[string]int
	items     []map[string]any
}

const txCancelPrefix = "Transaction cancelled, please refer cancellation reasons for specific reasons"

// TransactWriteItems executes up to 100 write actions atomically.
func (db *InMemoryDB) TransactWriteItems(
	ctx context.Context,
	input *dynamodb.TransactWriteItemsInput,
) (*dynamodb.TransactWriteItemsOutput, error) {
	if len(input.TransactItems) == 0 {
		return nil, NewValidationException("TransactItems must not be empty")
	}

	token := aws.ToString(input.ClientRequestToken)
	done, out, cleanupToken, err := db.checkTransactToken(token)
	if done {
		return out, err
	}
	defer cleanupToken()

	tableNames := db.transactTableNames(input.TransactItems)
	tables, lockErr := db.lockTablesWrite(ctx, tableNames)
	if lockErr != nil {
		return nil, lockErr
	}
	defer func() {
		for _, t := range tables {
			t.mu.Unlock()
		}
	}()

	// Phase 1: Check conditions
	reasons := make([]CancellationReason, len(input.TransactItems))
	for i := range reasons {
		reasons[i] = CancellationReason{Code: "None"}
	}

	canceled := false
	for i, ti := range input.TransactItems {
		if condErr := db.checkTransactWriteCondition(ctx, tables, ti, i, reasons); condErr != nil {
			canceled = true
		}
	}

	if canceled {
		return nil, NewTransactionCanceledException(txCancelPrefix, reasons)
	}

	// Phase 2: Apply writes with rollback on failure.
	if applyErr := db.applyTransactItems(ctx, tables, input.TransactItems); applyErr != nil {
		return nil, applyErr
	}

	// Record token as committed only after all writes have been applied.
	if token != "" {
		db.mu.Lock("TransactWriteItems.tokenCommit")
		db.txnTokens[token] = time.Now().Add(txnTokenTTL)
		db.mu.Unlock()
	}

	out = &dynamodb.TransactWriteItemsOutput{
		ConsumedCapacity: transactWriteConsumedCapacity(input.ReturnConsumedCapacity, input.TransactItems),
	}

	return out, nil
}

// checkTransactToken checks idempotency token state.
// Returns (true, output, cleanup, err) if the caller should return immediately,
// or (false, nil, cleanup, nil) if the transaction should proceed.
// When proceeding, the cleanup func removes the token from the pending map and
// must be called via defer in the caller.
func (db *InMemoryDB) checkTransactToken(
	token string,
) (bool, *dynamodb.TransactWriteItemsOutput, func(), error) {
	noop := func() {}
	if token == "" {
		return false, nil, noop, nil
	}

	var committed, inProgress bool
	db.mu.Lock("TransactWriteItems.tokenCheck")
	expiry, exists := db.txnTokens[token]
	committed = exists && time.Now().Before(expiry)
	_, inProgress = db.txnPending[token]
	if !committed && !inProgress {
		db.txnPending[token] = time.Now()
	}
	db.mu.Unlock()

	switch {
	case committed:
		return true, &dynamodb.TransactWriteItemsOutput{}, noop, nil
	case inProgress:
		return true, nil, noop, NewTransactionInProgressException(
			"A transaction with the given request token is currently in progress",
		)
	}

	cleanup := func() {
		db.mu.Lock("TransactWriteItems.tokenCleanup")
		delete(db.txnPending, token)
		db.mu.Unlock()
	}

	return false, nil, cleanup, nil
}

// applyTransactItems applies write items atomically, rolling back on any failure.
func (db *InMemoryDB) applyTransactItems(
	ctx context.Context,
	tables map[string]*Table,
	items []types.TransactWriteItem,
) error {
	snapshots := db.snapshotTables(tables)
	for i, ti := range items {
		if err := db.applyTransactWrite(ctx, tables, ti); err != nil {
			logger.Load(ctx).ErrorContext(ctx, "Transaction failed during apply phase, rolling back",
				"error", err,
				"itemIndex", i)
			db.rollbackTables(tables, snapshots)

			return err
		}
	}

	return nil
}

func transactWriteConsumedCapacity(
	req types.ReturnConsumedCapacity,
	items []types.TransactWriteItem,
) []types.ConsumedCapacity {
	if req == "" || req == types.ReturnConsumedCapacityNone {
		return nil
	}

	// Count write operations per table for accurate WCU reporting.
	perTable := make(map[string]int)
	for _, ti := range items {
		switch {
		case ti.Put != nil:
			perTable[aws.ToString(ti.Put.TableName)]++
		case ti.Delete != nil:
			perTable[aws.ToString(ti.Delete.TableName)]++
		case ti.Update != nil:
			perTable[aws.ToString(ti.Update.TableName)]++
		case ti.ConditionCheck != nil:
			perTable[aws.ToString(ti.ConditionCheck.TableName)]++
		}
	}

	caps := make([]types.ConsumedCapacity, 0, len(perTable))
	for name, n := range perTable {
		cu := float64(n)
		caps = append(caps, types.ConsumedCapacity{
			TableName:          aws.String(name),
			CapacityUnits:      aws.Float64(cu),
			WriteCapacityUnits: aws.Float64(cu),
		})
	}

	return caps
}

// TransactGetItems reads up to 100 items atomically.
func (db *InMemoryDB) TransactGetItems(
	ctx context.Context,
	input *dynamodb.TransactGetItemsInput,
) (*dynamodb.TransactGetItemsOutput, error) {
	if len(input.TransactItems) == 0 {
		return nil, NewValidationException("TransactItems must not be empty")
	}

	tableNames := make([]string, 0)
	seen := make(map[string]bool)

	for _, ti := range input.TransactItems {
		if ti.Get != nil {
			tableName := aws.ToString(ti.Get.TableName)
			if !seen[tableName] {
				tableNames = append(tableNames, tableName)
				seen[tableName] = true
			}
		}
	}
	sort.Strings(tableNames)

	tables, lockErr := db.lockTablesRead(ctx, tableNames)
	if lockErr != nil {
		return nil, lockErr
	}
	defer func() {
		for _, t := range tables {
			t.mu.RUnlock()
		}
	}()

	responses := make([]types.ItemResponse, 0, len(input.TransactItems))

	for _, ti := range input.TransactItems {
		resp, err := db.transactGetResponseItem(ti, tables)
		if err != nil {
			return nil, err
		}

		responses = append(responses, resp)
	}

	out := &dynamodb.TransactGetItemsOutput{
		Responses:        responses,
		ConsumedCapacity: transactReadConsumedCapacity(input.ReturnConsumedCapacity, input.TransactItems),
	}

	return out, nil
}

func (db *InMemoryDB) transactGetResponseItem(
	ti types.TransactGetItem,
	tables map[string]*Table,
) (types.ItemResponse, error) {
	if ti.Get == nil {
		return types.ItemResponse{}, nil
	}

	tableName := aws.ToString(ti.Get.TableName)
	table, ok := tables[tableName]
	if !ok {
		return types.ItemResponse{}, NewResourceNotFoundException(fmt.Sprintf("Table not found: %s", tableName))
	}

	pkDef, skDef := getPKAndSK(table.KeySchema)
	wireKey := models.FromSDKItem(ti.Get.Key)
	item := db.lookupItem(table, wireKey, pkDef.AttributeName, skDef.AttributeName)

	if item == nil || isItemExpired(item, table.TTLAttribute) {
		return types.ItemResponse{}, nil
	}

	result := item
	proj := aws.ToString(ti.Get.ProjectionExpression)
	if proj != "" {
		result = projectItem(item, proj, ti.Get.ExpressionAttributeNames)
	}

	sdkResult, _ := models.ToSDKItem(result)

	return types.ItemResponse{Item: sdkResult}, nil
}

func transactReadConsumedCapacity(
	req types.ReturnConsumedCapacity,
	items []types.TransactGetItem,
) []types.ConsumedCapacity {
	if req == "" || req == types.ReturnConsumedCapacityNone {
		return nil
	}

	// Count read operations per table for accurate RCU reporting.
	perTable := make(map[string]int)
	for _, ti := range items {
		if ti.Get != nil {
			perTable[aws.ToString(ti.Get.TableName)]++
		}
	}

	const rcuPerRead = 0.5 // eventually-consistent
	caps := make([]types.ConsumedCapacity, 0, len(perTable))

	for name, n := range perTable {
		cu := float64(n) * rcuPerRead
		caps = append(caps, types.ConsumedCapacity{
			TableName:         aws.String(name),
			CapacityUnits:     aws.Float64(cu),
			ReadCapacityUnits: aws.Float64(cu),
		})
	}

	return caps
}

func (db *InMemoryDB) transactTableNames(items []types.TransactWriteItem) []string {
	seen := make(map[string]bool)
	for _, ti := range items {
		switch {
		case ti.Put != nil:
			seen[aws.ToString(ti.Put.TableName)] = true
		case ti.Delete != nil:
			seen[aws.ToString(ti.Delete.TableName)] = true
		case ti.Update != nil:
			seen[aws.ToString(ti.Update.TableName)] = true
		case ti.ConditionCheck != nil:
			seen[aws.ToString(ti.ConditionCheck.TableName)] = true
		}
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}

func (db *InMemoryDB) lockTablesWrite(ctx context.Context, tableNames []string) (map[string]*Table, error) {
	region := getRegionFromContext(ctx, db)
	tables := make(map[string]*Table, len(tableNames))

	db.mu.RLock("TransactWriteItems")
	regionTables, exists := db.Tables[region]
	if !exists {
		db.mu.RUnlock()

		return nil, NewResourceNotFoundException(fmt.Sprintf("Table not found in region %s", region))
	}

	for _, name := range tableNames {
		t, ok := regionTables[name]
		if !ok {
			db.mu.RUnlock()

			return nil, NewResourceNotFoundException(fmt.Sprintf("Table not found: %s", name))
		}
		tables[name] = t
	}
	db.mu.RUnlock()

	for _, name := range tableNames {
		tables[name].mu.Lock("TransactWriteItems")
	}

	return tables, nil
}

func (db *InMemoryDB) lockTablesRead(ctx context.Context, tableNames []string) (map[string]*Table, error) {
	region := getRegionFromContext(ctx, db)
	tables := make(map[string]*Table, len(tableNames))

	db.mu.RLock("TransactGetItems")
	regionTables, exists := db.Tables[region]
	if !exists {
		db.mu.RUnlock()

		return nil, NewResourceNotFoundException(fmt.Sprintf("Table not found in region %s", region))
	}

	for _, name := range tableNames {
		t, ok := regionTables[name]
		if !ok {
			db.mu.RUnlock()

			return nil, NewResourceNotFoundException(fmt.Sprintf("Table not found: %s", name))
		}
		tables[name] = t
	}
	db.mu.RUnlock()

	for _, name := range tableNames {
		tables[name].mu.RLock("TransactGetItems")
	}

	return tables, nil
}

func (db *InMemoryDB) checkTransactWriteCondition(
	ctx context.Context,
	tables map[string]*Table,
	ti types.TransactWriteItem,
	idx int,
	reasons []CancellationReason,
) error {
	switch {
	case ti.Put != nil:
		return db.checkTransactPut(ctx, tables, ti.Put, idx, reasons)
	case ti.Delete != nil:
		return db.checkTransactCondExpr(
			ctx,
			tables[aws.ToString(ti.Delete.TableName)],
			models.FromSDKItem(ti.Delete.Key),
			aws.ToString(ti.Delete.ConditionExpression),
			models.FromSDKItem(ti.Delete.ExpressionAttributeValues),
			ti.Delete.ExpressionAttributeNames,
			idx,
			ti.Delete.ReturnValuesOnConditionCheckFailure,
			reasons,
		)
	case ti.Update != nil:
		return db.checkTransactCondExpr(
			ctx,
			tables[aws.ToString(ti.Update.TableName)],
			models.FromSDKItem(ti.Update.Key),
			aws.ToString(ti.Update.ConditionExpression),
			models.FromSDKItem(ti.Update.ExpressionAttributeValues),
			ti.Update.ExpressionAttributeNames,
			idx,
			ti.Update.ReturnValuesOnConditionCheckFailure,
			reasons,
		)
	case ti.ConditionCheck != nil:
		return db.checkTransactCondExpr(
			ctx,
			tables[aws.ToString(ti.ConditionCheck.TableName)],
			models.FromSDKItem(ti.ConditionCheck.Key),
			aws.ToString(ti.ConditionCheck.ConditionExpression),
			models.FromSDKItem(ti.ConditionCheck.ExpressionAttributeValues),
			ti.ConditionCheck.ExpressionAttributeNames,
			idx,
			ti.ConditionCheck.ReturnValuesOnConditionCheckFailure,
			reasons,
		)
	}

	return nil
}

func (db *InMemoryDB) checkTransactPut(
	ctx context.Context,
	tables map[string]*Table,
	input *types.Put,
	idx int,
	reasons []CancellationReason,
) error {
	table := tables[aws.ToString(input.TableName)]
	wireItem := models.FromSDKItem(input.Item)
	oldItem, _ := db.findMatchForPut(table, wireItem)

	cond := aws.ToString(input.ConditionExpression)
	if cond == "" {
		return nil
	}

	eav := models.FromSDKItem(input.ExpressionAttributeValues)

	if err := db.checkTransactCondExprRaw(
		ctx,
		oldItem,
		cond,
		eav,
		input.ExpressionAttributeNames,
		idx,
		input.ReturnValuesOnConditionCheckFailure,
		reasons,
	); err != nil {
		return err
	}

	return nil
}

func (db *InMemoryDB) checkTransactCondExpr(
	ctx context.Context,
	table *Table,
	key map[string]any,
	condExpr string,
	eavs map[string]any,
	eans map[string]string,
	idx int,
	rv types.ReturnValuesOnConditionCheckFailure,
	reasons []CancellationReason,
) error {
	if condExpr == "" {
		return nil
	}

	oldItem, _ := db.findMatchForPut(table, key)

	return db.checkTransactCondExprRaw(ctx, oldItem, condExpr, eavs, eans, idx, rv, reasons)
}

func (db *InMemoryDB) checkTransactCondExprRaw(
	ctx context.Context,
	item map[string]any,
	condExpr string,
	eavs map[string]any,
	eans map[string]string,
	idx int,
	rv types.ReturnValuesOnConditionCheckFailure,
	reasons []CancellationReason,
) error {
	log := logger.Load(ctx)
	log.DebugContext(ctx, "Evaluating Transaction condition",
		"index", idx,
		"expression", condExpr,
		"attributeNames", eans,
		"attributeValues", eavs)

	match, err := evaluateExpression(condExpr, item, eavs, eans)
	if err != nil {
		reasons[idx] = CancellationReason{
			Code:    "ValidationError",
			Message: err.Error(),
		}

		return err
	}
	if !match {
		reason := CancellationReason{
			Code:    "ConditionalCheckFailed",
			Message: "The conditional request failed",
		}

		if rv == types.ReturnValuesOnConditionCheckFailureAllOld && item != nil {
			sdkItem, _ := models.ToSDKItem(item)
			reason.Item = sdkItem
		}
		reasons[idx] = reason

		return errConditionalCheckFailed
	}

	return nil
}

func (db *InMemoryDB) applyTransactWrite(
	ctx context.Context,
	tables map[string]*Table,
	ti types.TransactWriteItem,
) error {
	switch {
	case ti.Put != nil:
		table := tables[aws.ToString(ti.Put.TableName)]
		wireItem := models.FromSDKItem(ti.Put.Item)
		if err := db.validateItem(wireItem, table); err != nil {
			return err
		}
		_, matchIndex := db.findMatchForPut(table, wireItem)
		db.doPut(table, wireItem, matchIndex)

	case ti.Delete != nil:
		table := tables[aws.ToString(ti.Delete.TableName)]
		wireKey := models.FromSDKItem(ti.Delete.Key)
		_, matchIndex := db.findMatchForPut(table, wireKey)
		if matchIndex != -1 {
			db.deleteItemAtIndex(table, matchIndex)
		}

	case ti.Update != nil:
		table := tables[aws.ToString(ti.Update.TableName)]
		wireKey := models.FromSDKItem(ti.Update.Key)
		oldItem, matchIndex := db.findMatchForPut(table, wireKey)

		// doUpdate expects *dynamodb.UpdateItemInput.
		// types.Update struct is similar but different package.
		// Use internal logic or construct dummy input?
		// Better to refactor doUpdate to take components, OR construct dummy input.
		// Constructing dummy input is easier refactor.

		dummyInput := &dynamodb.UpdateItemInput{
			Key:                       ti.Update.Key,
			TableName:                 ti.Update.TableName,
			UpdateExpression:          ti.Update.UpdateExpression,
			ExpressionAttributeNames:  ti.Update.ExpressionAttributeNames,
			ExpressionAttributeValues: ti.Update.ExpressionAttributeValues,
		}

		_, _, err := db.doUpdate(ctx, table, dummyInput, oldItem, matchIndex)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *InMemoryDB) snapshotTables(tables map[string]*Table) map[string]tableStateSnapshot {
	snapshots := make(map[string]tableStateSnapshot, len(tables))
	for name, t := range tables {
		// Shallow copy of Items slice (holds references to maps).
		// Since we always replace maps in the slice (never mutate in-place),
		// this is sufficient for restoring the table's item references.
		itemsCopy := make([]map[string]any, len(t.Items))
		copy(itemsCopy, t.Items)

		// Deep copy of indexes to ensure rollback restores correct mapping.
		pkIdxCopy := make(map[string]int, len(t.pkIndex))
		maps.Copy(pkIdxCopy, t.pkIndex)

		pkskIdxCopy := make(map[string]map[string]int, len(t.pkskIndex))
		for pk, skMap := range t.pkskIndex {
			skMapCopy := make(map[string]int, len(skMap))
			maps.Copy(skMapCopy, skMap)
			pkskIdxCopy[pk] = skMapCopy
		}

		snapshots[name] = tableStateSnapshot{
			items:     itemsCopy,
			pkIndex:   pkIdxCopy,
			pkskIndex: pkskIdxCopy,
		}
	}

	return snapshots
}

func (db *InMemoryDB) rollbackTables(tables map[string]*Table, snapshots map[string]tableStateSnapshot) {
	for name, t := range tables {
		if s, ok := snapshots[name]; ok {
			t.Items = s.items
			t.pkIndex = s.pkIndex
			t.pkskIndex = s.pkskIndex
		}
	}
}
