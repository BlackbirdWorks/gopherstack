package dynamodb

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const txCancelPrefix = "Transaction cancelled, please refer cancellation reasons for specific reasons"

// TransactWriteItems executes up to 100 write actions atomically.
func (db *InMemoryDB) TransactWriteItems(
	ctx context.Context,
	input *dynamodb.TransactWriteItemsInput,
) (*dynamodb.TransactWriteItemsOutput, error) {
	if len(input.TransactItems) == 0 {
		return nil, NewValidationException("TransactItems must not be empty")
	}

	// Idempotency: enforce ClientRequestToken semantics.
	// - committed token  → return success immediately (no re-apply)
	// - in-progress token → return TransactionInProgressException
	// - new token        → mark pending; clear on failure, commit on success
	token := aws.ToString(input.ClientRequestToken)
	if token != "" {
		var committed, inProgress bool
		db.mu.Lock("TransactWriteItems.tokenCheck")
		_, committed = db.txnTokens[token]
		_, inProgress = db.txnPending[token]
		if !committed && !inProgress {
			db.txnPending[token] = struct{}{}
		}
		db.mu.Unlock()

		switch {
		case committed:
			return &dynamodb.TransactWriteItemsOutput{}, nil
		case inProgress:
			return nil, NewTransactionInProgressException(
				"A transaction with the given request token is currently in progress",
			)
		}

		// Ensure the pending entry is cleaned up on any exit path.
		defer func() {
			db.mu.Lock("TransactWriteItems.tokenCleanup")
			delete(db.txnPending, token)
			db.mu.Unlock()
		}()
	}

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
	for i, ti := range input.TransactItems {
		if err := db.checkTransactWriteCondition(ctx, tables, ti, i); err != nil {
			return nil, err
		}
	}

	// Phase 2: Apply writes
	for _, ti := range input.TransactItems {
		if err := db.applyTransactWrite(ctx, tables, ti); err != nil {
			return nil, err
		}
	}

	// Record token as committed only after all writes have been applied.
	if token != "" {
		db.mu.Lock("TransactWriteItems.tokenCommit")
		db.txnTokens[token] = struct{}{}
		db.mu.Unlock()
	}

	out := &dynamodb.TransactWriteItemsOutput{
		ConsumedCapacity: transactWriteConsumedCapacity(input.ReturnConsumedCapacity, input.TransactItems),
	}

	return out, nil
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
) error {
	switch {
	case ti.Put != nil:
		return db.checkTransactPut(ctx, tables, ti.Put, idx)
	case ti.Delete != nil:
		return db.checkTransactCondExpr(
			ctx,
			tables[aws.ToString(ti.Delete.TableName)],
			models.FromSDKItem(ti.Delete.Key),
			aws.ToString(ti.Delete.ConditionExpression),
			models.FromSDKItem(ti.Delete.ExpressionAttributeValues),
			ti.Delete.ExpressionAttributeNames,
			idx,
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
		)
	}

	return nil
}

func (db *InMemoryDB) checkTransactPut(
	ctx context.Context,
	tables map[string]*Table,
	input *types.Put,
	idx int,
) error {
	table := tables[aws.ToString(input.TableName)]
	wireItem := models.FromSDKItem(input.Item)
	oldItem, _ := db.findMatchForPut(table, wireItem)

	// Since checkPutCondition expects *dynamodb.PutItemInput, we construct a dummy one
	// or create a internal checks that takes ConditionExpr string etc.
	// Reusing checkPutCondition is hard because type mismatch (types.Put vs dynamodb.PutItemInput).
	// I'll reuse checkTransactCondExpr logic for Put condition.

	cond := aws.ToString(input.ConditionExpression)
	if cond == "" {
		return nil
	}

	eav := models.FromSDKItem(input.ExpressionAttributeValues)

	if err := db.checkTransactCondExprRaw(ctx, oldItem, cond, eav, input.ExpressionAttributeNames, idx); err != nil {
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
) error {
	if condExpr == "" {
		return nil
	}

	oldItem, _ := db.findMatchForPut(table, key)

	return db.checkTransactCondExprRaw(ctx, oldItem, condExpr, eavs, eans, idx)
}

func (db *InMemoryDB) checkTransactCondExprRaw(
	ctx context.Context,
	item map[string]any,
	condExpr string,
	eavs map[string]any,
	eans map[string]string,
	idx int,
) error {
	log := logger.Load(ctx)
	log.DebugContext(ctx, "Evaluating Transaction condition",
		"index", idx,
		"expression", condExpr,
		"attributeNames", eans,
		"attributeValues", eavs)

	match, err := evaluateExpression(condExpr, item, eavs, eans)
	if err != nil {
		return NewTransactionCanceledException(fmt.Sprintf("%s [%d]: %s", txCancelPrefix, idx, err))
	}
	if !match {
		return NewTransactionCanceledException(
			fmt.Sprintf("%s [%d]: ConditionalCheckFailed", txCancelPrefix, idx),
		)
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
