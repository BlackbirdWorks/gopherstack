package dynamodb

import (
	"context"
	"errors"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
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
	pkIndex            map[string]int
	pkskIndex          map[string]map[string]int
	items              []map[string]any
	itemSizes          []int
	totalItemSizeBytes int64
}

const txCancelPrefix = "Transaction cancelled, please refer cancellation reasons for specific reasons"

// replicationOpDelete is the mutation op string for item deletion in global-table replication.
const replicationOpDelete = "DELETE"

// TransactWriteItems executes up to 100 write actions atomically.
func (db *InMemoryDB) TransactWriteItems(
	ctx context.Context,
	input *dynamodb.TransactWriteItemsInput,
) (*dynamodb.TransactWriteItemsOutput, error) {
	if len(input.TransactItems) == 0 {
		return nil, NewValidationException("TransactItems must not be empty")
	}

	if err := validateTransactItemCount(len(input.TransactItems), "TransactWriteItems"); err != nil {
		return nil, err
	}

	token := aws.ToString(input.ClientRequestToken)
	done, out, cleanupToken, err := db.checkTransactToken(token)
	if done {
		return out, err
	}
	defer cleanupToken()

	tableNames := db.transactTableNames(input.TransactItems)
	region := getRegionFromContext(ctx, db)

	payloads, applyErr := db.executeTransactWrite(ctx, tableNames, token, region, input)
	if applyErr != nil {
		return nil, applyErr
	}

	for _, p := range payloads {
		db.replicateItemMutation(p.tableName, p.globalTableName, p.region, p.item, p.op)
	}

	out = &dynamodb.TransactWriteItemsOutput{
		ConsumedCapacity: transactWriteConsumedCapacity(
			input.ReturnConsumedCapacity,
			input.TransactItems,
		),
	}

	return out, nil
}

// executeTransactWrite locks tables, validates conditions, applies writes, records the
// idempotency token, and returns replication payloads. All table locks are released
// before this function returns, so callers can safely apply cross-region replication.
func (db *InMemoryDB) executeTransactWrite(
	ctx context.Context,
	tableNames []string,
	token string,
	region string,
	input *dynamodb.TransactWriteItemsInput,
) ([]transactReplicationPayload, error) {
	tables, lockErr := db.lockTablesWrite(ctx, tableNames)
	if lockErr != nil {
		return nil, lockErr
	}

	// released guards against double-unlocking: table locks are released
	// explicitly, strictly BEFORE db.mu is ever acquired for the token commit --
	// this backend's lock order is always db.mu -> table.mu, and inverting it
	// here is a real ABBA deadlock against any db.mu-then-table.mu reader (e.g.
	// TaggedTables/ListContributorInsights). The deferred call remains as a
	// safety net so an early return or panic still releases the locks.
	released := false
	releaseTables := func() {
		if released {
			return
		}
		released = true
		for _, t := range tables {
			t.mu.Unlock()
		}
	}
	defer releaseTables()

	// Pre-phase: validate duplicate keys and total size.
	if dupErr := validateTransactWriteItems(input.TransactItems, tables); dupErr != nil {
		return nil, dupErr
	}

	// Phase 1: Check conditions.
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
	if writeErr := db.applyTransactItems(ctx, tables, input.TransactItems); writeErr != nil {
		return nil, writeErr
	}

	payloads := db.collectTransactReplicationPayloads(tables, region, input.TransactItems)

	// Release the table locks before ever touching db.mu (see releaseTables'
	// doc above), then record the token as committed now that all writes have
	// been applied.
	releaseTables()

	if token != "" {
		commitTransactTokenLocked(db, token)
	}

	return payloads, nil
}

// commitTransactTokenLocked records token as committed (with its TTL expiry)
// under a defer-protected db.mu.Lock.
func commitTransactTokenLocked(db *InMemoryDB, token string) {
	db.mu.Lock("TransactWriteItems.tokenCommit")
	defer db.mu.Unlock()

	db.txnTokens[token] = time.Now().Add(txnTokenTTL)
}

// transactReplicationPayload holds the data needed to replicate a single committed
// transactional write to global table replica regions.
type transactReplicationPayload struct {
	tableName       string
	globalTableName string
	region          string
	item            map[string]any
	op              string
}

// collectTransactReplicationPayloads collects per-item replication payloads from committed
// transactional writes. Must be called while the table write locks are held.
func (db *InMemoryDB) collectTransactReplicationPayloads(
	tables map[string]*Table,
	currentRegion string,
	items []types.TransactWriteItem,
) []transactReplicationPayload {
	var payloads []transactReplicationPayload

	for _, ti := range items {
		switch {
		case ti.Put != nil:
			tableName := aws.ToString(ti.Put.TableName)
			table, ok := tables[tableName]
			if !ok || table.GlobalTableName == "" {
				continue
			}

			wireItem := models.FromSDKItem(ti.Put.Item)
			payloads = append(payloads, transactReplicationPayload{
				tableName:       tableName,
				globalTableName: table.GlobalTableName,
				region:          currentRegion,
				item:            deepCopyItem(wireItem),
				op:              "PUT",
			})

		case ti.Delete != nil:
			tableName := aws.ToString(ti.Delete.TableName)
			table, ok := tables[tableName]
			if !ok || table.GlobalTableName == "" {
				continue
			}

			wireKey := models.FromSDKItem(ti.Delete.Key)
			payloads = append(payloads, transactReplicationPayload{
				tableName:       tableName,
				globalTableName: table.GlobalTableName,
				region:          currentRegion,
				item:            deepCopyItem(wireKey),
				op:              replicationOpDelete,
			})

		case ti.Update != nil:
			tableName := aws.ToString(ti.Update.TableName)
			table, ok := tables[tableName]
			if !ok || table.GlobalTableName == "" {
				continue
			}

			wireKey := models.FromSDKItem(ti.Update.Key)
			pkDef, skDef := getPKAndSK(table.KeySchema)
			finalItem := db.lookupItem(table, wireKey, pkDef.AttributeName, skDef.AttributeName)

			if finalItem == nil {
				continue
			}

			payloads = append(payloads, transactReplicationPayload{
				tableName:       tableName,
				globalTableName: table.GlobalTableName,
				region:          currentRegion,
				item:            deepCopyItem(finalItem),
				op:              "PUT",
			})
		}
	}

	return payloads
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

	committed, inProgress := checkAndMarkTransactTokenLocked(db, token)

	switch {
	case committed:
		return true, &dynamodb.TransactWriteItemsOutput{}, noop, nil
	case inProgress:
		return true, nil, noop, NewTransactionInProgressException(
			"A transaction with the given request token is currently in progress",
		)
	}

	cleanup := func() {
		deleteTransactPendingLocked(db, token)
	}

	return false, nil, cleanup, nil
}

// checkAndMarkTransactTokenLocked checks whether token is already committed or
// in-progress and, if neither, marks it in-progress, all under a single
// defer-protected db.mu.Lock (so the check-then-mark stays atomic).
func checkAndMarkTransactTokenLocked(db *InMemoryDB, token string) (bool, bool) {
	db.mu.Lock("TransactWriteItems.tokenCheck")
	defer db.mu.Unlock()

	expiry, exists := db.txnTokens[token]
	committed := exists && time.Now().Before(expiry)
	_, inProgress := db.txnPending[token]

	if !committed && !inProgress {
		db.txnPending[token] = time.Now()
	}

	return committed, inProgress
}

// deleteTransactPendingLocked removes token from db.txnPending under a
// defer-protected db.mu.Lock.
func deleteTransactPendingLocked(db *InMemoryDB, token string) {
	db.mu.Lock("TransactWriteItems.tokenCleanup")
	defer db.mu.Unlock()

	delete(db.txnPending, token)
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
			logger.Load(ctx).
				ErrorContext(ctx, "Transaction failed during apply phase, rolling back",
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

	if err := validateTransactItemCount(len(input.TransactItems), "TransactGetItems"); err != nil {
		return nil, err
	}

	tableNames := make([]string, 0, len(input.TransactItems))
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
		Responses: responses,
		ConsumedCapacity: transactReadConsumedCapacity(
			input.ReturnConsumedCapacity,
			input.TransactItems,
		),
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
		return types.ItemResponse{}, NewResourceNotFoundException("Table not found: " + tableName)
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

	names := collections.SortedKeys(seen)

	return names
}

// resolveTransactTablesRLocked resolves tableNames to their *Table pointers in
// region under a defer-protected db.mu.RLock, using op as the lock's metrics
// label. Returns ResourceNotFoundException if the region has no tables or any
// name doesn't resolve.
func (db *InMemoryDB) resolveTransactTablesRLocked(
	region string,
	tableNames []string,
	op string,
) (map[string]*Table, error) {
	db.mu.RLock(op)
	defer db.mu.RUnlock()

	if len(db.tablesByRegion.Get(region)) == 0 {
		return nil, NewResourceNotFoundException("Table not found in region " + region)
	}

	tables := make(map[string]*Table, len(tableNames))
	for _, name := range tableNames {
		t, ok := db.tables.Get(tableKey(region, name))
		if !ok {
			return nil, NewResourceNotFoundException("Table not found: " + name)
		}
		tables[name] = t
	}

	return tables, nil
}

func (db *InMemoryDB) lockTablesWrite(
	ctx context.Context,
	tableNames []string,
) (map[string]*Table, error) {
	region := getRegionFromContext(ctx, db)

	tables, err := db.resolveTransactTablesRLocked(region, tableNames, "TransactWriteItems")
	if err != nil {
		return nil, err
	}

	for _, name := range tableNames {
		tables[name].mu.Lock("TransactWriteItems")
	}

	return tables, nil
}

func (db *InMemoryDB) lockTablesRead(
	ctx context.Context,
	tableNames []string,
) (map[string]*Table, error) {
	region := getRegionFromContext(ctx, db)

	tables, err := db.resolveTransactTablesRLocked(region, tableNames, "TransactGetItems")
	if err != nil {
		return nil, err
	}

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
			// item is already in DynamoDB wire form ({"attr":{"S":...}}), which is the
			// shape AWS returns in CancellationReasons[].Item. Marshalling the smithy SDK
			// union types instead would emit {"Value":...} and break SDK parsing.
			reason.Item = item
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
		oldItem, matchIndex := db.findMatchForPut(table, wireItem)
		db.doPut(table, wireItem, matchIndex)
		// Capture stream event for the committed transactional write.
		if matchIndex != -1 {
			table.appendStreamRecord(streamEventModify, oldItem, deepCopyItem(wireItem), "", "")
		} else {
			table.appendStreamRecord(streamEventInsert, nil, deepCopyItem(wireItem), "", "")
		}

	case ti.Delete != nil:
		table := tables[aws.ToString(ti.Delete.TableName)]
		wireKey := models.FromSDKItem(ti.Delete.Key)
		oldItem, matchIndex := db.findMatchForPut(table, wireKey)
		if matchIndex != -1 {
			// Capture stream event (REMOVE) before the item is removed.
			table.appendStreamRecord(streamEventRemove, deepCopyItem(oldItem), nil, "", "")
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

		updated, _, err := db.doUpdate(ctx, table, dummyInput, oldItem, matchIndex)
		if err != nil {
			return err
		}
		// Capture stream event for the committed transactional update.
		if matchIndex != -1 {
			table.appendStreamRecord(
				streamEventModify, deepCopyItem(oldItem), deepCopyItem(updated), "", "",
			)
		} else {
			table.appendStreamRecord(streamEventInsert, nil, deepCopyItem(updated), "", "")
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

		// Snapshot itemSizes alongside Items so rollback restores the
		// len(itemSizes) == len(Items) invariant (and accurate size accounting).
		itemSizesCopy := make([]int, len(t.itemSizes))
		copy(itemSizesCopy, t.itemSizes)

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
			items:              itemsCopy,
			itemSizes:          itemSizesCopy,
			totalItemSizeBytes: t.totalItemSizeBytes,
			pkIndex:            pkIdxCopy,
			pkskIndex:          pkskIdxCopy,
		}
	}

	return snapshots
}

func (db *InMemoryDB) rollbackTables(
	tables map[string]*Table,
	snapshots map[string]tableStateSnapshot,
) {
	for name, t := range tables {
		if s, ok := snapshots[name]; ok {
			t.Items = s.items
			t.itemSizes = s.itemSizes
			t.totalItemSizeBytes = s.totalItemSizeBytes
			t.pkIndex = s.pkIndex
			t.pkskIndex = s.pkskIndex
		}
	}
}
