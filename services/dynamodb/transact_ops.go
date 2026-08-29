package dynamodb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	gsiIndexes         map[string]*secondaryIndex
	lsiIndexes         map[string]*secondaryIndex
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
	done, out, cleanupToken, err := db.checkTransactToken(token, hashTransactWriteItems(input.TransactItems))
	if done {
		return out, err
	}
	defer cleanupToken()

	tableNames := db.transactTableNames(input.TransactItems)
	region := getRegionFromContext(ctx, db)

	payloads, itemMetrics, applyErr := db.executeTransactWrite(ctx, tableNames, token, region, input)
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
		ItemCollectionMetrics: itemMetrics,
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
) ([]transactReplicationPayload, map[string][]types.ItemCollectionMetrics, error) {
	tables, lockErr := db.lockTablesWrite(ctx, tableNames)
	if lockErr != nil {
		return nil, nil, lockErr
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
		return nil, nil, dupErr
	}

	// Enforce throughput per table before any condition is checked or write applied.
	// PAY_PER_REQUEST tables bypass throttling.
	if thrErr := db.enforceTransactWriteThroughput(region, tables, input.TransactItems); thrErr != nil {
		return nil, nil, thrErr
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
		return nil, nil, NewTransactionCanceledException(txCancelPrefix, reasons)
	}

	// Phase 2: Apply writes with rollback on failure.
	itemMetrics, writeErr := db.applyTransactItems(
		ctx, tables, input.TransactItems, input.ReturnItemCollectionMetrics,
	)
	if writeErr != nil {
		return nil, nil, writeErr
	}

	payloads := db.collectTransactReplicationPayloads(tables, region, input.TransactItems)

	// Release the table locks before ever touching db.mu (see releaseTables'
	// doc above), then record the token as committed now that all writes have
	// been applied.
	releaseTables()

	if token != "" {
		commitTransactTokenLocked(db, token, hashTransactWriteItems(input.TransactItems))
	}

	return payloads, itemMetrics, nil
}

// commitTransactTokenLocked records token as committed (with its TTL expiry
// and request hash) under a defer-protected db.mu.Lock.
func commitTransactTokenLocked(db *InMemoryDB, token, hash string) {
	db.mu.Lock("TransactWriteItems.tokenCommit")
	defer db.mu.Unlock()

	db.txnTokens[token] = txnTokenRecord{expiry: time.Now().Add(txnTokenTTL), hash: hash}
}

// hashTransactWriteItems returns a stable fingerprint of a TransactWriteItems
// request, used to detect ClientRequestToken reuse with a different request
// (AWS DynamoDB's IdempotentParameterMismatchException). JSON-encoding
// preserves TransactItems' slice order and each Go struct's fixed field
// order, so the same request always hashes the same way.
func hashTransactWriteItems(items []types.TransactWriteItem) string {
	b, err := json.Marshal(items)
	if err != nil {
		return ""
	}

	sum := sha256.Sum256(b)

	return hex.EncodeToString(sum[:])
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

// checkTransactToken checks idempotency token state. hash is the caller's
// request fingerprint (see hashTransactWriteItems) -- reusing a committed
// token with a different hash is a real AWS DynamoDB error
// (IdempotentParameterMismatchException), not a matching replay.
// Returns (true, output, cleanup, err) if the caller should return immediately,
// or (false, nil, cleanup, nil) if the transaction should proceed.
// When proceeding, the cleanup func removes the token from the pending map and
// must be called via defer in the caller.
func (db *InMemoryDB) checkTransactToken(
	token, hash string,
) (bool, *dynamodb.TransactWriteItemsOutput, func(), error) {
	noop := func() {}
	if token == "" {
		return false, nil, noop, nil
	}

	committed, mismatched, inProgress := checkAndMarkTransactTokenLocked(db, token, hash)

	switch {
	case mismatched:
		return true, nil, noop, NewIdempotentParameterMismatchException(
			"the request parameters do not match a previous request with the given ClientRequestToken",
		)
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

// checkAndMarkTransactTokenLocked checks whether token is already committed
// (and if so, whether hash matches the request that committed it) or
// in-progress, and if neither, marks it in-progress, all under a single
// defer-protected db.mu.Lock (so the check-then-mark stays atomic).
func checkAndMarkTransactTokenLocked(db *InMemoryDB, token, hash string) (bool, bool, bool) {
	db.mu.Lock("TransactWriteItems.tokenCheck")
	defer db.mu.Unlock()

	rec, exists := db.txnTokens[token]
	committed := exists && time.Now().Before(rec.expiry)
	mismatched := committed && rec.hash != hash
	_, inProgress := db.txnPending[token]

	if !committed && !inProgress {
		db.txnPending[token] = time.Now()
	}

	return committed && !mismatched, mismatched, inProgress
}

// deleteTransactPendingLocked removes token from db.txnPending under a
// defer-protected db.mu.Lock.
func deleteTransactPendingLocked(db *InMemoryDB, token string) {
	db.mu.Lock("TransactWriteItems.tokenCleanup")
	defer db.mu.Unlock()

	delete(db.txnPending, token)
}

// transactItemMetric pairs a single committed write's ItemCollectionMetrics with
// the table it belongs to, so applyTransactItems can group them by table for the
// response's map[string][]types.ItemCollectionMetrics shape.
type transactItemMetric struct {
	tableName string
	metric    types.ItemCollectionMetrics
}

// applyTransactItems applies write items atomically, rolling back on any failure.
// Returns per-table ItemCollectionMetrics for the items actually written, when rim
// requests them.
func (db *InMemoryDB) applyTransactItems(
	ctx context.Context,
	tables map[string]*Table,
	items []types.TransactWriteItem,
	rim types.ReturnItemCollectionMetrics,
) (map[string][]types.ItemCollectionMetrics, error) {
	snapshots := db.snapshotTables(tables)
	metrics := make(map[string][]types.ItemCollectionMetrics)

	for i, ti := range items {
		m, err := db.applyTransactWrite(ctx, tables, ti, rim)
		if err != nil {
			logger.Load(ctx).
				ErrorContext(ctx, "Transaction failed during apply phase, rolling back",
					"error", err,
					"itemIndex", i)
			db.rollbackTables(tables, snapshots)

			return nil, err
		}
		if m != nil {
			metrics[m.tableName] = append(metrics[m.tableName], m.metric)
		}
	}

	return metrics, nil
}

// enforceTransactWriteThroughput charges each involved table's WCU bucket, one unit
// per write action targeting it (matching transactWriteConsumedCapacity's per-table
// count), before any condition check or write is applied. tables' locks are already
// held by the caller, so table.BillingMode is read directly. Real DynamoDB returns
// ProvisionedThroughputExceededException from TransactWriteItems exactly as it does
// from PutItem; without this, transactions silently bypassed throttling that every
// other write path enforces.
func (db *InMemoryDB) enforceTransactWriteThroughput(
	region string,
	tables map[string]*Table,
	items []types.TransactWriteItem,
) error {
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

	for name, n := range perTable {
		table := tables[name]
		if isOnDemandTable(table.BillingMode) {
			continue
		}

		if err := db.throttler.ConsumeWrite(throttleKey(region, name), float64(n)); err != nil {
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

	region := getRegionFromContext(ctx, db)
	if thrErr := db.enforceTransactReadThroughput(region, tables, input.TransactItems); thrErr != nil {
		return nil, thrErr
	}

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
	pkVal := BuildKeyStringFromSDK(ti.Get.Key, pkDef.AttributeName)
	var skVal string
	if skDef.AttributeName != "" {
		skVal = BuildKeyStringFromSDK(ti.Get.Key, skDef.AttributeName)
	}
	item := db.lookupItemByKeys(table, pkVal, skVal)

	if item == nil || isItemExpired(item, table.TTLAttribute) {
		return types.ItemResponse{}, nil
	}

	result := item
	proj := aws.ToString(ti.Get.ProjectionExpression)
	if proj != "" {
		var projErr error

		result, projErr = projectItem(item, proj, ti.Get.ExpressionAttributeNames)
		if projErr != nil {
			return types.ItemResponse{}, projErr
		}
	}

	sdkResult, _ := models.ToSDKItem(result)

	return types.ItemResponse{Item: sdkResult}, nil
}

// enforceTransactReadThroughput charges each involved table's RCU bucket before any
// item is read, using the same 0.5-RCU-per-read formula as transactReadConsumedCapacity.
// tables' locks are already held (read) by the caller. Real DynamoDB returns
// ProvisionedThroughputExceededException from TransactGetItems exactly as it does from
// GetItem; without this, transactional reads silently bypassed throttling that every
// other read path enforces.
func (db *InMemoryDB) enforceTransactReadThroughput(
	region string,
	tables map[string]*Table,
	items []types.TransactGetItem,
) error {
	const rcuPerRead = 0.5

	perTable := make(map[string]int)
	for _, ti := range items {
		if ti.Get != nil {
			perTable[aws.ToString(ti.Get.TableName)]++
		}
	}

	for name, n := range perTable {
		table := tables[name]
		if isOnDemandTable(table.BillingMode) {
			continue
		}

		cu := float64(n) * rcuPerRead

		if err := db.throttler.ConsumeRead(throttleKey(region, name), cu); err != nil {
			return err
		}
	}

	return nil
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

// lsiCollectionMetricFor returns the ItemCollectionMetrics for tableName/table given
// its already-current (post-write) collectionBytes, or nil when rim doesn't request
// metrics or the table has no LSI. Shared by the transactional put/delete/update
// paths below.
func lsiCollectionMetricFor(
	table *Table,
	tableName string,
	rim types.ReturnItemCollectionMetrics,
	itemKey map[string]types.AttributeValue,
	collectionBytes int64,
) *transactItemMetric {
	if rim != types.ReturnItemCollectionMetricsSize || len(table.LocalSecondaryIndexes) == 0 {
		return nil
	}

	m := buildItemCollectionMetrics(table, rim, pkOnlyKey(table, itemKey), collectionBytes)
	if m == nil {
		return nil
	}

	return &transactItemMetric{tableName: tableName, metric: *m}
}

func (db *InMemoryDB) applyTransactPut(
	table *Table,
	tableName string,
	put *types.Put,
	rim types.ReturnItemCollectionMetrics,
) (*transactItemMetric, error) {
	wireItem := models.FromSDKItem(put.Item)
	if err := db.validateItem(wireItem, table); err != nil {
		return nil, err
	}

	oldItem, matchIndex := db.findMatchForPut(table, wireItem)

	var metric *transactItemMetric
	if rim == types.ReturnItemCollectionMetricsSize && len(table.LocalSecondaryIndexes) > 0 {
		pkDef, _ := getPKAndSK(table.KeySchema)
		pkVal := BuildKeyString(wireItem, pkDef.AttributeName)
		collectionBytes := computeLSICollectionSize(table, pkVal, wireItem, matchIndex)
		metric = lsiCollectionMetricFor(table, tableName, rim, put.Item, collectionBytes)
	}

	db.doPut(table, wireItem, matchIndex)
	// Capture stream event for the committed transactional write.
	if matchIndex != -1 {
		table.appendStreamRecord(streamEventModify, oldItem, wireItem, "", "")
	} else {
		table.appendStreamRecord(streamEventInsert, nil, wireItem, "", "")
	}

	return metric, nil
}

func (db *InMemoryDB) applyTransactDelete(
	table *Table,
	tableName string,
	del *types.Delete,
	rim types.ReturnItemCollectionMetrics,
) (*transactItemMetric, error) {
	wireKey := models.FromSDKItem(del.Key)
	oldItem, matchIndex := db.findMatchForPut(table, wireKey)
	if matchIndex == -1 {
		return nil, nil //nolint:nilnil // no matching item: nothing to delete, nothing to report
	}

	var metric *transactItemMetric
	if rim == types.ReturnItemCollectionMetricsSize && len(table.LocalSecondaryIndexes) > 0 {
		pkDef, _ := getPKAndSK(table.KeySchema)
		pkVal := BuildKeyString(wireKey, pkDef.AttributeName)
		remaining := currentLSICollectionBytes(table, pkVal) - int64(table.itemSizes[matchIndex])
		metric = lsiCollectionMetricFor(table, tableName, rim, del.Key, remaining)
	}

	// Capture stream event (REMOVE) before the item is removed.
	table.appendStreamRecord(streamEventRemove, oldItem, nil, "", "")
	db.deleteItemAtIndex(table, matchIndex)

	return metric, nil
}

func (db *InMemoryDB) applyTransactUpdate(
	ctx context.Context,
	table *Table,
	tableName string,
	upd *types.Update,
	rim types.ReturnItemCollectionMetrics,
) (*transactItemMetric, error) {
	wireKey := models.FromSDKItem(upd.Key)
	oldItem, matchIndex := db.findMatchForPut(table, wireKey)

	dummyInput := &dynamodb.UpdateItemInput{
		Key:                       upd.Key,
		TableName:                 upd.TableName,
		UpdateExpression:          upd.UpdateExpression,
		ExpressionAttributeNames:  upd.ExpressionAttributeNames,
		ExpressionAttributeValues: upd.ExpressionAttributeValues,
	}

	updated, _, err := db.doUpdate(ctx, table, dummyInput, oldItem, matchIndex)
	if err != nil {
		return nil, err
	}

	// The item's post-write state is already committed to table.Items by doUpdate,
	// so the collection's current bytes already reflect this write.
	var metric *transactItemMetric
	if rim == types.ReturnItemCollectionMetricsSize && len(table.LocalSecondaryIndexes) > 0 {
		pkDef, _ := getPKAndSK(table.KeySchema)
		pkVal := BuildKeyString(updated, pkDef.AttributeName)
		metric = lsiCollectionMetricFor(table, tableName, rim, upd.Key, currentLSICollectionBytes(table, pkVal))
	}

	// Capture stream event for the committed transactional update.
	if matchIndex != -1 {
		table.appendStreamRecord(
			streamEventModify, oldItem, updated, "", "",
		)
	} else {
		table.appendStreamRecord(streamEventInsert, nil, updated, "", "")
	}

	return metric, nil
}

func (db *InMemoryDB) applyTransactWrite(
	ctx context.Context,
	tables map[string]*Table,
	ti types.TransactWriteItem,
	rim types.ReturnItemCollectionMetrics,
) (*transactItemMetric, error) {
	switch {
	case ti.Put != nil:
		tableName := aws.ToString(ti.Put.TableName)

		return db.applyTransactPut(tables[tableName], tableName, ti.Put, rim)

	case ti.Delete != nil:
		tableName := aws.ToString(ti.Delete.TableName)

		return db.applyTransactDelete(tables[tableName], tableName, ti.Delete, rim)

	case ti.Update != nil:
		tableName := aws.ToString(ti.Update.TableName)

		return db.applyTransactUpdate(ctx, tables[tableName], tableName, ti.Update, rim)
	}

	return nil, nil //nolint:nilnil // ConditionCheck-only item: no write applied, nothing to report
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
			gsiIndexes:         copySecondaryIndexMap(t.gsiIndexes),
			lsiIndexes:         copySecondaryIndexMap(t.lsiIndexes),
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
			t.gsiIndexes = s.gsiIndexes
			t.lsiIndexes = s.lsiIndexes
		}
	}
}
