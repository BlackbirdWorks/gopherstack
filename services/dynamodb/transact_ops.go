package dynamodb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

const txCancelPrefix = "Transaction cancelled, please refer cancellation reasons for specific reasons"

// cancellationReasonNone is the CancellationReason.Code for a transaction
// statement/item that did not itself fail.
const cancellationReasonNone = "None"

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

	execResult, applyErr := db.executeTransactWrite(ctx, tableNames, token, region, input)
	if applyErr != nil {
		return nil, applyErr
	}

	for _, p := range execResult.payloads {
		db.replicateItemMutation(p.tableName, p.globalTableName, p.region, p.item, p.op)
	}

	out = &dynamodb.TransactWriteItemsOutput{
		ConsumedCapacity: transactWriteConsumedCapacity(
			input.ReturnConsumedCapacity,
			input.TransactItems,
			execResult.gsiWCUByTable,
			execResult.lsiWCUByTable,
		),
		ItemCollectionMetrics: execResult.itemMetrics,
	}

	return out, nil
}

// transactWriteExecResult is executeTransactWrite's return: the committed
// writes' replication payloads, per-table ItemCollectionMetrics, and (when
// INDEXES was requested) each table's per-GSI/per-LSI WCU.
type transactWriteExecResult struct {
	itemMetrics   map[string][]types.ItemCollectionMetrics
	gsiWCUByTable map[string]map[string]float64
	lsiWCUByTable map[string]map[string]float64
	payloads      []transactReplicationPayload
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
) (transactWriteExecResult, error) {
	tables, lockErr := db.lockTablesWrite(ctx, tableNames)
	if lockErr != nil {
		return transactWriteExecResult{}, lockErr
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
		return transactWriteExecResult{}, dupErr
	}

	// Enforce throughput per table before any condition is checked or write applied.
	// PAY_PER_REQUEST tables bypass throttling.
	if thrErr := db.enforceTransactWriteThroughput(region, tables, input.TransactItems); thrErr != nil {
		return transactWriteExecResult{}, thrErr
	}

	// Phase 1: Check conditions.
	reasons := make([]CancellationReason, len(input.TransactItems))
	for i := range reasons {
		reasons[i] = CancellationReason{Code: cancellationReasonNone}
	}

	canceled := false
	for i, ti := range input.TransactItems {
		if condErr := db.checkTransactWriteCondition(ctx, tables, ti, i, reasons); condErr != nil {
			canceled = true
		}
	}

	if canceled {
		return transactWriteExecResult{}, NewTransactionCanceledException(txCancelPrefix, reasons)
	}

	// Phase 2: Apply writes with rollback on failure.
	wantIndexes := input.ReturnConsumedCapacity == types.ReturnConsumedCapacityIndexes
	applyResult, writeErr := db.applyTransactItems(
		ctx, tables, input.TransactItems, input.ReturnItemCollectionMetrics, wantIndexes,
	)
	if writeErr != nil {
		return transactWriteExecResult{}, writeErr
	}

	payloads := db.collectTransactReplicationPayloads(tables, region, input.TransactItems)

	// Release the table locks before ever touching db.mu (see releaseTables'
	// doc above), then record the token as committed now that all writes have
	// been applied.
	releaseTables()

	if token != "" {
		commitTransactTokenLocked(db, token, hashTransactWriteItems(input.TransactItems))
	}

	return transactWriteExecResult{
		payloads:      payloads,
		itemMetrics:   applyResult.itemMetrics,
		gsiWCUByTable: applyResult.gsiWCUByTable,
		lsiWCUByTable: applyResult.lsiWCUByTable,
	}, nil
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

// transactApplyResult is applyTransactItems' return: per-table
// ItemCollectionMetrics for the items actually written (when rim requests
// them), plus (when wantIndexes) each table's per-GSI/per-LSI WCU summed
// across every write action that targeted it.
type transactApplyResult struct {
	itemMetrics   map[string][]types.ItemCollectionMetrics
	gsiWCUByTable map[string]map[string]float64
	lsiWCUByTable map[string]map[string]float64
}

// applyTransactItems prepares (validates + computes, can fail, touches no table)
// then commits (infallible by construction) every write item. Since prepare runs
// entirely before commit, no rollback is ever needed: a failure surfaces before
// any table is touched.
func (db *InMemoryDB) applyTransactItems(
	ctx context.Context,
	tables map[string]*Table,
	items []types.TransactWriteItem,
	rim types.ReturnItemCollectionMetrics,
	wantIndexes bool,
) (transactApplyResult, error) {
	prepared, err := db.prepareTransactWrites(ctx, tables, items)
	if err != nil {
		return transactApplyResult{}, err
	}

	result := transactApplyResult{
		itemMetrics:   make(map[string][]types.ItemCollectionMetrics),
		gsiWCUByTable: make(map[string]map[string]float64),
		lsiWCUByTable: make(map[string]map[string]float64),
	}

	for i, p := range prepared {
		w := db.commitTransactWrite(tables, p, rim, wantIndexes)
		if w.metric != nil {
			result.itemMetrics[w.metric.tableName] = append(result.itemMetrics[w.metric.tableName], w.metric.metric)
		}
		if wantIndexes {
			tableName := transactWriteItemTableName(items[i])
			result.gsiWCUByTable[tableName] = mergeWCUMap(result.gsiWCUByTable[tableName], w.gsiWCU)
			result.lsiWCUByTable[tableName] = mergeWCUMap(result.lsiWCUByTable[tableName], w.lsiWCU)
		}
	}

	return result, nil
}

// preparedTransactWrite is one TransactWriteItem's already-validated, already-computed
// write, ready to commit. Exactly one field is non-nil (or none, for a
// ConditionCheck-only item, which writes nothing).
type preparedTransactWrite struct {
	put    *preparedTransactPut
	del    *preparedTransactDelete
	update *preparedTransactUpdate
}

type preparedTransactPut struct {
	action    *types.Put
	wireItem  map[string]any
	tableName string
}

type preparedTransactDelete struct {
	action    *types.Delete
	wireKey   map[string]any
	tableName string
}

type preparedTransactUpdate struct {
	action       *types.Update
	updated      map[string]any
	updatedPaths map[string]struct{}
	tableName    string
}

// prepareTransactWrites validates and computes every Put/Update/Delete in items
// against current table state, in order, without mutating any table. It returns
// the first error it hits, in the same order applyTransactItems used to hit it
// during apply before this split -- e.g. an out-of-size Put still fails at its own
// index, not earlier or later.
func (db *InMemoryDB) prepareTransactWrites(
	ctx context.Context,
	tables map[string]*Table,
	items []types.TransactWriteItem,
) ([]preparedTransactWrite, error) {
	prepared := make([]preparedTransactWrite, len(items))

	for i, ti := range items {
		p, err := db.prepareTransactWrite(ctx, tables, ti)
		if err != nil {
			return nil, err
		}
		prepared[i] = p
	}

	return prepared, nil
}

func (db *InMemoryDB) prepareTransactWrite(
	ctx context.Context,
	tables map[string]*Table,
	ti types.TransactWriteItem,
) (preparedTransactWrite, error) {
	switch {
	case ti.Put != nil:
		return db.prepareTransactPut(tables, ti.Put)
	case ti.Delete != nil:
		return preparedTransactWrite{del: prepareTransactDelete(ti.Delete)}, nil
	case ti.Update != nil:
		return db.prepareTransactUpdate(ctx, tables, ti.Update)
	}

	// ConditionCheck-only item: nothing to prepare or write.
	return preparedTransactWrite{}, nil
}

// prepareTransactPut validates the item against its table's schema/size rules --
// the only way a Put can fail (see applyTransactPut's former behaviour). matchIndex
// is deliberately NOT resolved here: an earlier Delete in the same transaction can
// swap-move another item's slot before this Put commits, so the index must be
// re-resolved fresh at commit time.
func (db *InMemoryDB) prepareTransactPut(
	tables map[string]*Table,
	put *types.Put,
) (preparedTransactWrite, error) {
	tableName := aws.ToString(put.TableName)
	wireItem := models.FromSDKItem(put.Item)

	if err := db.validateItem(wireItem, tables[tableName]); err != nil {
		return preparedTransactWrite{}, err
	}

	return preparedTransactWrite{
		put: &preparedTransactPut{tableName: tableName, action: put, wireItem: wireItem},
	}, nil
}

// prepareTransactDelete is infallible today (see the design audit on
// gopherstack-wdapu: a missing key is a silent no-op, matching applyTransactDelete's
// former behaviour), so it has no error return.
func prepareTransactDelete(del *types.Delete) *preparedTransactDelete {
	return &preparedTransactDelete{
		tableName: aws.ToString(del.TableName),
		action:    del,
		wireKey:   models.FromSDKItem(del.Key),
	}
}

// prepareTransactUpdate computes the updated item via computeUpdate, which is the
// only way an Update can fail (parse/eval errors, or the updated item failing
// validation). existing is only used to build "updated" via the pure merge in
// computeUpdate; matchIndex is re-resolved fresh at commit time for the same
// swap-move reason as prepareTransactPut.
func (db *InMemoryDB) prepareTransactUpdate(
	ctx context.Context,
	tables map[string]*Table,
	upd *types.Update,
) (preparedTransactWrite, error) {
	tableName := aws.ToString(upd.TableName)
	table := tables[tableName]
	wireKey := models.FromSDKItem(upd.Key)
	existing, _ := db.findMatchForPut(table, wireKey)

	dummyInput := &dynamodb.UpdateItemInput{
		Key:                       upd.Key,
		TableName:                 upd.TableName,
		UpdateExpression:          upd.UpdateExpression,
		ExpressionAttributeNames:  upd.ExpressionAttributeNames,
		ExpressionAttributeValues: upd.ExpressionAttributeValues,
	}

	updated, updatedPaths, err := db.computeUpdate(ctx, table, dummyInput, existing)
	if err != nil {
		return preparedTransactWrite{}, err
	}

	return preparedTransactWrite{
		update: &preparedTransactUpdate{
			tableName: tableName, action: upd, updated: updated, updatedPaths: updatedPaths,
		},
	}, nil
}

// commitTransactWrite dispatches a prepared write to its commit function. Every
// path is infallible by construction: everything that could fail already failed
// (or would have) during prepare.
func (db *InMemoryDB) commitTransactWrite(
	tables map[string]*Table,
	p preparedTransactWrite,
	rim types.ReturnItemCollectionMetrics,
	wantIndexes bool,
) transactSingleWriteResult {
	switch {
	case p.put != nil:
		return db.commitTransactPut(tables[p.put.tableName], p.put, rim, wantIndexes)
	case p.del != nil:
		return db.commitTransactDelete(tables[p.del.tableName], p.del, rim, wantIndexes)
	case p.update != nil:
		return db.commitTransactUpdate(tables[p.update.tableName], p.update, rim, wantIndexes)
	}

	return transactSingleWriteResult{}
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

// transactWriteConsumedCapacity builds per-table ConsumedCapacity for
// TransactWriteItems. Like BatchWriteItem, its Put/Update/Delete actions write
// to every GSI/LSI whose key attributes they populate, so INDEXES populates
// .GlobalSecondaryIndexes/.LocalSecondaryIndexes (dynamodb SDK
// api_op_TransactWriteItems.go:112-120, which -- unlike TransactGetItems --
// lists no base-table-only carve-out).
func transactWriteConsumedCapacity(
	req types.ReturnConsumedCapacity,
	items []types.TransactWriteItem,
	gsiWCUByTable, lsiWCUByTable map[string]map[string]float64,
) []types.ConsumedCapacity {
	if req == "" || req == types.ReturnConsumedCapacityNone {
		return nil
	}

	// Count write operations per table for accurate WCU reporting.
	perTable := make(map[string]int)
	for _, ti := range items {
		if name := transactWriteItemTableName(ti); name != "" {
			perTable[name]++
		}
	}

	tableNames := collections.SortedKeys(perTable)
	caps := make([]types.ConsumedCapacity, 0, len(tableNames))

	for _, name := range tableNames {
		cu := float64(perTable[name])
		caps = append(caps, *buildConsumedCapacityWithIndexes(
			name, req,
			0, cu,
			nil, gsiWCUByTable[name],
			nil, lsiWCUByTable[name],
		))
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

// transactWriteActionWCU is the WCU a single Put/Delete/Update transact-write
// action charges, matching enforceTransactWriteThroughput/
// transactWriteConsumedCapacity's existing "one unit per write action" model
// (the mock doesn't size-cost individual transact writes).
const transactWriteActionWCU = 1.0

// transactSingleWriteResult is one apply*'s return: the ItemCollectionMetrics
// entry when rim requested it, plus (when wantIndexes) the per-GSI/per-LSI WCU
// that single write action consumed.
type transactSingleWriteResult struct {
	metric *transactItemMetric
	gsiWCU map[string]float64
	lsiWCU map[string]float64
}

// commitTransactPut writes an already-validated put (see prepareTransactPut). It
// re-resolves oldItem/matchIndex fresh against table's current state rather than
// reusing anything computed during prepare: an earlier Delete in this same
// transaction may have swap-moved another item into this key's old slot, or (since
// keys are unique within one TransactWriteItems call, per validateTransactWriteItems)
// this key's own slot is otherwise unaffected -- either way, this lookup is O(1) and
// always correct, unlike a stale precomputed index would be.
func (db *InMemoryDB) commitTransactPut(
	table *Table,
	p *preparedTransactPut,
	rim types.ReturnItemCollectionMetrics,
	wantIndexes bool,
) transactSingleWriteResult {
	oldItem, matchIndex := db.findMatchForPut(table, p.wireItem)

	var metric *transactItemMetric
	if rim == types.ReturnItemCollectionMetricsSize && len(table.LocalSecondaryIndexes) > 0 {
		pkDef, _ := getPKAndSK(table.KeySchema)
		pkVal := BuildKeyString(p.wireItem, pkDef.AttributeName)
		collectionBytes := computeLSICollectionSize(table, pkVal, p.wireItem, matchIndex)
		metric = lsiCollectionMetricFor(table, p.tableName, rim, p.action.Item, collectionBytes)
	}

	db.doPut(table, p.wireItem, matchIndex)
	if matchIndex != -1 {
		table.appendStreamRecord(streamEventModify, oldItem, p.wireItem, "", "")
	} else {
		table.appendStreamRecord(streamEventInsert, nil, p.wireItem, "", "")
	}

	result := transactSingleWriteResult{metric: metric}
	if wantIndexes {
		result.gsiWCU, result.lsiWCU = calculateWriteIndexBreakdowns(table, transactWriteActionWCU, p.wireItem)
	}

	return result
}

// commitTransactDelete removes an already-prepared delete's target. See
// commitTransactPut's doc for why matchIndex is re-resolved here rather than reused.
func (db *InMemoryDB) commitTransactDelete(
	table *Table,
	p *preparedTransactDelete,
	rim types.ReturnItemCollectionMetrics,
	wantIndexes bool,
) transactSingleWriteResult {
	oldItem, matchIndex := db.findMatchForPut(table, p.wireKey)
	if matchIndex == -1 {
		return transactSingleWriteResult{}
	}

	var metric *transactItemMetric
	if rim == types.ReturnItemCollectionMetricsSize && len(table.LocalSecondaryIndexes) > 0 {
		pkDef, _ := getPKAndSK(table.KeySchema)
		pkVal := BuildKeyString(p.wireKey, pkDef.AttributeName)
		remaining := currentLSICollectionBytes(table, pkVal) - int64(table.itemSizes[matchIndex])
		metric = lsiCollectionMetricFor(table, p.tableName, rim, p.action.Key, remaining)
	}

	table.appendStreamRecord(streamEventRemove, oldItem, nil, "", "")
	db.deleteItemAtIndex(table, matchIndex)

	result := transactSingleWriteResult{metric: metric}
	if wantIndexes {
		result.gsiWCU, result.lsiWCU = calculateWriteIndexBreakdowns(table, transactWriteActionWCU, oldItem)
	}

	return result
}

// commitTransactUpdate writes an already-computed update's result (see
// prepareTransactUpdate) via commitUpdate. matchIndex/existing are re-resolved
// fresh here for the same reason as commitTransactPut.
func (db *InMemoryDB) commitTransactUpdate(
	table *Table,
	p *preparedTransactUpdate,
	rim types.ReturnItemCollectionMetrics,
	wantIndexes bool,
) transactSingleWriteResult {
	wireKey := models.FromSDKItem(p.action.Key)
	existing, matchIndex := db.findMatchForPut(table, wireKey)

	db.commitUpdate(table, existing, p.updated, matchIndex)

	// The item's post-write state is already committed to table.Items by
	// commitUpdate, so the collection's current bytes already reflect this write.
	var metric *transactItemMetric
	if rim == types.ReturnItemCollectionMetricsSize && len(table.LocalSecondaryIndexes) > 0 {
		pkDef, _ := getPKAndSK(table.KeySchema)
		pkVal := BuildKeyString(p.updated, pkDef.AttributeName)
		metric = lsiCollectionMetricFor(table, p.tableName, rim, p.action.Key, currentLSICollectionBytes(table, pkVal))
	}

	if matchIndex != -1 {
		table.appendStreamRecord(streamEventModify, existing, p.updated, "", "")
	} else {
		table.appendStreamRecord(streamEventInsert, nil, p.updated, "", "")
	}

	result := transactSingleWriteResult{metric: metric}
	if wantIndexes {
		// existing and p.updated are OR-alternatives (like UpdateItem's own
		// breakdown): an index write is charged once even if the item was a
		// member both before and after.
		result.gsiWCU, result.lsiWCU = calculateWriteIndexBreakdowns(table, transactWriteActionWCU, existing, p.updated)
	}

	return result
}

// transactWriteItemTableName returns the table name a TransactWriteItem
// targets, across all four action kinds (Put/Delete/Update/ConditionCheck).
func transactWriteItemTableName(ti types.TransactWriteItem) string {
	switch {
	case ti.Put != nil:
		return aws.ToString(ti.Put.TableName)
	case ti.Delete != nil:
		return aws.ToString(ti.Delete.TableName)
	case ti.Update != nil:
		return aws.ToString(ti.Update.TableName)
	case ti.ConditionCheck != nil:
		return aws.ToString(ti.ConditionCheck.TableName)
	}

	return ""
}
