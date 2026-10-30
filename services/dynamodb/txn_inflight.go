package dynamodb

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
)

// Transaction conflict handling:
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html#transaction-conflict-handling
const (
	txnConflictMessage         = "Transaction is ongoing for the item"
	cancellationReasonConflict = "TransactionConflict"
	metricTransactionConflict  = "TransactionConflict"
	chaosServiceName           = "dynamodb"
)

type itemRef struct {
	region string
	table  string
	key    string
}

// inflightItem is guarded by InMemoryDB.mu.
type inflightItem struct {
	writer *txnHandle
}

// txnTarget is one item an operation touches; attrs holds at least its key attributes.
type txnTarget struct {
	attrs map[string]types.AttributeValue
	ref   itemRef
	idx   int
}

// txnHandle is one in-flight transaction. refs is guarded by InMemoryDB.mu.
type txnHandle struct {
	db   *InMemoryDB
	op   string
	refs []itemRef
}

type txnHandleKey struct{}

// txnHookEvent is passed to InMemoryDB.txnHook while a transaction is in flight and holds no lock.
type txnHookEvent struct {
	Op   string
	Step int
}

type chaosHoldKey struct {
	ref itemRef
}

type chaosHold struct {
	until     time.Time
	remaining int
}

// txnState is the in-flight transaction registry plus chaos conflict state.
// Everything but the atomics is guarded by InMemoryDB.mu.
type txnState struct {
	inflight map[itemRef]*inflightItem
	holds    map[chaosHoldKey]chaosHold
	faults   atomic.Pointer[chaos.FaultStore]
	live     atomic.Int64
}

func newTxnState() *txnState {
	return &txnState{
		inflight: make(map[itemRef]*inflightItem),
		holds:    make(map[chaosHoldKey]chaosHold),
	}
}

func (s *txnState) reset() {
	s.inflight = make(map[itemRef]*inflightItem)
	s.holds = make(map[chaosHoldKey]chaosHold)
	s.live.Store(0)
}

// SetFaultStore wires the chaos store that drives dynamodb:transaction-conflict effect rules.
func (db *InMemoryDB) SetFaultStore(fs *chaos.FaultStore) { db.txn.faults.Store(fs) }

func (db *InMemoryDB) timeNow() time.Time {
	if db.clock != nil {
		return db.clock()
	}

	return time.Now()
}

func (db *InMemoryDB) runTxnHook(op string, step int) {
	if db.txnHook != nil {
		db.txnHook(txnHookEvent{Op: op, Step: step})
	}
}

func (db *InMemoryDB) chaosActive() bool {
	fs := db.txn.faults.Load()

	return fs != nil && !fs.Idle()
}

func txnHandleFrom(ctx context.Context) *txnHandle {
	h, _ := ctx.Value(txnHandleKey{}).(*txnHandle)

	return h
}

func (db *InMemoryDB) newTxnHandle(op string) *txnHandle {
	return &txnHandle{db: db, op: op}
}

// release clears every item the transaction holds.
func (h *txnHandle) release() {
	if h == nil {
		return
	}

	db := h.db
	db.mu.Lock("txn.release")
	defer db.mu.Unlock()

	for _, ref := range h.refs {
		e := db.txn.inflight[ref]
		if e == nil {
			continue
		}

		if e.writer == h {
			e.writer = nil
		}

		if e.writer == nil {
			delete(db.txn.inflight, ref)
		}
	}

	db.txn.live.Add(-int64(len(h.refs)))
	h.refs = nil
}

// itemKeyString joins an item's primary key values; ok is false when a key attribute is absent.
func itemKeyString(table *Table, attrs map[string]types.AttributeValue) (string, bool) {
	pkDef, skDef := getPKAndSK(table.KeySchema)

	pk := BuildKeyStringFromSDK(attrs, pkDef.AttributeName)
	if pk == "" {
		return "", false
	}

	if skDef.AttributeName == "" {
		return pk, true
	}

	sk := BuildKeyStringFromSDK(attrs, skDef.AttributeName)
	if sk == "" {
		return "", false
	}

	return pk + "\x00" + sk, true
}

func newTxnTarget(region, tableName string, table *Table, attrs map[string]types.AttributeValue) (txnTarget, bool) {
	if table == nil {
		return txnTarget{}, false
	}

	key, ok := itemKeyString(table, attrs)
	if !ok {
		return txnTarget{}, false
	}

	return txnTarget{ref: itemRef{region: region, table: tableName, key: key}, attrs: attrs}, true
}

// conflictsLocked reports a genuine overlap with an in-flight transaction. Caller holds db.mu.
func (db *InMemoryDB) conflictsLocked(ref itemRef, self *txnHandle) bool {
	e := db.txn.inflight[ref]
	if e == nil {
		return false
	}

	return e.writer != nil && e.writer != self
}

func chaosTargetMatches(t *chaos.EffectTarget, tg txnTarget) bool {
	if t == nil {
		return true
	}

	if t.Table != "" && t.Table != tg.ref.table {
		return false
	}

	for name, want := range t.Key {
		if BuildKeyStringFromSDK(tg.attrs, name) != want {
			return false
		}
	}

	return true
}

// forcedConflictLocked applies dynamodb:transaction-conflict effect rules as a phantom in-flight
// TransactWriteItems on the item. Caller holds db.mu.
func (db *InMemoryDB) forcedConflictLocked(op string, tg txnTarget) bool {
	fs := db.txn.faults.Load()
	if fs == nil || fs.Idle() {
		return false
	}

	rules := fs.MatchEffects(chaos.EffectDynamoDBTransactionConflict, chaosServiceName, op, tg.ref.region)
	for _, r := range rules {
		if !chaosTargetMatches(r.Target, tg) {
			continue
		}

		hk := chaosHoldKey{ref: tg.ref}
		if db.consumeHoldLocked(hk) {
			recordChaosActivity(fs, r, op, tg, true)

			return true
		}

		if !r.ShouldTrigger() {
			recordChaosActivity(fs, r, op, tg, false)

			continue
		}

		if r.HoldOps > 0 || r.HoldMs > 0 {
			db.txn.holds[hk] = chaosHold{
				remaining: r.HoldOps,
				until:     db.timeNow().Add(time.Duration(r.HoldMs) * time.Millisecond),
			}
		}

		recordChaosActivity(fs, r, op, tg, true)

		return true
	}

	return false
}

func recordChaosActivity(fs *chaos.FaultStore, r chaos.FaultRule, op string, tg txnTarget, triggered bool) {
	ev := chaos.ActivityEvent{
		Timestamp:   time.Now(),
		Service:     chaosServiceName,
		Operation:   op,
		Region:      tg.ref.region,
		Probability: r.EffectiveProbability(),
		Triggered:   triggered,
	}
	if triggered {
		ev.FaultApplied = r.Effect
	}

	fs.RecordActivity(ev)
}

// consumeHoldLocked reports whether an earlier forced conflict is still being held on the item.
func (db *InMemoryDB) consumeHoldLocked(hk chaosHoldKey) bool {
	h, ok := db.txn.holds[hk]
	if !ok {
		return false
	}

	byOps := h.remaining > 0
	byTime := db.timeNow().Before(h.until)

	if !byOps && !byTime {
		delete(db.txn.holds, hk)

		return false
	}

	if byOps {
		h.remaining--
		if h.remaining == 0 && !byTime {
			delete(db.txn.holds, hk)
		} else {
			db.txn.holds[hk] = h
		}
	}

	return true
}

// beginTxn atomically checks targets for conflicts and, when none conflict, marks them in flight.
// It returns the indices of the conflicting targets; on conflict nothing is registered.
func (db *InMemoryDB) beginTxn(h *txnHandle, hold bool, targets []txnTarget) []int {
	db.mu.Lock("txn.begin")
	defer db.mu.Unlock()

	var conflicts []int

	for _, tg := range targets {
		forced := db.forcedConflictLocked(h.op, tg)
		if forced || db.conflictsLocked(tg.ref, h) {
			conflicts = append(conflicts, tg.idx)
		}
	}

	if len(conflicts) > 0 {
		return conflicts
	}

	if hold {
		db.markLocked(h, targets)
	}

	return nil
}

func (db *InMemoryDB) markLocked(h *txnHandle, targets []txnTarget) {
	for _, tg := range targets {
		e := db.txn.inflight[tg.ref]
		if e == nil {
			e = &inflightItem{}
			db.txn.inflight[tg.ref] = e
		}

		if e.writer == h {
			continue
		}

		e.writer = h

		h.refs = append(h.refs, tg.ref)
		db.txn.live.Add(1)
	}
}

// checkSingleWrite rejects a PutItem/UpdateItem/DeleteItem that overlaps an in-flight write transaction
// with TransactionConflictException. Inside an ExecuteTransaction (ctx carries its handle) the statement
// is a transaction write and the item is then held by that transaction.
func (db *InMemoryDB) checkSingleWrite(
	ctx context.Context,
	op, tableName string,
	table *Table,
	attrs map[string]types.AttributeValue,
) error {
	owner := txnHandleFrom(ctx)
	if owner == nil && db.txn.live.Load() == 0 && !db.chaosActive() {
		return nil
	}

	region := getRegionFromContext(ctx, db)

	tg, ok := newTxnTarget(region, tableName, table, attrs)
	if !ok {
		return nil
	}

	opName := op
	if owner != nil {
		opName = owner.op
	}

	db.mu.Lock("txn.checkSingle")
	conflict := db.forcedConflictLocked(opName, tg) || db.conflictsLocked(tg.ref, owner)

	if !conflict && owner != nil {
		db.markLocked(owner, []txnTarget{tg})
	}
	db.mu.Unlock()

	if !conflict {
		return nil
	}

	db.emitTransactionConflict(region, tableName, 1)

	return NewTransactionConflictException(txnConflictMessage)
}

// filterBatchConflicts removes from requests the writes that overlap an in-flight transaction.
func (db *InMemoryDB) filterBatchConflicts(
	region, tableName string,
	table *Table,
	requests []types.WriteRequest,
) ([]types.WriteRequest, []types.WriteRequest) {
	targets := make([]txnTarget, len(requests))
	valid := make([]bool, len(requests))

	for i, req := range requests {
		var attrs map[string]types.AttributeValue

		switch {
		case req.PutRequest != nil:
			attrs = req.PutRequest.Item
		case req.DeleteRequest != nil:
			attrs = req.DeleteRequest.Key
		}

		targets[i], valid[i] = newTxnTarget(region, tableName, table, attrs)
	}

	var kept, conflicted []types.WriteRequest

	db.mu.Lock("txn.checkBatch")
	defer db.mu.Unlock()

	for i, req := range requests {
		if valid[i] && (db.forcedConflictLocked(opBatchWriteItem, targets[i]) ||
			db.conflictsLocked(targets[i].ref, nil)) {
			conflicted = append(conflicted, req)

			continue
		}

		kept = append(kept, req)
	}

	return kept, conflicted
}

func (db *InMemoryDB) emitTransactionConflict(region, tableName string, n int) {
	for range n {
		db.metrics.Put(region, ddbMetricNamespace, metricTransactionConflict, ddbUnitCount, 1, ddbTableDim(tableName))
	}
}

// txCancelMessage is the TransactionCanceledException message: the fixed prefix plus the reason codes.
func txCancelMessage(reasons []CancellationReason) string {
	codes := make([]string, len(reasons))
	for i, r := range reasons {
		codes[i] = r.Code
	}

	return txCancelPrefix + " [" + strings.Join(codes, ", ") + "]"
}

// conflictCancellation builds the TransactionCanceledException for the conflicting item indices.
func conflictCancellation(n int, conflicts []int) *Error {
	reasons := make([]CancellationReason, n)
	for i := range reasons {
		reasons[i] = CancellationReason{Code: cancellationReasonNone}
	}

	for _, i := range conflicts {
		reasons[i] = CancellationReason{Code: cancellationReasonConflict, Message: txnConflictMessage}
	}

	return NewTransactionCanceledException(txCancelMessage(reasons), reasons)
}

// beginTransactWrite marks a TransactWriteItems' items in flight; the caller must release the handle.
func (db *InMemoryDB) beginTransactWrite(
	ctx context.Context,
	items []types.TransactWriteItem,
) (*txnHandle, error) {
	region := getRegionFromContext(ctx, db)

	tables, err := db.resolveTransactTablesRLocked(region, db.transactTableNames(items), "TransactWriteItems.begin")
	if err != nil {
		return nil, err
	}

	targets := make([]txnTarget, 0, len(items))

	for i, ti := range items {
		name, attrs := transactWriteItemKey(ti)

		if tg, ok := newTxnTarget(region, name, tables[name], attrs); ok {
			tg.idx = i
			targets = append(targets, tg)
		}
	}

	h := db.newTxnHandle(opTransactWriteItems)
	if conflicts := db.beginTxn(h, true, targets); len(conflicts) > 0 {
		for _, i := range conflicts {
			db.emitTransactionConflict(region, transactWriteItemTableName(items[i]), 1)
		}

		return nil, conflictCancellation(len(items), conflicts)
	}

	return h, nil
}

func transactWriteItemKey(ti types.TransactWriteItem) (string, map[string]types.AttributeValue) {
	switch {
	case ti.Put != nil:
		return aws.ToString(ti.Put.TableName), ti.Put.Item
	case ti.Delete != nil:
		return aws.ToString(ti.Delete.TableName), ti.Delete.Key
	case ti.Update != nil:
		return aws.ToString(ti.Update.TableName), ti.Update.Key
	case ti.ConditionCheck != nil:
		return aws.ToString(ti.ConditionCheck.TableName), ti.ConditionCheck.Key
	}

	return "", nil
}

// beginTransactRead checks a TransactGetItems' items for conflicts; it holds nothing (caller still releases).
func (db *InMemoryDB) beginTransactRead(
	ctx context.Context,
	items []types.TransactGetItem,
	tableNames []string,
) (*txnHandle, error) {
	region := getRegionFromContext(ctx, db)

	tables, err := db.resolveTransactTablesRLocked(region, tableNames, "TransactGetItems.begin")
	if err != nil {
		return nil, err
	}

	targets := make([]txnTarget, 0, len(items))

	for i, ti := range items {
		if ti.Get == nil {
			continue
		}

		name := aws.ToString(ti.Get.TableName)

		if tg, ok := newTxnTarget(region, name, tables[name], ti.Get.Key); ok {
			tg.idx = i
			targets = append(targets, tg)
		}
	}

	h := db.newTxnHandle(opTransactGetItems)
	if conflicts := db.beginTxn(h, false, targets); len(conflicts) > 0 {
		for _, i := range conflicts {
			db.emitTransactionConflict(region, aws.ToString(items[i].Get.TableName), 1)
		}

		return nil, conflictCancellation(len(items), conflicts)
	}

	return h, nil
}
