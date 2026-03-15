package dynamodb

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// Export internal functions for testing in the dynamodb_test package.
// This allows us to satisfy the testpackage linter while still unit testing
// the package's internal logic.

// EvaluateExpression is now exported in expressions.go

func CompareValues(lhs any, op string, rhs any) bool {
	return dynamoattr.CompareValues(lhs, op, rhs)
}

func UnwrapAttributeValue(v any) any {
	return dynamoattr.UnwrapAttributeValue(v)
}

// ExtractFunctionArgs is no longer supported

func FindExclusiveStartIndex(
	items []map[string]any,
	startKey map[string]any,
	keySchema []models.KeySchemaElement,
) int {
	return findExclusiveStartIndex(items, startKey, keySchema)
}

func CompareAny(v1, v2 any, typ string) int {
	return compareAny(v1, v2, typ)
}

func ToString(val any) string {
	return dynamoattr.ToString(val)
}

func ApplyGSIProjection(
	item map[string]any,
	proj models.Projection,
	tableKeySchema []models.KeySchemaElement,
	indexKeySchema []models.KeySchemaElement,
) map[string]any {
	return applyGSIProjection(item, proj, tableKeySchema, indexKeySchema)
}

func ParseStr(v any) string {
	return dynamoattr.ToString(v)
}

func (db *InMemoryDB) ExtractKeySchema(
	table *Table,
	indexName string,
) ([]models.KeySchemaElement, *models.Projection, error) {
	return db.extractKeySchema(table, indexName)
}

func (db *InMemoryDB) SortCandidates(
	candidates []map[string]any,
	skDef models.KeySchemaElement,
	table *Table,
	scanIndexForward bool,
) {
	db.sortCandidates(candidates, skDef, table, scanIndexForward)
}

func (t *Table) PKIndex() map[string]int {
	return t.pkIndex
}

func (t *Table) PKSKIndex() map[string]map[string]int {
	return t.pkskIndex
}

func (t *Table) InitializeIndexes() {
	t.initializeIndexes()
}

func (t *Table) RebuildIndexes() {
	t.rebuildIndexes()
}

func (j *Janitor) SweepTTL(ctx context.Context) {
	j.sweepTTL(ctx)
}

// StreamShardID exposes the canonical shard ID for stream tests.
const StreamShardID = streamShardID

func (j *Janitor) RunOnce(ctx context.Context) {
	j.runOnce(ctx)
}

// LookupReplicationPauseKeyForTest exposes lookupReplicationPauseKey for unit tests.
func (db *InMemoryDB) LookupReplicationPauseKeyForTest(tableARNOrName string) (any, string, bool) {
	t, k, found := db.lookupReplicationPauseKey(tableARNOrName)

	return t, k, found
}

// InjectExpiredReplicationPauseForTest directly inserts an expired entry
// into fisReplicationPaused without starting a cleanup goroutine, allowing
// tests to exercise the lazy-eviction path in IsReplicationPaused.
func (db *InMemoryDB) InjectExpiredReplicationPauseForTest(tableARN string) {
	db.mu.Lock("InjectExpiredReplicationPauseForTest")
	defer db.mu.Unlock()

	db.fisReplicationPaused[tableARN] = time.Now().Add(-time.Hour) // already expired
}

// ScheduleReplicationPauseCleanupForTest exposes scheduleReplicationPauseCleanup for tests.
func (db *InMemoryDB) ScheduleReplicationPauseCleanupForTest(
	ctx context.Context,
	tableARNs []string,
	dur time.Duration,
) {
	db.scheduleReplicationPauseCleanup(ctx, tableARNs, dur)
}

// DeepCopyItem exposes deepCopyItem for testing.
func DeepCopyItem(item map[string]any) map[string]any {
	return deepCopyItem(item)
}

// TxnTokenCount returns the number of committed idempotency tokens currently stored.
func (db *InMemoryDB) TxnTokenCount() int {
	db.mu.RLock("TxnTokenCount")
	defer db.mu.RUnlock()

	return len(db.txnTokens)
}

// InjectExpiredTxnTokenForTest inserts an already-expired token into the committed map.
func (db *InMemoryDB) InjectExpiredTxnTokenForTest(token string) {
	db.mu.Lock("InjectExpiredTxnTokenForTest")
	defer db.mu.Unlock()

	db.txnTokens[token] = time.Now().Add(-time.Hour) // already expired
}

// StreamARNIndexSize returns the number of entries in the stream ARN reverse index.
func (db *InMemoryDB) StreamARNIndexSize() int {
	db.mu.RLock("StreamARNIndexSize")
	defer db.mu.RUnlock()

	return len(db.streamARNIndex)
}

// LookupStreamARNIndex looks up a table by stream ARN in the reverse index (for tests).
func (db *InMemoryDB) LookupStreamARNIndex(streamARN string) (*Table, bool) {
	db.mu.RLock("LookupStreamARNIndex")
	defer db.mu.RUnlock()

	t, ok := db.streamARNIndex[streamARN]

	return t, ok
}

// SweepTxnTokens exposes sweepTxnTokens for tests.
func (j *Janitor) SweepTxnTokens() {
	j.sweepTxnTokens()
}

// SweepTxnPending exposes sweepTxnPending for tests.
func (j *Janitor) SweepTxnPending() {
	j.sweepTxnPending()
}

// TxnPendingCount returns the number of in-progress idempotency tokens.
func (db *InMemoryDB) TxnPendingCount() int {
	db.mu.RLock("TxnPendingCount")
	defer db.mu.RUnlock()

	return len(db.txnPending)
}

// InjectStaleTxnPendingForTest inserts a stale in-progress token into the pending map.
func (db *InMemoryDB) InjectStaleTxnPendingForTest(token string) {
	db.mu.Lock("InjectStaleTxnPendingForTest")
	defer db.mu.Unlock()

	db.txnPending[token] = time.Now().Add(-time.Hour) // already stale
}

// StreamRecordsInOrder exposes the ordered ring-buffer view for tests as a flat slice.
func (t *Table) StreamRecordsInOrder() []StreamRecord {
	tail, head := t.streamRecordsInOrder()
	if len(head) == 0 {
		return tail
	}

	result := make([]StreamRecord, 0, len(tail)+len(head))
	result = append(result, tail...)
	result = append(result, head...)

	return result
}

// SweepExprCache exposes ExpressionCache.Sweep for tests.
func (db *InMemoryDB) SweepExprCache() {
	db.exprCache.Sweep()
}

// ExprCacheGet exposes ExpressionCache.Get for tests.
func (db *InMemoryDB) ExprCacheGet(key string) (any, bool) {
	return db.exprCache.Get(key)
}

// ExprCachePut exposes ExpressionCache.Put for tests.
func (db *InMemoryDB) ExprCachePut(key string, value any) {
	db.exprCache.Put(key, value)
}

// NewExpressionCacheWithTTL exposes newExpressionCacheWithTTL for tests.
func NewExpressionCacheWithTTL(capacity int, ttl time.Duration) *ExpressionCache {
	return newExpressionCacheWithTTL(capacity, ttl)
}
