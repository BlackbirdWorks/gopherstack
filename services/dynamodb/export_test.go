package dynamodb

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

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
func (j *Janitor) SweepTxnTokens(ctx context.Context) {
	j.sweepTxnTokens(ctx)
}

// SweepTxnPending exposes sweepTxnPending for tests.
func (j *Janitor) SweepTxnPending(ctx context.Context) {
	j.sweepTxnPending(ctx)
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
func (t *Table) StreamRecordsInOrder() []models.StreamRecord {
	tail, head := t.streamRecordsInOrder()
	if len(head) == 0 {
		return tail
	}

	result := make([]models.StreamRecord, 0, len(tail)+len(head))
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

// SweepBefore exposes ExpressionCache.sweepBefore for deterministic tests.
// It removes entries whose TTL expired before the given cutoff time, allowing
// tests to verify sweep behaviour without relying on wall-clock timing.
func (c *ExpressionCache) SweepBefore(cutoff time.Time) { c.sweepBefore(cutoff) }

// HasEntry reports whether key is present in the cache without performing lazy
// TTL eviction. Used after SweepBefore to inspect raw cache state.
func (c *ExpressionCache) HasEntry(key string) bool { return c.has(key) }

// PutAt exposes ExpressionCache.putAt for tests that need deterministic expiry.
// It adds an entry with the given explicit expiresAt timestamp instead of computing
// one from the cache TTL.
func (c *ExpressionCache) PutAt(key string, value any, expiresAt time.Time) {
	c.putAt(key, value, expiresAt)
}

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the timeout.
func (h *DynamoDBHandler) GetJanitorTaskTimeout() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.TaskTimeout
}

// AddKinesisDestination adds a Kinesis stream ARN to the given table's destination list.
// Used in tests to pre-populate Kinesis destinations without going through the HTTP API.
func (db *InMemoryDB) AddKinesisDestination(tableName, streamARN string) {
	db.mu.RLock("AddKinesisDestination")
	regionTables, regionExists := db.Tables[db.defaultRegion]

	var table *Table
	var tableExists bool

	if regionExists {
		table, tableExists = regionTables[tableName]
	}

	db.mu.RUnlock()

	if !tableExists {
		return
	}

	table.mu.Lock("AddKinesisDestination")
	table.KinesisDestinations = append(table.KinesisDestinations, streamARN)
	table.mu.Unlock()
}

// TableExistsInRegion reports whether a table with the given name exists in the given region.
// Used in tests to verify that CreateGlobalTable creates replica tables.
func (db *InMemoryDB) TableExistsInRegion(region, tableName string) bool {
	db.mu.RLock("TableExistsInRegion")
	defer db.mu.RUnlock()

	regionTables, ok := db.Tables[region]
	if !ok {
		return false
	}

	_, exists := regionTables[tableName]

	return exists
}

// GetTableGlobalTableName returns the GlobalTableName field of the named table
// in the backend's default region. Returns "" if the table is not part of a global table.
func (db *InMemoryDB) GetTableGlobalTableName(tableName string) string {
	db.mu.RLock("GetTableGlobalTableName")
	regionTables, ok := db.Tables[db.defaultRegion]

	var table *Table
	if ok {
		table = regionTables[tableName]
	}

	db.mu.RUnlock()

	if table == nil {
		return ""
	}

	table.mu.RLock("GetTableGlobalTableName-field")
	defer table.mu.RUnlock()

	return table.GlobalTableName
}

// GetItems returns a deep-copied snapshot of the table's Items slice under the table's read lock.
// Used in tests to inspect item state without triggering the full query path.
// Deep copying ensures the returned slice is safe to read concurrently with ongoing mutations.
func (t *Table) GetItems() []map[string]any {
	t.mu.RLock("GetItems")
	defer t.mu.RUnlock()

	snapshot := make([]map[string]any, len(t.Items))

	for i, item := range t.Items {
		snapshot[i] = deepCopyItem(item)
	}

	return snapshot
}

// ApplyGlobalTableLimit exposes applyGlobalTableLimit for testing.
// In particular it lets tests verify that a zero limit does not cause a panic.
func ApplyGlobalTableLimit(list []types.GlobalTable, limit *int32) ([]types.GlobalTable, *string) {
	return applyGlobalTableLimit(list, limit)
}

// TxnTokensCount returns the number of committed idempotency tokens in the backend.
// (Alias that uses the plural 'Tokens' for readability alongside TxnTokensMaxCap.)
func (db *InMemoryDB) TxnTokensCount() int {
	return db.TxnTokenCount()
}

// AddTxnToken inserts a committed idempotency token with the given expiry.
// Used by tests to pre-populate the map without going through the full TransactWriteItems path.
func (db *InMemoryDB) AddTxnToken(token string, expiry time.Time) {
	db.mu.Lock("AddTxnToken")
	defer db.mu.Unlock()

	db.txnTokens[token] = expiry
}

// StreamRecordCount returns the number of stream record slots currently allocated
// for the named table (including zero-value / compacted entries).
func (db *InMemoryDB) StreamRecordCount(tableName string) int {
	db.mu.RLock("StreamRecordCount")
	regionTables := db.Tables[db.defaultRegion]
	tbl := regionTables[tableName]
	db.mu.RUnlock()

	if tbl == nil {
		return -1
	}

	tbl.mu.RLock("StreamRecordCountTable")
	defer tbl.mu.RUnlock()

	return len(tbl.StreamRecords)
}

// StreamShards returns a copy of the shard genealogy for the named table's stream.
func (db *InMemoryDB) StreamShards(tableName string) []StreamShard {
	db.mu.RLock("StreamShards")
	regionTables := db.Tables[db.defaultRegion]
	tbl := regionTables[tableName]
	db.mu.RUnlock()

	if tbl == nil {
		return nil
	}

	tbl.mu.RLock("StreamShards.table")
	defer tbl.mu.RUnlock()

	out := make([]StreamShard, len(tbl.streamShards))
	copy(out, tbl.streamShards)

	return out
}

// StreamTrimSeq returns the current trim horizon (oldest seq still in buffer) for the named table.
func (db *InMemoryDB) StreamTrimSeq(tableName string) int64 {
	db.mu.RLock("StreamTrimSeq")
	regionTables := db.Tables[db.defaultRegion]
	tbl := regionTables[tableName]
	db.mu.RUnlock()

	if tbl == nil {
		return -1
	}

	tbl.mu.RLock("StreamTrimSeq.table")
	defer tbl.mu.RUnlock()

	return tbl.streamTrimSeq
}

// IteratorStoreSize returns the number of active iterator tokens in the store.
func (db *InMemoryDB) IteratorStoreSize() int {
	return db.iteratorStore.Size()
}

// SweepIterators removes expired iterator tokens from the store.
func (db *InMemoryDB) SweepIterators() {
	db.iteratorStore.Sweep()
}

// ParseSeqNum exposes parseSeqNum for tests.
func ParseSeqNum(s string) (int64, error) {
	return parseSeqNum(s)
}

// SeqNumString exposes seqNumString for tests.
func SeqNumString(seq int64) string {
	return seqNumString(seq)
}

// TxnTokensMaxCap exposes the package-level cap constant for testing.
const TxnTokensMaxCap = txnTokensMaxCap

// TxnPendingMaxCap exposes the package-level cap constant for testing.
const TxnPendingMaxCap = txnPendingMaxCap

// TTLSweepBatchSize exposes the package-level batch size constant for testing.
const TTLSweepBatchSize = defaultTTLSweepBatchSize

// ExportCount returns the number of exports stored in the backend.
func (db *InMemoryDB) ExportCount() int {
	db.mu.RLock("ExportCount")
	defer db.mu.RUnlock()

	return len(db.exports)
}

// ExportDescFields is the exported form of exportDescriptionFields for use in tests.
type ExportDescFields = exportDescriptionFields

// StoreExportForTest inserts an export record directly without going through the HTTP handler.
func (db *InMemoryDB) StoreExportForTest(exportARN, tableARN, bucket, status string) {
	db.storeExport(exportDescriptionFields{
		ExportArn:    exportARN,
		ExportStatus: status,
		TableArn:     tableARN,
		S3Bucket:     bucket,
	})
}

// GetKeySchemaForPartiQLForTest exposes getKeySchemaForPartiQL for testing.
func (db *InMemoryDB) GetKeySchemaForPartiQLForTest(ctx context.Context, tableName string) (any, error) {
	return db.getKeySchemaForPartiQL(ctx, tableName)
}

// NewJanitorForTest creates a Janitor for the given backend with test-friendly defaults.
func NewJanitorForTest(db *InMemoryDB) *Janitor {
	return &Janitor{
		Backend:           db,
		TaskTimeout:       5 * time.Second,
		ttlSweepBatchSize: defaultTTLSweepBatchSize,
	}
}

// TTLSweepBatchSizeForTest returns the janitor TTL sweep batch size used for tests.
func (j *Janitor) TTLSweepBatchSizeForTest() int {
	return j.ttlSweepBatchSize
}

// NthSmallestForTest exposes nthSmallest for unit testing.
func NthSmallestForTest(ts []time.Time, n int) time.Time {
	return nthSmallest(ts, n)
}

// ---- accuracy_audit.go exports ----

// ValidateEAVTypes exposes validateEAVTypes for external tests.
func ValidateEAVTypes(eav map[string]any) error {
	return validateEAVTypes(eav)
}

// ValidateAttributeNames exposes validateAttributeNames for external tests.
func ValidateAttributeNames(item map[string]any) error {
	return validateAttributeNames(item)
}

// IsItemExpiredWithGrace exposes isItemExpiredWithGrace for external tests.
func IsItemExpiredWithGrace(item map[string]any, ttlAttr string, grace time.Duration) bool {
	return isItemExpiredWithGrace(item, ttlAttr, grace)
}

// BuildConsumedCapacityWithIndexes exposes buildConsumedCapacityWithIndexes for external tests.
func BuildConsumedCapacityWithIndexes(
	tableName string,
	req types.ReturnConsumedCapacity,
	tableRCU, tableWCU float64,
	gsiRCU, gsiWCU map[string]float64,
	lsiRCU, lsiWCU map[string]float64,
) *types.ConsumedCapacity {
	return buildConsumedCapacityWithIndexes(tableName, req, tableRCU, tableWCU, gsiRCU, gsiWCU, lsiRCU, lsiWCU)
}

// ValidateTransactWriteItems exposes validateTransactWriteItems for external tests.
func ValidateTransactWriteItems(items []types.TransactWriteItem, tables map[string]*Table) error {
	return validateTransactWriteItems(items, tables)
}

// ValidateScanSegment exposes validateScanSegment for external tests.
func ValidateScanSegment(segment, totalSegments int32) error {
	return validateScanSegment(segment, totalSegments)
}

func ValidateTableName(name string) error {
	return validateTableName(name)
}

func ValidateListTablesLimit(limit *int32) error {
	return validateListTablesLimit(limit)
}

func ValidatePositiveLimit(limit *int32) error {
	return validatePositiveLimit(limit)
}

func ValidatePutDeleteReturnValues(rv types.ReturnValue) error {
	return validatePutDeleteReturnValues(rv)
}

func ValidateTransactItemCount(n int, opName string) error {
	return validateTransactItemCount(n, opName)
}

func ValidateExpressionAttributeNames(ean map[string]string) error {
	return validateExpressionAttributeNames(ean)
}

func CheckUnusedExpressionAttributeNames(ean map[string]string, exprs ...string) error {
	return checkUnusedExpressionAttributeNames(ean, exprs...)
}

func CheckUnusedExpressionAttributeValues(eav map[string]any, exprs ...string) error {
	return checkUnusedExpressionAttributeValues(eav, exprs...)
}

func ValidateUpdateDoesNotModifyKeys(
	updateExpr string,
	ean map[string]string,
	keySchema []models.KeySchemaElement,
) error {
	return validateUpdateDoesNotModifyKeys(updateExpr, ean, keySchema)
}

func ValidateSetNoDuplicates(k string, items []any) error {
	return validateSetNoDuplicates(k, items)
}

func ValidateCreateTableKeySchema(schema []models.KeySchemaElement) error {
	return validateCreateTableKeySchema(schema)
}

func ValidateProvisionedThroughput(pt *types.ProvisionedThroughput, billingMode types.BillingMode) error {
	return validateProvisionedThroughput(pt, billingMode)
}

func ValidateNumberNoLeadingZeros(k, n string) error {
	return validateNumberNoLeadingZeros(k, n)
}

// HandleRequest exposes the handler's dispatch method for use in tests.
// It calls the internal dispatch function and returns the result.
func (h *DynamoDBHandler) HandleRequest(ctx context.Context, action string, body []byte) (any, error) {
	return h.dispatch(ctx, action, body)
}

// ExtractExportARNForTest extracts the ExportArn field from an exportTableToPointInTimeOutput
// returned by the export handler. Returns "" when the output is not of the expected type.
func ExtractExportARNForTest(out any) string {
	v, ok := out.(*exportTableToPointInTimeOutput)
	if !ok {
		return ""
	}

	return v.ExportDescription.ExportArn
}
