// Package dynamodb implements the AWS DynamoDB mock service.
// accuracy_audit.go contains accuracy improvements from issue #1678.
package dynamodb

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// ============================================================
// Section 1: ReturnConsumedCapacity INDEXES granularity
// ============================================================

const (
	// maxAttributeNameLength is the maximum allowed length for a DynamoDB attribute name.
	maxAttributeNameLength = 255
	// consistentReadMultiplier is the RCU multiplier for strongly-consistent reads.
	consistentReadMultiplier = 2.0
	// maxGSICount is the maximum number of Global Secondary Indexes per table.
	maxGSICount = 20
	// maxLSICount is the maximum number of Local Secondary Indexes per table.
	maxLSICount = 5
	// maxTransactWriteSizeBytes is the maximum total size for TransactWriteItems (4 MB).
	maxTransactWriteSizeBytes = 4 * 1024 * 1024
	// maxParallelScanSegments is the upper bound on TotalSegments for parallel Scan.
	maxParallelScanSegments = 1_000_000
	// shardIteratorTTL is how long a shard iterator token remains valid (15 min per AWS spec).
	shardIteratorTTL = 15 * time.Minute
	// shardIteratorTokenLen is the number of random bytes used in each opaque iterator token.
	shardIteratorTokenLen = 16
)

// buildConsumedCapacityWithIndexes constructs a ConsumedCapacity response that
// includes per-index breakdowns when req == INDEXES, and a table-level summary
// when req == TOTAL. Returns nil for NONE or empty.
func buildConsumedCapacityWithIndexes(
	tableName string,
	req types.ReturnConsumedCapacity,
	tableRCU, tableWCU float64,
	gsiRCU, gsiWCU map[string]float64,
	lsiRCU, lsiWCU map[string]float64,
) *types.ConsumedCapacity {
	if req == "" || req == types.ReturnConsumedCapacityNone {
		return nil
	}

	totalRCU, totalWCU := sumCapacityMaps(tableRCU, tableWCU, gsiRCU, gsiWCU, lsiRCU, lsiWCU)

	cc := buildBaseConsumedCapacity(tableName, totalRCU, totalWCU)

	if req == types.ReturnConsumedCapacityIndexes {
		applyIndexBreakdowns(cc, tableRCU, tableWCU, gsiRCU, gsiWCU, lsiRCU, lsiWCU)
	}

	return cc
}

// sumCapacityMaps totals RCU and WCU across table and all index maps.
func sumCapacityMaps(
	tableRCU, tableWCU float64,
	gsiRCU, gsiWCU map[string]float64,
	lsiRCU, lsiWCU map[string]float64,
) (float64, float64) {
	totalRCU := tableRCU
	totalWCU := tableWCU

	for _, v := range gsiRCU {
		totalRCU += v
	}

	for _, v := range gsiWCU {
		totalWCU += v
	}

	for _, v := range lsiRCU {
		totalRCU += v
	}

	for _, v := range lsiWCU {
		totalWCU += v
	}

	return totalRCU, totalWCU
}

// buildBaseConsumedCapacity creates the base ConsumedCapacity with table name and totals.
func buildBaseConsumedCapacity(tableName string, totalRCU, totalWCU float64) *types.ConsumedCapacity {
	cc := &types.ConsumedCapacity{
		TableName:     aws.String(tableName),
		CapacityUnits: aws.Float64(totalRCU + totalWCU),
	}

	if totalRCU > 0 {
		cc.ReadCapacityUnits = aws.Float64(totalRCU)
	}

	if totalWCU > 0 {
		cc.WriteCapacityUnits = aws.Float64(totalWCU)
	}

	return cc
}

// applyIndexBreakdowns populates INDEXES-granularity fields on cc.
func applyIndexBreakdowns(
	cc *types.ConsumedCapacity,
	tableRCU, tableWCU float64,
	gsiRCU, gsiWCU map[string]float64,
	lsiRCU, lsiWCU map[string]float64,
) {
	if tableRCU > 0 || tableWCU > 0 {
		cc.Table = buildTableCapacity(tableRCU, tableWCU)
	}

	if len(gsiRCU) > 0 || len(gsiWCU) > 0 {
		cc.GlobalSecondaryIndexes = buildIndexCapacityMap(gsiRCU, gsiWCU)
	}

	if len(lsiRCU) > 0 || len(lsiWCU) > 0 {
		cc.LocalSecondaryIndexes = buildIndexCapacityMap(lsiRCU, lsiWCU)
	}
}

// buildTableCapacity constructs the per-table Capacity breakdown for INDEXES granularity.
func buildTableCapacity(rcu, wcu float64) *types.Capacity {
	c := &types.Capacity{CapacityUnits: aws.Float64(rcu + wcu)}

	if rcu > 0 {
		c.ReadCapacityUnits = aws.Float64(rcu)
	}

	if wcu > 0 {
		c.WriteCapacityUnits = aws.Float64(wcu)
	}

	return c
}

// buildIndexCapacityMap merges separate RCU/WCU index maps into a map[string]types.Capacity.
func buildIndexCapacityMap(rcuMap, wcuMap map[string]float64) map[string]types.Capacity {
	keys := make(map[string]struct{})
	for k := range rcuMap {
		keys[k] = struct{}{}
	}

	for k := range wcuMap {
		keys[k] = struct{}{}
	}

	out := make(map[string]types.Capacity, len(keys))

	for k := range keys {
		c := types.Capacity{}
		r := rcuMap[k]
		w := wcuMap[k]

		if r > 0 {
			c.ReadCapacityUnits = aws.Float64(r)
		}

		if w > 0 {
			c.WriteCapacityUnits = aws.Float64(w)
		}

		c.CapacityUnits = aws.Float64(r + w)
		out[k] = c
	}

	return out
}

// ============================================================
// Section 2: ConsistentRead RCU doubling
// ============================================================

// applyConsistentReadMultiplier doubles the RCU cost when ConsistentRead is true.
// AWS DynamoDB charges 1 RCU per 4 KB for strongly-consistent reads vs
// 0.5 RCU per 4 KB for eventually-consistent reads.
func applyConsistentReadMultiplier(rcu float64, consistentRead bool) float64 {
	if consistentRead {
		return rcu * consistentReadMultiplier
	}

	return rcu
}

// ============================================================
// Section 3: ProjectionExpression vs AttributesToGet validation
// ============================================================

// validateProjectionParams returns an error when both ProjectionExpression and
// AttributesToGet are supplied (AWS rejects this combination).
func validateProjectionParams(projectionExpr string, attributesToGet []string) error {
	if projectionExpr != "" && len(attributesToGet) > 0 {
		return NewValidationException(
			"Cannot specify both AttributesToGet and ProjectionExpression",
		)
	}

	return nil
}

// resolveProjection returns the effective projection string, falling back to
// a comma-separated AttributesToGet list when ProjectionExpression is empty.
func resolveProjection(projectionExpr string, attributesToGet []string) string {
	if projectionExpr != "" {
		return projectionExpr
	}

	if len(attributesToGet) == 0 {
		return ""
	}

	return strings.Join(attributesToGet, ",")
}

// ============================================================
// Section 4: Attribute name length validation
// ============================================================

// validateAttributeNames checks that no attribute name in the item exceeds 255
// characters or is empty. Called for PutItem, UpdateItem, and TransactWrite.
func validateAttributeNames(item map[string]any) error {
	for name := range item {
		if name == "" {
			return NewValidationException("Attribute name must not be empty")
		}

		if len(name) > maxAttributeNameLength {
			return NewValidationException(
				fmt.Sprintf(
					"Attribute name is too long; maximum is %d characters, got %d",
					maxAttributeNameLength, len(name),
				),
			)
		}
	}

	return nil
}

// ============================================================
// Section 5: GSI/LSI creation limits
// ============================================================

// validateGSICount returns a LimitExceededException when the proposed GSI list
// would exceed maxGSICount (20) per table.
func validateGSICount(gsiList []models.GlobalSecondaryIndex, additions int) error {
	if len(gsiList)+additions > maxGSICount {
		return NewLimitExceededException(
			fmt.Sprintf(
				"Too many global secondary indexes; maximum is %d",
				maxGSICount,
			),
		)
	}

	return nil
}

// validateLSICount returns a LimitExceededException when the LSI list exceeds maxLSICount (5).
func validateLSICount(lsiList []models.LocalSecondaryIndex) error {
	if len(lsiList) > maxLSICount {
		return NewLimitExceededException(
			fmt.Sprintf(
				"Too many local secondary indexes; maximum is %d",
				maxLSICount,
			),
		)
	}

	return nil
}

// ============================================================
// Section 6: TransactWriteItems duplicate-key detection & 4MB size limit
// ============================================================

// transactWriteKey is the canonical key used for duplicate detection.
type transactWriteKey struct {
	TableName string
	KeyJSON   string
}

// validateTransactWriteItems checks for duplicate keys and total size limit.
// Returns TransactionCanceledException on duplicate keys, ValidationException on size.
func validateTransactWriteItems(
	items []types.TransactWriteItem,
	tables map[string]*Table,
) error {
	seen := make(map[transactWriteKey]bool, len(items))
	totalBytes := 0

	for i, ti := range items {
		tableName, keyItem, itemForSize := extractTransactWriteKeyAndItem(ti)
		if tableName == "" {
			continue
		}

		// Accumulate size estimate.
		if itemForSize != nil {
			sz, _ := CalculateItemSize(models.FromSDKItem(itemForSize))
			totalBytes += sz
		}

		if totalBytes > maxTransactWriteSizeBytes {
			return NewValidationException(
				"Transaction size exceeded: maximum allowed is 4 MB",
			)
		}

		if keyItem == nil {
			continue
		}

		wireKey := models.FromSDKItem(keyItem)

		// Resolve the table to extract only the key attributes.
		if table, ok := tables[tableName]; ok {
			pkDef, skDef := getPKAndSK(table.KeySchema)
			wireKey = map[string]any{pkDef.AttributeName: wireKey[pkDef.AttributeName]}
			if skDef.AttributeName != "" {
				wireKey[skDef.AttributeName] = keyItem[skDef.AttributeName]
			}
		}

		keyBytes, err := json.Marshal(wireKey)
		if err != nil {
			continue
		}

		twk := transactWriteKey{TableName: tableName, KeyJSON: string(keyBytes)}
		if seen[twk] {
			reasons := makeDuplicateKeyReasons(items, i)

			return NewTransactionCanceledException(
				txCancelPrefix, reasons,
			)
		}

		seen[twk] = true
	}

	return nil
}

// extractTransactWriteKeyAndItem returns the table name, key map, and item map
// from a single TransactWriteItem (for duplicate detection and size accounting).
// ConditionCheck is excluded from duplicate key detection because AWS DynamoDB
// allows one ConditionCheck and one write operation on the same key.
func extractTransactWriteKeyAndItem(
	ti types.TransactWriteItem,
) (string, map[string]types.AttributeValue, map[string]types.AttributeValue) {
	switch {
	case ti.Put != nil:
		return aws.ToString(ti.Put.TableName), ti.Put.Item, ti.Put.Item
	case ti.Delete != nil:
		return aws.ToString(ti.Delete.TableName), ti.Delete.Key, nil
	case ti.Update != nil:
		return aws.ToString(ti.Update.TableName), ti.Update.Key, nil
	case ti.ConditionCheck != nil:
		// ConditionCheck is not a write; exclude from duplicate key tracking.
		// Size is also not counted for ConditionCheck-only operations.
		return "", nil, nil
	}

	return "", nil, nil
}

// makeDuplicateKeyReasons builds a cancellation reasons slice with a
// DuplicateItem code at position idx (all others are "None").
func makeDuplicateKeyReasons(items []types.TransactWriteItem, idx int) []CancellationReason {
	reasons := make([]CancellationReason, len(items))
	for i := range reasons {
		reasons[i] = CancellationReason{Code: "None"}
	}

	if idx >= 0 && idx < len(reasons) {
		reasons[idx] = CancellationReason{
			Code:    "DuplicateItem",
			Message: "Transaction contains more than one action for the same item",
		}
	}

	return reasons
}

// ============================================================
// Section 7: Scan Segment/TotalSegments validation
// ============================================================

// validateScanSegment returns a ValidationException when Segment or TotalSegments
// are out of range. AWS requires: 0 ≤ Segment < TotalSegments, 1 ≤ TotalSegments ≤ 1_000_000.
func validateScanSegment(segment, totalSegments int32) error {
	if totalSegments < 0 {
		// Not set — single-segment scan, no validation needed.
		return nil
	}

	if totalSegments < 1 || totalSegments > maxParallelScanSegments {
		return NewValidationException(
			fmt.Sprintf(
				"TotalSegments must be between 1 and %d, got %d",
				maxParallelScanSegments, totalSegments,
			),
		)
	}

	if segment < 0 || segment >= totalSegments {
		return NewValidationException(
			fmt.Sprintf(
				"Segment must be between 0 and TotalSegments-1 (%d), got %d",
				totalSegments-1, segment,
			),
		)
	}

	return nil
}

// ============================================================
// Section 8: Throttling — PAY_PER_REQUEST bypass
// ============================================================

// isOnDemandTable returns true when the table is in PAY_PER_REQUEST billing mode.
// PAY_PER_REQUEST tables are never throttled.
func isOnDemandTable(billingMode string) bool {
	return billingMode == string(types.BillingModePayPerRequest)
}

// ============================================================
// Section 9: Opaque stream shard iterators
// ============================================================

// shardIteratorEntry holds server-side state for an opaque shard iterator token.
type shardIteratorEntry struct {
	ExpiresAt time.Time
	TableName string
	StartSeq  int64
}

// ShardIteratorStore maps opaque random tokens to server-side iterator state.
// It is goroutine-safe.
type ShardIteratorStore struct {
	entries map[string]*shardIteratorEntry
	mu      sync.Mutex
}

// NewShardIteratorStore creates an empty ShardIteratorStore.
func NewShardIteratorStore() *ShardIteratorStore {
	return &ShardIteratorStore{
		entries: make(map[string]*shardIteratorEntry),
	}
}

// Put stores a new iterator entry and returns the opaque token.
func (s *ShardIteratorStore) Put(tableName string, startSeq int64) (string, error) {
	token, err := generateOpaqueToken()
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	s.entries[token] = &shardIteratorEntry{
		TableName: tableName,
		StartSeq:  startSeq,
		ExpiresAt: time.Now().Add(shardIteratorTTL),
	}
	s.mu.Unlock()

	return token, nil
}

// Get retrieves the entry for a token. Returns nil if the token is unknown.
func (s *ShardIteratorStore) Get(token string) *shardIteratorEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.entries[token]
}

// Delete removes a token from the store.
func (s *ShardIteratorStore) Delete(token string) {
	s.mu.Lock()
	delete(s.entries, token)
	s.mu.Unlock()
}

// Sweep removes expired entries from the store.
func (s *ShardIteratorStore) Sweep() {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	for token, entry := range s.entries {
		if now.After(entry.ExpiresAt) {
			delete(s.entries, token)
		}
	}
}

// generateOpaqueToken generates a cryptographically-random hex token.
func generateOpaqueToken() (string, error) {
	b := make([]byte, shardIteratorTokenLen)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate iterator token: %w", err)
	}

	return hex.EncodeToString(b), nil
}

// ============================================================
// Section 10: ON_DEMAND billing mode helpers
// ============================================================

// ============================================================
// Section 12: ExpressionAttributeValues type matching
// ============================================================

// validateEAVTypes checks that expression attribute values are structurally valid
// wire-format attribute values (each must be a single-key type map).
func validateEAVTypes(eav map[string]any) error {
	for name, val := range eav {
		m, ok := val.(map[string]any)
		if !ok {
			return NewValidationException(fmt.Sprintf(
				"ExpressionAttributeValues contains invalid value for key %q: "+
					"must be a DynamoDB attribute value map",
				name,
			))
		}

		if len(m) != 1 {
			return NewValidationException(
				fmt.Sprintf("ExpressionAttributeValues[%q]: expected exactly one type key, got %d", name, len(m)),
			)
		}

		for typeKey := range m {
			if !isValidDynamoDBTypeKey(typeKey) {
				return NewValidationException(
					fmt.Sprintf("ExpressionAttributeValues[%q]: unknown type key %q", name, typeKey),
				)
			}
		}
	}

	return nil
}

// isValidDynamoDBTypeKey returns true for recognised DynamoDB attribute type keys.
func isValidDynamoDBTypeKey(key string) bool {
	switch key {
	case "S", "N", "B", "BOOL", "NULL", "SS", "NS", "BS", "M", "L":
		return true
	}

	return false
}

// ============================================================
// Section 13: TTL sweep grace period
// ============================================================

// TTLGracePeriod is the extra time added after an item's TTL timestamp before it
// is actually evicted. AWS DynamoDB documents a 48-hour grace period in production.
// Tests should pass 0 to avoid timing dependencies.
var TTLGracePeriod = 0 * time.Second //nolint:gochecknoglobals // intentional package-level default

// isItemExpiredWithGrace reports whether an item's TTL attribute has expired,
// accounting for the configured grace period.
func isItemExpiredWithGrace(item map[string]any, ttlAttr string, gracePeriod time.Duration) bool {
	if ttlAttr == "" {
		return false
	}

	val, ok := item[ttlAttr]
	if !ok {
		return false
	}

	m, ok := val.(map[string]any)
	if !ok {
		return false
	}

	nStr, ok := m["N"].(string)
	if !ok {
		return false
	}

	var ts float64
	if _, err := fmt.Sscanf(nStr, "%f", &ts); err != nil {
		return false
	}

	expiry := time.Unix(int64(ts), 0).Add(gracePeriod)

	return time.Now().After(expiry)
}
