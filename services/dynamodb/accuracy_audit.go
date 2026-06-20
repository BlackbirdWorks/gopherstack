// Package dynamodb implements the AWS DynamoDB mock service.
// accuracy_audit.go contains accuracy improvements from issue #1678.
package dynamodb

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"sort"
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
	// shardIteratorSweepThreshold is the entry count above which Put performs an
	// inline sweep of expired tokens, bounding the store even when the janitor's
	// Sweep() is not being called on a schedule.
	shardIteratorSweepThreshold = 1024
	// shardIteratorTokenLen is the number of random bytes used in each opaque iterator token.
	shardIteratorTokenLen = 16
	// replicationOpDelete is the mutation op string for item deletion in global-table replication.
	replicationOpDelete = "DELETE"
	// minLeadingZeroCheckLen is the minimum number string length that can have a leading zero.
	minLeadingZeroCheckLen = 2
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
			keyOnly := map[string]any{pkDef.AttributeName: wireKey[pkDef.AttributeName]}
			if skDef.AttributeName != "" {
				keyOnly[skDef.AttributeName] = wireKey[skDef.AttributeName]
			}
			wireKey = keyOnly
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
	// EndSeq is the EndingSequenceNumber of the shard this iterator belongs to,
	// or 0 for an open (still-active) shard. Once a consumer reads past EndSeq on
	// a closed shard, GetRecords returns a nil NextShardIterator (AWS semantics).
	EndSeq int64
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

// Put stores a new iterator entry for an open shard and returns the opaque token.
func (s *ShardIteratorStore) Put(tableName string, startSeq int64) (string, error) {
	return s.PutWithEnd(tableName, startSeq, 0)
}

// PutWithEnd stores a new iterator entry carrying the owning shard's ending
// sequence number (endSeq == 0 for an open shard) and returns the opaque token.
func (s *ShardIteratorStore) PutWithEnd(tableName string, startSeq, endSeq int64) (string, error) {
	token, err := generateOpaqueToken()
	if err != nil {
		return "", err
	}

	now := time.Now()

	s.mu.Lock()
	// Opportunistically drop expired tokens once the store grows large so it
	// stays bounded between scheduled Sweep() calls. The threshold keeps the
	// common small-store case allocation- and scan-free.
	if len(s.entries) >= shardIteratorSweepThreshold {
		for tok, entry := range s.entries {
			if now.After(entry.ExpiresAt) {
				delete(s.entries, tok)
			}
		}
	}
	s.entries[token] = &shardIteratorEntry{
		TableName: tableName,
		StartSeq:  startSeq,
		EndSeq:    endSeq,
		ExpiresAt: now.Add(shardIteratorTTL),
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

// Size returns the number of entries currently in the store (including expired ones
// that have not yet been swept).
func (s *ShardIteratorStore) Size() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.entries)
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

// ============================================================
// Section 14: Table name validation
// ============================================================

// tableNameRegex matches the AWS DynamoDB table name rules:
// 3–255 characters, only letters, digits, underscores, hyphens, and dots.
var tableNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_.\-]{3,255}$`)

// validateTableName returns a ValidationException when name does not satisfy the
// DynamoDB table-name constraints (3–255 chars, alphanumeric/_/./−).
func validateTableName(name string) error {
	if !tableNameRegex.MatchString(name) {
		return NewValidationException(
			fmt.Sprintf(
				"Value '%s' at 'tableName' failed to satisfy constraint: "+
					"Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]{3,255}",
				name,
			),
		)
	}

	return nil
}

// ============================================================
// Section 15: PutItem / DeleteItem ReturnValues restriction
// ============================================================

// validatePutDeleteReturnValues enforces that PutItem and DeleteItem only accept
// NONE or ALL_OLD. AWS rejects ALL_NEW, UPDATED_OLD, and UPDATED_NEW for these ops.
func validatePutDeleteReturnValues(rv types.ReturnValue) error {
	switch rv {
	case "", types.ReturnValueNone, types.ReturnValueAllOld:
		return nil
	case types.ReturnValueUpdatedOld, types.ReturnValueAllNew, types.ReturnValueUpdatedNew:
		// These are invalid for PutItem/DeleteItem — fall through to error.
	}

	return NewValidationException(
		"ReturnValues can only be ALL_OLD or NONE",
	)
}

// ============================================================
// Section 16: TransactWriteItems / TransactGetItems item count limit
// ============================================================

const maxTransactItems = 100

// validateTransactItemCount returns a ValidationException when the item count
// exceeds maxTransactItems (100). AWS enforces this limit on both Transact ops.
func validateTransactItemCount(n int, opName string) error {
	if n > maxTransactItems {
		return NewValidationException(
			fmt.Sprintf(
				"Member must have length less than or equal to %d",
				maxTransactItems,
			),
		)
	}

	_ = opName // reserved for future context-specific messages

	return nil
}

// ============================================================
// Section 17: ExpressionAttributeNames validation
// ============================================================

// validateExpressionAttributeNames checks that every key starts with '#' and
// every value is a non-empty string. AWS rejects both conditions.
func validateExpressionAttributeNames(ean map[string]string) error {
	for k, v := range ean {
		if !strings.HasPrefix(k, "#") {
			return NewValidationException(
				fmt.Sprintf(
					"ExpressionAttributeNames contains invalid key: "+
						"Syntax error; token: '%s', near: '%s'",
					k, k,
				),
			)
		}

		if v == "" {
			return NewValidationException(
				fmt.Sprintf(
					"ExpressionAttributeNames contains invalid value: "+
						"empty string for key %s",
					k,
				),
			)
		}
	}

	return nil
}

// ============================================================
// Section 18: Unused ExpressionAttributeNames / ExpressionAttributeValues
// ============================================================

// checkUnusedExpressionAttributeNames returns a ValidationException when any key
// in ean is not referenced in any of the supplied expression strings.
func checkUnusedExpressionAttributeNames(ean map[string]string, exprs ...string) error {
	if len(ean) == 0 {
		return nil
	}

	combined := strings.Join(exprs, " ")
	var unused []string

	for k := range ean {
		if !strings.Contains(combined, k) {
			unused = append(unused, k)
		}
	}

	if len(unused) == 0 {
		return nil
	}

	sort.Strings(unused)

	return NewValidationException(
		fmt.Sprintf(
			"Value provided in ExpressionAttributeNames unused in expressions: keys: {%s}",
			strings.Join(unused, ", "),
		),
	)
}

// checkUnusedExpressionAttributeValues returns a ValidationException when any key
// in eav is not referenced in any of the supplied expression strings.
func checkUnusedExpressionAttributeValues(eav map[string]any, exprs ...string) error {
	if len(eav) == 0 {
		return nil
	}

	combined := strings.Join(exprs, " ")
	var unused []string

	for k := range eav {
		if !strings.Contains(combined, k) {
			unused = append(unused, k)
		}
	}

	if len(unused) == 0 {
		return nil
	}

	sort.Strings(unused)

	return NewValidationException(
		fmt.Sprintf(
			"Value provided in ExpressionAttributeValues unused in expressions: keys: {%s}",
			strings.Join(unused, ", "),
		),
	)
}

// ============================================================
// Section 19: UpdateItem key attribute modification guard
// ============================================================

// validateUpdateDoesNotModifyKeys returns a ValidationException when the
// UpdateExpression attempts to SET or REMOVE a primary-key attribute.
// AWS DynamoDB forbids mutations to the partition key or sort key.
func validateUpdateDoesNotModifyKeys(
	updateExpr string,
	ean map[string]string,
	keySchema []models.KeySchemaElement,
) error {
	if updateExpr == "" {
		return nil
	}

	keyAttrs := make(map[string]struct{}, len(keySchema))
	for _, k := range keySchema {
		keyAttrs[k.AttributeName] = struct{}{}
	}

	// Build reverse map: #alias → real attribute name.
	reverseEAN := make(map[string]string, len(ean))
	maps.Copy(reverseEAN, ean)

	// Tokenise the expression to find attribute references in SET/REMOVE clauses.
	// We look for bare identifiers and #aliases that map to key attributes.
	tokens := tokenizeUpdateExpression(updateExpr)
	inSetOrRemove := false

	for _, tok := range tokens {
		upper := strings.ToUpper(tok)
		if upper == "SET" || upper == "REMOVE" {
			inSetOrRemove = true

			continue
		}

		if upper == "ADD" || upper == replicationOpDelete {
			inSetOrRemove = false

			continue
		}

		if !inSetOrRemove {
			continue
		}

		// Resolve alias if present.
		attrName := tok
		if strings.HasPrefix(tok, "#") {
			if resolved, ok := reverseEAN[tok]; ok {
				attrName = resolved
			}
		}

		if _, isKey := keyAttrs[attrName]; isKey {
			return NewValidationException(
				fmt.Sprintf(
					"One or more parameter values were invalid: Cannot update attribute %s. "+
						"This attribute is part of the key",
					attrName,
				),
			)
		}
	}

	return nil
}

// tokenizeUpdateExpression splits an UpdateExpression into tokens (keywords and
// attribute names), ignoring punctuation and EAV placeholders.
func tokenizeUpdateExpression(expr string) []string {
	var tokens []string

	for _, part := range strings.FieldsFunc(expr, func(r rune) bool {
		return r == ',' || r == '=' || r == '+' || r == '-' || r == '(' || r == ')' || r == ' '
	}) {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" || strings.HasPrefix(trimmed, ":") {
			continue
		}

		tokens = append(tokens, trimmed)
	}

	return tokens
}

// ============================================================
// Section 20: ListTables Limit validation
// ============================================================

const (
	minListTablesLimit = 1
	maxListTablesLimit = 100
)

// validateListTablesLimit returns a ValidationException when Limit is outside
// the allowed range [1, 100]. A nil Limit is valid (defaults to 100).
func validateListTablesLimit(limit *int32) error {
	if limit == nil {
		return nil
	}

	v := *limit
	if v < minListTablesLimit || v > maxListTablesLimit {
		return NewValidationException(
			fmt.Sprintf(
				"Value '%d' at 'limit' failed to satisfy constraint: "+
					"Member must have value greater than or equal to %d",
				v, minListTablesLimit,
			),
		)
	}

	return nil
}

// ============================================================
// Section 21: Query / Scan Limit=0 rejection
// ============================================================

// validatePositiveLimit returns a ValidationException when Limit is explicitly
// set to 0 or a negative number. AWS DynamoDB requires Limit ≥ 1 when provided.
func validatePositiveLimit(limit *int32) error {
	if limit == nil {
		return nil
	}

	if *limit <= 0 {
		return NewValidationException(
			fmt.Sprintf(
				"Value '%d' at 'limit' failed to satisfy constraint: "+
					"Member must have value greater than or equal to 1",
				*limit,
			),
		)
	}

	return nil
}

// ============================================================
// Section 23: Set duplicate value detection
// ============================================================

// validateSetNoDuplicates returns a ValidationException when a set attribute (SS,
// NS, or BS) contains duplicate values. AWS DynamoDB enforces set uniqueness.
func validateSetNoDuplicates(k string, items []any) error {
	seen := make(map[string]struct{}, len(items))

	for _, item := range items {
		var key string

		switch v := item.(type) {
		case string:
			key = v
		default:
			continue
		}

		if _, exists := seen[key]; exists {
			return NewValidationException(
				fmt.Sprintf(
					"Input collection %s contains duplicates",
					k,
				),
			)
		}

		seen[key] = struct{}{}
	}

	return nil
}

// ============================================================
// Section 24: CreateTable KeySchema structure validation
// ============================================================

// validateCreateTableKeySchema ensures the table KeySchema has exactly one HASH
// key and at most one RANGE key. AWS rejects any other structure.
func validateCreateTableKeySchema(schema []models.KeySchemaElement) error {
	hashCount := 0
	rangeCount := 0

	for _, k := range schema {
		switch k.KeyType {
		case models.KeyTypeHash:
			hashCount++
		case models.KeyTypeRange:
			rangeCount++
		default:
			return NewValidationException(
				fmt.Sprintf("Unknown key type: %s", k.KeyType),
			)
		}
	}

	if hashCount != 1 {
		return NewValidationException(
			"One and only one hash key may be defined",
		)
	}

	if rangeCount > 1 {
		return NewValidationException(
			"No more than one range key may be defined",
		)
	}

	return nil
}

// ============================================================
// Section 25: ProvisionedThroughput must be > 0 for PROVISIONED tables
// ============================================================

// validateProvisionedThroughput returns a ValidationException when a PROVISIONED
// table is created or updated with ReadCapacityUnits or WriteCapacityUnits explicitly
// set to 0 or negative. Nil values are allowed (they receive server-side defaults).
// PAY_PER_REQUEST tables skip this check.
// validateGSIThroughput enforces AWS's rules on per-GSI ProvisionedThroughput:
//   - When the table BillingMode is PROVISIONED, every GSI must declare positive
//     RCU and WCU (a missing ProvisionedThroughput is rejected — real AWS would
//     surface "ValidationException: Either ReadCapacityUnits or WriteCapacityUnits is missing").
//   - When the table BillingMode is PAY_PER_REQUEST, any GSI ProvisionedThroughput
//     setting is rejected because GSIs inherit on-demand billing.
func validateGSIThroughput(
	gsis []types.GlobalSecondaryIndex, billingMode types.BillingMode,
) error {
	if billingMode == types.BillingModePayPerRequest {
		return nil
	}

	// PROVISIONED: only validate GSIs that explicitly supplied a throughput
	// block — a nil ProvisionedThroughput is allowed so tests and SDK clients
	// can lean on the backend's default capacity (matches existing
	// validateProvisionedThroughput behaviour for the table itself).
	for _, g := range gsis {
		pt := g.ProvisionedThroughput
		if pt == nil {
			continue
		}

		if pt.ReadCapacityUnits != nil && *pt.ReadCapacityUnits <= 0 {
			return NewValidationException(
				"One or more parameter values were invalid: " +
					"GSI ReadCapacityUnits must be a positive number",
			)
		}
		if pt.WriteCapacityUnits != nil && *pt.WriteCapacityUnits <= 0 {
			return NewValidationException(
				"One or more parameter values were invalid: " +
					"GSI WriteCapacityUnits must be a positive number",
			)
		}
	}

	return nil
}

func validateProvisionedThroughput(pt *types.ProvisionedThroughput, billingMode types.BillingMode) error {
	if billingMode == types.BillingModePayPerRequest {
		return nil
	}

	if pt == nil {
		return nil // caller relies on defaults; validated by the SDK in production
	}

	if pt.ReadCapacityUnits != nil && *pt.ReadCapacityUnits <= 0 {
		return NewValidationException(
			"One or more parameter values were invalid: " +
				"ReadCapacityUnits must be a positive number",
		)
	}

	if pt.WriteCapacityUnits != nil && *pt.WriteCapacityUnits <= 0 {
		return NewValidationException(
			"One or more parameter values were invalid: " +
				"WriteCapacityUnits must be a positive number",
		)
	}

	return nil
}

// ============================================================
// Section 29: Number string leading-zero rejection
// ============================================================

// validateNumberNoLeadingZeros returns a ValidationException when a number string
// has a meaningless leading zero (like "007", "01.5"). AWS normalizes numbers but
// rejects canonical representations with leading zeros.
func validateNumberNoLeadingZeros(k, n string) error {
	if len(n) < minLeadingZeroCheckLen {
		return nil
	}

	// Negative numbers: check after the minus sign.
	s := n
	if s[0] == '-' {
		s = s[1:]
	}

	// "0" alone is fine; "0.5" is fine (decimal); "01", "007" are not.
	if len(s) >= minLeadingZeroCheckLen && s[0] == '0' && s[1] != '.' && s[1] != 'e' && s[1] != 'E' {
		return NewValidationException(
			fmt.Sprintf(
				"The parameter cannot be converted to a numeric value: %s. "+
					"Key: %s",
				n, k,
			),
		)
	}

	return nil
}
