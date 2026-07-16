package dynamodb

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

const (
	estimatedMatchRateDivisor = 2
	minScanAllocationSize     = 10
	batchSizeLimit            = 25
	expectedPKParts           = 2
)

// rcuForCount returns the RCU cost for n eventually-consistent item reads.
// Each item costs 0.5 RCU; returns 0 when n is 0 (empty scan/query has no cost).
func rcuForCount(n int) float64 {
	const halfRCU = 0.5

	return float64(n) * halfRCU
}

// isItemExpired returns true when the table has a TTL attribute configured and
// the item's TTL value is in the past. Items without the attribute are never expired.
func isItemExpired(item map[string]any, ttlAttr string) bool {
	if ttlAttr == "" {
		return false
	}

	raw, ok := item[ttlAttr]
	if !ok {
		return false
	}

	// Unwrap DynamoDB value
	val := dynamoattr.UnwrapAttributeValue(raw)

	var ttl int64
	switch v := val.(type) {
	case string:
		ttl, _ = strconv.ParseInt(v, 10, 64)
	case int64:
		ttl = v
	case float64:
		ttl = int64(v)
	default:
		return false
	}

	if ttl == 0 {
		return false
	}

	return time.Now().Unix() > ttl
}

func (db *InMemoryDB) getTable(ctx context.Context, name string) (*Table, error) {
	table, exists := db.getTableRLock(ctx, name)
	if !exists {
		return nil, NewResourceNotFoundException("Requested resource not found")
	}

	status := tableStatusRLocked(table)

	if status != statusActive && status != "" {
		return nil, NewResourceNotFoundException("Requested resource not found")
	}

	return table, nil
}

// tableStatusRLocked returns table.Status under a defer-protected RLock, so a
// panic partway through can never leave a read lock held forever.
func tableStatusRLocked(table *Table) string {
	table.mu.RLock("checkStatus")
	defer table.mu.RUnlock()

	return table.Status
}

// getKeySchemaForPartiQL returns the key schema for the named table.
// Results are memoised in the expression cache (TTL: 10 minutes) to avoid
// repeated global-lock acquisitions on every PartiQL SELECT / UPDATE / DELETE.
// The cache is keyed by "partiql:ks:<tableName>" and is automatically invalidated
// when the entry expires, ensuring schema changes (e.g. recreation) are picked up.
func (db *InMemoryDB) getKeySchemaForPartiQL(
	ctx context.Context,
	tableName string,
) ([]models.KeySchemaElement, error) {
	cacheKey := "partiql:ks:" + tableName

	if v, ok := db.exprCache.Get(cacheKey); ok {
		ks, isKS := v.([]models.KeySchemaElement)
		if isKS {
			return ks, nil
		}
	}

	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	ks := copyKeySchemaRLocked(table)

	db.exprCache.Put(cacheKey, ks)

	return ks, nil
}

// copyKeySchemaRLocked returns a copy of table.KeySchema under a
// defer-protected RLock.
func copyKeySchemaRLocked(table *Table) []models.KeySchemaElement {
	table.mu.RLock("getKeySchemaForPartiQL")
	defer table.mu.RUnlock()

	ks := make([]models.KeySchemaElement, len(table.KeySchema))
	copy(ks, table.KeySchema)

	return ks
}

func (db *InMemoryDB) getTableRLock(ctx context.Context, name string) (*Table, bool) {
	db.mu.RLock("getTable")
	defer db.mu.RUnlock()

	region := getRegionFromContext(ctx, db)

	return db.tables.Get(tableKey(region, name))
}

func getPKAndSK(
	keySchema []models.KeySchemaElement,
) (models.KeySchemaElement, models.KeySchemaElement) {
	var pkDef, skDef models.KeySchemaElement
	for _, k := range keySchema {
		switch k.KeyType {
		case models.KeyTypeHash:
			pkDef = k
		case models.KeyTypeRange:
			skDef = k
		}
	}

	return pkDef, skDef
}

func resolveAttrName(name string, attrNames map[string]string) string {
	if !strings.HasPrefix(name, "#") {
		return name
	}

	if val, ok := attrNames[name]; ok {
		return val
	}

	return ""
}

func dbExtractValueFromToken(token string, attrValues map[string]any) string {
	val := dynamoattr.ResolveValue(token, attrValues)

	return dynamoattr.ToString(val)
}

func (db *InMemoryDB) lookupItem(
	table *Table,
	key map[string]any,
	pkName, skName string,
) map[string]any {
	pkVal := BuildKeyString(key, pkName)
	if skName != "" {
		skVal := BuildKeyString(key, skName)
		if skMap, hasPK := table.pkskIndex[pkVal]; hasPK {
			if itemIdx, hasSK := skMap[skVal]; hasSK {
				return table.Items[itemIdx]
			}
		}

		return nil
	}

	if itemIdx, found := table.pkIndex[pkVal]; found {
		return table.Items[itemIdx]
	}

	return nil
}

func (db *InMemoryDB) lookupItemWithIndex(
	table *Table,
	key map[string]any,
	pkName, skName string,
) (map[string]any, int) {
	pkVal := BuildKeyString(key, pkName)
	if skName != "" {
		skVal := BuildKeyString(key, skName)
		if skMap, hasPK := table.pkskIndex[pkVal]; hasPK {
			if itemIdx, hasSK := skMap[skVal]; hasSK {
				return table.Items[itemIdx], itemIdx
			}
		}

		return nil, -1
	}

	if itemIdx, found := table.pkIndex[pkVal]; found {
		return table.Items[itemIdx], itemIdx
	}

	return nil, -1
}

func extractKey(item map[string]any, schema []models.KeySchemaElement) map[string]any {
	key := make(map[string]any)
	for _, k := range schema {
		if val, ok := item[k.AttributeName]; ok {
			key[k.AttributeName] = val
		}
	}

	return key
}

// extractKeyWithBase builds a LastEvaluatedKey for index (GSI/LSI) queries.
// AWS DynamoDB requires the response to contain both the index key attributes
// AND the base-table primary key so that pagination tokens are unambiguous even
// when multiple items share the same index sort-key value.
func extractKeyWithBase(
	item map[string]any,
	indexSchema []models.KeySchemaElement,
	tableSchema []models.KeySchemaElement,
) map[string]any {
	key := extractKey(item, indexSchema)
	for _, k := range tableSchema {
		if _, exists := key[k.AttributeName]; !exists {
			if val, ok := item[k.AttributeName]; ok {
				key[k.AttributeName] = val
			}
		}
	}

	return key
}

// compareAttributeValues compares two DynamoDB attribute values without reflection.
// Values are always map[string]any with a single type key (e.g. {"S": "foo"}).
func compareAttributeValues(v1, v2 any) bool {
	m1, ok1 := v1.(map[string]any)
	m2, ok2 := v2.(map[string]any)

	if !ok1 || !ok2 {
		return fmt.Sprintf("%v", v1) == fmt.Sprintf("%v", v2)
	}

	if len(m1) != len(m2) {
		return false
	}

	for typeKey, leftVal := range m1 {
		rightVal, exists := m2[typeKey]
		if !exists {
			return false
		}

		if !compareTypedField(typeKey, leftVal, rightVal) {
			return false
		}
	}

	return true
}

// compareTypedField compares one DynamoDB-typed attribute field (M/L/SS/NS/BS or
// a scalar S/N/B/BOOL). Container types fall back to scalar comparison when the
// expected Go shape does not match, matching the original behaviour.
func compareTypedField(typeKey string, leftVal, rightVal any) bool {
	switch typeKey {
	case "M":
		return compareMapField(leftVal, rightVal)
	case "L":
		return compareListField(leftVal, rightVal)
	case "SS", "NS":
		return compareStringSetField(leftVal, rightVal)
	case "BS":
		return compareByteSetField(leftVal, rightVal)
	default:
		return compareScalarField(leftVal, rightVal)
	}
}

func compareMapField(leftVal, rightVal any) bool {
	m1, ok1 := leftVal.(map[string]any)
	m2, ok2 := rightVal.(map[string]any)
	if !ok1 || !ok2 {
		return compareScalarField(leftVal, rightVal)
	}

	if len(m1) != len(m2) {
		return false
	}

	for k, child1 := range m1 {
		child2, ok := m2[k]
		if !ok || !compareAttributeValues(child1, child2) {
			return false
		}
	}

	return true
}

func compareListField(leftVal, rightVal any) bool {
	l1, ok1 := leftVal.([]any)
	l2, ok2 := rightVal.([]any)
	if !ok1 || !ok2 {
		return compareScalarField(leftVal, rightVal)
	}

	if len(l1) != len(l2) {
		return false
	}

	for i := range l1 {
		if !compareAttributeValues(l1[i], l2[i]) {
			return false
		}
	}

	return true
}

func compareStringSetField(leftVal, rightVal any) bool {
	s1, ok1 := leftVal.([]string)
	s2, ok2 := rightVal.([]string)
	if !ok1 || !ok2 {
		return compareScalarField(leftVal, rightVal)
	}

	if len(s1) != len(s2) {
		return false
	}

	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}

	return true
}

func compareByteSetField(leftVal, rightVal any) bool {
	b1, ok1 := leftVal.([][]byte)
	b2, ok2 := rightVal.([][]byte)
	if !ok1 || !ok2 {
		return compareScalarField(leftVal, rightVal)
	}

	if len(b1) != len(b2) {
		return false
	}

	for i := range b1 {
		if !bytes.Equal(b1[i], b2[i]) {
			return false
		}
	}

	return true
}

// compareScalarField handles S/N (string), B ([]byte) and BOOL, with a
// stringified fallback for anything else.
func compareScalarField(leftVal, rightVal any) bool {
	if s1, okL := leftVal.(string); okL {
		if s2, okR := rightVal.(string); okR {
			return s1 == s2
		}
	}

	if b1, okL := leftVal.([]byte); okL {
		if b2, okR := rightVal.([]byte); okR {
			return bytes.Equal(b1, b2)
		}
	}

	if bl1, okL := leftVal.(bool); okL {
		if bl2, okR := rightVal.(bool); okR {
			return bl1 == bl2
		}
	}

	return fmt.Sprintf("%v", leftVal) == fmt.Sprintf("%v", rightVal)
}

func applyGSIProjection(
	item map[string]any,
	projection models.Projection,
	tableSchema []models.KeySchemaElement,
	gsiSchema []models.KeySchemaElement,
) map[string]any {
	if projection.ProjectionType == "ALL" {
		return item
	}

	newItem := make(map[string]any)
	for _, k := range tableSchema {
		if val, ok := item[k.AttributeName]; ok {
			newItem[k.AttributeName] = val
		}
	}

	for _, k := range gsiSchema {
		if val, ok := item[k.AttributeName]; ok {
			newItem[k.AttributeName] = val
		}
	}

	if projection.ProjectionType == "INCLUDE" {
		for _, attr := range projection.NonKeyAttributes {
			if val, ok := item[attr]; ok {
				newItem[attr] = val
			}
		}
	}

	return newItem
}

func compareAny(v1, v2 any, typ string) int {
	if v1 == nil || v2 == nil {
		return 0
	}

	switch typ {
	case "N":
		return compareNumbers(v1, v2)
	case "S":
		return compareStrings(v1, v2)
	case typeBOOL:
		return compareBools(v1, v2)
	case "B":
		b1 := toBytes(v1)
		b2 := toBytes(v2)

		return bytes.Compare(b1, b2)
	}

	// Fallback: convert to string for unknown or complex types (rare path)
	s1 := fmt.Sprintf("%v", v1)
	s2 := fmt.Sprintf("%v", v2)
	if s1 < s2 {
		return -1
	}
	if s1 > s2 {
		return 1
	}

	return 0
}

// fallbackCompare provides a deterministic ordering for values whose concrete type
// doesn't match the expected type for a given DynamoDB attribute. It converts both
// values to their [fmt.Sprintf] string representation and compares lexicographically,
// ensuring stable sort order for pagination even with unexpected/mismatched types.
func fallbackCompare(v1, v2 any) int {
	s1 := fmt.Sprintf("%v", v1)
	s2 := fmt.Sprintf("%v", v2)
	if s1 < s2 {
		return -1
	}
	if s1 > s2 {
		return 1
	}

	return 0
}

func compareNumbers(v1, v2 any) int {
	f1, _ := dynamoattr.ParseNumeric(v1)
	f2, _ := dynamoattr.ParseNumeric(v2)
	if f1 < f2 {
		return -1
	}
	if f1 > f2 {
		return 1
	}

	return 0
}

func compareStrings(v1, v2 any) int {
	s1, ok1 := v1.(string)
	s2, ok2 := v2.(string)
	if !ok1 || !ok2 {
		return fallbackCompare(v1, v2)
	}
	if s1 < s2 {
		return -1
	}
	if s1 > s2 {
		return 1
	}

	return 0
}

func compareBools(v1, v2 any) int {
	b1, ok1 := v1.(bool)
	b2, ok2 := v2.(bool)
	if !ok1 || !ok2 {
		return fallbackCompare(v1, v2)
	}
	if b1 == b2 {
		return 0
	}
	if !b1 { // false < true
		return -1
	}

	return 1
}

func toBytes(v any) []byte {
	switch b := v.(type) {
	case []byte:
		return b
	case string:
		// Wire format stores binary attributes as base64-encoded strings.
		decoded, err := base64.StdEncoding.DecodeString(b)
		if err != nil {
			return []byte(b) // Fall back to raw bytes if not valid base64
		}

		return decoded
	default:
		return nil
	}
}

// Helpers moved to utils.go

// snapshotIndexForQuery returns index copies appropriate for a single Query call.
// It must be called with table.mu held (read lock).
//
//   - GSI/LSI queries (idxName != ""):  the primary index is never consulted; return nil maps.
//   - Primary queries with known PK:    copy only that PK's entries — avoids O(n) full copy.
//   - Primary queries with unknown PK:  fall back to copying the full index.
func (db *InMemoryDB) snapshotIndexForQuery(
	table *Table,
	idxName string,
	pkValue string,
) (map[string]int, map[string]map[string]int) {
	if idxName != "" {
		// GSI/LSI query — primary index not used.
		return nil, nil
	}

	if pkValue != "" {
		return snapshotSinglePKIndex(table, pkValue)
	}

	// Unknown PK value — fall back to full index copy.
	return snapshotFullIndex(table)
}

// snapshotSinglePKIndex copies only the entries for pkValue from the primary index.
// Must be called with the table read-lock held.
func snapshotSinglePKIndex(
	table *Table,
	pkValue string,
) (map[string]int, map[string]map[string]int) {
	if table.pkskIndex != nil {
		return snapshotPKSKEntry(table.pkskIndex, pkValue)
	}

	if table.pkIndex != nil {
		return snapshotPKEntry(table.pkIndex, pkValue)
	}

	return nil, nil
}

// snapshotPKSKEntry copies a single partition key's sort-key map from a pksk index.
func snapshotPKSKEntry(
	pkskIndex map[string]map[string]int,
	pkValue string,
) (map[string]int, map[string]map[string]int) {
	skMap, ok := pkskIndex[pkValue]
	if !ok {
		return nil, make(map[string]map[string]int) // empty — no matching PK
	}

	m2 := make(map[string]int, len(skMap))
	maps.Copy(m2, skMap)

	return nil, map[string]map[string]int{pkValue: m2}
}

// snapshotPKEntry copies a single partition key entry from a pk-only index.
func snapshotPKEntry(
	pkIndex map[string]int,
	pkValue string,
) (map[string]int, map[string]map[string]int) {
	idx, ok := pkIndex[pkValue]
	if !ok {
		return make(map[string]int), nil // empty — no matching PK
	}

	return map[string]int{pkValue: idx}, nil
}

// snapshotFullIndex copies the entire primary index.
// Must be called with the table read-lock held.
func snapshotFullIndex(table *Table) (map[string]int, map[string]map[string]int) {
	pkIndexCopy := make(map[string]int, len(table.pkIndex))
	maps.Copy(pkIndexCopy, table.pkIndex)
	pkskIndexCopy := make(map[string]map[string]int, len(table.pkskIndex))

	for k, m := range table.pkskIndex {
		m2 := make(map[string]int, len(m))
		maps.Copy(m2, m)
		pkskIndexCopy[k] = m2
	}

	return pkIndexCopy, pkskIndexCopy
}

// snapshotItemsByOffset builds a sparse offset-keyed map containing only the
// item pointers referenced by pkIndexCopy or pkskIndexCopy (#57). The caller
// must already hold the table read-lock when these index copies were made.
func snapshotItemsByOffset(
	table *Table,
	pkIndexCopy map[string]int,
	pkskIndexCopy map[string]map[string]int,
) map[int]map[string]any {
	offsets := make(map[int]struct{})

	for _, idx := range pkIndexCopy {
		offsets[idx] = struct{}{}
	}

	for _, skMap := range pkskIndexCopy {
		for _, idx := range skMap {
			offsets[idx] = struct{}{}
		}
	}

	result := make(map[int]map[string]any, len(offsets))

	for idx := range offsets {
		if idx >= 0 && idx < len(table.Items) {
			result[idx] = table.Items[idx]
		}
	}

	return result
}

// getAttributeType returns the attribute type for a given attribute name, or defaultType if not found.
func getAttributeType(
	attrDefs []models.AttributeDefinition,
	attrName string,
	defaultType string,
) string {
	for _, ad := range attrDefs {
		if ad.AttributeName == attrName {
			return ad.AttributeType
		}
	}

	return defaultType
}

// findExclusiveStartIndex finds the index after the ExclusiveStartKey in the candidates list.
// Returns 0 if ExclusiveStartKey is nil or not found.
func findExclusiveStartIndex(
	candidates []map[string]any,
	exclusiveStartKey map[string]any,
	keySchema []models.KeySchemaElement,
	tableKeySchema []models.KeySchemaElement,
) int {
	if exclusiveStartKey == nil {
		return 0
	}

	pkDef, skDef := getPKAndSK(keySchema)
	tablePKDef, tableSKDef := getPKAndSK(tableKeySchema)

	for i, item := range candidates {
		if itemMatchesStartKeyMap(item, exclusiveStartKey, pkDef, skDef, tablePKDef, tableSKDef) {
			return i + 1
		}
	}

	return 0
}

// itemMatchesStartKeyMap reports whether item matches the ExclusiveStartKey for the given
// index and base-table key schemas. Base-table keys are used for disambiguation when
// index sort keys repeat (GSI/LSI pagination).
func itemMatchesStartKeyMap(
	item, startKey map[string]any,
	pkDef, skDef models.KeySchemaElement,
	tablePKDef, tableSKDef models.KeySchemaElement,
) bool {
	if !compareAttributeValues(item[pkDef.AttributeName], startKey[pkDef.AttributeName]) {
		return false
	}

	if skDef.AttributeName != "" &&
		!compareAttributeValues(item[skDef.AttributeName], startKey[skDef.AttributeName]) {
		return false
	}

	// Disambiguate using the base-table PK when the ExclusiveStartKey includes it.
	if tablePKDef.AttributeName == "" || tablePKDef.AttributeName == pkDef.AttributeName {
		return true
	}

	if tblPKVal, ok := startKey[tablePKDef.AttributeName]; ok {
		if !compareAttributeValues(item[tablePKDef.AttributeName], tblPKVal) {
			return false
		}
	}

	if tableSKDef.AttributeName == "" || tableSKDef.AttributeName == skDef.AttributeName {
		return true
	}

	if tblSKVal, ok := startKey[tableSKDef.AttributeName]; ok {
		return compareAttributeValues(item[tableSKDef.AttributeName], tblSKVal)
	}

	return true
}
