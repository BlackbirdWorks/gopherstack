package dynamodb

import (
	"bytes"
	"context"
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

	return table, nil
}

func (db *InMemoryDB) getTableRLock(ctx context.Context, name string) (*Table, bool) {
	db.mu.RLock("getTable")
	defer db.mu.RUnlock()

	region := getRegionFromContext(ctx, db)
	regionTables, regionExists := db.Tables[region]
	if regionExists {
		if table, tableExists := regionTables[name]; tableExists {
			return table, true
		}
	}

	return nil, false
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

// compareAttributeValues compares two DynamoDB attribute values without reflection.
// Values are always map[string]any with a single type key (e.g. {"S": "foo"}).
func compareAttributeValues(v1, v2 any) bool {
	m1, ok1 := v1.(map[string]any)
	m2, ok2 := v2.(map[string]any)

	if !ok1 || !ok2 {
		// Fallback for bare Go primitives (shouldn't occur in normal operation).
		return fmt.Sprintf("%v", v1) == fmt.Sprintf("%v", v2)
	}

	for typeKey, val1 := range m1 {
		val2, exists := m2[typeKey]
		if !exists {
			return false
		}

		s1, isStr1 := val1.(string)
		s2, isStr2 := val2.(string)

		if isStr1 && isStr2 {
			return s1 == s2
		}

		// Binary attribute (B type) — use bytes.Equal for correct comparison.
		b1, isByte1 := val1.([]byte)
		b2, isByte2 := val2.([]byte)
		if isByte1 && isByte2 {
			return bytes.Equal(b1, b2)
		}

		// Nested map (e.g. M, L types) — fall back to string representation.
		return fmt.Sprintf("%v", val1) == fmt.Sprintf("%v", val2)
	}

	return len(m2) == 0
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

	// Handle numeric types directly (fast path, no allocations)
	if typ == "N" {
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

	// Handle string types directly without fmt.Sprintf allocation (fast path)
	if typ == "S" {
		s1Str, ok1 := v1.(string)
		s2Str, ok2 := v2.(string)
		if !ok1 || !ok2 {
			// Fallback to general comparison if not string
			goto Fallback
		}
		if s1Str < s2Str {
			return -1
		}
		if s1Str > s2Str {
			return 1
		}

		return 0
	}

Fallback:

	// Fallback: convert to string only for unknown or complex types (rare path)
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
func snapshotSinglePKIndex(table *Table, pkValue string) (map[string]int, map[string]map[string]int) {
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
func snapshotPKEntry(pkIndex map[string]int, pkValue string) (map[string]int, map[string]map[string]int) {
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
) int {
	if exclusiveStartKey == nil {
		return 0
	}

	pkDef, skDef := getPKAndSK(keySchema)

	for i, item := range candidates {
		matches := compareAttributeValues(
			item[pkDef.AttributeName],
			exclusiveStartKey[pkDef.AttributeName],
		)
		if matches && skDef.AttributeName != "" {
			matches = compareAttributeValues(
				item[skDef.AttributeName],
				exclusiveStartKey[skDef.AttributeName],
			)
		}

		if matches {
			return i + 1
		}
	}

	return 0
}
