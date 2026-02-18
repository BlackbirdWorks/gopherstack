package dynamodb

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"Gopherstack/dynamodb/models"
	"Gopherstack/pkgs/dynamoattr"
)

const (
	estimatedMatchRateDivisor = 2
	minScanAllocationSize     = 10
	batchSizeLimit            = 25
	expectedPKParts           = 2
)

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

func (db *InMemoryDB) getTable(name string) (*Table, error) {
	db.mu.RLock()
	table, exists := db.Tables[name]
	db.mu.RUnlock()

	if !exists {
		return nil, NewResourceNotFoundException("Requested resource not found")
	}

	return table, nil
}

func getPKAndSK(keySchema []models.KeySchemaElement) (models.KeySchemaElement, models.KeySchemaElement) {
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

		// Nested map (e.g. M, L types) — fall back to string representation.
		return fmt.Sprintf("%v", val1) == fmt.Sprintf("%v", v2)
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
		if ok1 && ok2 {
			if s1Str < s2Str {
				return -1
			}
			if s1Str > s2Str {
				return 1
			}
			return 0
		}
	}

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

// getAttributeType returns the attribute type for a given attribute name, or defaultType if not found.
func getAttributeType(attrDefs []models.AttributeDefinition, attrName string, defaultType string) string {
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
		matches := compareAttributeValues(item[pkDef.AttributeName], exclusiveStartKey[pkDef.AttributeName])
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
