package dynamodb

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

const (
	MaxItemSize         = 400 * 1024 // 400 KB
	MaxPartitionKeySize = 2048       // 2048 bytes
	MaxSortKeySize      = 1024       // 1024 bytes

	wcuBytes = 1024 // 1 WCU per KB
	rcuBytes = 4096 // 1 RCU per 4 KB

	// base64GroupBits is the number of raw bytes encoded in each 4-character base64 group.
	base64GroupBits = 3
	// base64GroupChars is the number of base64 characters per encoded group.
	base64GroupChars = 4
	// ddbContainerOverhead is the fixed overhead DynamoDB adds for Map and List containers.
	ddbContainerOverhead = 3
	// perItemOverhead is the fixed overhead DynamoDB adds for each item.
	perItemOverhead = 100
)

// WriteCapacityUnits returns the WCUs consumed by a write: ceil(size / 1KB), minimum 1.
func WriteCapacityUnits(item map[string]any) float64 {
	size, err := CalculateItemSize(item)
	if err != nil || size <= 0 {
		return 1.0
	}

	return float64((size + wcuBytes - 1) / wcuBytes)
}

// ReadCapacityUnits returns the RCUs consumed by an eventually-consistent read:
// 0.5 RCU per 4 KB (ceiling), minimum 0.5.
func ReadCapacityUnits(item map[string]any) float64 {
	size, err := CalculateItemSize(item)
	if err != nil || size <= 0 {
		return models.ConsumedReadUnit
	}

	return float64((size+rcuBytes-1)/rcuBytes) * models.ConsumedReadUnit
}

// CalculateItemSize approximates the DynamoDB-encoded size of a wire-format item in bytes.
func CalculateItemSize(item map[string]any) (int, error) {
	if item == nil {
		return 0, nil
	}

	size := int64(perItemOverhead)

	for attrName, attrVal := range item {
		size += int64(len(attrName)) + CalculateAttrSize(attrVal)
	}

	return int(size), nil
}

// CalculateAttrSize estimates the encoded size of a single DynamoDB wire-format attribute value.
func CalculateAttrSize(v any) int64 {
	m, isMap := v.(map[string]any)
	if !isMap {
		return 1
	}

	if s, ok := m["S"].(string); ok {
		return int64(len(s))
	}
	if n, ok := m["N"].(string); ok {
		return calcNumericSize(n)
	}
	if b, ok := m["B"].(string); ok {
		return base64DecodedLen(b)
	}
	if _, ok := m["BOOL"]; ok {
		return 1
	}
	if _, ok := m["NULL"]; ok {
		return 1
	}
	if total, ok := calcSSSize(m["SS"]); ok {
		return total
	}
	if total, ok := calcNSSize(m["NS"]); ok {
		return total
	}
	if total, ok := calcBSSize(m["BS"]); ok {
		return total
	}
	if nested, ok := m["M"].(map[string]any); ok {
		return calcMapSize(nested)
	}
	if list, ok := m["L"].([]any); ok {
		return calcListSize(list)
	}

	return 1
}

// calcNumericSize returns the byte size used by a DynamoDB numeric attribute value.
// An empty string is treated as size 1 because DynamoDB requires at least one digit
// and stores a minimum of 1 byte for any number attribute.
func calcNumericSize(n string) int64 {
	sz := len(n)
	if sz == 0 {
		sz = 1
	}

	return int64(sz)
}

// base64DecodedLen returns the exact decoded byte length of a standard base64-encoded
// string, accounting for '=' padding characters. This avoids the overcounting that
// occurs with the naive len(s)*3/4 formula when padding is present.
// For example, "Zg==" encodes 1 byte but len*3/4 = 3; this function returns 1.
func base64DecodedLen(s string) int64 {
	n := len(s)
	if n == 0 {
		return 0
	}

	// base64 produces ceil(rawLen/3)*4 chars; each group of 4 chars → 3 bytes.
	decoded := int64(n) * base64GroupBits / base64GroupChars

	// Subtract the bytes represented by padding characters.
	// Valid base64 with double-padding has s[n-2]='=' and s[n-1]='='.
	// Valid base64 with single-padding has only s[n-1]='='.
	if n >= 2 && s[n-2] == '=' && s[n-1] == '=' {
		decoded -= 2
	} else if n >= 1 && s[n-1] == '=' {
		decoded--
	}

	return decoded
}

func calcSSSize(v any) (int64, bool) {
	switch ss := v.(type) {
	case []string:
		var total int64
		for _, s := range ss {
			total += int64(len(s))
		}

		return total, true
	case []any:
		var total int64
		for _, s := range ss {
			if str, ok := s.(string); ok {
				total += int64(len(str))
			}
		}

		return total, true
	}

	return 0, false
}

func calcNSSize(v any) (int64, bool) {
	switch ns := v.(type) {
	case []string:
		var total int64
		for _, n := range ns {
			total += calcNumericSize(n)
		}

		return total, true
	case []any:
		var total int64
		for _, n := range ns {
			if str, ok := n.(string); ok {
				total += calcNumericSize(str)
			}
		}

		return total, true
	}

	return 0, false
}

func calcBSSize(v any) (int64, bool) {
	switch bs := v.(type) {
	case []string:
		var total int64
		for _, b := range bs {
			total += base64DecodedLen(b)
		}

		return total, true
	case []any:
		var total int64
		for _, b := range bs {
			if s, isStr := b.(string); isStr {
				total += base64DecodedLen(s)
			}
		}

		return total, true
	}

	return 0, false
}

func calcMapSize(nested map[string]any) int64 {
	total := int64(ddbContainerOverhead)
	for k, val := range nested {
		total += int64(len(k)) + CalculateAttrSize(val)
	}

	return total
}

func calcListSize(list []any) int64 {
	total := int64(ddbContainerOverhead)
	for _, elem := range list {
		total += CalculateAttrSize(elem)
	}

	return total
}

func ValidateItemSize(item map[string]any) error {
	size, err := CalculateItemSize(item)
	if err != nil {
		return err // Internal validation error
	}
	if size > MaxItemSize {
		return NewValidationException(
			fmt.Sprintf("Item size %d exceeds limit %d", size, MaxItemSize),
		)
	}

	return nil
}

func validateKeySchema(item map[string]any, schema []models.KeySchemaElement) error {
	for _, k := range schema {
		val, ok := item[k.AttributeName]
		if !ok {
			return NewValidationException(fmt.Sprintf("Missing key element: %s", k.AttributeName))
		}

		if err := validateKeyAttribute(k, val); err != nil {
			return err
		}
	}

	return nil
}

// validateKeyAttribute checks a single key attribute value for type constraints and size limits.
func validateKeyAttribute(k models.KeySchemaElement, val any) error {
	valMap, isMap := val.(map[string]any)
	if !isMap {
		return nil
	}

	if sVal, hasS := valMap["S"]; hasS {
		if str, isStr := sVal.(string); isStr && str == "" {
			return NewValidationException(fmt.Sprintf(
				"One or more parameter values not valid. "+
					"The AttributeValue for a key attribute cannot contain an empty string value. Key: %s",
				k.AttributeName,
			))
		}
	}

	// AWS key size limit is based on the attribute value size alone (name + value bytes).
	attrSize := int(int64(len(k.AttributeName)) + CalculateAttrSize(val))

	limit := MaxPartitionKeySize
	if k.KeyType == "RANGE" {
		limit = MaxSortKeySize
	}

	if attrSize > limit {
		return NewValidationException(fmt.Sprintf(
			"Key element %s size %d exceeds limit %d",
			k.AttributeName,
			attrSize,
			limit,
		))
	}

	return nil
}

// ValidateDataTypes checks basic type conformance.
func ValidateDataTypes(item map[string]any) error {
	for k, v := range item {
		if err := validateAttribute(k, v); err != nil {
			return err
		}
	}

	return nil
}

func validateAttribute(k string, v any) error {
	valMap, ok := v.(map[string]any)
	if !ok {
		return NewValidationException(fmt.Sprintf("Attribute %s must be a map", k))
	}

	if len(valMap) != 1 {
		return NewValidationException(
			fmt.Sprintf("Attribute %s must contain exactly one type specifier", k),
		)
	}

	for t, val := range valMap {
		if err := validateTypeValue(k, t, val); err != nil {
			return err
		}
	}

	return nil
}

const (
	typeS    = "S"
	typeN    = "N"
	typeBOOL = "BOOL"
	typeNULL = "NULL"
	typeB    = "B"
	typeL    = "L"
	typeM    = "M"
	typeSS   = "SS"
	typeNS   = "NS"
	typeBS   = "BS"
)

func validateTypeValue(k, t string, val any) error {
	switch t {
	case typeS, typeN, typeBOOL, typeNULL, typeB:
		return validateScalarValue(k, t, val)
	case typeL, typeM:
		return validateComplexValue(k, t, val)
	case typeSS, typeNS, typeBS:
		return validateSetValue(k, t, val)
	default:
		return NewValidationException(fmt.Sprintf("Attribute %s has unknown type: %s", k, t))
	}
}

func validateSetValue(k, t string, val any) error {
	list, err := normalizeSetList(k, t, val)
	if err != nil {
		return err
	}

	if len(list) == 0 {
		return NewValidationException(fmt.Sprintf("Attribute %s of type %s cannot be empty", k, t))
	}

	for _, item := range list {
		if itemErr := validateSetItem(k, t, item); itemErr != nil {
			return itemErr
		}
	}

	return nil
}

func normalizeSetList(k, t string, val any) ([]any, error) {
	switch v := val.(type) {
	case []any:
		return v, nil
	case []string:
		list := make([]any, len(v))
		for i, s := range v {
			list[i] = s
		}

		return list, nil
	case [][]byte:
		list := make([]any, len(v))
		for i, b := range v {
			list[i] = string(b)
		}

		return list, nil
	default:
		return nil, NewValidationException(
			fmt.Sprintf("Attribute %s of type %s must be a list, got %T", k, t, val),
		)
	}
}

func validateSetItem(k, t string, item any) error {
	switch t {
	case typeSS:
		if _, ok := item.(string); !ok {
			return NewValidationException(fmt.Sprintf("Attribute %s elements must be strings", k))
		}
	case typeNS:
		s, ok := item.(string)
		if !ok {
			return NewValidationException(
				fmt.Sprintf(
					"Attribute %s elements must be strings (numbers represented as strings)",
					k,
				),
			)
		}
		if _, err := strconv.ParseFloat(s, 64); err != nil {
			return NewValidationException(
				fmt.Sprintf("Attribute %s element %s must be a valid number", k, s),
			)
		}
	case typeBS:
		if _, ok := item.(string); !ok {
			// We expect base64 strings in the wire format for B/BS
			return NewValidationException(
				fmt.Sprintf("Attribute %s elements must be base64-encoded strings", k),
			)
		}
	}

	return nil
}

func validateScalarValue(k, t string, val any) error {
	switch t {
	case "S":
		if _, ok := val.(string); !ok {
			return NewValidationException(fmt.Sprintf("Attribute %s of type S must be a string", k))
		}
	case "N":
		valStr, ok := val.(string)
		if !ok {
			return NewValidationException(fmt.Sprintf("Attribute %s of type N must be a string", k))
		}
		if _, err := strconv.ParseFloat(valStr, 64); err != nil {
			return NewValidationException(
				fmt.Sprintf("Attribute %s of type N must be a valid number", k),
			)
		}
	case "BOOL":
		if _, ok := val.(bool); !ok {
			return NewValidationException(
				fmt.Sprintf("Attribute %s of type BOOL must be a boolean", k),
			)
		}
	case "B":
		if _, ok := val.(string); !ok {
			return NewValidationException(
				fmt.Sprintf("Attribute %s of type B must be a base64 string", k),
			)
		}
	}

	return nil
}

func validateComplexValue(k, t string, val any) error {
	switch t {
	case "L":
		list, ok := val.([]any)
		if !ok {
			return NewValidationException(fmt.Sprintf("Attribute %s of type L must be a list", k))
		}
		_ = list
	case "M":
		m, ok := val.(map[string]any)
		if !ok {
			return NewValidationException(fmt.Sprintf("Attribute %s of type M must be a map", k))
		}
		if err := ValidateDataTypes(m); err != nil {
			return err
		}
	}

	return nil
}

// validateQueryKeyValues checks that ExpressionAttributeValues referenced by key
// attribute conditions in the KeyConditionExpression do not contain empty string values.
func validateQueryKeyValues(
	exprParts []string,
	keySchema []models.KeySchemaElement,
	eav map[string]any,
	attrNames map[string]string,
) error {
	keyNames := buildKeyNamesMap(keySchema, attrNames)

	for _, part := range exprParts {
		part = strings.TrimSpace(part)

		keyAttr := findKeyAttributeInExpression(part, keyNames)
		if keyAttr == "" {
			continue
		}

		if err := checkEAVForEmptyStrings(part, eav, keyAttr); err != nil {
			return err
		}
	}

	return nil
}

func buildKeyNamesMap(keySchema []models.KeySchemaElement, attrNames map[string]string) map[string]string {
	keyNames := make(map[string]string, len(keySchema))
	for _, k := range keySchema {
		keyNames[k.AttributeName] = k.AttributeName
	}

	for alias, name := range attrNames {
		if actual, isKey := keyNames[name]; isKey {
			keyNames[alias] = actual
		}
	}

	return keyNames
}

func findKeyAttributeInExpression(part string, keyNames map[string]string) string {
	for name, actual := range keyNames {
		if containsToken(part, name) {
			return actual
		}
	}

	return ""
}

func checkEAVForEmptyStrings(part string, eav map[string]any, keyAttr string) error {
	for tok, val := range eav {
		if !containsToken(part, tok) {
			continue
		}

		valMap, ok := val.(map[string]any)
		if !ok {
			continue
		}

		sVal, hasS := valMap["S"]
		if !hasS {
			continue
		}

		str, ok := sVal.(string)
		if ok && str == "" {
			return NewValidationException(fmt.Sprintf(
				"One or more parameter values not valid. "+
					"The AttributeValue for a key attribute cannot contain an empty string value. Key: %s",
				keyAttr,
			))
		}
	}

	return nil
}

// containsToken reports whether token appears in expr as a complete identifier
// token (not as a substring of a longer identifier).
func containsToken(expr, token string) bool {
	idx := strings.Index(expr, token)
	if idx < 0 {
		return false
	}
	end := idx + len(token)
	before := idx == 0 || !isIdentChar(expr[idx-1])
	after := end == len(expr) || !isIdentChar(expr[end])

	return before && after
}

func isIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}
