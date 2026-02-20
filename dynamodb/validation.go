package dynamodb

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/blackbirdworks/gopherstack/dynamodb/models"
)

const (
	MaxItemSize         = 400 * 1024 // 400 KB
	MaxPartitionKeySize = 2048       // 2048 bytes
	MaxSortKeySize      = 1024       // 1024 bytes

	wcuBytes = 1024 // 1 WCU per KB
	rcuBytes = 4096 // 1 RCU per 4 KB
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

// CalculateItemSize approximates the DynamoDB item size.
// Size = sum of (len(attribute_name) + len(attribute_value))
// For simplicity in V1, we serialize to JSON and use the length, which is a rough upper bound/approximation.
// A more accurate specific calculation would iterate keys and values.
func CalculateItemSize(item map[string]any) (int, error) {
	// Accurate calculation per AWS:
	// Strings: UTF-8 bytes
	// Numbers: Approximate bytes? JSON len is decent proxy.
	// Binary: Raw bytes.
	// Boolean: 1 byte.
	// Null: 1 byte.
	// Map/List: Overhead + elements.

	// For this implementation, let's just use a JSON dump size as a safe proxy.
	// It overestimates structure syntax ({, ", :) but underestimates nothing.
	// AWS size is pure data size.
	// Let's implement a recursive sizer for better accuracy if needed,
	// but JSON marshal is robust enough for "Validation & Limits" phase 1.
	b, err := json.Marshal(item)
	if err != nil {
		return 0, err
	}

	return len(b), nil
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

		// Check size
		// We need to unwrap if it's a DynamoDB JSON format (e.g. {"S": "val"}) or raw?
		// The `item` map typically comes from `PutItemInput` which uses `map[string]any`
		// but the values are ostensibly map[string]any (the "S" wrapper).

		// Helper to get raw value size
		// We reuse calculateItemSize for the value part
		// Or just marshal the value
		b, _ := json.Marshal(val)
		size := len(b) // Approximation

		limit := MaxPartitionKeySize
		if k.KeyType == "RANGE" {
			limit = MaxSortKeySize
		}

		if size > limit {
			return NewValidationException(
				fmt.Sprintf(
					"Key element %s size %d exceeds limit %d",
					k.AttributeName,
					size,
					limit,
				),
			)
		}
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
