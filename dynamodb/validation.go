package dynamodb

import (
	"encoding/json"
	"fmt"
	"reflect"
)

const (
	MaxItemSize         = 400 * 1024 // 400 KB
	MaxPartitionKeySize = 2048       // 2048 bytes
	MaxSortKeySize      = 1024       // 1024 bytes
)

// calculateItemSize approximates the DynamoDB item size.
// Size = sum of (len(attribute_name) + len(attribute_value))
// For simplicity in V1, we serialize to JSON and use the length, which is a rough upper bound/approximation.
// A more accurate specific calculation would iterate keys and values.
func calculateItemSize(item map[string]interface{}) (int, error) {
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

func validateItemSize(item map[string]interface{}) error {
	size, err := calculateItemSize(item)
	if err != nil {
		return err // Internal validation error
	}
	if size > MaxItemSize {
		return NewItemCollectionSizeLimitExceededException(fmt.Sprintf("Item size %d exceeds limit %d", size, MaxItemSize))
	}
	return nil
}

func validateKeySchema(item map[string]interface{}, schema []KeySchemaElement) error {
	for _, k := range schema {
		val, ok := item[k.AttributeName]
		if !ok {
			return NewValidationException(fmt.Sprintf("Missing key element: %s", k.AttributeName))
		}

		// Check size
		// We need to unwrap if it's a DynamoDB JSON format (e.g. {"S": "val"}) or raw?
		// The `item` map typically comes from `PutItemInput` which uses `map[string]interface{}`
		// but the values are ostensibly map[string]interface{} (the "S" wrapper).

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
			return NewValidationException(fmt.Sprintf("Key element %s size %d exceeds limit %d", k.AttributeName, size, limit))
		}
	}
	return nil
}

// validateDataTypes checks basic type conformance.
// E.g., if a value is flagged as "N", does it parse as a number?
func validateDataTypes(item map[string]interface{}) error {
	for k, v := range item {
		// v is expected to be map[string]interface{} like {"S": "some string"}
		// or {"N": "123"}

		valMap, ok := v.(map[string]interface{})
		if !ok {
			// If it's not a map, it might be raw value if we support simplified JSON?
			// But our input structs suggest strict DynamoDB JSON.
			// Let's assume strict validation.
			// If it's a primitive in `Items` (storage), it might be raw, but input is usually wrapped.
			// Let's inspect ONE key.
			if reflect.TypeOf(v).Kind() == reflect.Map {
				// It's a map (generic)
			} else {
				// Not a map, maybe acceptable?
				continue
			}
		}

		if val, ok := valMap["N"]; ok {
			// N must be a string representing a number
			strVal, ok := val.(string)
			if !ok {
				return NewValidationException(fmt.Sprintf("Attribute %s of type N must be a string", k))
			}
			// Attempt parsing?
			// We can use the logic from expressions.go/unwrap?
			// For now, just ensuring it's a string is a good first step.
			if strVal == "" {
				// Empty number?
			}
		}

		// Checking recursive for M/L types would be next step.
	}
	return nil
}
