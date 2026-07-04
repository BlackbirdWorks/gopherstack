package apigateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
)

// errSchemaViolation is the sentinel returned when a request body fails JSON Schema
// validation. Callers surface the AWS "Invalid request body" 400 rather than the
// detailed message, matching API Gateway's opaque body-validation response.
var errSchemaViolation = errors.New("request body does not conform to schema")

// validateJSONAgainstSchema validates a JSON document against a JSON Schema (the
// draft-4 subset AWS API Gateway models use). It supports type, required, properties,
// additionalProperties (bool), items, enum, minimum/maximum (incl. exclusive*),
// minLength/maxLength, pattern-free string checks, minItems/maxItems, and local $ref
// (#/definitions/...). An unparseable or empty schema is treated as "no constraint".
func validateJSONAgainstSchema(body []byte, schema string) error {
	schema = strings.TrimSpace(schema)
	if schema == "" {
		return nil
	}

	root, ok := parseSchemaObject(schema)
	if !ok {
		// A malformed model schema cannot constrain the request; do not reject.
		return nil
	}

	var doc any
	if err := json.Unmarshal(body, &doc); err != nil {
		return errSchemaViolation
	}

	v := &schemaValidator{root: root}

	return v.validate(root, doc)
}

// parseSchemaObject unmarshals a JSON Schema document into a map, reporting ok=false
// when the schema is not a parseable JSON object.
func parseSchemaObject(schema string) (map[string]any, bool) {
	var root map[string]any
	if err := json.Unmarshal([]byte(schema), &root); err != nil {
		return nil, false
	}

	return root, true
}

// schemaValidator carries the root schema so that local $ref pointers can be resolved.
type schemaValidator struct {
	root map[string]any
}

func (v *schemaValidator) validate(schema map[string]any, value any) error {
	if resolved, ok := v.resolveRef(schema); ok {
		schema = resolved
	}

	if err := v.validateType(schema, value); err != nil {
		return err
	}
	if err := v.validateEnum(schema, value); err != nil {
		return err
	}

	switch typed := value.(type) {
	case map[string]any:
		return v.validateObject(schema, typed)
	case []any:
		return v.validateArray(schema, typed)
	case string:
		return v.validateString(schema, typed)
	case float64:
		return v.validateNumber(schema, typed)
	default:
		return nil
	}
}

// resolveRef follows a local "$ref": "#/definitions/Name" (or "#") pointer one hop.
func (v *schemaValidator) resolveRef(schema map[string]any) (map[string]any, bool) {
	ref, ok := schema["$ref"].(string)
	if !ok || !strings.HasPrefix(ref, "#") {
		return nil, false
	}

	pointer := strings.TrimPrefix(ref, "#")
	pointer = strings.TrimPrefix(pointer, "/")
	if pointer == "" {
		return v.root, true
	}

	cur := any(v.root)
	for seg := range strings.SplitSeq(pointer, "/") {
		m, isMap := cur.(map[string]any)
		if !isMap {
			return nil, false
		}
		cur, ok = m[seg]
		if !ok {
			return nil, false
		}
	}

	resolved, isMap := cur.(map[string]any)

	return resolved, isMap
}

func (v *schemaValidator) validateType(schema map[string]any, value any) error {
	rawType, ok := schema["type"]
	if !ok {
		return nil
	}

	types := toStringSlice(rawType)
	if len(types) == 0 {
		return nil
	}

	for _, t := range types {
		if jsonTypeMatches(t, value) {
			return nil
		}
	}

	return fmt.Errorf("%w: expected type %v", errSchemaViolation, types)
}

func (v *schemaValidator) validateEnum(schema map[string]any, value any) error {
	rawEnum, ok := schema["enum"].([]any)
	if !ok {
		return nil
	}

	for _, candidate := range rawEnum {
		if jsonValuesEqual(candidate, value) {
			return nil
		}
	}

	return fmt.Errorf("%w: value not in enum", errSchemaViolation)
}

func (v *schemaValidator) validateObject(schema map[string]any, obj map[string]any) error {
	if err := v.validateRequired(schema, obj); err != nil {
		return err
	}

	props, _ := schema["properties"].(map[string]any)
	for name, sub := range props {
		child, present := obj[name]
		if !present {
			continue
		}
		subSchema, isMap := sub.(map[string]any)
		if !isMap {
			continue
		}
		if err := v.validate(subSchema, child); err != nil {
			return err
		}
	}

	return v.validateAdditionalProperties(schema, obj, props)
}

func (v *schemaValidator) validateRequired(schema map[string]any, obj map[string]any) error {
	required, ok := schema["required"].([]any)
	if !ok {
		return nil
	}
	for _, r := range required {
		name, isStr := r.(string)
		if !isStr {
			continue
		}
		if _, present := obj[name]; !present {
			return fmt.Errorf("%w: missing required property %q", errSchemaViolation, name)
		}
	}

	return nil
}

func (v *schemaValidator) validateAdditionalProperties(
	schema map[string]any, obj, props map[string]any,
) error {
	ap, ok := schema["additionalProperties"]
	if !ok {
		return nil
	}
	// additionalProperties: false forbids properties not named in "properties".
	if allowed, isBool := ap.(bool); isBool && !allowed {
		for name := range obj {
			if _, declared := props[name]; !declared {
				return fmt.Errorf("%w: additional property %q not allowed", errSchemaViolation, name)
			}
		}
	}

	return nil
}

func (v *schemaValidator) validateArray(schema map[string]any, arr []any) error {
	if minItems, ok := toFloat(schema["minItems"]); ok && float64(len(arr)) < minItems {
		return fmt.Errorf("%w: array shorter than minItems", errSchemaViolation)
	}
	if maxItems, ok := toFloat(schema["maxItems"]); ok && float64(len(arr)) > maxItems {
		return fmt.Errorf("%w: array longer than maxItems", errSchemaViolation)
	}

	itemSchema, ok := schema["items"].(map[string]any)
	if !ok {
		return nil
	}
	for _, item := range arr {
		if err := v.validate(itemSchema, item); err != nil {
			return err
		}
	}

	return nil
}

func (v *schemaValidator) validateString(schema map[string]any, s string) error {
	if minLen, ok := toFloat(schema["minLength"]); ok && float64(len(s)) < minLen {
		return fmt.Errorf("%w: string shorter than minLength", errSchemaViolation)
	}
	if maxLen, ok := toFloat(schema["maxLength"]); ok && float64(len(s)) > maxLen {
		return fmt.Errorf("%w: string longer than maxLength", errSchemaViolation)
	}

	return nil
}

func (v *schemaValidator) validateNumber(schema map[string]any, n float64) error {
	if minimum, ok := toFloat(schema["minimum"]); ok {
		exclusive, _ := schema["exclusiveMinimum"].(bool)
		if (exclusive && n <= minimum) || (!exclusive && n < minimum) {
			return fmt.Errorf("%w: number below minimum", errSchemaViolation)
		}
	}
	if maximum, ok := toFloat(schema["maximum"]); ok {
		exclusive, _ := schema["exclusiveMaximum"].(bool)
		if (exclusive && n >= maximum) || (!exclusive && n > maximum) {
			return fmt.Errorf("%w: number above maximum", errSchemaViolation)
		}
	}

	return nil
}

// jsonTypeMatches reports whether value satisfies a JSON Schema "type" keyword.
func jsonTypeMatches(schemaType string, value any) bool {
	switch schemaType {
	case "object":
		_, ok := value.(map[string]any)

		return ok
	case "array":
		_, ok := value.([]any)

		return ok
	case "string":
		_, ok := value.(string)

		return ok
	case "boolean":
		_, ok := value.(bool)

		return ok
	case "null":
		return value == nil
	case "number":
		_, ok := value.(float64)

		return ok
	case "integer":
		f, ok := value.(float64)

		return ok && f == math.Trunc(f)
	default:
		return true
	}
}

func toStringSlice(raw any) []string {
	switch t := raw.(type) {
	case string:
		return []string{t}
	case []any:
		out := make([]string, 0, len(t))
		for _, v := range t {
			if s, ok := v.(string); ok {
				out = append(out, s)
			}
		}

		return out
	default:
		return nil
	}
}

func toFloat(raw any) (float64, bool) {
	f, ok := raw.(float64)

	return f, ok
}

// jsonValuesEqual compares two decoded JSON values for equality (used by enum checks).
func jsonValuesEqual(a, b any) bool {
	ab, err := json.Marshal(a)
	if err != nil {
		return false
	}
	bb, err := json.Marshal(b)
	if err != nil {
		return false
	}

	return string(ab) == string(bb)
}
