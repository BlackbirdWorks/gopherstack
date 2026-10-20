package cloudformation

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
)

// Property kinds in cfn_property_schemas.json, written by cmd/cfnpropgen.
const (
	propKindString  = "string"
	propKindNumber  = "number"
	propKindBoolean = "boolean"
	propKindList    = "list"
	propKindMap     = "map"

	numberTypeName = "Number"
	boolFalseText  = "false"
)

// propertySchemas maps resource type -> property name -> kind. A type absent
// from the table is not checked; "any" kinds accept every value.
type propertySchemas map[string]map[string]string

//go:embed cfn_property_schemas.json
var cfnPropertySchemasJSON []byte

//nolint:gochecknoglobals // lazily populated cache of the embedded table
var (
	cfnPropertySchemas     propertySchemas
	cfnPropertySchemasOnce sync.Once
)

func embeddedPropertySchemas() propertySchemas {
	cfnPropertySchemasOnce.Do(func() {
		if err := json.Unmarshal(cfnPropertySchemasJSON, &cfnPropertySchemas); err != nil {
			panic("cloudformation: invalid embedded cfn_property_schemas.json: " + err.Error())
		}
	})

	return cfnPropertySchemas
}

// isIntrinsicValue reports whether v is an intrinsic call (Ref, Fn::*) whose
// resolved type is unknown at validation time.
func isIntrinsicValue(v any) bool {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return false
	}

	for k := range m {
		return k == samKeyRef || strings.HasPrefix(k, "Fn::")
	}

	return false
}

func jsonTypeName(v any) string {
	switch val := v.(type) {
	case string:
		return "String"
	case bool:
		return "Boolean"
	case []any:
		return "JSONArray"
	case map[string]any:
		return "JSONObject"
	case float64:
		if val == math.Trunc(val) {
			return "Integer"
		}

		return numberTypeName
	}

	return fmt.Sprintf("%T", v)
}

// propertyKindMatches applies CloudFormation's scalar coercion: strings accept
// numbers and booleans, numbers and booleans accept their string spellings.
func propertyKindMatches(kind string, v any) bool {
	if v == nil || isIntrinsicValue(v) {
		return true
	}

	switch kind {
	case propKindString:
		switch v.(type) {
		case string, float64, bool:
			return true
		}

		return false
	case propKindNumber:
		switch val := v.(type) {
		case float64:
			return true
		case string:
			_, err := strconv.ParseFloat(val, 64)

			return err == nil
		}

		return false
	case propKindBoolean:
		switch val := v.(type) {
		case bool:
			return true
		case string:
			return val == boolTrue || val == boolFalseText
		}

		return false
	case propKindList:
		_, ok := v.([]any)

		return ok
	case propKindMap:
		_, ok := v.(map[string]any)

		return ok
	}

	return true
}

func propertySchemaIssues(props map[string]any, schema map[string]string) ([]string, []string) {
	var unknown, mismatched []string

	for _, key := range sortedKeys(props) {
		kind, known := schema[key]

		switch {
		case !known:
			unknown = append(unknown, key)
		case !propertyKindMatches(kind, props[key]):
			mismatched = append(mismatched, fmt.Sprintf(
				"#/%s: expected type: %s, found: %s", key, kind, jsonTypeName(props[key])))
		}
	}

	return unknown, mismatched
}

// validatePropertySchemas rejects unknown and wrong-typed resource properties
// for every type in schemas.
func validatePropertySchemas(tmpl *Template, schemas propertySchemas) error {
	for _, id := range sortedKeys(tmpl.Resources) {
		res := tmpl.Resources[id]

		schema, ok := schemas[res.Type]
		if !ok {
			continue
		}

		unknown, mismatched := propertySchemaIssues(res.Properties, schema)
		if len(unknown) > 0 {
			return templateFormatErr("Encountered unsupported property %s", unknown[0])
		}

		if len(mismatched) > 0 {
			return fmt.Errorf("%w: Properties validation failed for resource %s with message:\n%s",
				ErrPropertyValidation, id, strings.Join(mismatched, "\n"))
		}
	}

	return nil
}
