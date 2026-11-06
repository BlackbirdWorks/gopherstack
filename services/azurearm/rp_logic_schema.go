package azurearm

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

// validateLogicSchema checks v against the JSON Schema subset trigger and ParseJson schemas use. Type names are
// matched case-insensitively because designer-generated schemas capitalise them.
func validateLogicSchema(schema map[string]any, v any) error {
	return validateSchemaNode(schema, normLogic(v), "")
}

func schemaTypeName(v any) string {
	switch t := v.(type) {
	case nil:
		return "Null"
	case bool:
		return "Boolean"
	case int64:
		return "Integer"
	case float64:
		return "Float"
	case string:
		return logicTypeString
	case []any:
		return "Array"
	case map[string]any:
		return "Object"
	default:
		return fmt.Sprintf("%T", t)
	}
}

func typeMatches(want string, v any) bool {
	got := strings.ToLower(schemaTypeName(v))

	switch strings.ToLower(want) {
	case "number":
		return got == logicVarInteger || got == logicVarFloat
	case logicVarFloat:
		return got == logicVarInteger || got == logicVarFloat
	}

	return strings.ToLower(want) == got
}

func validateSchemaNode(schema map[string]any, v any, path string) error {
	if err := validateSchemaType(schema, v, path); err != nil {
		return err
	}

	if err := validateSchemaScalars(schema, v, path); err != nil {
		return err
	}

	switch t := v.(type) {
	case map[string]any:
		return validateSchemaObject(schema, t, path)
	case []any:
		return validateSchemaArray(schema, t, path)
	}

	return nil
}

func validateSchemaType(schema map[string]any, v any, path string) error {
	where := schemaWhere(path)

	var types []string

	switch t := schema[fieldType].(type) {
	case string:
		types = []string{t}
	case []any:
		types = make([]string, 0, len(t))
		for _, x := range t {
			types = append(types, stringOf(x))
		}
	}

	if len(types) == 0 {
		return nil
	}

	for _, t := range types {
		if typeMatches(t, v) {
			return nil
		}
	}

	return schemaErr("Invalid type. Expected %s but got %s.%s", strings.Join(types, ", "), schemaTypeName(v), where)
}

func schemaWhere(path string) string {
	if path == "" {
		return ""
	}

	return " Path '" + path + "'."
}

func validateSchemaScalars(schema map[string]any, v any, path string) error {
	where := schemaWhere(path)

	if enum, ok := schema["enum"].([]any); ok {
		found := false

		for _, e := range enum {
			if logicEquals(e, v) {
				found = true
			}
		}

		if !found {
			return schemaErr("Value %s is not defined in enum.%s", logicString(v), where)
		}
	}

	if f, _, ok := logicNum(v); ok && schemaIsNumber(v) {
		if m, has := logicNumOnly(normLogic(schema["minimum"])); has && f < m {
			return schemaErr("Integer %s is less than minimum value of %v.%s", logicString(v), m, where)
		}

		if m, has := logicNumOnly(normLogic(schema["maximum"])); has && f > m {
			return schemaErr("Integer %s exceeds maximum value of %v.%s", logicString(v), m, where)
		}
	}

	if s, ok := v.(string); ok {
		return validateSchemaString(schema, s, where)
	}

	return nil
}

func schemaIsNumber(v any) bool {
	switch v.(type) {
	case int64, float64:
		return true
	}

	return false
}

func validateSchemaString(schema map[string]any, s, where string) error {
	n := utf8.RuneCountInString(s)

	if m, ok := logicInt(normLogic(schema["minLength"])); ok && int64(n) < m {
		return schemaErr("String '%s' is less than minimum length of %d.%s", s, m, where)
	}

	if m, ok := logicInt(normLogic(schema["maxLength"])); ok && int64(n) > m {
		return schemaErr("String '%s' exceeds maximum length of %d.%s", s, m, where)
	}

	if pat := stringOf(schema["pattern"]); pat != "" {
		re, err := regexp.Compile(pat)
		if err == nil && !re.MatchString(s) {
			return schemaErr("String '%s' does not match regex pattern '%s'.%s", s, pat, where)
		}
	}

	return nil
}

func validateSchemaObject(schema map[string]any, obj map[string]any, path string) error {
	props, _ := objectOf(schema["properties"])

	if req, ok := schema["required"].([]any); ok {
		var missing []string

		for _, r := range req {
			if _, has := obj[stringOf(r)]; !has {
				missing = append(missing, stringOf(r))
			}
		}

		if len(missing) > 0 {
			sort.Strings(missing)

			return schemaErr(
				"Required properties are missing from object: %s.%s",
				strings.Join(missing, ", "),
				schemaWhere(path),
			)
		}
	}

	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		sub, ok := objectOf(props[k])
		if !ok {
			if ap, isBool := schema["additionalProperties"].(bool); isBool && !ap {
				return schemaErr(
					"Property '%s' has not been defined and the schema does not allow additional properties.%s",
					k,
					schemaWhere(path),
				)
			}

			continue
		}

		if err := validateSchemaNode(sub, obj[k], joinSchemaPath(path, k)); err != nil {
			return err
		}
	}

	return nil
}

func joinSchemaPath(path, k string) string {
	if path == "" {
		return k
	}

	return path + "." + k
}

func validateSchemaArray(schema map[string]any, arr []any, path string) error {
	where := schemaWhere(path)

	if m, ok := logicInt(normLogic(schema["minItems"])); ok && int64(len(arr)) < m {
		return schemaErr("Array item count %d is less than minimum count of %d.%s", len(arr), m, where)
	}

	if m, ok := logicInt(normLogic(schema["maxItems"])); ok && int64(len(arr)) > m {
		return schemaErr("Array item count %d exceeds maximum count of %d.%s", len(arr), m, where)
	}

	items, ok := objectOf(schema["items"])
	if !ok {
		return nil
	}

	for i, x := range arr {
		if err := validateSchemaNode(items, x, fmt.Sprintf("%s[%d]", path, i)); err != nil {
			return err
		}
	}

	return nil
}

// logicSchemaError carries a service-style validation message verbatim.
type logicSchemaError string

func (e logicSchemaError) Error() string { return string(e) }

func schemaErr(format string, a ...any) error { return logicSchemaError(fmt.Sprintf(format, a...)) }
