package cloudformation

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// ErrEmptyTemplate is returned when a template body is empty.
var ErrEmptyTemplate = errors.New("template body is empty")

// Template represents a parsed CloudFormation template.
type Template struct {
	Parameters               map[string]TemplateParameter `json:"Parameters"               yaml:"Parameters"`
	Resources                map[string]TemplateResource  `json:"Resources"                yaml:"Resources"`
	Outputs                  map[string]TemplateOutput    `json:"Outputs"                  yaml:"Outputs"`
	Mappings                 map[string]any               `json:"Mappings"                 yaml:"Mappings"`
	Conditions               map[string]any               `json:"Conditions"               yaml:"Conditions"`
	AWSTemplateFormatVersion string                       `json:"AWSTemplateFormatVersion" yaml:"AWSTemplateFormatVersion"`
	Description              string                       `json:"Description"              yaml:"Description"`
}

// TemplateParameter represents a CloudFormation template parameter.
type TemplateParameter struct {
	Type        string `json:"Type"        yaml:"Type"`
	Default     any    `json:"Default"     yaml:"Default"`
	Description string `json:"Description" yaml:"Description"`
}

// TemplateResource represents a CloudFormation template resource.
type TemplateResource struct {
	Properties map[string]any `json:"Properties" yaml:"Properties"`
	Type       string         `json:"Type"       yaml:"Type"`
}

// TemplateOutputExport holds the export name for a template output.
type TemplateOutputExport struct {
	Name any `json:"Name" yaml:"Name"`
}

// TemplateOutput represents a CloudFormation template output.
type TemplateOutput struct {
	Value       any                   `json:"Value"       yaml:"Value"`
	Export      *TemplateOutputExport `json:"Export"      yaml:"Export"`
	Description string                `json:"Description" yaml:"Description"`
}

// ParseTemplate parses a CloudFormation template from a JSON or YAML string.
func ParseTemplate(body string) (*Template, error) {
	body = strings.TrimSpace(body)
	if body == "" {
		return nil, ErrEmptyTemplate
	}

	var tmpl Template

	if strings.HasPrefix(body, "{") {
		if err := json.Unmarshal([]byte(body), &tmpl); err != nil {
			return nil, fmt.Errorf("failed to parse JSON template: %w", err)
		}

		return &tmpl, nil
	}

	if err := yaml.Unmarshal([]byte(body), &tmpl); err != nil {
		return nil, fmt.Errorf("failed to parse YAML template: %w", err)
	}

	return &tmpl, nil
}

// ResolveParameters merges template defaults with provided overrides.
func ResolveParameters(tmpl *Template, overrides []Parameter) map[string]string {
	resolved := make(map[string]string)

	for name, param := range tmpl.Parameters {
		if param.Default != nil {
			resolved[name] = fmt.Sprintf("%v", param.Default)
		}
	}

	for _, p := range overrides {
		resolved[p.ParameterKey] = p.ParameterValue
	}

	return resolved
}

// resolveCtx holds all context needed to resolve a CloudFormation value.
type resolveCtx struct {
	params      map[string]string
	physicalIDs map[string]string
	exports     map[string]string
	conditions  map[string]bool
	mappings    map[string]any
}

// evaluateConditions evaluates the Conditions section of a template and returns
// a map of condition name to bool. It uses fixed-point iteration to handle
// conditions that reference other conditions (via the Condition key).
func evaluateConditions(raw map[string]any, params, physicalIDs map[string]string) map[string]bool {
	result := make(map[string]bool, len(raw))

	// Iterate until stable to handle cross-condition references.
	for range len(raw) + 1 {
		changed := false

		for name, expr := range raw {
			prev := result[name]
			next := evalConditionExpr(expr, params, physicalIDs, result)
			result[name] = next

			if next != prev {
				changed = true
			}
		}

		if !changed {
			break
		}
	}

	return result
}

func evalConditionExpr(expr any, params, physicalIDs map[string]string, conditions map[string]bool) bool {
	m, isMagic := expr.(map[string]any)
	if !isMagic {
		return false
	}

	if args, isEquals := m["Fn::Equals"].([]any); isEquals && len(args) == 2 {
		a := resolveScalar(args[0], params, physicalIDs)
		b := resolveScalar(args[1], params, physicalIDs)

		return a == b
	}

	if condName, isCond := m["Condition"].(string); isCond {
		return conditions[condName]
	}

	if args, isAnd := m["Fn::And"].([]any); isAnd {
		return evalAndExpr(args, params, physicalIDs, conditions)
	}

	if args, isOr := m["Fn::Or"].([]any); isOr {
		return evalOrExpr(args, params, physicalIDs, conditions)
	}

	if arg, isNot := m["Fn::Not"].([]any); isNot && len(arg) == 1 {
		return !evalConditionExpr(arg[0], params, physicalIDs, conditions)
	}

	return false
}

func evalAndExpr(args []any, params, physicalIDs map[string]string, conditions map[string]bool) bool {
	for _, a := range args {
		if !evalConditionExpr(a, params, physicalIDs, conditions) {
			return false
		}
	}

	return true
}

func evalOrExpr(args []any, params, physicalIDs map[string]string, conditions map[string]bool) bool {
	for _, a := range args {
		if evalConditionExpr(a, params, physicalIDs, conditions) {
			return true
		}
	}

	return false
}

// resolveScalar resolves a simple scalar value (Ref or string).
func resolveScalar(v any, params, physicalIDs map[string]string) string {
	switch val := v.(type) {
	case string:
		return val
	case map[string]any:
		if ref, ok := val["Ref"].(string); ok {
			if pval, exists := params[ref]; exists {
				return pval
			}
			if pid, exists := physicalIDs[ref]; exists {
				return pid
			}

			return ref
		}
	}

	return fmt.Sprintf("%v", v)
}

// ResolveValue resolves a CloudFormation property value, handling intrinsic functions.
func ResolveValue(v any, params map[string]string, physicalIDs map[string]string) string {
	ctx := resolveCtx{
		params:      params,
		physicalIDs: physicalIDs,
	}

	return resolveValueCtx(v, ctx)
}

func resolveValueCtx(v any, ctx resolveCtx) string {
	if v == nil {
		return ""
	}

	switch val := v.(type) {
	case string:
		return val
	case bool:
		if val {
			return boolTrue
		}

		return "false"
	case int, int64, float64:
		return fmt.Sprintf("%v", val)
	case map[string]any:
		return resolveMapIntrinsic(val, ctx)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func resolveMapIntrinsic(val map[string]any, ctx resolveCtx) string {
	if ref, isRef := val["Ref"].(string); isRef {
		return resolveRef(ref, ctx)
	}

	if result, handled := resolveCollectionIntrinsic(val, ctx); handled {
		return result
	}

	return resolveMiscIntrinsic(val, ctx)
}

func resolveRef(ref string, ctx resolveCtx) string {
	if pval, exists := ctx.params[ref]; exists {
		return pval
	}

	if pid, exists := ctx.physicalIDs[ref]; exists {
		return pid
	}

	return ref
}

func resolveCollectionIntrinsic(val map[string]any, ctx resolveCtx) (string, bool) {
	if subStr, isSub := val["Fn::Sub"].(string); isSub {
		return resolveSub(subStr, ctx), true
	}

	if joinArgs, isJoin := val["Fn::Join"].([]any); isJoin && len(joinArgs) >= 2 {
		return resolveJoin(joinArgs, ctx), true
	}

	if splitArgs, isSplit := val["Fn::Split"].([]any); isSplit && len(splitArgs) == 2 {
		return resolveSplit(splitArgs, ctx), true
	}

	if selectArgs, isSelect := val["Fn::Select"].([]any); isSelect && len(selectArgs) == 2 {
		return resolveSelect(selectArgs, ctx), true
	}

	return "", false
}

func resolveMiscIntrinsic(val map[string]any, ctx resolveCtx) string {
	if findArgs, isFind := val["Fn::FindInMap"].([]any); isFind && len(findArgs) == 3 {
		return resolveFindInMap(findArgs, ctx)
	}

	if ifArgs, isIf := val["Fn::If"].([]any); isIf && len(ifArgs) == 3 {
		return resolveIf(ifArgs, ctx)
	}

	if exportName, hasImport := val["Fn::ImportValue"]; hasImport {
		name := resolveValueCtx(exportName, ctx)
		if ctx.exports != nil {
			if expVal, exists := ctx.exports[name]; exists {
				return expVal
			}
		}

		return name
	}

	return fmt.Sprintf("%v", val)
}

func resolveSub(s string, ctx resolveCtx) string {
	result := s
	for key, val := range ctx.params {
		result = strings.ReplaceAll(result, "${"+key+"}", val)
	}
	for key, val := range ctx.physicalIDs {
		result = strings.ReplaceAll(result, "${"+key+"}", val)
	}

	return result
}

func resolveJoin(args []any, ctx resolveCtx) string {
	sep, _ := args[0].(string)
	items, _ := args[1].([]any)
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, resolveValueCtx(item, ctx))
	}

	return strings.Join(parts, sep)
}

func resolveSplit(args []any, ctx resolveCtx) string {
	// Fn::Split returns a list; we return a comma-joined representation
	// so it can be consumed by Fn::Select or similar.
	delimiter, _ := args[0].(string)
	sourceStr := resolveValueCtx(args[1], ctx)

	return strings.Join(strings.Split(sourceStr, delimiter), ",")
}

func resolveSelect(args []any, ctx resolveCtx) string {
	var index int

	switch idx := args[0].(type) {
	case int:
		index = idx
	case float64:
		index = int(idx)
	case string:
		// If parsing fails, index remains 0 (zero value), which is the fallback.
		_, _ = fmt.Sscanf(idx, "%d", &index)
	}

	switch items := args[1].(type) {
	case []any:
		if index >= 0 && index < len(items) {
			return resolveValueCtx(items[index], ctx)
		}
	case string:
		// Might be a comma-separated list from Fn::Split
		parts := strings.Split(items, ",")
		if index >= 0 && index < len(parts) {
			return strings.TrimSpace(parts[index])
		}
	}

	return ""
}

func resolveFindInMap(args []any, ctx resolveCtx) string {
	mapName := resolveValueCtx(args[0], ctx)
	topKey := resolveValueCtx(args[1], ctx)
	secondKey := resolveValueCtx(args[2], ctx)

	if ctx.mappings == nil {
		return ""
	}

	topMap, ok := ctx.mappings[mapName]
	if !ok {
		return ""
	}

	m1, ok := topMap.(map[string]any)
	if !ok {
		return ""
	}

	midVal, ok := m1[topKey]
	if !ok {
		return ""
	}

	m2, ok := midVal.(map[string]any)
	if !ok {
		return ""
	}

	val, ok := m2[secondKey]
	if !ok {
		return ""
	}

	return resolveValueCtx(val, ctx)
}

func resolveIf(args []any, ctx resolveCtx) string {
	condName, _ := args[0].(string)

	var condTrue bool
	if ctx.conditions != nil {
		condTrue = ctx.conditions[condName]
	}

	if condTrue {
		return resolveValueCtx(args[1], ctx)
	}

	return resolveValueCtx(args[2], ctx)
}

// resolveOutputsWithContext resolves template outputs using the full resolve context.
// It also returns a map of export name -> value for outputs that define Export.Name.
func resolveOutputsWithContext(
	tmpl *Template,
	ctx resolveCtx,
) ([]Output, map[string]string) {
	if len(tmpl.Outputs) == 0 {
		return nil, nil
	}

	outputs := make([]Output, 0, len(tmpl.Outputs))
	exports := make(map[string]string)

	for key, o := range tmpl.Outputs {
		value := resolveValueCtx(o.Value, ctx)
		out := Output{
			OutputKey:   key,
			OutputValue: value,
			Description: o.Description,
		}

		if o.Export != nil {
			exportName := resolveValueCtx(o.Export.Name, ctx)
			if exportName != "" {
				out.ExportName = exportName
				exports[exportName] = value
			}
		}

		outputs = append(outputs, out)
	}

	return outputs, exports
}

// collectImportValues scans a template body for all Fn::ImportValue references.
func collectImportValues(templateBody string) []string {
	if templateBody == "" {
		return nil
	}

	tmpl, err := ParseTemplate(templateBody)
	if err != nil {
		return nil
	}

	var refs []string
	for _, res := range tmpl.Resources {
		collectImportValuesFromValue(res.Properties, &refs)
	}

	for _, out := range tmpl.Outputs {
		collectImportValuesFromValue(out.Value, &refs)
	}

	return refs
}

func collectImportValuesFromValue(v any, refs *[]string) {
	switch val := v.(type) {
	case map[string]any:
		if importVal, hasImport := val["Fn::ImportValue"]; hasImport {
			if name, isStr := importVal.(string); isStr {
				*refs = append(*refs, name)
			}
		}

		for _, child := range val {
			collectImportValuesFromValue(child, refs)
		}
	case []any:
		for _, item := range val {
			collectImportValuesFromValue(item, refs)
		}
	}
}
