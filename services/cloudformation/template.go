package cloudformation

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"net"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// ErrEmptyTemplate is returned when a template body is empty.
var ErrEmptyTemplate = errors.New("template body is empty")

// ErrParameterValidation is returned when a parameter value fails AllowedValues validation.
var ErrParameterValidation = errors.New("parameter validation failed")

// splitSep is the internal separator used by Fn::Split to encode a list as a string.
// A null byte cannot appear in CloudFormation string values, making it unambiguous.
const splitSep = "\x00"

const (
	resTypeStepFunctionsStateMachine = "AWS::StepFunctions::StateMachine"
	attrNameArn                      = "Arn"
)

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
	Default               any      `json:"Default"               yaml:"Default"`
	MaxValue              *float64 `json:"MaxValue"              yaml:"MaxValue"`
	MinValue              *float64 `json:"MinValue"              yaml:"MinValue"`
	MaxLength             *int     `json:"MaxLength"             yaml:"MaxLength"`
	MinLength             *int     `json:"MinLength"             yaml:"MinLength"`
	Type                  string   `json:"Type"                  yaml:"Type"`
	Description           string   `json:"Description"           yaml:"Description"`
	AllowedPattern        string   `json:"AllowedPattern"        yaml:"AllowedPattern"`
	ConstraintDescription string   `json:"ConstraintDescription" yaml:"ConstraintDescription"`
	AllowedValues         []string `json:"AllowedValues"         yaml:"AllowedValues"`
	NoEcho                bool     `json:"NoEcho"                yaml:"NoEcho"`
}

// TemplateResource represents a CloudFormation template resource.
// DependsOn may be a single resource name (string) or a list of names ([]string).
type TemplateResource struct {
	Properties     map[string]any `json:"Properties"     yaml:"Properties"`
	Type           string         `json:"Type"           yaml:"Type"`
	DeletionPolicy string         `json:"DeletionPolicy" yaml:"DeletionPolicy"`
	DependsOn      []string       `json:"-"              yaml:"-"`
}

// UnmarshalJSON implements [json.Unmarshaler] for TemplateResource so that
// DependsOn can be either a JSON string or a JSON array of strings.
func (r *TemplateResource) UnmarshalJSON(data []byte) error {
	type plain struct {
		DependsOn      any            `json:"DependsOn"`
		Properties     map[string]any `json:"Properties"`
		Type           string         `json:"Type"`
		DeletionPolicy string         `json:"DeletionPolicy"`
	}

	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}

	r.Type = p.Type
	r.Properties = p.Properties
	r.DependsOn = parseDependsOn(p.DependsOn)
	r.DeletionPolicy = p.DeletionPolicy

	return nil
}

// UnmarshalYAML implements yaml.Unmarshaler for TemplateResource.
func (r *TemplateResource) UnmarshalYAML(unmarshal func(any) error) error {
	type plain struct {
		DependsOn      any            `yaml:"DependsOn"`
		Properties     map[string]any `yaml:"Properties"`
		Type           string         `yaml:"Type"`
		DeletionPolicy string         `yaml:"DeletionPolicy"`
	}

	var p plain
	if err := unmarshal(&p); err != nil {
		return err
	}

	r.Type = p.Type
	r.Properties = p.Properties
	r.DependsOn = parseDependsOn(p.DependsOn)
	r.DeletionPolicy = p.DeletionPolicy

	return nil
}

// parseDependsOn converts the raw DependsOn value (string or []string) into a []string.
func parseDependsOn(v any) []string {
	if v == nil {
		return nil
	}

	switch d := v.(type) {
	case string:

		return []string{d}
	case []any:
		out := make([]string, 0, len(d))
		for _, item := range d {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}

		return out
	default:

		return nil
	}
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

	// Expand AWS Language Extensions (Fn::ForEach) before typed unmarshalling,
	// since ForEach keys are arrays that don't fit the TemplateResource shape.
	if strings.Contains(body, forEachPrefix) {
		expanded, err := expandLanguageExtensions(body)
		if err != nil {
			return nil, err
		}
		body = expanded
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

const forEachPrefix = "Fn::ForEach::"

// ErrForEach is returned when an Fn::ForEach block is malformed.
var ErrForEach = errors.New("invalid Fn::ForEach")

// expandLanguageExtensions parses a template into a generic document, expands any
// Fn::ForEach loops in the Resources section, and re-serialises it to JSON. This
// implements the CloudFormation Languages Extensions transform for iterated
// resource generation.
func expandLanguageExtensions(body string) (string, error) {
	doc, err := parseGenericTemplate(body)
	if err != nil {
		return "", err
	}

	resources, ok := doc["Resources"].(map[string]any)
	if !ok {
		return body, nil
	}

	expanded, err := expandForEachMap(resources)
	if err != nil {
		return "", err
	}
	doc["Resources"] = expanded

	out, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("failed to serialise expanded template: %w", err)
	}

	return string(out), nil
}

// parseGenericTemplate decodes a JSON or YAML template into a generic map.
func parseGenericTemplate(body string) (map[string]any, error) {
	var doc map[string]any
	if strings.HasPrefix(strings.TrimSpace(body), "{") {
		if err := json.Unmarshal([]byte(body), &doc); err != nil {
			return nil, fmt.Errorf("failed to parse JSON template: %w", err)
		}

		return doc, nil
	}
	if err := yaml.Unmarshal([]byte(body), &doc); err != nil {
		return nil, fmt.Errorf("failed to parse YAML template: %w", err)
	}

	return doc, nil
}

// expandForEachMap walks a resources map, expanding every Fn::ForEach entry into
// concrete resources and preserving non-loop entries. Nested loops are expanded
// recursively.
func expandForEachMap(resources map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(resources))
	for _, key := range collections.SortedKeys(resources) {
		val := resources[key]
		if strings.HasPrefix(key, forEachPrefix) {
			if err := expandForEachEntry(val, out); err != nil {
				return nil, err
			}

			continue
		}
		out[key] = val
	}

	return out, nil
}

// expandForEachEntry expands a single Fn::ForEach block. The value must be a
// 3-element array: [identifier, collection, outputTemplate]. For each item in
// the collection, every entry of the output template is emitted with ${identifier}
// substituted by the item value (in both the logical-ID key and the body).
func expandForEachEntry(val any, out map[string]any) error {
	parts, ok := val.([]any)
	if !ok || len(parts) != 3 {
		return fmt.Errorf("%w: expected [identifier, collection, template]", ErrForEach)
	}

	identifier, ok := parts[0].(string)
	if !ok {
		return fmt.Errorf("%w: identifier must be a string", ErrForEach)
	}

	collection := forEachCollection(parts[1])
	outputTemplate, ok := parts[2].(map[string]any)
	if !ok {
		return fmt.Errorf("%w: output template must be a map", ErrForEach)
	}

	// The output template may itself contain nested Fn::ForEach loops.
	outputTemplate, err := expandForEachMap(outputTemplate)
	if err != nil {
		return err
	}

	placeholder := "${" + identifier + "}"
	for _, item := range collection {
		for tmplKey, tmplBody := range outputTemplate {
			resKey := strings.ReplaceAll(tmplKey, placeholder, item)
			out[resKey] = substituteForEach(tmplBody, placeholder, item)
		}
	}

	return nil
}

// forEachCollection normalises a ForEach collection value (a list literal or a
// comma-delimited string) into a slice of string items.
func forEachCollection(v any) []string {
	switch c := v.(type) {
	case []any:
		items := make([]string, 0, len(c))
		for _, e := range c {
			items = append(items, fmt.Sprintf("%v", e))
		}

		return items
	case string:
		if c == "" {
			return nil
		}

		return strings.Split(c, ",")
	}

	return nil
}

// substituteForEach recursively replaces the ${identifier} placeholder with the
// loop item value throughout a value tree (map keys, strings, and nested lists).
func substituteForEach(v any, placeholder, item string) any {
	switch val := v.(type) {
	case string:
		return strings.ReplaceAll(val, placeholder, item)
	case map[string]any:
		out := make(map[string]any, len(val))
		for k, child := range val {
			newKey := strings.ReplaceAll(k, placeholder, item)
			out[newKey] = substituteForEach(child, placeholder, item)
		}

		return out
	case []any:
		out := make([]any, len(val))
		for i, child := range val {
			out[i] = substituteForEach(child, placeholder, item)
		}

		return out
	}

	return v
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

// ValidateParameters checks parameter values against AllowedValues, AllowedPattern,
// MinValue/MaxValue (Number type), and MinLength/MaxLength (String type) constraints.
func ValidateParameters(tmpl *Template, resolved map[string]string) error {
	for _, name := range collections.SortedKeys(tmpl.Parameters) {
		param := tmpl.Parameters[name]
		val, ok := resolved[name]
		if !ok {
			continue
		}
		if err := validateParamConstraints(name, val, param); err != nil {
			return err
		}
	}

	return nil
}

// validateParamConstraints checks all constraints on a single resolved parameter value.
func validateParamConstraints(name, val string, param TemplateParameter) error {
	if err := validateAllowedValues(name, val, param); err != nil {
		return err
	}
	if err := validateAllowedPattern(name, val, param); err != nil {
		return err
	}
	if err := validateNumericRange(name, val, param); err != nil {
		return err
	}

	return validateStringLength(name, val, param)
}

func constraintMsg(fallback string, param TemplateParameter) string {
	if param.ConstraintDescription != "" {
		return param.ConstraintDescription
	}

	return fallback
}

func validateAllowedValues(name, val string, param TemplateParameter) error {
	if len(param.AllowedValues) == 0 || slices.Contains(param.AllowedValues, val) {
		return nil
	}
	msg := constraintMsg(fmt.Sprintf("Parameter %s must be one of %v", name, param.AllowedValues), param)

	return fmt.Errorf("%w: %s", ErrParameterValidation, msg)
}

func validateAllowedPattern(name, val string, param TemplateParameter) error {
	if param.AllowedPattern == "" {
		return nil
	}
	re, err := regexp.Compile(param.AllowedPattern)
	if err != nil {
		return fmt.Errorf("%w: parameter %s AllowedPattern is not a valid regex: %w", ErrParameterValidation, name, err)
	}
	if re.MatchString(val) {
		return nil
	}
	msg := constraintMsg(fmt.Sprintf("Parameter %s must match pattern %s", name, param.AllowedPattern), param)

	return fmt.Errorf("%w: %s", ErrParameterValidation, msg)
}

func validateNumericRange(name, val string, param TemplateParameter) error {
	if param.Type != "Number" || (param.MinValue == nil && param.MaxValue == nil) {
		return nil
	}
	n, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return fmt.Errorf("%w: parameter %s must be a number, got %q", ErrParameterValidation, name, val)
	}
	if param.MinValue != nil && n < *param.MinValue {
		msg := constraintMsg(fmt.Sprintf("Parameter %s must be >= %v", name, *param.MinValue), param)

		return fmt.Errorf("%w: %s", ErrParameterValidation, msg)
	}
	if param.MaxValue != nil && n > *param.MaxValue {
		msg := constraintMsg(fmt.Sprintf("Parameter %s must be <= %v", name, *param.MaxValue), param)

		return fmt.Errorf("%w: %s", ErrParameterValidation, msg)
	}

	return nil
}

func validateStringLength(name, val string, param TemplateParameter) error {
	if param.MinLength != nil && len(val) < *param.MinLength {
		msg := constraintMsg(fmt.Sprintf("Parameter %s must be at least %d characters", name, *param.MinLength), param)

		return fmt.Errorf("%w: %s", ErrParameterValidation, msg)
	}
	if param.MaxLength != nil && len(val) > *param.MaxLength {
		msg := constraintMsg(fmt.Sprintf("Parameter %s must be at most %d characters", name, *param.MaxLength), param)

		return fmt.Errorf("%w: %s", ErrParameterValidation, msg)
	}

	return nil
}

// resolveCtx holds all context needed to resolve a CloudFormation value.
type resolveCtx struct {
	params        map[string]string
	physicalIDs   map[string]string
	resourceTypes map[string]string // logicalID → CFN resource type (e.g. "AWS::S3::Bucket")
	exports       map[string]string
	conditions    map[string]bool
	mappings      map[string]any
	macros        *MacroRegistry // optional: enables Fn::Transform macro invocation
	accountID     string
	region        string
	stackName     string
}

// subVarPattern matches ${VarName} or ${Logical.Attr} in Fn::Sub strings.
var subVarPattern = regexp.MustCompile(`\$\{([^}]+)\}`)

// awsNoValueSentinel is returned when Ref: AWS::NoValue is resolved.
const awsNoValueSentinel = "\x02AWS::NoValue"

// evaluateConditions evaluates the Conditions section of a template and returns
// a map of condition name to bool. It uses fixed-point iteration with sorted keys
// and snapshot-based reads to ensure deterministic, order-independent evaluation
// even when conditions reference other conditions (via the Condition key).
func evaluateConditions(raw map[string]any, params, physicalIDs map[string]string) map[string]bool {
	result := make(map[string]bool, len(raw))

	// Build a sorted key list for deterministic iteration order.
	names := collections.SortedKeys(raw)

	// Iterate until stable. Each pass reads from a snapshot of the previous
	// state so that results don't depend on the evaluation order within a pass.
	for range len(raw) + 1 {
		prev := make(map[string]bool, len(result))
		maps.Copy(prev, result)

		changed := false

		for _, name := range names {
			next := evalConditionExpr(raw[name], params, physicalIDs, prev)
			if result[name] != next {
				result[name] = next
				changed = true
			}
		}

		if !changed {
			break
		}
	}

	return result
}

func evalConditionExpr(
	expr any,
	params, physicalIDs map[string]string,
	conditions map[string]bool,
) bool {
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

func evalAndExpr(
	args []any,
	params, physicalIDs map[string]string,
	conditions map[string]bool,
) bool {
	for _, a := range args {
		if !evalConditionExpr(a, params, physicalIDs, conditions) {
			return false
		}
	}

	return true
}

func evalOrExpr(
	args []any,
	params, physicalIDs map[string]string,
	conditions map[string]bool,
) bool {
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
	// Pseudo-parameters (#10)
	switch ref {
	case "AWS::Region":
		if ctx.region != "" {
			return ctx.region
		}

		return cfnDefaultRegion
	case "AWS::AccountId":
		if ctx.accountID != "" {
			return ctx.accountID
		}

		return "000000000000"
	case "AWS::StackName":
		if ctx.stackName != "" {
			return ctx.stackName
		}

		return "unknown-stack"
	case "AWS::Partition":
		return arn.PartitionForRegion(ctx.region)
	case "AWS::URLSuffix":
		if strings.HasPrefix(ctx.region, "cn-") {
			return "amazonaws.com.cn"
		}

		return "amazonaws.com"
	case "AWS::NoValue":
		return awsNoValueSentinel
	case "AWS::StackId":
		return "arn:aws:cloudformation:" + ctx.region + ":" + ctx.accountID + ":stack/" + ctx.stackName + "/unknown"
	case "AWS::NotificationARNs":
		return ""
	}

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

	// Two-arg Fn::Sub: ["template", {"Var": "value", ...}] (#11)
	if subArgs, isSub := val["Fn::Sub"].([]any); isSub && len(subArgs) == 2 {
		tmplStr, _ := subArgs[0].(string)
		varMap, _ := subArgs[1].(map[string]any)
		subCtx := ctx
		newParams := maps.Clone(ctx.params)
		for k, v := range varMap {
			newParams[k] = resolveValueCtx(v, ctx)
		}
		subCtx.params = newParams

		return resolveSub(tmplStr, subCtx), true
	}

	// Fn::GetAtt: [LogicalID, AttributeName] (#9)
	if getAttArgs, isGetAtt := val["Fn::GetAtt"].([]any); isGetAtt && len(getAttArgs) == 2 {
		logicalID, _ := getAttArgs[0].(string)
		attrName, _ := getAttArgs[1].(string)

		return resolveGetAtt(logicalID, attrName, ctx), true
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
		return resolveImportValue(exportName, ctx)
	}

	// Fn::Base64: base64-encode input (#13)
	if b64Input, hasB64 := val["Fn::Base64"]; hasB64 {
		s := resolveValueCtx(b64Input, ctx)

		return base64.StdEncoding.EncodeToString([]byte(s))
	}

	// Fn::GetAZs: return canned AZ list for region (#13)
	if regionVal, hasAZs := val["Fn::GetAZs"]; hasAZs {
		region := resolveValueCtx(regionVal, ctx)
		if region == "" {
			region = ctx.region
		}

		return strings.Join(getAZsForRegion(region), splitSep)
	}

	// Fn::Cidr: compute CIDR subnets (#13)
	if cidrArgs, hasCidr := val["Fn::Cidr"].([]any); hasCidr && len(cidrArgs) >= 2 {
		return resolveCidr(cidrArgs, ctx)
	}

	// Fn::Length: count of list elements (#13)
	if lenVal, hasLen := val["Fn::Length"]; hasLen {
		return resolveLength(lenVal, ctx)
	}

	// Fn::ToJsonString: serialize value to JSON string (#13)
	if jsonVal, hasJSON := val["Fn::ToJsonString"]; hasJSON {
		data, err := json.Marshal(jsonVal)
		if err != nil {
			return ""
		}

		return string(data)
	}

	// Fn::Transform: invoke registered macro if available, else return as-is.
	if transformSpec, hasTransform := val["Fn::Transform"].(map[string]any); hasTransform {
		return resolveTransformIntrinsic(transformSpec, val, ctx)
	}

	return fmt.Sprintf("%v", val)
}

// resolveTransformIntrinsic handles the Fn::Transform intrinsic function.
func resolveTransformIntrinsic(transformSpec, val map[string]any, ctx resolveCtx) string {
	macroName, _ := transformSpec["Name"].(string)
	if macroName != "" && ctx.macros != nil {
		result := invokeMacroTransform(macroName, transformSpec, ctx)
		if result != "" {
			return result
		}
	}

	return fmt.Sprintf("%v", val)
}

func resolveImportValue(exportName any, ctx resolveCtx) string {
	name := resolveValueCtx(exportName, ctx)
	if ctx.exports != nil {
		if expVal, exists := ctx.exports[name]; exists {
			return expVal
		}
	}

	return unresolvedImportMarker(name)
}

func resolveSub(s string, ctx resolveCtx) string {
	return subVarPattern.ReplaceAllStringFunc(s, func(match string) string {
		varName := match[2 : len(match)-1] // strip ${ and }

		// Check explicit params (includes overrides from two-arg form).
		if v, ok := ctx.params[varName]; ok {
			return v
		}

		// ${LogicalID.Attribute} — GetAtt-style ref (#11)
		if logicalID, attrName, ok := strings.Cut(varName, "."); ok {
			return resolveGetAtt(logicalID, attrName, ctx)
		}

		// ${LogicalID} — plain physical ID ref
		if pid, ok := ctx.physicalIDs[varName]; ok {
			return pid
		}

		// Pseudo-parameters and anything else via resolveRef
		resolved := resolveRef(varName, ctx)
		if resolved == varName {
			// Unknown variable: leave placeholder intact
			return match
		}

		return resolved
	})
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
	// Fn::Split produces a list. We encode it as a null-byte-delimited string
	// so that Fn::Select can later consume it without ambiguity.
	delimiter, _ := args[0].(string)
	sourceStr := resolveValueCtx(args[1], ctx)

	return strings.Join(strings.Split(sourceStr, delimiter), splitSep)
}

func resolveSelect(args []any, ctx resolveCtx) string {
	index := extractSelectIndex(args[0])

	// Special case: second argument is Fn::Split evaluated inline.
	if splitMap, isSplitMap := args[1].(map[string]any); isSplitMap {
		if result, ok := resolveSelectFromSplit(splitMap, index, ctx); ok {
			return result
		}
	}

	return resolveSelectFromItems(args[1], index, ctx)
}

// resolveSelectFromSplit handles the inline Fn::Split case for Fn::Select.
func resolveSelectFromSplit(splitMap map[string]any, index int, ctx resolveCtx) (string, bool) {
	splitArgs, isSplit := splitMap["Fn::Split"].([]any)
	if !isSplit || len(splitArgs) != 2 {
		return "", false
	}

	delimiter, _ := splitArgs[0].(string)
	sourceStr := resolveValueCtx(splitArgs[1], ctx)
	parts := strings.Split(sourceStr, delimiter)

	if index >= 0 && index < len(parts) {
		return strings.TrimSpace(parts[index]), true
	}

	return "", true
}

// resolveSelectFromItems selects from a list, encoded string, or intrinsic map.
func resolveSelectFromItems(second any, index int, ctx resolveCtx) string {
	switch items := second.(type) {
	case []any:
		if index >= 0 && index < len(items) {
			return resolveValueCtx(items[index], ctx)
		}
	case string:
		return selectFromEncodedString(items, index)
	case map[string]any:
		resolved := resolveValueCtx(items, ctx)

		return selectFromEncodedString(resolved, index)
	}

	return ""
}

func selectFromEncodedString(s string, index int) string {
	if strings.Contains(s, splitSep) {
		parts := strings.Split(s, splitSep)
		if index >= 0 && index < len(parts) {
			return strings.TrimSpace(parts[index])
		}
	} else if index == 0 {
		return s
	}

	return ""
}

// extractSelectIndex parses the first argument of Fn::Select into an int index.
func extractSelectIndex(v any) int {
	switch idx := v.(type) {
	case int:

		return idx
	case float64:

		return int(idx)
	case string:
		var i int
		// If parsing fails, index remains 0 (zero value), which is the fallback.
		_, _ = fmt.Sscanf(idx, "%d", &i)

		return i
	}

	return 0
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

// unresolvedImportMarker returns a distinctive placeholder for an Fn::ImportValue
// that could not be resolved. The marker is detectable by validateImportValues.
func unresolvedImportMarker(name string) string {
	return "\x01unresolved-import:" + name
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

// validateImportValues checks that all Fn::ImportValue references in the template
// can be resolved against the available exports. Returns an error describing the
// first missing export, or nil if all imports are satisfied.
func validateImportValues(
	tmpl *Template,
	resolvedParams map[string]string,
	exports map[string]string,
) error {
	for _, res := range tmpl.Resources {
		if err := validateImportValuesInValue(res.Properties, resolvedParams, exports); err != nil {
			return err
		}
	}

	for _, out := range tmpl.Outputs {
		if err := validateImportValuesInValue(out.Value, resolvedParams, exports); err != nil {
			return err
		}
	}

	return nil
}

func validateImportValuesInValue(v any, params map[string]string, exports map[string]string) error {
	switch val := v.(type) {
	case map[string]any:
		if importVal, hasImport := val["Fn::ImportValue"]; hasImport {
			name := resolveScalar(importVal, params, nil)
			if _, ok := exports[name]; !ok {
				return fmt.Errorf("%w: %s", ErrExportNotFound, name)
			}
		}

		for _, child := range val {
			if err := validateImportValuesInValue(child, params, exports); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range val {
			if err := validateImportValuesInValue(item, params, exports); err != nil {
				return err
			}
		}
	}

	return nil
}

// collectImportValues scans a template body for all Fn::ImportValue export name references.
// It uses the resolved params to evaluate non-literal import names (e.g. Ref, Fn::Sub).
func collectImportValues(templateBody string, params map[string]string) []string {
	if templateBody == "" {
		return nil
	}

	tmpl, err := ParseTemplate(templateBody)
	if err != nil {
		return nil
	}

	var refs []string
	for _, res := range tmpl.Resources {
		collectImportValuesFromValue(res.Properties, params, &refs)
	}

	for _, out := range tmpl.Outputs {
		collectImportValuesFromValue(out.Value, params, &refs)
	}

	return refs
}

func collectImportValuesFromValue(v any, params map[string]string, refs *[]string) {
	switch val := v.(type) {
	case map[string]any:
		if importVal, hasImport := val["Fn::ImportValue"]; hasImport {
			// Resolve the import name — it may be a literal string or a Ref intrinsic
			// such as {"Ref": "ExportNameParam"}.
			name := resolveScalar(importVal, params, nil)
			if name != "" {
				*refs = append(*refs, name)
			}
		}

		for _, child := range val {
			collectImportValuesFromValue(child, params, refs)
		}
	case []any:
		for _, item := range val {
			collectImportValuesFromValue(item, params, refs)
		}
	}
}

// resolveGetAtt resolves Fn::GetAtt [logicalID, attributeName] using the ctx (#9).
func resolveGetAtt(logicalID, attrName string, ctx resolveCtx) string {
	physID := ctx.physicalIDs[logicalID]
	resType := ctx.resourceTypes[logicalID]

	// Custom resource Data outputs are stored in physicalIDs as "logicalID/Key".
	if resType == cfnTypeCustomResource || strings.HasPrefix(resType, "Custom::") {
		if v := getCustomResourceAttrFromPhysicalIDs(logicalID, attrName, ctx.physicalIDs); v != "" {
			return v
		}
	}

	return getResourceAttribute(resType, physID, attrName, ctx.accountID, ctx.region)
}

// getResourceAttribute derives the named attribute value from a resource's physicalID and type.
func getResourceAttribute(resType, physID, attrName, accountID, region string) string {
	if physID == "" {
		return ""
	}

	switch resType {
	case "AWS::S3::Bucket":
		return getS3BucketAttribute(physID, attrName, region)
	case "AWS::DynamoDB::Table":
		return getDynamoDBTableAttribute(physID, attrName, accountID, region)
	case "AWS::Lambda::Function":
		return getLambdaFunctionAttribute(physID, attrName, accountID, region)
	case "AWS::SQS::Queue":
		return getSQSQueueAttribute(physID, attrName, accountID, region)
	case "AWS::SNS::Topic":
		return getSNSTopicAttribute(physID, attrName)
	case "AWS::KMS::Key":
		return getKMSKeyAttribute(physID, attrName, accountID, region)
	case "AWS::IAM::Role":
		return getIAMRoleAttribute(physID, attrName)
	case "AWS::Logs::LogGroup":
		return getLogsLogGroupAttribute(physID, attrName, accountID, region)
	case "AWS::SecretsManager::Secret":
		return getSecretsManagerSecretAttribute(physID, attrName)
	case resTypeStepFunctionsStateMachine:
		return getStateMachineAttribute(physID, attrName)
	case "AWS::CloudFormation::Stack":
		return getCloudFormationStackAttribute(physID, attrName)
	}

	if v, ok := getExtraResourceAttribute(resType, physID, attrName, accountID, region); ok {
		return v
	}

	return physID
}

func getLambdaFunctionAttribute(physID, attrName, accountID, region string) string {
	if attrName == attrNameArn {
		return arn.Build("lambda", region, accountID, "function:"+physID)
	}

	return physID
}

func getSNSTopicAttribute(physID, attrName string) string {
	// For SNS topics the physical ID is the topic ARN
	// (arn:aws:sns:<region>:<account>:<topicName>).
	switch attrName {
	case "TopicArn", attrNameArn:
		return physID
	case "TopicName":
		if idx := strings.LastIndex(physID, ":"); idx >= 0 {
			return physID[idx+1:]
		}

		return physID
	}

	return physID
}

func getIAMRoleAttribute(physID, _ string) string {
	// For IAM roles the physical ID is the role ARN
	// (arn:aws:iam::<account>:role/<roleName>); Arn resolves to it directly.
	return physID
}

func getLogsLogGroupAttribute(physID, attrName, accountID, region string) string {
	if attrName == attrNameArn {
		return arn.Build("logs", region, accountID, "log-group:"+physID+":*")
	}

	return physID
}

func getSecretsManagerSecretAttribute(physID, attrName string) string {
	if attrName == "Id" {
		return physID
	}

	return physID
}

func getCloudFormationStackAttribute(physID, attrName string) string {
	if attrName == "Outputs" {
		return physID
	}

	return physID
}

func getS3BucketAttribute(physID, attrName, region string) string {
	switch attrName {
	case attrNameArn:
		return arn.BuildS3(physID)
	case "DomainName":
		return physID + ".s3.amazonaws.com"
	case "RegionalDomainName":
		if region != "" {
			return physID + ".s3." + region + ".amazonaws.com"
		}

		return physID + ".s3.amazonaws.com"
	case "WebsiteURL":
		if region != "" {
			return "http://" + physID + ".s3-website." + region + ".amazonaws.com"
		}

		return "http://" + physID + ".s3-website.us-east-1.amazonaws.com"
	}

	return physID
}

func getDynamoDBTableAttribute(physID, attrName, accountID, region string) string {
	switch attrName {
	case attrNameArn:
		return arn.Build("dynamodb", region, accountID, "table/"+physID)
	case "StreamArn":
		return arn.Build(
			"dynamodb",
			region,
			accountID,
			"table/"+physID+"/stream/2000-01-01T00:00:00.000",
		)
	}

	return physID
}

func getSQSQueueAttribute(physID, attrName, accountID, region string) string {
	queueName := sqsQueueNameFromURL(physID)

	switch attrName {
	case attrNameArn:
		return arn.Build("sqs", region, accountID, queueName)
	case "QueueName":
		return queueName
	case "QueueUrl":
		return physID
	}

	return physID
}

func getKMSKeyAttribute(physID, attrName, accountID, region string) string {
	switch attrName {
	case attrNameArn:
		return arn.Build("kms", region, accountID, "key/"+physID)
	case "KeyId":
		return physID
	}

	return physID
}

func getStateMachineAttribute(physID, attrName string) string {
	switch attrName {
	case attrNameArn:
		return physID
	case "Name":
		parts := strings.Split(physID, ":")
		if len(parts) > 0 {
			return parts[len(parts)-1]
		}

		return physID
	}

	return physID
}

// sqsQueueNameFromURL extracts the queue name from an SQS queue URL.
// URL format: scheme://host/accountID/queueName
func sqsQueueNameFromURL(queueURL string) string {
	parts := strings.Split(queueURL, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}

	return queueURL
}

// getAZsForRegion returns a canned list of availability zones for the given region.
func getAZsForRegion(region string) []string {
	switch region {
	case "us-east-1":
		return []string{
			"us-east-1a",
			"us-east-1b",
			"us-east-1c",
			"us-east-1d",
			"us-east-1e",
			"us-east-1f",
		}
	case "us-east-2":
		return []string{"us-east-2a", "us-east-2b", "us-east-2c"}
	case "us-west-1":
		return []string{"us-west-1a", "us-west-1b"}
	case "us-west-2":
		return []string{"us-west-2a", "us-west-2b", "us-west-2c", "us-west-2d"}
	case "eu-west-1":
		return []string{"eu-west-1a", "eu-west-1b", "eu-west-1c"}
	case "eu-west-2":
		return []string{"eu-west-2a", "eu-west-2b", "eu-west-2c"}
	case "eu-central-1":
		return []string{"eu-central-1a", "eu-central-1b", "eu-central-1c"}
	case "ap-southeast-1":
		return []string{"ap-southeast-1a", "ap-southeast-1b", "ap-southeast-1c"}
	case "ap-southeast-2":
		return []string{"ap-southeast-2a", "ap-southeast-2b", "ap-southeast-2c"}
	case "ap-northeast-1":
		return []string{"ap-northeast-1a", "ap-northeast-1b", "ap-northeast-1c"}
	default:
		// Default: three zones using the region name
		return []string{region + "a", region + "b", region + "c"}
	}
}

// cidrArgsWithBits is the minimum arg count for Fn::Cidr to include a cidrBits argument.
const cidrArgsWithBits = 3

// resolveCidr computes CIDR subnets from [ipBlock, count, cidrBits] arguments.
func resolveCidr(args []any, ctx resolveCtx) string {
	ipBlock := resolveValueCtx(args[0], ctx)
	countStr := resolveValueCtx(args[1], ctx)

	var cidrBitsStr string
	if len(args) >= cidrArgsWithBits {
		cidrBitsStr = resolveValueCtx(args[2], ctx)
	}

	_, ipNet, err := net.ParseCIDR(ipBlock)
	if err != nil {
		return ""
	}

	var count int
	const maxCount = 256
	c, countErr := strconv.Atoi(countStr)
	if countErr != nil || c <= 0 || c > maxCount {
		return ""
	}
	count = c

	// Determine prefix length for subnets.
	var newBits int
	if cidrBitsStr != "" {
		if _, err = fmt.Sscanf(cidrBitsStr, "%d", &newBits); err != nil {
			return ""
		}
	} else {
		// Default: split evenly
		newBits = 4
	}

	ones, bits := ipNet.Mask.Size()
	subnetBits := min(ones+newBits, bits)

	subnets := make([]string, 0, count)
	baseIP := ipNet.IP.To4()
	if baseIP == nil {
		baseIP = ipNet.IP.To16()
	}

	step := new(big.Int).Lsh(big.NewInt(1), uint(bits-subnetBits))
	ipInt := new(big.Int).SetBytes(baseIP)

	for range count {
		subnet := make(net.IP, len(baseIP))
		ipInt.FillBytes(subnet)
		subnets = append(subnets, fmt.Sprintf("%s/%d", subnet.String(), subnetBits))
		ipInt.Add(ipInt, step)
	}

	return strings.Join(subnets, splitSep)
}

// resolveLength returns the count of elements in a list value.
func resolveLength(v any, ctx resolveCtx) string {
	switch val := v.(type) {
	case []any:
		return strconv.Itoa(len(val))
	case string:
		// Could be a splitSep-encoded list
		if strings.Contains(val, splitSep) {
			return strconv.Itoa(len(strings.Split(val, splitSep)))
		}

		return "1"
	case map[string]any:
		// Resolve first, then check result
		resolved := resolveValueCtx(val, ctx)
		if strings.Contains(resolved, splitSep) {
			return strconv.Itoa(len(strings.Split(resolved, splitSep)))
		}

		return "1"
	}

	return "0"
}

// invokeMacroTransform calls the named macro with the Fn::Transform fragment,
// returning the resolved string, or empty string if the macro is unavailable.
func invokeMacroTransform(macroName string, transformSpec map[string]any, ctx resolveCtx) string {
	if ctx.macros == nil {
		return ""
	}

	// Extract the Parameters from the transform spec and resolve them.
	params, _ := transformSpec["Parameters"].(map[string]any)
	resolvedParams := make(map[string]string, len(params))

	for k, v := range params {
		resolvedParams[k] = resolveValueCtx(v, ctx)
	}

	// Build a minimal fragment from the transform spec parameters.
	fragmentJSON, err := json.Marshal(resolvedParams)
	if err != nil {
		return ""
	}

	// MacroRegistry.InvokeMacro requires a Lambda backend — we don't have one in
	// resolveCtx, so we return empty here and let callers with a backend invoke macros
	// at template-level (top-level Transform key) instead.
	_ = macroName
	_ = fragmentJSON
	_ = ctx.macros

	return ""
}

// getCustomResourceAttrFromPhysicalIDs resolves a Fn::GetAtt attribute for a custom
// resource by looking up the stored Data output in the physicalIDs map.
func getCustomResourceAttrFromPhysicalIDs(
	logicalID, attrName string,
	physicalIDs map[string]string,
) string {
	return physicalIDs[logicalID+"/"+attrName]
}
