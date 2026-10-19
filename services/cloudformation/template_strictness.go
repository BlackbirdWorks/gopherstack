package cloudformation

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
)

//go:embed cfn_resource_services.json
var cfnResourceServicesJSON []byte

//nolint:gochecknoglobals // lazily populated cache of the embedded service list
var (
	cfnResourceServices     map[string]struct{}
	cfnResourceServicesOnce sync.Once
)

func knownResourceServices() map[string]struct{} {
	cfnResourceServicesOnce.Do(func() {
		var raw []string
		if err := json.Unmarshal(cfnResourceServicesJSON, &raw); err != nil {
			panic("cloudformation: invalid embedded cfn_resource_services.json: " + err.Error())
		}

		cfnResourceServices = make(map[string]struct{}, len(raw))
		for _, svc := range raw {
			cfnResourceServices[svc] = struct{}{}
		}
	})

	return cfnResourceServices
}

const (
	typeSegments   = 3
	moduleSuffix   = "::MODULE"
	customPrefix   = "Custom::"
	alexaPrefix    = "Alexa::"
	fnIf           = "Fn::If"
	blockResources = "Resources"
	blockOutputs   = "Outputs"
	pseudoNoValue  = "AWS::NoValue"
)

var parameterTypePattern = regexp.MustCompile(
	`^(String|Number|List<Number>|CommaDelimitedList|AWS::.+|List<AWS::.+>)$`,
)

// typeKnown reports whether a type name is a privately registered or activated type.
type typeKnown func(typeName string) bool

func resourceTypeRecognized(typeName string, registered typeKnown) bool {
	switch {
	case strings.HasPrefix(typeName, customPrefix),
		strings.HasPrefix(typeName, alexaPrefix),
		strings.HasSuffix(typeName, moduleSuffix):
		return true
	}

	if parts := strings.Split(typeName, "::"); len(parts) >= typeSegments && parts[0] == "AWS" {
		if _, ok := knownResourceServices()[parts[1]]; ok {
			return true
		}
	}

	return registered != nil && registered(typeName)
}

// unrecognizedResourceTypesErr mirrors "Template format error: Unrecognized
// resource types: [X]". An AWS:: type is recognised by its service namespace,
// so an unmodelled resource of a known service is accepted, not rejected.
func unrecognizedResourceTypesErr(tmpl *Template, registered typeKnown) error {
	set := map[string]struct{}{}

	for _, res := range tmpl.Resources {
		if !resourceTypeRecognized(res.Type, registered) {
			set[res.Type] = struct{}{}
		}
	}

	if len(set) == 0 {
		return nil
	}

	names := make([]string, 0, len(set))
	for n := range set {
		names = append(names, n)
	}

	sort.Strings(names)

	return templateFormatErr("Unrecognized resource types: [%s]", strings.Join(names, ", "))
}

// templateSectionsErr checks the shape of Parameters and Outputs members.
func templateSectionsErr(tmpl *Template) error {
	for _, name := range sortedKeys(tmpl.Parameters) {
		p := tmpl.Parameters[name]
		if p.Type == "" {
			return templateFormatErr("[/Parameters/%s] Every Parameters object must contain a Type member.", name)
		}

		if !parameterTypePattern.MatchString(p.Type) {
			return templateFormatErr("Unrecognized parameter type: %s", p.Type)
		}
	}

	for _, name := range sortedKeys(tmpl.Outputs) {
		if tmpl.Outputs[name].Value == nil {
			return templateFormatErr("Every Outputs member must contain a Value object.")
		}
	}

	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// unresolvedConditionsErr mirrors "Unresolved condition dependencies [X] in the
// <Block> block of the template" for a Condition name no Conditions entry defines.
func unresolvedConditionsErr(tmpl *Template) error {
	collect := func(visit func(add func(string))) []string {
		set := map[string]struct{}{}

		visit(func(name string) {
			if _, ok := tmpl.Conditions[name]; !ok {
				set[name] = struct{}{}
			}
		})

		out := make([]string, 0, len(set))
		for n := range set {
			out = append(out, n)
		}

		sort.Strings(out)

		return out
	}

	blocks := []struct {
		visit func(add func(string))
		name  string
	}{
		{name: "Conditions", visit: func(add func(string)) {
			for _, c := range tmpl.Conditions {
				scanConditionExpr(c, add)
			}
		}},
		{name: blockResources, visit: func(add func(string)) {
			for _, r := range tmpl.Resources {
				if r.Condition != "" {
					add(r.Condition)
				}

				scanFnIfConditions(r.Properties, add)
			}
		}},
		{name: blockOutputs, visit: func(add func(string)) {
			for _, o := range tmpl.Outputs {
				if o.Condition != "" {
					add(o.Condition)
				}

				scanFnIfConditions(o.Value, add)
			}
		}},
	}

	for _, blk := range blocks {
		if missing := collect(blk.visit); len(missing) > 0 {
			return templateFormatErr(
				"Unresolved condition dependencies [%s] in the %s block of the template",
				strings.Join(missing, ", "), blk.name,
			)
		}
	}

	return nil
}

// scanConditionExpr finds {"Condition": "Name"} references inside a Conditions entry.
func scanConditionExpr(v any, add func(string)) {
	switch val := v.(type) {
	case map[string]any:
		if name, ok := val["Condition"].(string); ok && len(val) == 1 {
			add(name)
		}

		for _, child := range val {
			scanConditionExpr(child, add)
		}
	case []any:
		for _, child := range val {
			scanConditionExpr(child, add)
		}
	}
}

func scanFnIfConditions(v any, add func(string)) {
	switch val := v.(type) {
	case map[string]any:
		if args, ok := val[fnIf].([]any); ok && len(args) > 0 {
			if name, isStr := args[0].(string); isStr {
				add(name)
			}
		}

		for _, child := range val {
			scanFnIfConditions(child, add)
		}
	case []any:
		for _, child := range val {
			scanFnIfConditions(child, add)
		}
	}
}

// pruneConditional drops resources and outputs whose Condition evaluates false.
func pruneConditional(tmpl *Template, resolved map[string]string) {
	if tmpl == nil {
		return
	}

	var conds map[string]bool

	eval := func(name string) bool {
		if conds == nil {
			conds = evaluateConditions(tmpl.Conditions, resolved, nil)
		}

		return conds[name]
	}

	for id, res := range tmpl.Resources {
		if res.Condition != "" && !eval(res.Condition) {
			delete(tmpl.Resources, id)
		}
	}

	for id, out := range tmpl.Outputs {
		if out.Condition != "" && !eval(out.Condition) {
			delete(tmpl.Outputs, id)
		}
	}
}

//go:embed cfn_required_properties.json
var cfnRequiredPropertiesJSON []byte

//nolint:gochecknoglobals // lazily populated cache of the embedded table
var (
	cfnRequiredProperties     map[string][]string
	cfnRequiredPropertiesOnce sync.Once
)

// requiredResourceProperties lists properties the CloudFormation resource
// schema marks required. Only types whose required set is certain are listed;
// every other type stays unchecked rather than guessed.
func requiredResourceProperties() map[string][]string {
	cfnRequiredPropertiesOnce.Do(func() {
		if err := json.Unmarshal(cfnRequiredPropertiesJSON, &cfnRequiredProperties); err != nil {
			panic("cloudformation: invalid embedded cfn_required_properties.json: " + err.Error())
		}
	})

	return cfnRequiredProperties
}

func propertyPresent(props map[string]any, key string) bool {
	v, ok := props[key]
	if !ok || v == nil {
		return false
	}

	if m, isMap := v.(map[string]any); isMap && len(m) == 1 && m["Ref"] == pseudoNoValue {
		return false
	}

	return true
}

// validateRequiredProperties mirrors the registry-schema failure
// "Properties validation failed for resource X with message: #: required key [P] not found".
func validateRequiredProperties(tmpl *Template) error {
	required := requiredResourceProperties()

	for _, id := range sortedKeys(tmpl.Resources) {
		res := tmpl.Resources[id]

		var missing []string

		for _, key := range required[res.Type] {
			if !propertyPresent(res.Properties, key) {
				missing = append(missing, fmt.Sprintf("#: required key [%s] not found", key))
			}
		}

		if len(missing) > 0 {
			return fmt.Errorf("%w: Properties validation failed for resource %s with message:\n%s",
				ErrPropertyValidation, id, strings.Join(missing, "\n"))
		}
	}

	return nil
}

// evaluateRules checks Rules assertions against the resolved parameter values.
func evaluateRules(tmpl *Template, resolved map[string]string) error {
	for _, name := range sortedKeys(tmpl.Rules) {
		rule, _ := tmpl.Rules[name].(map[string]any)

		if cond, ok := rule["RuleCondition"]; ok && !evalRuleExpr(cond, resolved) {
			continue
		}

		asserts, _ := rule["Assertions"].([]any)
		for _, a := range asserts {
			am, _ := a.(map[string]any)

			if evalRuleExpr(am["Assert"], resolved) {
				continue
			}

			desc, _ := am["AssertDescription"].(string)

			return awsErrorf(ErrRuleAssertion, "Rule '%s' assertion failed: %s", name, desc)
		}
	}

	return nil
}

func ruleScalar(v any, resolved map[string]string) string {
	switch val := v.(type) {
	case string:
		return val
	case map[string]any:
		if ref, ok := val["Ref"].(string); ok {
			return resolved[ref]
		}
	}

	return fmt.Sprint(v)
}

func ruleList(v any, resolved map[string]string) []string {
	if list, ok := v.([]any); ok {
		out := make([]string, 0, len(list))
		for _, item := range list {
			out = append(out, ruleScalar(item, resolved))
		}

		return out
	}

	raw := ruleScalar(v, resolved)
	if raw == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}

	return parts
}

// evalRuleExpr evaluates the rule-function subset that works on parameter
// values; an unsupported function counts as satisfied rather than guessed.
func evalRuleExpr(expr any, resolved map[string]string) bool {
	m, ok := expr.(map[string]any)
	if !ok {
		return true
	}

	for _, fn := range ruleFns() {
		if args, is := m[fn.name].([]any); is && fn.arity(len(args)) {
			return fn.eval(args, resolved)
		}
	}

	return true
}

const (
	ruleUnary  = 1
	ruleBinary = 2
)

type ruleFn struct {
	arity func(n int) bool
	eval  func(args []any, resolved map[string]string) bool
	name  string
}

func arityIs(want int) func(int) bool { return func(n int) bool { return n == want } }

func arityAny(int) bool { return true }

func ruleFns() []ruleFn {
	return []ruleFn{
		{name: "Fn::Equals", arity: arityIs(ruleBinary), eval: ruleEquals},
		{name: "Fn::Contains", arity: arityIs(ruleBinary), eval: ruleContains},
		{name: "Fn::EachMemberEquals", arity: arityIs(ruleBinary), eval: ruleEachMemberEquals},
		{name: "Fn::EachMemberIn", arity: arityIs(ruleBinary), eval: ruleEachMemberIn},
		{name: "Fn::And", arity: arityAny, eval: ruleAnd},
		{name: "Fn::Or", arity: arityAny, eval: ruleOr},
		{name: "Fn::Not", arity: arityIs(ruleUnary), eval: ruleNot},
	}
}

func ruleEquals(args []any, resolved map[string]string) bool {
	return ruleScalar(args[0], resolved) == ruleScalar(args[1], resolved)
}

func ruleContains(args []any, resolved map[string]string) bool {
	return slices.Contains(ruleList(args[0], resolved), ruleScalar(args[1], resolved))
}

func ruleEachMemberEquals(args []any, resolved map[string]string) bool {
	want := ruleScalar(args[1], resolved)

	return !slices.ContainsFunc(ruleList(args[0], resolved), func(s string) bool { return s != want })
}

func ruleEachMemberIn(args []any, resolved map[string]string) bool {
	allowed := ruleList(args[1], resolved)

	return !slices.ContainsFunc(
		ruleList(args[0], resolved),
		func(s string) bool { return !slices.Contains(allowed, s) },
	)
}

func ruleAnd(args []any, resolved map[string]string) bool {
	return !slices.ContainsFunc(args, func(a any) bool { return !evalRuleExpr(a, resolved) })
}

func ruleOr(args []any, resolved map[string]string) bool {
	return slices.ContainsFunc(args, func(a any) bool { return evalRuleExpr(a, resolved) })
}

func ruleNot(args []any, resolved map[string]string) bool {
	return !evalRuleExpr(args[0], resolved)
}
