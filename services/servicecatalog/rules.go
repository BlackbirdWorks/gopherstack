package servicecatalog

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
)

const pairArgs = 2

// ResourceLookup resolves the account resources behind AWS-specific parameter types for Fn::RefAll,
// Fn::ValueOf and Fn::ValueOfAll.
type ResourceLookup interface {
	ResourceIDs(ctx context.Context, paramType string) ([]string, bool)
	ResourceAttribute(ctx context.Context, paramType, id, attribute string) (string, bool)
}

type ruleAssertion struct {
	Assert            any    `json:"Assert"`
	AssertDescription string `json:"AssertDescription"`
}

type ruleDef struct {
	RuleCondition any             `json:"RuleCondition"`
	Assertions    []ruleAssertion `json:"Assertions"`
}

type ruleDoc struct {
	Rules map[string]ruleDef `json:"Rules"`
}

// ruleEnv is what a rule expression reads: the launch parameter values and types and the account resources.
type ruleEnv struct {
	values map[string]string
	types  map[string]string
	ids    func(paramType string) ([]string, bool)
	attr   func(paramType, id, attribute string) (string, bool)
}

func newRuleEnv(ctx context.Context, lookup ResourceLookup, values, types map[string]string) *ruleEnv {
	e := &ruleEnv{values: values, types: types}

	if lookup != nil {
		e.ids = func(t string) ([]string, bool) { return lookup.ResourceIDs(ctx, t) }
		e.attr = func(t, id, a string) (string, bool) { return lookup.ResourceAttribute(ctx, t, id, a) }
	}

	return e
}

func isRuleFunc(fn string) bool {
	return slices.Contains([]string{
		"Ref", "Fn::Equals", "Fn::Not", "Fn::And", "Fn::Or", "Fn::Contains", "Fn::EachMemberEquals",
		"Fn::EachMemberIn", "Fn::RefAll", "Fn::ValueOf", "Fn::ValueOfAll",
	}, fn)
}

// validateTemplateRules rejects a Rules document CloudFormation would not accept.
func validateTemplateRules(rules map[string]any) error {
	raw, err := json.Marshal(map[string]any{"Rules": rules})
	if err != nil {
		return invalid("Rules is not valid JSON")
	}

	var doc ruleDoc
	if err = json.Unmarshal(raw, &doc); err != nil {
		return invalid("Rules is malformed: %v", err)
	}

	for _, name := range sortedKeys(doc.Rules) {
		if err = validateRule(name, doc.Rules[name]); err != nil {
			return err
		}
	}

	return nil
}

func validateRule(name string, r ruleDef) error {
	if len(r.Assertions) == 0 {
		return invalid("Rule %q needs at least one assertion", name)
	}

	if r.RuleCondition != nil {
		if err := validateRuleExpr(name, r.RuleCondition); err != nil {
			return err
		}
	}

	for _, a := range r.Assertions {
		if a.Assert == nil {
			return invalid("Rule %q has an assertion without Assert", name)
		}

		if err := validateRuleExpr(name, a.Assert); err != nil {
			return err
		}
	}

	return nil
}

func validateRuleExpr(rule string, expr any) error {
	switch v := expr.(type) {
	case []any:
		for _, e := range v {
			if err := validateRuleExpr(rule, e); err != nil {
				return err
			}
		}
	case map[string]any:
		if len(v) != 1 {
			return invalid("Rule %q has an expression that is not a single function", rule)
		}

		for fn, arg := range v {
			if !isRuleFunc(fn) {
				return invalid("Rule %q uses %q, which is not a rule function", rule, fn)
			}

			return validateRuleExpr(rule, arg)
		}
	}

	return nil
}

// checkTemplateRules evaluates the Rules of TEMPLATE constraints against the launch parameters.
// An expression that cannot be resolved (no resource lookup wired) makes its assertion pass.
func checkTemplateRules(env *ruleEnv, constraintParams []string) error {
	for _, raw := range constraintParams {
		var doc ruleDoc
		if err := json.Unmarshal([]byte(raw), &doc); err != nil {
			continue
		}

		for _, name := range sortedKeys(doc.Rules) {
			if err := env.checkRule(name, doc.Rules[name]); err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *ruleEnv) checkRule(name string, r ruleDef) error {
	if r.RuleCondition != nil {
		if ok, known := e.eval(r.RuleCondition); known && !asBool(ok) {
			return nil
		}
	}

	for _, a := range r.Assertions {
		if ok, known := e.eval(a.Assert); known && !asBool(ok) {
			if a.AssertDescription == "" {
				return invalid("Rule %q assertion failed", name)
			}

			return invalid("Rule %q assertion failed: %s", name, a.AssertDescription)
		}
	}

	return nil
}

func asBool(v any) bool {
	b, _ := v.(bool)

	return b
}

// eval evaluates a CloudFormation rule expression; known is false when it cannot be resolved.
func (e *ruleEnv) eval(expr any) (any, bool) {
	switch v := expr.(type) {
	case string, bool, float64:
		return v, true
	case []any:
		out := make([]any, 0, len(v))

		for _, x := range v {
			r, ok := e.eval(x)
			if !ok {
				return nil, false
			}

			out = append(out, r)
		}

		return out, true
	case map[string]any:
		return e.evalFunc(v)
	}

	return nil, false
}

func (e *ruleEnv) evalFunc(m map[string]any) (any, bool) {
	if len(m) != 1 {
		return nil, false
	}

	for fn, arg := range m {
		switch fn {
		case "Ref":
			name, _ := arg.(string)
			val, ok := e.values[name]

			return val, ok
		case "Fn::RefAll":
			return e.refAll(arg)
		}

		args, ok := e.eval(arg)
		list, isList := args.([]any)

		if !ok || !isList {
			return nil, false
		}

		return e.apply(fn, list)
	}

	return nil, false
}

func (e *ruleEnv) apply(fn string, args []any) (any, bool) {
	switch fn {
	case "Fn::Equals":
		return ruleEquals(args)
	case "Fn::Not":
		return ruleNot(args)
	case "Fn::And":
		return ruleAnd(args), true
	case "Fn::Or":
		return ruleOr(args), true
	case "Fn::Contains":
		return ruleContains(args)
	case "Fn::EachMemberEquals":
		return ruleEachMemberEquals(args)
	case "Fn::EachMemberIn":
		return ruleEachMemberIn(args)
	case "Fn::ValueOf":
		return e.valueOf(args)
	case "Fn::ValueOfAll":
		return e.valueOfAll(args)
	}

	return nil, false
}

func (e *ruleEnv) refAll(arg any) (any, bool) {
	typ, isStr := arg.(string)
	if !isStr || e.ids == nil {
		return nil, false
	}

	ids, ok := e.ids(typ)
	if !ok {
		return nil, false
	}

	out := make([]any, 0, len(ids))
	for _, id := range ids {
		out = append(out, id)
	}

	return out, true
}

// valueOf is Fn::ValueOf: an attribute of the resource(s) a parameter points at.
func (e *ruleEnv) valueOf(args []any) (any, bool) {
	if len(args) != pairArgs || e.ids == nil {
		return nil, false
	}

	name, attr := toStr(args[0]), toStr(args[1])

	val, ok := e.values[name]
	if !ok {
		return nil, false
	}

	typ := e.types[name]
	isList := strings.HasPrefix(typ, "List<") || strings.HasPrefix(typ, "CommaDelimitedList")
	typ = strings.TrimSuffix(strings.TrimPrefix(typ, "List<"), ">")

	var out []any

	for _, el := range asList(val) {
		a, found := e.attr(typ, toStr(el), attr)
		if !found {
			return nil, false
		}

		out = append(out, a)
	}

	if !isList && len(out) == 1 {
		return out[0], true
	}

	return out, true
}

// valueOfAll is Fn::ValueOfAll: an attribute of every resource of a parameter type.
func (e *ruleEnv) valueOfAll(args []any) (any, bool) {
	if len(args) != pairArgs || e.ids == nil {
		return nil, false
	}

	typ, attr := toStr(args[0]), toStr(args[1])

	ids, ok := e.ids(typ)
	if !ok {
		return nil, false
	}

	out := make([]any, 0, len(ids))

	for _, id := range ids {
		if a, found := e.attr(typ, id, attr); found {
			out = append(out, a)
		}
	}

	return out, true
}

func ruleEquals(args []any) (any, bool) {
	if len(args) != pairArgs {
		return nil, false
	}

	return toStr(args[0]) == toStr(args[1]), true
}

func ruleNot(args []any) (any, bool) {
	if len(args) != 1 {
		return nil, false
	}

	return !asBool(args[0]), true
}

func ruleAnd(args []any) bool {
	for _, a := range args {
		if !asBool(a) {
			return false
		}
	}

	return true
}

func ruleOr(args []any) bool { return slices.ContainsFunc(args, asBool) }

func ruleContains(args []any) (any, bool) {
	if len(args) != pairArgs {
		return nil, false
	}

	for _, e := range asList(args[0]) {
		if toStr(e) == toStr(args[1]) {
			return true, true
		}
	}

	return false, true
}

func ruleEachMemberEquals(args []any) (any, bool) {
	if len(args) != pairArgs {
		return nil, false
	}

	for _, e := range asList(args[0]) {
		if toStr(e) != toStr(args[1]) {
			return false, true
		}
	}

	return true, true
}

func ruleEachMemberIn(args []any) (any, bool) {
	if len(args) != pairArgs {
		return nil, false
	}

	allowed := asList(args[1])

	for _, e := range asList(args[0]) {
		found := false

		for _, a := range allowed {
			found = found || toStr(a) == toStr(e)
		}

		if !found {
			return false, true
		}
	}

	return true, true
}

func asList(v any) []any {
	switch l := v.(type) {
	case []any:
		return l
	case string:
		parts := strings.Split(l, ",")
		out := make([]any, 0, len(parts))

		for _, p := range parts {
			out = append(out, strings.TrimSpace(p))
		}

		return out
	}

	return nil
}

func toStr(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case bool:
		if s {
			return "true"
		}

		return "false"
	case float64:
		b, _ := json.Marshal(s)

		return string(b)
	}

	return ""
}
