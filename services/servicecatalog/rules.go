package servicecatalog

import (
	"encoding/json"
	"slices"
	"strings"
)

const pairArgs = 2

type ruleDoc struct {
	Rules map[string]struct {
		RuleCondition any `json:"RuleCondition"`
		Assertions    []struct {
			Assert            any    `json:"Assert"`
			AssertDescription string `json:"AssertDescription"`
		} `json:"Assertions"`
	} `json:"Rules"`
}

// checkTemplateRules evaluates the Rules of TEMPLATE constraints against the launch parameters.
// A function outside the supported subset makes its assertion pass rather than block the launch.
func checkTemplateRules(constraintParams []string, values map[string]string) error {
	for _, raw := range constraintParams {
		var doc ruleDoc
		if err := json.Unmarshal([]byte(raw), &doc); err != nil {
			continue
		}

		for _, name := range sortedKeys(doc.Rules) {
			if err := checkRule(name, doc.Rules[name].RuleCondition, doc.Rules[name].Assertions, values); err != nil {
				return err
			}
		}
	}

	return nil
}

func checkRule(name string, condition any, assertions []struct {
	Assert            any    `json:"Assert"`
	AssertDescription string `json:"AssertDescription"`
}, values map[string]string,
) error {
	if condition != nil {
		if ok, known := evalRule(condition, values); known && !asBool(ok) {
			return nil
		}
	}

	for _, a := range assertions {
		if ok, known := evalRule(a.Assert, values); known && !asBool(ok) {
			return invalid("rule %s failed: %s", name, strOr(a.AssertDescription, "assertion not satisfied"))
		}
	}

	return nil
}

func asBool(v any) bool {
	b, _ := v.(bool)

	return b
}

// evalRule evaluates a CloudFormation rule expression; known is false for unsupported functions.
func evalRule(expr any, values map[string]string) (any, bool) {
	switch v := expr.(type) {
	case string, bool, float64:
		return v, true
	case []any:
		out := make([]any, 0, len(v))

		for _, e := range v {
			r, ok := evalRule(e, values)
			if !ok {
				return nil, false
			}

			out = append(out, r)
		}

		return out, true
	case map[string]any:
		return evalRuleFunc(v, values)
	}

	return nil, false
}

func evalRuleFunc(m map[string]any, values map[string]string) (any, bool) {
	if len(m) != 1 {
		return nil, false
	}

	for fn, arg := range m {
		if fn == "Ref" {
			name, _ := arg.(string)
			val, ok := values[name]

			return val, ok
		}

		args, ok := evalRule(arg, values)
		list, isList := args.([]any)

		if !ok || !isList {
			return nil, false
		}

		return applyRuleFunc(fn, list)
	}

	return nil, false
}

func applyRuleFunc(fn string, args []any) (any, bool) {
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
	}

	return nil, false
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
