package rum

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/eventpattern"
)

// matchPattern reports whether doc satisfies an EventBridge-style event pattern.
func matchPattern(pattern, doc map[string]any) bool {
	for key, pv := range pattern {
		have, present := doc[key]

		switch p := pv.(type) {
		case map[string]any:
			sub, isObj := have.(map[string]any)
			if !isObj || !matchPattern(p, sub) {
				return false
			}
		case []any:
			if !matchRules(p, have, present) {
				return false
			}
		default:
			return false
		}
	}

	return true
}

func matchRules(rules []any, have any, present bool) bool {
	if list, isList := have.([]any); isList {
		for _, el := range list {
			if matchRules(rules, el, true) {
				return true
			}
		}

		return false
	}

	for _, rule := range rules {
		if matchRule(rule, have, present) {
			return true
		}
	}

	return false
}

func matchRule(rule, have any, present bool) bool {
	obj, isObj := rule.(map[string]any)
	if !isObj {
		return present && scalarEqual(rule, have)
	}

	for op, arg := range obj {
		if !matchOperator(op, arg, have, present) {
			return false
		}
	}

	return len(obj) > 0
}

const (
	opExists     = "exists"
	opNumeric    = "numeric"
	opPrefix     = "prefix"
	opSuffix     = "suffix"
	opEqualsFold = "equals-ignore-case"
	opAnythingBu = "anything-but"
)

var errNotObject = errors.New("not a JSON object")

func matchOperator(op string, arg, have any, present bool) bool {
	switch op {
	case opExists:
		want, ok := arg.(bool)

		return ok && want == present
	case opNumeric:
		n, isNum := eventpattern.ToFloat64(have)
		rules, ok := arg.([]any)

		return present && isNum && ok && eventpattern.MatchNumericRules(n, rules)
	case opPrefix, opSuffix, opEqualsFold:
		return present && matchString(op, arg, have)
	case opAnythingBu:
		return present && matchAnythingBut(arg, have)
	default:
		return false
	}
}

func matchString(op string, arg, have any) bool {
	s, isStr := have.(string)
	p, ok := arg.(string)

	if !isStr || !ok {
		return false
	}

	switch op {
	case opPrefix:
		return strings.HasPrefix(s, p)
	case opSuffix:
		return strings.HasSuffix(s, p)
	default:
		return strings.EqualFold(s, p)
	}
}

func matchAnythingBut(arg, have any) bool {
	if list, isList := arg.([]any); isList {
		return !slices.ContainsFunc(list, func(a any) bool { return scalarEqual(a, have) })
	}

	return !scalarEqual(arg, have)
}

func scalarEqual(a, b any) bool {
	if x, ok := eventpattern.ToFloat64(a); ok {
		y, isNum := eventpattern.ToFloat64(b)

		return isNum && x == y
	}

	switch a.(type) {
	case string, bool, nil:
		return a == b
	default:
		return false
	}
}

// lookupPath resolves a dotted path ("event_details.duration") in a decoded event.
func lookupPath(doc map[string]any, path string) (any, bool) {
	var cur any = doc

	for seg := range strings.SplitSeq(path, ".") {
		obj, isObj := cur.(map[string]any)
		if !isObj {
			return nil, false
		}

		next, ok := obj[seg]
		if !ok {
			return nil, false
		}

		cur = next
	}

	return cur, true
}

func pathString(doc map[string]any, path string) (string, bool) {
	v, ok := lookupPath(doc, path)
	if !ok {
		return "", false
	}

	if s, isStr := v.(string); isStr {
		return s, true
	}

	return fmt.Sprint(v), true
}

func parseObject(s string) (map[string]any, error) {
	var out map[string]any
	if err := json.Unmarshal([]byte(s), &out); err != nil || out == nil {
		return nil, fmt.Errorf("%w: %q", errNotObject, s)
	}

	return out, nil
}
