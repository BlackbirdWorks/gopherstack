package azurearm

import (
	"fmt"
	"path"
	"regexp"
	"strings"
)

const (
	opLike        = "like"
	opContains    = "contains"
	opContainsKey = "containsKey"
	opMatch       = "match"
)

func comparePolicy(op string, left, right any) (bool, error) {
	switch op {
	case "exists":
		want := true

		switch v := right.(type) {
		case bool:
			want = v
		case string:
			want = strings.EqualFold(v, "true")
		}

		return (left != nil) == want, nil
	case "equals":
		return policyEqual(left, right), nil
	case "notEquals":
		return !policyEqual(left, right), nil
	case opLike, "notLike", opMatch, "notMatch", "matchInsensitively", "notMatchInsensitively", opContains,
		"notContains":
		return comparePolicyText(op, left, right), nil
	case "in", "notIn", opContainsKey, "notContainsKey":
		return comparePolicyCollection(op, left, right), nil
	}

	return comparePolicyOrder(op, left, right)
}

func comparePolicyText(op string, left, right any) bool {
	if left == nil {
		return strings.HasPrefix(op, "not")
	}

	l, r := fmt.Sprint(left), fmt.Sprint(right)

	var ok bool

	switch strings.TrimPrefix(strings.ToLower(op[:1])+op[1:], "not") {
	case opLike, "Like":
		ok = wildcardMatch(r, l)
	case opContains, "Contains":
		ok = strings.Contains(strings.ToLower(l), strings.ToLower(r))
	default:
		ok = patternMatch(r, l, strings.Contains(op, "Insensitively"))
	}

	return ok == !strings.HasPrefix(op, "not")
}

func comparePolicyCollection(op string, left, right any) bool {
	ok := false

	if strings.Contains(op, "Key") {
		for k := range mapOf(left) {
			if strings.EqualFold(k, fmt.Sprint(right)) {
				ok = true
			}
		}

		return ok == (op == opContainsKey)
	}

	for _, r := range anyList(right) {
		if policyEqual(left, r) {
			ok = true
		}
	}

	return ok == (op == "in")
}

func comparePolicyOrder(op string, left, right any) (bool, error) {
	if left == nil {
		return false, nil
	}

	l, lerr := toFloat(left)
	r, rerr := toFloat(right)

	var cmp int

	if lerr == nil && rerr == nil {
		switch {
		case l < r:
			cmp = -1
		case l > r:
			cmp = 1
		}
	} else {
		cmp = strings.Compare(strings.ToLower(fmt.Sprint(left)), strings.ToLower(fmt.Sprint(right)))
	}

	switch op {
	case "less":
		return cmp < 0, nil
	case "lessOrEquals":
		return cmp <= 0, nil
	case "greater":
		return cmp > 0, nil
	case "greaterOrEquals":
		return cmp >= 0, nil
	}

	return false, errPolicyUnsupported
}

func toFloat(v any) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case int:
		return float64(n), nil
	}

	var f float64

	_, err := fmt.Sscanf(fmt.Sprint(v), "%g", &f)

	return f, err
}

// policyEqual compares case-insensitively for strings, as the policy language does.
func policyEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	return strings.EqualFold(fmt.Sprint(a), fmt.Sprint(b))
}

// wildcardMatch implements like: * matches any run of characters, case-insensitively.
func wildcardMatch(pattern, s string) bool {
	ok, err := path.Match(strings.ReplaceAll(strings.ToLower(pattern), "?", "[?]"), strings.ToLower(s))
	if err == nil && !strings.Contains(pattern, "/") && !strings.Contains(s, "/") {
		return ok
	}

	re := regexp.MustCompile("(?i)^" + strings.ReplaceAll(regexp.QuoteMeta(pattern), `\*`, ".*") + "$")

	return re.MatchString(s)
}

// patternMatch implements match: # is a digit, ? a letter and . any character; other characters are literal.
func patternMatch(pattern, s string, insensitive bool) bool {
	var sb strings.Builder

	sb.WriteString("^")

	for _, r := range pattern {
		switch r {
		case '#':
			sb.WriteString(`\d`)
		case '?':
			sb.WriteString(`[A-Za-z]`)
		case '.':
			sb.WriteString(`.`)
		default:
			sb.WriteString(regexp.QuoteMeta(string(r)))
		}
	}

	sb.WriteString("$")

	if insensitive {
		return regexp.MustCompile("(?i)" + sb.String()).MatchString(s)
	}

	return regexp.MustCompile(sb.String()).MatchString(s)
}

func applyAppend(res *policyResource, then map[string]any) {
	for _, d := range anyList(then["details"]) {
		det := mapOf(d)

		tag, ok := tagFieldName(strOfAny(det["field"]))
		if !ok {
			continue
		}

		tags := ensureTags(res)
		if _, has := tags[tag]; !has {
			tags[tag] = det["value"]
		}
	}
}

func applyModify(res *policyResource, then map[string]any, env *policyEnv) {
	for _, o := range anyList(mapOf(then["details"])["operations"]) {
		op := mapOf(o)

		tag, ok := tagFieldName(strOfAny(op["field"]))
		if !ok {
			continue
		}

		val, err := evalPolicyValue(op["value"], env)
		if err != nil {
			continue
		}

		tags := ensureTags(res)

		switch strings.ToLower(strOfAny(op["operation"])) {
		case "addorreplace":
			tags[tag] = val
		case "add":
			if _, has := tags[tag]; !has {
				tags[tag] = val
			}
		case "remove":
			delete(tags, tag)
		}
	}
}

func ensureTags(res *policyResource) map[string]any {
	if res.body == nil {
		res.body = map[string]any{}
	}

	tags, ok := res.body[policyFieldTags].(map[string]any)
	if !ok {
		tags = map[string]any{}
		res.body[policyFieldTags] = tags
	}

	return tags
}
