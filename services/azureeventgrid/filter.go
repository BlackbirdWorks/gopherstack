package azureeventgrid

import (
	"encoding/json"
	"strings"
)

var attrAliases = map[string]string{ //nolint:gochecknoglobals // static alias table
	"eventtype": attrType, attrType: "eventtype", "eventtime": "time", "time": "eventtime",
}

func foldKey(m map[string]any, key string) (any, bool) {
	if v, ok := m[key]; ok {
		return v, true
	}

	lk := strings.ToLower(key)
	for k, v := range m {
		if strings.ToLower(k) == lk {
			return v, true
		}
	}

	if alias, ok := attrAliases[lk]; ok {
		return foldKey(m, alias)
	}

	return nil, false
}

// lookup resolves a filter key such as "Subject", "data.a.b" or "data/a/b" in an event.
func lookup(ev map[string]any, key string) (any, bool) {
	parts := strings.FieldsFunc(key, func(r rune) bool { return r == '.' || r == '/' })
	if len(parts) == 0 {
		return nil, false
	}

	var cur any = ev

	for i, p := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}

		var v any
		if i == 0 {
			v, ok = foldKey(m, p)
		} else {
			v, ok = m[p]
		}

		if !ok {
			return nil, false
		}

		cur = v
	}

	return cur, true
}

func strField(ev map[string]any, key string) string {
	v, _ := lookup(ev, key)
	s, _ := v.(string)

	return s
}

// Matches reports whether the event passes the basic and advanced filters.
func (f Filter) Matches(ev map[string]any) bool {
	return f.matchesType(ev) && f.matchesSubject(strField(ev, "subject")) && f.matchesAdvanced(ev)
}

func (f Filter) matchesType(ev map[string]any) bool {
	if len(f.IncludedEventTypes) == 0 {
		return true
	}

	typ := strField(ev, "eventType")
	for _, t := range f.IncludedEventTypes {
		if t == "all" || strings.EqualFold(t, typ) {
			return true
		}
	}

	return false
}

func (f Filter) matchesSubject(subject string) bool {
	prefix, suffix := f.SubjectBeginsWith, f.SubjectEndsWith
	if !f.IsSubjectCaseSensitive {
		subject, prefix, suffix = strings.ToLower(subject), strings.ToLower(prefix), strings.ToLower(suffix)
	}

	return strings.HasPrefix(subject, prefix) && strings.HasSuffix(subject, suffix)
}

func (f Filter) matchesAdvanced(ev map[string]any) bool {
	for _, af := range f.AdvancedFilters {
		if !af.matches(ev, f.EnableAdvancedFilteringOnArrays) {
			return false
		}
	}

	return true
}

func (a AdvancedFilter) negative() bool {
	switch a.OperatorType {
	case "NumberNotIn", "NumberNotInRange", opStringNotIn, opStringNotBegins, opStringNotEnds,
		opStringNotContain, "IsNullOrUndefined":
		return true
	}

	return false
}

func (a AdvancedFilter) matches(ev map[string]any, arrays bool) bool {
	v, ok := lookup(ev, a.Key)
	if a.OperatorType == "IsNullOrUndefined" {
		return !ok || v == nil
	}

	if a.OperatorType == "IsNotNull" {
		return ok && v != nil
	}

	if !ok {
		return a.negative()
	}

	if arr, isArr := v.([]any); isArr {
		return arrays && a.matchesArray(arr)
	}

	return a.matchScalar(v)
}

func (a AdvancedFilter) matchesArray(arr []any) bool {
	if len(arr) == 0 {
		return a.negative()
	}

	for _, el := range arr {
		got := a.matchScalar(el)
		if a.negative() && !got {
			return false
		}

		if !a.negative() && got {
			return true
		}
	}

	return a.negative()
}

func (a AdvancedFilter) matchScalar(v any) bool {
	switch x := v.(type) {
	case string:
		return a.matchString(x)
	case float64:
		return a.matchNumber(x)
	case json.Number:
		f, err := x.Float64()

		return err == nil && a.matchNumber(f)
	case bool:
		b, ok := a.Value.(bool)

		return a.OperatorType == "BoolEquals" && ok && b == x
	}

	return false
}

func (a AdvancedFilter) strValues() []string {
	out := make([]string, 0, len(a.Values))

	for _, v := range a.Values {
		if s, ok := v.(string); ok {
			out = append(out, strings.ToLower(s))
		}
	}

	return out
}

func (a AdvancedFilter) matchString(s string) bool {
	s = strings.ToLower(s)
	in := false

	for _, want := range a.strValues() {
		if stringOp(a.OperatorType, s, want) {
			in = true

			break
		}
	}

	switch a.OperatorType {
	case "StringIn", "StringBeginsWith", "StringEndsWith", "StringContains":
		return in
	case opStringNotIn, opStringNotBegins, opStringNotEnds, opStringNotContain:
		return !in
	}

	return false
}

func stringOp(op, s, want string) bool {
	switch op {
	case "StringIn", opStringNotIn:
		return s == want
	case "StringBeginsWith", opStringNotBegins:
		return strings.HasPrefix(s, want)
	case "StringEndsWith", opStringNotEnds:
		return strings.HasSuffix(s, want)
	case "StringContains", opStringNotContain:
		return strings.Contains(s, want)
	}

	return false
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()

		return f, err == nil
	}

	return 0, false
}

func (a AdvancedFilter) numValues() []float64 {
	out := make([]float64, 0, len(a.Values))

	for _, v := range a.Values {
		if f, ok := toFloat(v); ok {
			out = append(out, f)
		}
	}

	return out
}

func (a AdvancedFilter) inRange(n float64) bool {
	for _, v := range a.Values {
		pair, ok := v.([]any)
		if !ok || len(pair) != 2 {
			continue
		}

		lo, ok1 := toFloat(pair[0])
		hi, ok2 := toFloat(pair[1])

		if ok1 && ok2 && n >= lo && n <= hi {
			return true
		}
	}

	return false
}

func (a AdvancedFilter) matchNumber(n float64) bool {
	one, hasOne := toFloat(a.Value)

	switch a.OperatorType {
	case "NumberIn", "NumberNotIn":
		in := false

		for _, w := range a.numValues() {
			in = in || w == n
		}

		return in == (a.OperatorType == "NumberIn")
	case "NumberInRange":
		return a.inRange(n)
	case "NumberNotInRange":
		return !a.inRange(n)
	case "NumberLessThan":
		return hasOne && n < one
	case "NumberLessThanOrEquals":
		return hasOne && n <= one
	case "NumberGreaterThan":
		return hasOne && n > one
	case "NumberGreaterThanOrEquals":
		return hasOne && n >= one
	}

	return false
}
