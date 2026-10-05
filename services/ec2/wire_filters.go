package ec2

import (
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
)

// wireFilter is one Filter.N entry resolved against a response item's wire fields.
type wireFilter struct {
	name     string
	tagKey   string
	segs     []string
	patterns []string
	kind     wireFilterKind
}

type wireFilterKind int

const (
	wireTagKeyPath   = "tagSet.key"
	wireTagValuePath = "tagSet.value"
	escapedRuneWidth = 2
)

const (
	wireFilterPath wireFilterKind = iota
	wireFilterTagKey
	wireFilterTagValue
	wireFilterTag
)

// parseWireFilters reads Filter.N and Filters.N, the two flat keys the SDK uses.
func parseWireFilters(vals url.Values) []wireFilter {
	out := make([]wireFilter, 0, len(vals))

	for name, patterns := range parseEC2Filters(vals) {
		out = append(out, newWireFilter(name, patterns))
	}

	for name, patterns := range parseEC2FilterListKeyed(vals, "Filters") {
		out = append(out, newWireFilter(name, patterns))
	}

	return out
}

func newWireFilter(name string, patterns []string) wireFilter {
	f := wireFilter{name: name, patterns: patterns}

	switch {
	case name == filterKeyTagKey:
		f.kind = wireFilterTagKey
	case name == filterKeyTagValue:
		f.kind = wireFilterTagValue
	case strings.HasPrefix(name, "tag:"):
		f.kind, f.tagKey = wireFilterTag, strings.TrimPrefix(name, "tag:")
	default:
		f.segs = strings.Split(name, ".")
	}

	return f
}

// filterWireItems keeps items matching every filter (AND) with any-value (OR) semantics.
func filterWireItems(items reflect.Value, filters []wireFilter) (reflect.Value, error) {
	elem := items.Type().Elem()

	for _, f := range filters {
		if f.kind == wireFilterPath && !wirePathResolves(elem, f.segs) {
			return items, fmt.Errorf("%w: The filter '%s' is invalid", ErrInvalidParameter, f.name)
		}
	}

	out := reflect.MakeSlice(items.Type(), 0, items.Len())

	for i := range items.Len() {
		if itemMatchesFilters(items.Index(i), filters) {
			out = reflect.Append(out, items.Index(i))
		}
	}

	return out, nil
}

func itemMatchesFilters(item reflect.Value, filters []wireFilter) bool {
	for _, f := range filters {
		if !itemMatchesFilter(item, f) {
			return false
		}
	}

	return true
}

func itemMatchesFilter(item reflect.Value, f wireFilter) bool {
	if f.kind != wireFilterPath {
		return tagFilterMatches(item, f)
	}

	var got []string

	collectWireValues(item, f.segs, &got)

	for _, g := range got {
		if anyEqual(g, f.patterns) {
			return true
		}
	}

	return false
}

func tagFilterMatches(item reflect.Value, f wireFilter) bool {
	var keys, vals []string

	collectWireValues(item, strings.Split(wireTagKeyPath, "."), &keys)
	collectWireValues(item, strings.Split(wireTagValuePath, "."), &vals)

	for i, k := range keys {
		switch f.kind {
		case wireFilterTagKey:
			if anyEqual(k, f.patterns) {
				return true
			}
		case wireFilterTagValue:
			if i < len(vals) && anyEqual(vals[i], f.patterns) {
				return true
			}
		case wireFilterTag:
			if k == f.tagKey && i < len(vals) && anyEqual(vals[i], f.patterns) {
				return true
			}
		case wireFilterPath:
		}
	}

	return false
}

// wireFieldByName finds the struct field whose xml element equals seg ignoring case and hyphens.
func wireFieldByName(t reflect.Type, seg string) (int, bool) {
	want := strings.ReplaceAll(seg, "-", "")

	for i := range t.NumField() {
		name, _, _ := strings.Cut(t.Field(i).Tag.Get("xml"), ",")
		name, _, _ = strings.Cut(name, ">")

		if name != "" && strings.EqualFold(name, want) {
			return i, true
		}
	}

	return 0, false
}

func wirePathResolves(t reflect.Type, segs []string) bool {
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice {
		t = t.Elem()
	}

	if len(segs) == 0 {
		return true
	}

	if t.Kind() != reflect.Struct {
		return false
	}

	if i, ok := wireFieldByName(t, segs[0]); ok {
		return wirePathResolves(t.Field(i).Type, segs[1:])
	}

	if f, ok := t.FieldByName("Items"); ok {
		return wirePathResolves(f.Type, segs)
	}

	return false
}

func collectWireValues(v reflect.Value, segs []string, out *[]string) {
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return
		}

		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Slice:
		for i := range v.Len() {
			collectWireValues(v.Index(i), segs, out)
		}
	case reflect.Struct:
		collectWireStruct(v, segs, out)
	case reflect.String:
		*out = append(*out, v.String())
	case reflect.Bool:
		*out = append(*out, strconv.FormatBool(v.Bool()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		*out = append(*out, strconv.FormatInt(v.Int(), 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		*out = append(*out, strconv.FormatUint(v.Uint(), 10))
	case reflect.Float32, reflect.Float64:
		*out = append(*out, strconv.FormatFloat(v.Float(), 'f', -1, 64))
	default:
	}
}

func collectWireStruct(v reflect.Value, segs []string, out *[]string) {
	if len(segs) == 0 {
		return
	}

	if i, ok := wireFieldByName(v.Type(), segs[0]); ok {
		collectWireValues(v.Field(i), segs[1:], out)

		return
	}

	if items := v.FieldByName("Items"); items.IsValid() {
		collectWireValues(items, segs, out)
	}
}

// wildcardMatch reports whether s matches pattern, where "*" matches any run,
// "?" one character and "\" escapes the next; matching is case-sensitive.
func wildcardMatch(pattern, s string) bool {
	if !strings.ContainsAny(pattern, `*?\`) {
		return pattern == s
	}

	p, t := []rune(pattern), []rune(s)
	pi, ti, star, mark := 0, 0, -1, 0

	for ti < len(t) {
		switch {
		case pi < len(p) && p[pi] == '*':
			star, mark = pi, ti
			pi++
		case pi < len(p) && p[pi] == '?':
			pi++
			ti++
		case pi < len(p) && literalAt(p, pi) == t[ti]:
			pi += literalWidth(p, pi)
			ti++
		case star >= 0:
			mark++
			pi, ti = star+1, mark
		default:
			return false
		}
	}

	for pi < len(p) && p[pi] == '*' {
		pi++
	}

	return pi == len(p)
}

func literalWidth(p []rune, i int) int {
	if p[i] == '\\' && i+1 < len(p) {
		return escapedRuneWidth
	}

	return 1
}

func literalAt(p []rune, i int) rune {
	if p[i] == '\\' && i+1 < len(p) {
		return p[i+1]
	}

	return p[i]
}
