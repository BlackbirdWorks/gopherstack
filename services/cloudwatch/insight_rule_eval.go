package cloudwatch

import (
	"bytes"
	"encoding/json"
	"math"
	"slices"
	"strconv"
	"strings"
)

// fieldLookup returns the leaf value at a rule field path in one log event.
type fieldLookup func(path string) (string, bool)

// lookupFor builds a field reader for msg, or false when msg is not in the rule's LogFormat.
func (s *insightRuleSpec) lookupFor(msg string) (fieldLookup, bool) {
	if s.LogFormat == insightFormatCLF {
		tokens := splitCLF(msg)

		return func(path string) (string, bool) {
			n, ok := s.clfIndex(path)
			if !ok || n > len(tokens) {
				return "", false
			}

			return tokens[n-1], true
		}, true
	}

	dec := json.NewDecoder(bytes.NewReader([]byte(msg)))
	dec.UseNumber()

	var root map[string]any
	if err := dec.Decode(&root); err != nil || root == nil {
		return nil, false
	}

	return func(path string) (string, bool) { return jsonLeaf(root, path) }, true
}

func jsonLeaf(root any, path string) (string, bool) {
	cur := root

	for seg := range strings.SplitSeq(strings.TrimPrefix(path, "$."), ".") {
		name, idxs, ok := splitSegment(seg)
		if !ok {
			return "", false
		}

		obj, isObj := cur.(map[string]any)
		if !isObj {
			return "", false
		}

		if cur, ok = obj[name]; !ok {
			return "", false
		}

		for _, i := range idxs {
			arr, isArr := cur.([]any)
			if !isArr || i >= len(arr) {
				return "", false
			}

			cur = arr[i]
		}
	}

	switch v := cur.(type) {
	case string:
		return v, true
	case json.Number:
		return v.String(), true
	case bool:
		return strconv.FormatBool(v), true
	default:
		return "", false
	}
}

func splitSegment(seg string) (string, []int, bool) {
	name, rest, _ := strings.Cut(seg, "[")
	if rest == "" {
		return name, nil, name != ""
	}

	var idxs []int

	for part := range strings.SplitSeq("["+rest, "[") {
		if part == "" {
			continue
		}

		n, err := strconv.Atoi(strings.TrimSuffix(part, "]"))
		if err != nil || n < 0 {
			return "", nil, false
		}

		idxs = append(idxs, n)
	}

	return name, idxs, name != ""
}

// splitCLF splits a Common Log Format line on spaces, keeping "..." and [...] groups whole.
func splitCLF(msg string) []string {
	var (
		tokens []string
		cur    strings.Builder
		closer byte
	)

	flush := func() {
		if cur.Len() > 0 {
			tokens = append(tokens, trimQuotes(cur.String()))
			cur.Reset()
		}
	}

	for i := range len(msg) {
		c := msg[i]

		switch {
		case closer != 0:
			cur.WriteByte(c)

			if c == closer {
				closer = 0
			}
		case c == ' ':
			flush()
		case c == '"' || c == '[':
			closer = '"'
			if c == '[' {
				closer = ']'
			}

			cur.WriteByte(c)
		default:
			cur.WriteByte(c)
		}
	}

	flush()

	return tokens
}

// observe evaluates one event: its contributor key values and aggregation value.
func (s *insightRuleSpec) observe(msg string) ([]string, float64, bool) {
	get, ok := s.lookupFor(msg)
	if !ok {
		return nil, 0, false
	}

	for _, f := range s.Contribution.Filters {
		if !f.matches(get) {
			return nil, 0, false
		}
	}

	keys := make([]string, 0, len(s.Contribution.Keys))

	for _, k := range s.Contribution.Keys {
		v, present := get(k)
		if !present {
			return nil, 0, false
		}

		keys = append(keys, v)
	}

	if !s.aggregatesSum() {
		return keys, 1, true
	}

	raw, present := get(s.Contribution.ValueOf)
	if !present {
		return nil, 0, false
	}

	val, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(val) || math.IsInf(val, 0) {
		return nil, 0, false
	}

	return keys, val, true
}

func (f insightFilter) matches(get fieldLookup) bool {
	v, present := get(f.Match)

	if f.IsPresent != nil {
		return present == *f.IsPresent
	}

	if !present {
		return false
	}

	switch {
	case f.In != nil:
		return slices.Contains(f.In, v)
	case f.NotIn != nil:
		return !slices.Contains(f.NotIn, v)
	case f.StartsWith != nil:
		return anyPrefix(f.StartsWith, v)
	case f.NotStartsWith != nil:
		return !anyPrefix(f.NotStartsWith, v)
	}

	return f.numericMatches(v)
}

func (f insightFilter) numericMatches(v string) bool {
	n, err := strconv.ParseFloat(v, 64)
	if err != nil || math.IsNaN(n) {
		return false
	}

	switch {
	case f.GreaterThan != nil:
		return n > *f.GreaterThan
	case f.LessThan != nil:
		return n < *f.LessThan
	case f.EqualTo != nil:
		return n == *f.EqualTo
	case f.NotEqualTo != nil:
		return n != *f.NotEqualTo
	}

	return false
}

func anyPrefix(list []string, v string) bool {
	return slices.ContainsFunc(list, func(p string) bool { return strings.HasPrefix(v, p) })
}
