package azureappconfig

import (
	"fmt"
	"strings"
)

const escapePair = 2

type matchKind int

const (
	matchAll matchKind = iota
	matchNull
	matchExact
	matchPrefix
	matchSuffix
	matchContains
)

type pattern struct {
	text string
	kind matchKind
}

// filter is a parsed key or label filter; the empty filter matches everything.
type filter []pattern

type tagFilter struct {
	name, value string
	absent      bool
}

func splitUnescaped(raw string) []string {
	var (
		out []string
		cur strings.Builder
	)

	for i := 0; i < len(raw); i++ {
		switch c := raw[i]; {
		case c == '\\' && i+1 < len(raw):
			cur.WriteByte(c)
			i++
			cur.WriteByte(raw[i])
		case c == ',':
			out = append(out, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}

	return append(out, cur.String())
}

func unescape(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}

	var b strings.Builder

	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
		}

		b.WriteByte(s[i])
	}

	return b.String()
}

func endsUnescapedStar(s string) bool {
	if !strings.HasSuffix(s, "*") {
		return false
	}

	slashes := 0

	for i := len(s) - escapePair; i >= 0 && s[i] == '\\'; i-- {
		slashes++
	}

	return slashes%escapePair == 0
}

func parseToken(tok string) pattern {
	switch tok {
	case "*":
		return pattern{kind: matchAll}
	case `\0`, "\x00":
		return pattern{kind: matchNull}
	}

	lead := strings.HasPrefix(tok, "*")
	if lead {
		tok = tok[1:]
	}

	trail := endsUnescapedStar(tok)
	if trail {
		tok = tok[:len(tok)-1]
	}

	kind := matchExact

	switch {
	case lead && trail:
		kind = matchContains
	case trail:
		kind = matchPrefix
	case lead:
		kind = matchSuffix
	}

	return pattern{text: unescape(tok), kind: kind}
}

func parseFilter(raw string) (filter, error) {
	if raw == "" {
		return nil, nil
	}

	toks := splitUnescaped(raw)
	if len(toks) > maxFilterItems {
		return nil, fmt.Errorf("%w: at most %d filter values are allowed", ErrInvalidArgument, maxFilterItems)
	}

	out := make(filter, 0, len(toks))
	for _, t := range toks {
		out = append(out, parseToken(t))
	}

	return out, nil
}

func (p pattern) matches(s string) bool {
	switch p.kind {
	case matchAll:
		return true
	case matchNull:
		return s == ""
	case matchExact:
		return s == p.text
	case matchPrefix:
		return strings.HasPrefix(s, p.text)
	case matchSuffix:
		return strings.HasSuffix(s, p.text)
	case matchContains:
		return strings.Contains(s, p.text)
	}

	return false
}

func (f filter) matches(s string) bool {
	if len(f) == 0 {
		return true
	}

	for _, p := range f {
		if p.matches(s) {
			return true
		}
	}

	return false
}

func parseTagFilters(raw []string) ([]tagFilter, error) {
	if len(raw) > maxTagFilters {
		return nil, fmt.Errorf("%w: at most %d tag filters are allowed", ErrInvalidArgument, maxTagFilters)
	}

	out := make([]tagFilter, 0, len(raw))

	for _, r := range raw {
		name, value, ok := strings.Cut(r, "=")
		if !ok || name == "" {
			return nil, fmt.Errorf("%w: tag filter %q must be name=value", ErrInvalidArgument, r)
		}

		tf := tagFilter{name: unescape(name), value: unescape(value)}
		if value == `\0` || value == "\x00" {
			tf = tagFilter{name: tf.name, absent: true}
		}

		out = append(out, tf)
	}

	return out, nil
}

func tagsMatch(tags map[string]string, filters []tagFilter) bool {
	for _, f := range filters {
		v, ok := tags[f.name]
		if f.absent {
			if ok {
				return false
			}

			continue
		}

		if !ok || v != f.value {
			return false
		}
	}

	return true
}

// Selector is the parsed key/label/tags filter of a list request.
type Selector struct {
	key   filter
	label filter
	tags  []tagFilter
}

func NewSelector(key, label string, tags []string) (Selector, error) {
	k, err := parseFilter(key)
	if err != nil {
		return Selector{}, err
	}

	l, err := parseFilter(label)
	if err != nil {
		return Selector{}, err
	}

	t, err := parseTagFilters(tags)
	if err != nil {
		return Selector{}, err
	}

	return Selector{key: k, label: l, tags: t}, nil
}

func (s Selector) matches(kv KeyValue) bool {
	return s.key.matches(kv.Key) && s.label.matches(kv.Label) && tagsMatch(kv.Tags, s.tags)
}
