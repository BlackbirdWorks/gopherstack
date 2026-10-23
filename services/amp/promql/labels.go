// Package promql implements a PromQL subset: selectors, range functions, aggregations and binary operators.
package promql

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
)

// NameLabel is the label that carries the metric name.
const NameLabel = "__name__"

const (
	sepKV    = 0xff
	sepEntry = 0xfe
)

// Labels is a label set.
type Labels map[string]string

// Sample is one timestamped value; T is Unix milliseconds.
type Sample struct {
	T int64   `json:"t"`
	V float64 `json:"v"`
}

// Series is a labelled run of samples ordered by time.
type Series struct {
	Labels  Labels
	Samples []Sample
}

// MatchType selects how a Matcher compares a label value.
type MatchType int

// Matcher comparison kinds.
const (
	MatchEqual MatchType = iota
	MatchNotEqual
	MatchRegexp
	MatchNotRegexp
)

// Matcher filters series by one label.
type Matcher struct {
	re    *regexp.Regexp
	Name  string
	Value string
	Type  MatchType
}

// NewMatcher builds a matcher; regular expressions are fully anchored like Prometheus.
func NewMatcher(t MatchType, name, value string) (*Matcher, error) {
	m := &Matcher{Type: t, Name: name, Value: value}

	if t == MatchRegexp || t == MatchNotRegexp {
		re, err := regexp.Compile("^(?:" + value + ")$")
		if err != nil {
			return nil, fmt.Errorf("invalid regular expression %q: %w", value, err)
		}

		m.re = re
	}

	return m, nil
}

// Matches reports whether v satisfies the matcher; an absent label is the empty string.
func (m *Matcher) Matches(v string) bool {
	switch m.Type {
	case MatchEqual:
		return v == m.Value
	case MatchNotEqual:
		return v != m.Value
	case MatchRegexp:
		return m.re.MatchString(v)
	case MatchNotRegexp:
		return !m.re.MatchString(v)
	}

	return false
}

// MatchesAll reports whether lbls satisfies every matcher.
func MatchesAll(lbls Labels, ms []*Matcher) bool {
	for _, m := range ms {
		if !m.Matches(lbls[m.Name]) {
			return false
		}
	}

	return true
}

// Storage supplies series to the engine.
type Storage interface {
	// Select returns series matching ms with at least the samples in [minT, maxT] (Unix ms).
	Select(ms []*Matcher, minT, maxT int64) []Series
}

// Fingerprint is a canonical string for a label set.
func Fingerprint(l Labels) string {
	keys := sortedKeys(l)

	var sb strings.Builder

	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteByte(sepKV)
		sb.WriteString(l[k])
		sb.WriteByte(sepEntry)
	}

	return sb.String()
}

func sortedKeys(l Labels) []string {
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	return keys
}

// Clone returns a copy of l.
func (l Labels) Clone() Labels { return l.clone() }

func (l Labels) clone() Labels {
	out := make(Labels, len(l))
	maps.Copy(out, l)

	return out
}

func (l Labels) without(names ...string) Labels {
	out := l.clone()
	for _, n := range names {
		delete(out, n)
	}

	return out
}

// pick keeps only names (on semantics) or drops them (ignoring/without semantics).
func (l Labels) pick(names []string, keep bool) Labels {
	if !keep {
		return l.without(names...)
	}

	out := make(Labels, len(names))

	for _, n := range names {
		if v, ok := l[n]; ok {
			out[n] = v
		}
	}

	return out
}
