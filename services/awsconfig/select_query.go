package awsconfig

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// select_query.go implements a minimal evaluator for the SQL-like query
// language accepted by SelectResourceConfig / SelectAggregateResourceConfig:
//
//	SELECT <field>[, <field>...] [WHERE <field> (= | LIKE) '<value>' [AND ...]]
//
// It is intentionally minimal (no OR, no parentheses, no functions) but
// evaluates real predicates against the account's discovered resource
// configuration instead of ignoring the query entirely.

var (
	selectWithWhereRe = regexp.MustCompile(`(?is)^\s*SELECT\s+(.+?)\s+WHERE\s+(.+?)\s*;?\s*$`)
	selectOnlyRe      = regexp.MustCompile(`(?is)^\s*SELECT\s+(.+?)\s*;?\s*$`)
	andSplitRe        = regexp.MustCompile(`(?i)\s+AND\s+`)
	predicateRe       = regexp.MustCompile(`(?is)^\s*([\w.]+)\s+(=|LIKE)\s+'([^']*)'\s*$`)
)

// selectPredicate is a single "field OP 'value'" clause from a WHERE query.
type selectPredicate struct {
	Field string
	Op    string
	Value string
}

// parsedSelectQuery is the parsed form of a SELECT ... [WHERE ...] expression.
type parsedSelectQuery struct {
	Fields     []string
	Predicates []selectPredicate
}

// parseSelectQuery parses a SELECT query into its field list and WHERE
// predicates. Unparseable input yields a query with no fields/predicates
// (evaluateSelectQuery then matches nothing), rather than panicking.
func parseSelectQuery(expr string) parsedSelectQuery {
	var fieldsPart, wherePart string

	switch {
	case selectWithWhereRe.MatchString(expr):
		m := selectWithWhereRe.FindStringSubmatch(expr)
		fieldsPart, wherePart = m[1], m[2]
	case selectOnlyRe.MatchString(expr):
		fieldsPart = selectOnlyRe.FindStringSubmatch(expr)[1]
	}

	return parsedSelectQuery{
		Fields:     splitFields(fieldsPart),
		Predicates: parsePredicates(wherePart),
	}
}

func splitFields(fieldsPart string) []string {
	var fields []string

	for f := range strings.SplitSeq(fieldsPart, ",") {
		if f = strings.TrimSpace(f); f != "" {
			fields = append(fields, f)
		}
	}

	return fields
}

func parsePredicates(wherePart string) []selectPredicate {
	if wherePart == "" {
		return nil
	}

	clauses := andSplitRe.Split(wherePart, -1)
	predicates := make([]selectPredicate, 0, len(clauses))

	for _, clause := range clauses {
		m := predicateRe.FindStringSubmatch(clause)
		if m == nil {
			continue
		}

		predicates = append(predicates, selectPredicate{Field: m[1], Op: strings.ToUpper(m[2]), Value: m[3]})
	}

	return predicates
}

// resolveSelectField returns the value of a named field for a resource: the
// well-known identity fields (resourceId/resourceType), or a key looked up in
// the resource's parsed Configuration JSON (an optional "configuration."
// prefix is stripped first).
func resolveSelectField(item *ResourceConfigItem, config map[string]any, field string) (any, bool) {
	switch strings.ToLower(field) {
	case "resourceid":
		return item.ResourceID, true
	case "resourcetype":
		return item.ResourceType, true
	}

	key := field
	if strings.HasPrefix(strings.ToLower(field), "configuration.") {
		key = field[len("configuration."):]
	}

	v, ok := config[key]

	return v, ok
}

func matchPredicate(item *ResourceConfigItem, config map[string]any, p selectPredicate) bool {
	v, ok := resolveSelectField(item, config, p.Field)
	if !ok {
		return false
	}

	s := fmt.Sprint(v)

	switch p.Op {
	case "=":
		return s == p.Value
	case "LIKE":
		return likeMatch(s, p.Value)
	default:
		return false
	}
}

// likeMatch implements SQL LIKE semantics for the '%' wildcard only (no '_'
// single-char wildcard support, which AWS Config queries rarely rely on).
func likeMatch(s, pattern string) bool {
	parts := strings.Split(pattern, "%")
	for i, p := range parts {
		parts[i] = regexp.QuoteMeta(p)
	}

	re := "(?s)^" + strings.Join(parts, ".*") + "$"

	matched, err := regexp.MatchString(re, s)

	return err == nil && matched
}

// evaluateSelectQuery runs a parsed SELECT/WHERE query against a set of
// resource configuration items, returning one JSON-encoded object per
// matching resource containing just the selected fields (mirroring the real
// SelectResourceConfig wire shape: Results is a []string of JSON rows).
// Items are evaluated in a deterministic (resourceType, resourceId) order.
func evaluateSelectQuery(items []*ResourceConfigItem, expression string) []string {
	q := parseSelectQuery(expression)

	sorted := make([]*ResourceConfigItem, len(items))
	copy(sorted, items)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].ResourceType != sorted[j].ResourceType {
			return sorted[i].ResourceType < sorted[j].ResourceType
		}

		return sorted[i].ResourceID < sorted[j].ResourceID
	})

	out := make([]string, 0, len(sorted))

	for _, item := range sorted {
		var config map[string]any

		_ = json.Unmarshal([]byte(item.Configuration), &config)

		if config == nil {
			config = map[string]any{}
		}

		if !matchesAll(item, config, q.Predicates) {
			continue
		}

		if row, ok := marshalSelectedFields(item, config, q.Fields); ok {
			out = append(out, row)
		}
	}

	return out
}

func matchesAll(item *ResourceConfigItem, config map[string]any, predicates []selectPredicate) bool {
	for _, p := range predicates {
		if !matchPredicate(item, config, p) {
			return false
		}
	}

	return true
}

func marshalSelectedFields(item *ResourceConfigItem, config map[string]any, fields []string) (string, bool) {
	row := make(map[string]any, len(fields))

	for _, f := range fields {
		if v, ok := resolveSelectField(item, config, f); ok {
			row[f] = v
		}
	}

	encoded, err := json.Marshal(row)
	if err != nil {
		return "", false
	}

	return string(encoded), true
}
