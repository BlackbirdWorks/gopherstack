package appsync

import (
	"encoding/json"
	"fmt"
)

const (
	dslRange  = "range"
	filterAnd = "and"
)

// toQueryDSL implements $util.transform.toElasticsearchQueryDSL /
// toOpenSearchQueryDSL: a filter input object becomes a bool query string,
// fields and operators ANDed (AppSync "transformation helpers" reference).
func toQueryDSL(_ *utilEnv, a []any) (any, error) {
	in, ok := argAt(a, 0).(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: query DSL input must be a map", ErrValidation)
	}

	q, err := filterDSL(in)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(q)

	return string(b), err
}

func boolOf(kind string, clauses any) map[string]any {
	return map[string]any{"bool": map[string]any{kind: clauses}}
}

func filterDSL(in map[string]any) (map[string]any, error) {
	clauses := []any{}

	for _, field := range sortedKeys(in) {
		var (
			c   map[string]any
			err error
		)

		switch field {
		case filterAnd, "or":
			c, err = groupDSL(field, in[field])
		case "not":
			c, err = notDSL(in[field])
		default:
			c, err = fieldDSL(field, in[field])
		}

		if err != nil {
			return nil, err
		}

		clauses = append(clauses, c)
	}

	return boolOf("must", clauses), nil
}

func groupDSL(kind string, v any) (map[string]any, error) {
	subs := []any{}

	for _, item := range listOf(v) {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: %q expects a list of filter objects", ErrValidation, kind)
		}

		q, err := filterDSL(m)
		if err != nil {
			return nil, err
		}

		subs = append(subs, q)
	}

	if kind == filterAnd {
		return boolOf("must", subs), nil
	}

	return boolOf("should", subs), nil
}

func notDSL(v any) (map[string]any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: \"not\" expects a filter object", ErrValidation)
	}

	q, err := filterDSL(m)
	if err != nil {
		return nil, err
	}

	return boolOf("must_not", q), nil
}

func fieldDSL(field string, v any) (map[string]any, error) {
	ops, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: field %q expects an operator object", ErrValidation, field)
	}

	clauses := []any{}

	for _, op := range sortedKeys(ops) {
		c, err := operatorDSL(field, op, ops[op])
		if err != nil {
			return nil, err
		}

		clauses = append(clauses, c)
	}

	return boolOf("must", clauses), nil
}

func operatorDSL(field, op string, v any) (map[string]any, error) {
	leaf := func(kind string) map[string]any { return map[string]any{kind: map[string]any{field: v}} }

	switch op {
	case "eq":
		return leaf("term"), nil
	case "ne":
		return boolOf("must_not", leaf("term")), nil
	case "gt", "gte", "lt", "lte":
		return map[string]any{dslRange: map[string]any{field: map[string]any{op: v}}}, nil
	case dslRange:
		bounds := listOf(v)
		if len(bounds) != 2 { //nolint:mnd // range is [lower, upper]
			return nil, fmt.Errorf("%w: range on %q expects [lower, upper]", ErrValidation, field)
		}

		return map[string]any{dslRange: map[string]any{field: map[string]any{"gte": bounds[0], "lte": bounds[1]}}}, nil
	case "wildcard", "regexp", "match":
		return leaf(op), nil
	case "matchPhrase":
		return leaf("match_phrase"), nil
	case "matchPhrasePrefix":
		return leaf("match_phrase_prefix"), nil
	case "exists":
		exists := map[string]any{"exists": map[string]any{"field": field}}
		if b, isBool := v.(bool); isBool && !b {
			return boolOf("must_not", exists), nil
		}

		return exists, nil
	default:
		return nil, fmt.Errorf("%w: unsupported query DSL operator %q on %q", ErrValidation, op, field)
	}
}
