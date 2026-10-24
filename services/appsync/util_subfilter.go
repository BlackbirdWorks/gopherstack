package appsync

import (
	"fmt"
	"slices"
)

func isSubFilterOp(op string) bool {
	switch op {
	case "eq", "ne", "le", "lt", "ge", "gt", "contains", "notContains", mBeginsWith, "in", "notIn", opBetween,
		"containsAny":
		return true
	}

	return false
}

type subFilter struct {
	Value     any    `json:"value"`
	FieldName string `json:"fieldName"`
	Operator  string `json:"operator"`
}

// toSubscriptionFilter converts a filter object into a subscription filter
// ({"filterGroup":[{"filters":[...]}]}): fields are AND-ed within a group,
// "or" members fan out into separate groups, "and" members merge in.
func toSubscriptionFilter(_ *utilEnv, a []any) (any, error) {
	in, ok := argAt(a, 0).(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: filter must be a map", ErrValidation)
	}

	ignored := map[string]bool{}
	for _, f := range listOf(argAt(a, 1)) {
		ignored[vrender(f)] = true
	}

	groups, err := filterGroups(in, ignored)
	if err != nil {
		return nil, err
	}

	out := make([]any, 0, len(groups))

	for _, g := range groups {
		fs := make([]any, len(g))
		for i, f := range g {
			fs[i] = map[string]any{"fieldName": f.FieldName, "operator": f.Operator, "value": f.Value}
		}

		out = append(out, map[string]any{"filters": fs})
	}

	return map[string]any{"filterGroup": out}, nil
}

// filterGroups expands a filter object into disjunctive normal form.
func filterGroups(in map[string]any, ignored map[string]bool) ([][]subFilter, error) {
	groups := [][]subFilter{{}}

	for _, k := range sortedKeys(in) {
		var (
			next [][]subFilter
			err  error
		)

		switch {
		case ignored[k]:
			continue
		case k == "or":
			next, err = orGroups(in[k], ignored)
		case k == "and":
			next, err = andGroups(in[k], ignored)
		default:
			next, err = fieldGroups(k, in[k])
		}

		if err != nil {
			return nil, err
		}

		groups = crossGroups(groups, next)
	}

	return groups, nil
}

func fieldGroups(field string, spec any) ([][]subFilter, error) {
	ops, ok := spec.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: filter for %q must be a map of operators", ErrValidation, field)
	}

	var fs []subFilter

	for _, op := range sortedKeys(ops) {
		if !isSubFilterOp(op) {
			return nil, fmt.Errorf("%w: unsupported filter operator %q", ErrValidation, op)
		}

		fs = append(fs, subFilter{FieldName: field, Operator: op, Value: ops[op]})
	}

	return [][]subFilter{fs}, nil
}

func orGroups(v any, ignored map[string]bool) ([][]subFilter, error) {
	var out [][]subFilter

	for _, item := range listOf(v) {
		m, _ := item.(map[string]any)

		gs, err := filterGroups(m, ignored)
		if err != nil {
			return nil, err
		}

		out = append(out, gs...)
	}

	if len(out) == 0 {
		return [][]subFilter{{}}, nil
	}

	return out, nil
}

func andGroups(v any, ignored map[string]bool) ([][]subFilter, error) {
	out := [][]subFilter{{}}

	for _, item := range listOf(v) {
		m, _ := item.(map[string]any)

		gs, err := filterGroups(m, ignored)
		if err != nil {
			return nil, err
		}

		out = crossGroups(out, gs)
	}

	return out, nil
}

func crossGroups(a, b [][]subFilter) [][]subFilter {
	out := make([][]subFilter, 0, len(a)*len(b))

	for _, x := range a {
		for _, y := range b {
			out = append(out, slices.Concat(x, y))
		}
	}

	return out
}
