package expr_test

import (
	"Gopherstack/dynamodb/expr"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConditionParsingAndEvaluation(t *testing.T) {
	t.Parallel()

	item := map[string]any{
		"pk":     map[string]any{"S": "user123"},
		"age":    map[string]any{"N": "25"},
		"active": map[string]any{"BOOL": true},
		"tags":   map[string]any{"SS": []string{"gold", "premium"}},
		"info": map[string]any{
			"M": map[string]any{
				"city": map[string]any{"S": "New York"},
			},
		},
	}

	attrValues := map[string]any{
		":minAge": map[string]any{"N": "18"},
		":prefix": map[string]any{"S": "user"},
		":city":   map[string]any{"S": "New York"},
		":tag":    map[string]any{"S": "gold"},
	}

	tests := []struct {
		name      string
		exprStr   string
		wantMatch bool
	}{
		{
			name:      "Simple Equality",
			exprStr:   "pk = :prefix",
			wantMatch: false,
		},
		{
			name:      "Begins With",
			exprStr:   "begins_with(pk, :prefix)",
			wantMatch: true,
		},
		{
			name:      "Numeric Comparison",
			exprStr:   "age >= :minAge",
			wantMatch: true,
		},
		{
			name:      "Logical AND",
			exprStr:   "age >= :minAge AND active = :true", // Note: active is mapped to BOOL, but evaluator needs to handle it.
			wantMatch: true,
		},
		{
			name:      "Logical AND with values",
			exprStr:   "age >= :minAge AND active = active", // active is a path to a BOOL
			wantMatch: true,
		},
		{
			name:      "Nested Path",
			exprStr:   "info.city = :city",
			wantMatch: true,
		},
		{
			name:      "Function contains",
			exprStr:   "contains(tags, :tag)",
			wantMatch: true,
		},
		{
			name:      "Size Function",
			exprStr:   "size(tags) > :one",
			wantMatch: true,
		},
	}

	// Add more values for specific tests
	attrValues[":true"] = map[string]any{"BOOL": true}
	attrValues[":one"] = map[string]any{"N": "1"}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := expr.NewLexer(tc.exprStr)
			p := expr.NewParser(l)
			node, err := p.ParseCondition()
			require.NoError(t, err, "Parse error for: %s", tc.exprStr)

			eval := &expr.Evaluator{
				Item:       item,
				AttrValues: attrValues,
			}
			result, err := eval.Evaluate(node)
			require.NoError(t, err, "Eval error for: %s", tc.exprStr)
			assert.Equal(t, tc.wantMatch, result, "Match mismatch for: %s", tc.exprStr)
		})
	}
}

func TestOperatorPrecedence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		exprStr string
		want    bool
	}{
		{"a = :one OR b = :two AND c = :four", true},    // (a=1) OR (b=2 AND c=4) -> true OR false -> true
		{"(a = :one OR b = :two) AND c = :four", false}, // (1=1 OR 2=2) AND 3=4 -> true AND false -> false
	}

	for _, tc := range tests {
		t.Run(tc.exprStr, func(t *testing.T) {
			t.Parallel()
			item := map[string]any{"a": map[string]any{"N": "1"}, "b": map[string]any{"N": "2"}, "c": map[string]any{"N": "3"}}
			eval := &expr.Evaluator{Item: item}
			l := expr.NewLexer(tc.exprStr)
			p := expr.NewParser(l)
			node, err := p.ParseCondition()
			require.NoError(t, err)
			vals := map[string]any{":one": map[string]any{"N": "1"}, ":two": map[string]any{"N": "2"}, ":four": map[string]any{"N": "4"}}
			eval.AttrValues = vals
			result, err := eval.Evaluate(node)
			require.NoError(t, err)
			assert.Equal(t, tc.want, result)
		})
	}
}

func TestUpdateParsing(t *testing.T) {
	t.Parallel()
	exprStr := "SET #a = :v1, #b = :v2 REMOVE #c"
	l := expr.NewLexer(exprStr)
	p := expr.NewParser(l)
	u, err := p.ParseUpdate()
	require.NoError(t, err)

	assert.Equal(t, 2, len(u.Actions))
	assert.Equal(t, expr.TokenSET, u.Actions[0].Type)
	assert.Equal(t, 2, len(u.Actions[0].Items))
	assert.Equal(t, expr.TokenREMOVE, u.Actions[1].Type)
	assert.Equal(t, 1, len(u.Actions[1].Items))
}
