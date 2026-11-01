package expr_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/expr"
)

func TestNumberPrecision_Conditions(t *testing.T) {
	t.Parallel()

	const (
		low  = "12345678901234567890123456789012345677"
		mid  = "12345678901234567890123456789012345678"
		high = "12345678901234567890123456789012345679"
	)

	tests := []struct {
		name string
		expr string
		item string
		a, b string
		want bool
	}{
		{name: "eq differs last digit", expr: "n = :a", item: mid, a: high, want: false},
		{name: "eq same", expr: "n = :a", item: mid, a: mid, want: true},
		{name: "ne", expr: "n <> :a", item: mid, a: high, want: true},
		{name: "lt", expr: "n < :a", item: mid, a: high, want: true},
		{name: "gt", expr: "n > :a", item: mid, a: low, want: true},
		{name: "le equal", expr: "n <= :a", item: mid, a: mid, want: true},
		{name: "ge lower", expr: "n >= :a", item: mid, a: high, want: false},
		{name: "between in", expr: "n BETWEEN :a AND :b", item: mid, a: mid, b: high, want: true},
		{name: "between out", expr: "n BETWEEN :a AND :b", item: low, a: mid, b: high, want: false},
		{name: "in miss", expr: "n IN (:a, :b)", item: mid, a: low, b: high, want: false},
		{name: "in hit", expr: "n IN (:a, :b)", item: mid, a: low, b: mid, want: true},
		{name: "exponent", expr: "n = :a", item: "1E+3", a: "1000", want: true},
		{name: "trailing zero", expr: "n = :a", item: "1.0", a: "1", want: true},
		{name: "negative order", expr: "n < :a", item: "-" + high, a: "-" + mid, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			node, err := expr.NewParser(expr.NewLexer(tt.expr)).ParseCondition()
			require.NoError(t, err)

			values := map[string]any{
				":a": map[string]any{"N": tt.a},
				":b": map[string]any{"N": tt.b},
			}
			item := map[string]any{"n": map[string]any{"N": tt.item}}

			got, err := (&expr.Evaluator{Item: item, AttrValues: values}).Evaluate(node)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)

			compiled, err := expr.CompileCondition(node, nil, values).Evaluate(item)
			require.NoError(t, err)
			assert.Equal(t, tt.want, compiled)
		})
	}
}
