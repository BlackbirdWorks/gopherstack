package expr_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/expr"
)

func BenchmarkEvaluator_FilterItem(b *testing.B) {
	node, err := expr.NewParser(expr.NewLexer("#v BETWEEN :lo AND :hi AND #n <> :other")).ParseCondition()
	if err != nil {
		b.Fatal(err)
	}

	item := map[string]any{
		"pk":   map[string]any{"S": "u1"},
		"val":  map[string]any{"N": "1234"},
		"name": map[string]any{"S": "name 1234"},
	}
	names := map[string]string{"#v": "val", "#n": "name"}
	values := map[string]any{
		":lo":    map[string]any{"N": "1000"},
		":hi":    map[string]any{"N": "2000"},
		":other": map[string]any{"S": "x"},
	}

	b.ReportAllocs()

	for b.Loop() {
		eval := &expr.Evaluator{Item: item, AttrNames: names, AttrValues: values}
		if _, evalErr := eval.Evaluate(node); evalErr != nil {
			b.Fatal(evalErr)
		}
	}
}

func TestEvaluator_NumericStringComparison(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		left  string
		right string
		want  bool
	}{
		{name: "equal ints", left: "42", right: "42", want: true},
		{name: "leading zeros", left: "007", right: "7", want: true},
		{name: "negative zero", left: "-0", right: "0", want: true},
		{name: "negative", left: "-15", right: "-15", want: true},
		{name: "int vs decimal", left: "5", right: "5.0", want: true},
		{name: "exponent", left: "1e3", right: "1000", want: true},
		{name: "plus sign", left: "+5", right: "5", want: true},
		{name: "fifteen digits", left: "999999999999999", right: "999999999999999", want: true},
		{name: "sixteen digits differ", left: "9007199254740993", right: "9007199254740992", want: true},
		{name: "different", left: "41", right: "42", want: false},
	}

	node, err := expr.NewParser(expr.NewLexer("a = :v")).ParseCondition()
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			eval := &expr.Evaluator{
				Item:       map[string]any{"a": map[string]any{"N": tt.left}},
				AttrValues: map[string]any{":v": map[string]any{"N": tt.right}},
			}

			got, evalErr := eval.Evaluate(node)
			require.NoError(t, evalErr)
			assert.Equal(t, tt.want, got)
		})
	}
}
