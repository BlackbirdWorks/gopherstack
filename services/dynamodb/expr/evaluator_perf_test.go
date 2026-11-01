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

	b.Run("fresh", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			eval := &expr.Evaluator{Item: item, AttrNames: names, AttrValues: values}
			if _, evalErr := eval.Evaluate(node); evalErr != nil {
				b.Fatal(evalErr)
			}
		}
	})

	b.Run("compiled", func(b *testing.B) {
		compiled := expr.CompileCondition(node, names, values)
		b.ReportAllocs()

		for b.Loop() {
			if _, evalErr := compiled.Evaluate(item); evalErr != nil {
				b.Fatal(evalErr)
			}
		}
	})
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
		{name: "sixteen digits differ", left: "9007199254740993", right: "9007199254740992", want: false},
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

func TestCompiledCondition_MatchesEvaluate(t *testing.T) {
	t.Parallel()

	items := []map[string]any{
		{"val": map[string]any{"N": "5"}, "name": map[string]any{"S": "a"}},
		{"val": map[string]any{"N": "1500"}, "name": map[string]any{"S": "x"}},
		{"val": map[string]any{"S": "1500"}, "name": map[string]any{"S": "b"}},
		{"val": map[string]any{"N": "2.5e3"}},
		{"val": map[string]any{"N": "-1500"}, "name": map[string]any{"S": ""}},
		{"val": map[string]any{"N": "12345678901234567890123456789012345678"}},
		{"val": map[string]any{"N": "12345678901234567890123456789012345679"}},
		{"val": map[string]any{"N": "-12345678901234567890123456789012345678"}},
		{"val": map[string]any{"N": "1E+3"}, "name": map[string]any{"S": "xylophone"}},
		{"val": map[string]any{"N": "0.0000000000000000000000000000000000001"}},
		{"name": map[string]any{"NULL": true}},
		{"val": map[string]any{"BOOL": true}, "name": map[string]any{"B": []byte("x")}},
		{
			"m": map[string]any{"M": map[string]any{
				"inner": map[string]any{"N": "7"},
				"l":     map[string]any{"L": []any{map[string]any{"S": "p"}, map[string]any{"N": "9"}}},
			}},
			"l":    map[string]any{"L": []any{map[string]any{"N": "3"}, map[string]any{"S": "zz"}}},
			"tags": map[string]any{"SS": []any{"a", "b"}},
			"#lit": map[string]any{"N": "11"},
		},
		{"val": map[string]any{"N": "1", "S": "multi"}},
		{"val": map[string]any{"weird": "x"}},
		{},
	}
	names := map[string]string{
		"#v": "val", "#n": "name", "#m": "m", "#i": "inner", "#l": "l", "#t": "tags",
	}
	values := map[string]any{
		":lo":    map[string]any{"N": "1000"},
		":hi":    map[string]any{"N": "2000"},
		":x":     map[string]any{"S": "x"},
		":null":  map[string]any{"NULL": true},
		":t":     map[string]any{"BOOL": true},
		":big":   map[string]any{"N": "12345678901234567890123456789012345678"},
		":neg":   map[string]any{"N": "-1500"},
		":exp":   map[string]any{"N": "1e3"},
		":p":     map[string]any{"S": "xy"},
		":empty": map[string]any{"S": ""},
		":num":   map[string]any{"N": "3"},
		":seven": map[string]any{"N": "7"},
		":a":     map[string]any{"S": "a"},
	}

	tests := []struct {
		name string
		expr string
	}{
		{"between", "#v BETWEEN :lo AND :hi"},
		{"between_missing_upper", "#v BETWEEN :lo AND :nope"},
		{"compare_num", "#v > :lo"},
		{"compare_all_ops", "#v <= :lo OR #v < :lo OR #v >= :hi OR #v = :exp OR #v <> :exp"},
		{"compare_str", "#n = :x"},
		{"compare_str_order", "#n >= :x"},
		{"not_equal", "#n <> :x"},
		{"null", "#n = :null"},
		{"bool", "#v = :t"},
		{"and_or", "#v < :lo OR (#v >= :hi AND #n <> :x)"},
		{"not", "NOT (#v > :lo)"},
		{"missing_placeholder", "#v = :nope"},
		{"missing_placeholder_left", ":nope = #v"},
		{"missing_short_circuit", "#v < :lo AND #v = :nope"},
		{"missing_short_circuit_or", "#v >= :lo OR #v = :nope"},
		{"placeholder_both_sides", ":lo < :hi"},
		{"big_eq", "#v = :big"},
		{"big_gt", "#v > :big"},
		{"neg", "#v < :neg"},
		{"exp", "#v = :exp"},
		{"begins_with", "begins_with(#n, :p)"},
		{"begins_with_empty", "begins_with(#n, :empty)"},
		{"begins_with_num_path", "begins_with(#v, :lo)"},
		{"begins_with_missing_prefix", "begins_with(#n, :nope)"},
		{"begins_with_nested", "begins_with(#m.#l[0], :a)"},
		{"begins_with_literal_name", "begins_with(#n, :p) AND begins_with(name, :p)"},
		{"exists", "attribute_exists(#v)"},
		{"not_exists", "attribute_not_exists(#n)"},
		{"exists_nested", "attribute_exists(#m.#i)"},
		{"not_exists_nested", "attribute_not_exists(#m.#i)"},
		{"exists_index", "attribute_exists(#l[1]) AND attribute_not_exists(#l[5])"},
		{"nested_map_cmp", "#m.#i = :seven"},
		{"nested_list_cmp", "#l[0] = :num"},
		{"nested_deep_list", "#m.#l[1] > :num"},
		{"index_on_map", "#m[0] = :num"},
		{"key_on_list", "#l.x = :num"},
		{"unresolved_hash", "#lit = :seven OR #zz = :seven"},
		{"contains_set", "contains(#t, :a)"},
		{"contains_str", "contains(#n, :a)"},
		{"size_cmp", "size(#n) > :num"},
		{"size_missing", "size(#n) > :nope"},
		{"in_list", "#n IN (:x, :a)"},
		{"in_missing", "#n IN (:x, :nope)"},
		{"attribute_type", "attribute_type(#v, :x)"},
		{"fn_wrong_args", "begins_with(#n)"},
		{"bare_placeholder", ":t"},
		{"bare_path", "#v"},
		{"and_bare_placeholder", "#v > :lo AND :t"},
		{"size_left", "size(#n) = size(name)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			node, err := expr.NewParser(expr.NewLexer(tt.expr)).ParseCondition()
			if err != nil {
				t.Skipf("parse: %v", err)
			}

			compiled := expr.CompileCondition(node, names, values)

			for pass := range 2 {
				for i, item := range items {
					fresh := &expr.Evaluator{Item: item, AttrNames: names, AttrValues: values}
					res, wantErr := fresh.Evaluate(node)
					want, _ := res.(bool)
					if wantErr != nil {
						want = false
					}

					got, gotErr := compiled.Evaluate(item)

					assert.Equal(t, wantErr != nil, gotErr != nil, "pass %d item %d err", pass, i)
					assert.Equal(t, want, got, "pass %d item %d", pass, i)
				}
			}
		})
	}
}
