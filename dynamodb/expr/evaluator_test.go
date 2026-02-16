package expr_test

import (
	"Gopherstack/dynamodb/expr"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluator_UnwrapAttributeValue(t *testing.T) {
	t.Parallel()

	eval := &expr.Evaluator{}
	tests := []struct {
		input    any
		expected any
		name     string
	}{
		{name: "String", input: map[string]any{"S": "val"}, expected: "val"},
		{name: "Number", input: map[string]any{"N": "123"}, expected: "123"},
		{name: "Binary", input: map[string]any{"B": []byte("bin")}, expected: []byte("bin")},
		{name: "Bool", input: map[string]any{"BOOL": true}, expected: true},
		{name: "Null", input: map[string]any{"NULL": true}, expected: nil},
		{name: "Map", input: map[string]any{"M": map[string]any{"k": "v"}}, expected: map[string]any{"k": "v"}},
		{name: "List", input: map[string]any{"L": []any{"a"}}, expected: []any{"a"}},
		{name: "StringSet", input: map[string]any{"SS": []string{"a"}}, expected: []string{"a"}},
		{name: "NumberSet", input: map[string]any{"NS": []string{"1"}}, expected: []string{"1"}},
		{
			name:     "BinarySet",
			input:    map[string]any{"BS": [][]byte{[]byte("a")}},
			expected: [][]byte{[]byte("a")},
		},
		{name: "RawValue", input: "raw", expected: "raw"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, eval.UnwrapAttributeValue(tt.input))
		})
	}
}

func TestEvaluator_CalculateSize(t *testing.T) {
	t.Parallel()

	eval := &expr.Evaluator{}
	tests := []struct {
		input    any
		name     string
		expected float64
	}{
		{name: "String", input: map[string]any{"S": "abc"}, expected: 3},
		{name: "Binary", input: map[string]any{"B": []byte{1, 2}}, expected: 2},
		{name: "List", input: map[string]any{"L": []any{1, 2, 3}}, expected: 3},
		{name: "Map", input: map[string]any{"M": map[string]any{"k": "v"}}, expected: 1},
		{name: "StringSet", input: map[string]any{"SS": []string{"a", "b"}}, expected: 2},
		{name: "BinarySet", input: map[string]any{"BS": [][]byte{[]byte("a")}}, expected: 1},
		{name: "Unsupported", input: map[string]any{"BOOL": true}, expected: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.InDelta(t, tt.expected, eval.CalculateSize(tt.input), 0)
		})
	}
}

func TestEvaluator_Evaluate_Errors(t *testing.T) {
	t.Parallel()

	eval := &expr.Evaluator{
		AttrValues: map[string]any{":v": map[string]any{"S": "val"}},
	}

	t.Run("ValuePlaceholderNotFound", func(t *testing.T) {
		t.Parallel()
		_, err := eval.Evaluate(&expr.ValuePlaceholder{Name: ":missing"})
		assert.ErrorIs(t, err, expr.ErrValuePlaceholderNotFound)
	})

	t.Run("UnsupportedNode", func(t *testing.T) {
		t.Parallel()
		_, err := eval.Evaluate(nil)
		assert.ErrorIs(t, err, expr.ErrUnsupportedNodeType)
	})
}

func TestEvaluator_ApplyUpdate(t *testing.T) {
	t.Parallel()

	item := map[string]any{
		"a": map[string]any{"S": "val1"},
		"b": map[string]any{"N": "10"},
		"c": map[string]any{"M": map[string]any{"d": map[string]any{"S": "val2"}}},
	}

	eval := &expr.Evaluator{
		Item: item,
		AttrValues: map[string]any{
			":newVal": map[string]any{"S": "new"},
			":inc":    map[string]any{"N": "5"},
		},
	}

	// SET a = :newVal
	update := &expr.UpdateExpr{
		Actions: []expr.UpdateAction{
			{
				Type: expr.TokenSET,
				Items: []expr.UpdateItem{
					{
						Path:  &expr.PathExpr{Elements: []expr.PathElement{{Name: "a", Type: expr.ElementKey}}},
						Value: &expr.ValuePlaceholder{Name: ":newVal"},
					},
				},
			},
		},
	}

	err := eval.ApplyUpdate(update)
	require.NoError(t, err)
	assert.Equal(t, "new", eval.UnwrapAttributeValue(eval.Item["a"]))

	// REMOVE c.d
	remove := &expr.UpdateExpr{
		Actions: []expr.UpdateAction{
			{
				Type: expr.TokenREMOVE,
				Items: []expr.UpdateItem{
					{
						Path: &expr.PathExpr{Elements: []expr.PathElement{
							{Name: "c", Type: expr.ElementKey},
							{Name: "d", Type: expr.ElementKey},
						}},
					},
				},
			},
		},
	}

	err = eval.ApplyUpdate(remove)
	require.NoError(t, err)
	c := eval.UnwrapAttributeValue(eval.Item["c"]).(map[string]any)
	_, exists := c["d"]
	assert.False(t, exists)
}

func TestEvaluator_ApplyAdd(t *testing.T) {
	t.Parallel()

	item := map[string]any{
		"num": map[string]any{"N": "10"},
	}

	eval := &expr.Evaluator{
		Item:       item,
		AttrValues: map[string]any{":inc": map[string]any{"N": "5"}},
	}

	err := eval.ExportedApplyAdd(
		[]expr.PathElement{{Name: "num", Type: expr.ElementKey}},
		map[string]any{"N": "5"},
	)
	require.NoError(t, err)
	assert.Equal(t, "15", eval.UnwrapAttributeValue(eval.Item["num"]))

	err = eval.ExportedApplyAdd(
		[]expr.PathElement{{Name: "newNum", Type: expr.ElementKey}},
		map[string]any{"N": "1"},
	)
	require.NoError(t, err)
	assert.Equal(t, "1", eval.UnwrapAttributeValue(eval.Item["newNum"]))
}

func TestEvaluator_FunctionErrors(t *testing.T) {
	t.Parallel()

	eval := &expr.Evaluator{}

	tests := []struct {
		wantErr error
		fn      *expr.FunctionExpr
		name    string
	}{
		{
			name: "size wrong args",
			fn: &expr.FunctionExpr{
				Name: "size",
				Args: []expr.Node{
					&expr.ValuePlaceholder{Name: ":v"},
					&expr.ValuePlaceholder{Name: ":v"},
				},
			},
			wantErr: expr.ErrSizeExpectsOneArg,
		},
		{
			name:    "attribute_exists wrong args",
			fn:      &expr.FunctionExpr{Name: "attribute_exists", Args: []expr.Node{}},
			wantErr: expr.ErrAttributeExistsExpectsOne,
		},
		{
			name: "attribute_exists not path",
			fn: &expr.FunctionExpr{
				Name: "attribute_exists",
				Args: []expr.Node{&expr.ValuePlaceholder{Name: ":v"}},
			},
			wantErr: expr.ErrAttributeExistsExpectsPath,
		},
		{
			name:    "attribute_not_exists wrong args",
			fn:      &expr.FunctionExpr{Name: "attribute_not_exists", Args: []expr.Node{}},
			wantErr: expr.ErrAttributeNExistsExpectsOne,
		},
		{
			name: "attribute_not_exists not path",
			fn: &expr.FunctionExpr{
				Name: "attribute_not_exists",
				Args: []expr.Node{&expr.ValuePlaceholder{Name: ":v"}},
			},
			wantErr: expr.ErrAttributeNExistsExpectsPath,
		},
		{
			name: "begins_with wrong args",
			fn: &expr.FunctionExpr{
				Name: "begins_with",
				Args: []expr.Node{&expr.ValuePlaceholder{Name: ":v"}},
			},
			wantErr: expr.ErrBeginsWithExpectsTwo,
		},
		{
			name: "contains wrong args",
			fn: &expr.FunctionExpr{
				Name: "contains",
				Args: []expr.Node{&expr.ValuePlaceholder{Name: ":v"}},
			},
			wantErr: expr.ErrContainsExpectsTwo,
		},
		{
			name:    "unknown function",
			fn:      &expr.FunctionExpr{Name: "unknown"},
			wantErr: expr.ErrUnknownFunction,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := eval.EvaluateFunction(tt.fn)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestEvaluator_Mutate_Errors(t *testing.T) {
	t.Parallel()

	eval := &expr.Evaluator{}

	t.Run("ExpectedMapForKey", func(t *testing.T) {
		t.Parallel()
		_, err := eval.Mutate(
			[]any{},
			[]expr.PathElement{{Name: "foo", Type: expr.ElementKey}},
			"val",
			false,
		)
		assert.ErrorIs(t, err, expr.ErrExpectedMapForKey)
	})

	t.Run("ExpectedListAtIndex", func(t *testing.T) {
		t.Parallel()
		_, err := eval.Mutate(
			map[string]any{"foo": "bar"},
			[]expr.PathElement{{Name: "foo", Type: expr.ElementIndex, Index: 0}},
			"val",
			false,
		)
		assert.ErrorIs(t, err, expr.ErrExpectedListAtIndex)
	})

	t.Run("ExpectedListForIndex", func(t *testing.T) {
		t.Parallel()
		_, err := eval.Mutate(
			"not a list",
			[]expr.PathElement{{Name: "foo", Type: expr.ElementIndex, Index: 0}},
			"val",
			false,
		)
		assert.ErrorIs(t, err, expr.ErrExpectedListForIndex)
	})

	t.Run("IndexOutOfRange", func(t *testing.T) {
		t.Parallel()
		_, err := eval.Mutate(
			[]any{},
			[]expr.PathElement{{Name: "foo", Type: expr.ElementIndex, Index: 0}},
			"val",
			false,
		)
		assert.ErrorIs(t, err, expr.ErrIndexOutOfRange)
	})
}

func TestEvaluator_Not(t *testing.T) {
	t.Parallel()

	pathA := []expr.PathElement{{Name: "a", Type: expr.ElementKey}}
	cmp := func(_ string) *expr.ComparisonExpr {
		return &expr.ComparisonExpr{
			Left:     &expr.PathExpr{Elements: pathA},
			Operator: expr.TokenEqual,
			Right:    &expr.ValuePlaceholder{Name: ":v"},
		}
	}

	tests := []struct {
		expr     *expr.NotExpr
		item     map[string]any
		vals     map[string]any
		name     string
		expected bool
	}{
		{
			name:     "not true becomes false",
			expr:     &expr.NotExpr{Expression: cmp("x")},
			item:     map[string]any{"a": map[string]any{"S": "x"}},
			vals:     map[string]any{":v": map[string]any{"S": "x"}},
			expected: false,
		},
		{
			name:     "not false becomes true",
			expr:     &expr.NotExpr{Expression: cmp("x")},
			item:     map[string]any{"a": map[string]any{"S": "y"}},
			vals:     map[string]any{":v": map[string]any{"S": "x"}},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ev := &expr.Evaluator{Item: tt.item, AttrValues: tt.vals}
			result, err := ev.Evaluate(tt.expr)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluator_Between(t *testing.T) {
	t.Parallel()

	tests := []struct {
		val      any
		lower    any
		upper    any
		name     string
		expected bool
	}{
		{
			name:     "in range",
			val:      map[string]any{"N": "5"},
			lower:    map[string]any{"N": "1"},
			upper:    map[string]any{"N": "10"},
			expected: true,
		},
		{
			name:     "at lower bound",
			val:      map[string]any{"N": "1"},
			lower:    map[string]any{"N": "1"},
			upper:    map[string]any{"N": "10"},
			expected: true,
		},
		{
			name:     "at upper bound",
			val:      map[string]any{"N": "10"},
			lower:    map[string]any{"N": "1"},
			upper:    map[string]any{"N": "10"},
			expected: true,
		},
		{
			name:     "below range",
			val:      map[string]any{"N": "0"},
			lower:    map[string]any{"N": "1"},
			upper:    map[string]any{"N": "10"},
			expected: false,
		},
		{
			name:     "above range",
			val:      map[string]any{"N": "11"},
			lower:    map[string]any{"N": "1"},
			upper:    map[string]any{"N": "10"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ev := &expr.Evaluator{
				AttrValues: map[string]any{":val": tt.val, ":lo": tt.lower, ":hi": tt.upper},
			}
			node := &expr.BetweenExpr{
				Value: &expr.ValuePlaceholder{Name: ":val"},
				Lower: &expr.ValuePlaceholder{Name: ":lo"},
				Upper: &expr.ValuePlaceholder{Name: ":hi"},
			}
			result, err := ev.Evaluate(node)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluator_In(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		val      any
		cands    []any
		expected bool
	}{
		{
			name:     "match first",
			val:      map[string]any{"S": "a"},
			cands:    []any{map[string]any{"S": "a"}, map[string]any{"S": "b"}},
			expected: true,
		},
		{
			name:     "match second",
			val:      map[string]any{"S": "b"},
			cands:    []any{map[string]any{"S": "a"}, map[string]any{"S": "b"}},
			expected: true,
		},
		{
			name:     "no match",
			val:      map[string]any{"S": "c"},
			cands:    []any{map[string]any{"S": "a"}, map[string]any{"S": "b"}},
			expected: false,
		},
		{
			name:     "empty candidates",
			val:      map[string]any{"S": "a"},
			cands:    []any{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			vals := map[string]any{":val": tt.val}
			candNodes := make([]expr.Node, len(tt.cands))
			for i, c := range tt.cands {
				key := fmt.Sprintf(":c%d", i)
				vals[key] = c
				candNodes[i] = &expr.ValuePlaceholder{Name: key}
			}
			ev := &expr.Evaluator{AttrValues: vals}
			node := &expr.InExpr{
				Value:      &expr.ValuePlaceholder{Name: ":val"},
				Candidates: candNodes,
			}
			result, err := ev.Evaluate(node)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluator_CompareValues(t *testing.T) {
	t.Parallel()

	ev := &expr.Evaluator{}

	tests := []struct {
		lhs      any
		rhs      any
		name     string
		op       expr.TokenType
		expected bool
	}{
		{
			name:     "equal strings",
			lhs:      map[string]any{"S": "a"},
			op:       expr.TokenEqual,
			rhs:      map[string]any{"S": "a"},
			expected: true,
		},
		{
			name:     "not equal strings",
			lhs:      map[string]any{"S": "a"},
			op:       expr.TokenNotEqual,
			rhs:      map[string]any{"S": "b"},
			expected: true,
		},
		{
			name:     "less num",
			lhs:      map[string]any{"N": "1"},
			op:       expr.TokenLess,
			rhs:      map[string]any{"N": "2"},
			expected: true,
		},
		{
			name:     "greater num",
			lhs:      map[string]any{"N": "3"},
			op:       expr.TokenGreater,
			rhs:      map[string]any{"N": "2"},
			expected: true,
		},
		{
			name:     "less equal num",
			lhs:      map[string]any{"N": "2"},
			op:       expr.TokenLessEqual,
			rhs:      map[string]any{"N": "2"},
			expected: true,
		},
		{
			name:     "greater equal num",
			lhs:      map[string]any{"N": "3"},
			op:       expr.TokenGreaterEqual,
			rhs:      map[string]any{"N": "2"},
			expected: true,
		},
		{
			name:     "less strings",
			lhs:      map[string]any{"S": "a"},
			op:       expr.TokenLess,
			rhs:      map[string]any{"S": "b"},
			expected: true,
		},
		{
			name:     "greater strings",
			lhs:      map[string]any{"S": "b"},
			op:       expr.TokenGreater,
			rhs:      map[string]any{"S": "a"},
			expected: true,
		},
		{
			name:     "unknown op",
			lhs:      map[string]any{"S": "a"},
			op:       expr.TokenType(999),
			rhs:      map[string]any{"S": "a"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, ev.CompareValues(tt.lhs, tt.op, tt.rhs))
		})
	}
}

func TestEvaluator_ToString(t *testing.T) {
	t.Parallel()

	ev := &expr.Evaluator{}

	tests := []struct {
		input    any
		name     string
		expected string
	}{
		{name: "string", input: "hello", expected: "hello"},
		{name: "bool true", input: true, expected: "true"},
		{name: "bool false", input: false, expected: "false"},
		{name: "float64", input: float64(3.14), expected: "3.14"},
		{name: "int", input: int(42), expected: "42"},
		{name: "int64", input: int64(99), expected: "99"},
		{name: "int32", input: int32(7), expected: "7"},
		{name: "nil", input: nil, expected: ""},
		{name: "json fallback", input: []string{"a", "b"}, expected: `["a","b"]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, ev.ToString(tt.input))
		})
	}
}

func TestEvaluator_NavigateList(t *testing.T) {
	t.Parallel()

	ev := &expr.Evaluator{
		Item: map[string]any{
			"items": map[string]any{"L": []any{
				map[string]any{"S": "first"},
				map[string]any{"S": "second"},
			}},
		},
	}

	tests := []struct {
		expected any
		name     string
		path     []expr.PathElement
		exists   bool
	}{
		{
			name: "index 0",
			path: []expr.PathElement{
				{Name: "items", Type: expr.ElementKey},
				{Type: expr.ElementIndex, Index: 0},
			},
			expected: map[string]any{"S": "first"},
			exists:   true,
		},
		{
			name: "index 1",
			path: []expr.PathElement{
				{Name: "items", Type: expr.ElementKey},
				{Type: expr.ElementIndex, Index: 1},
			},
			expected: map[string]any{"S": "second"},
			exists:   true,
		},
		{
			name: "out of bounds",
			path: []expr.PathElement{
				{Name: "items", Type: expr.ElementKey},
				{Type: expr.ElementIndex, Index: 5},
			},
			exists: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			val, ok := ev.GetValueAtPath(ev.Item, tt.path)
			assert.Equal(t, tt.exists, ok)
			if tt.exists {
				assert.Equal(t, tt.expected, val)
			}
		})
	}
}

func TestEvaluator_ApplyProjection(t *testing.T) {
	t.Parallel()

	item := map[string]any{
		"a": map[string]any{"S": "val_a"},
		"b": map[string]any{"N": "42"},
		"c": map[string]any{"S": "val_c"},
	}

	tests := []struct {
		name     string
		paths    []expr.Node
		wantKeys []string
	}{
		{
			name: "project two fields",
			paths: []expr.Node{
				&expr.PathExpr{Elements: []expr.PathElement{{Name: "a", Type: expr.ElementKey}}},
				&expr.PathExpr{Elements: []expr.PathElement{{Name: "b", Type: expr.ElementKey}}},
			},
			wantKeys: []string{"a", "b"},
		},
		{
			name: "project non-existent field",
			paths: []expr.Node{
				&expr.PathExpr{Elements: []expr.PathElement{{Name: "a", Type: expr.ElementKey}}},
				&expr.PathExpr{Elements: []expr.PathElement{{Name: "missing", Type: expr.ElementKey}}},
			},
			wantKeys: []string{"a"},
		},
		{
			name:     "non-path node skipped",
			paths:    []expr.Node{&expr.ValuePlaceholder{Name: ":v"}},
			wantKeys: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ev := &expr.Evaluator{Item: item}
			result := ev.ApplyProjection(&expr.ProjectionExpr{Paths: tt.paths})
			assert.Len(t, result, len(tt.wantKeys))
			for _, k := range tt.wantKeys {
				assert.Contains(t, result, k)
			}
		})
	}
}

func TestEvaluator_ApplyUpdate_AddAction(t *testing.T) {
	t.Parallel()

	item := map[string]any{
		"count": map[string]any{"N": "5"},
	}

	ev := &expr.Evaluator{
		Item:       item,
		AttrValues: map[string]any{":inc": map[string]any{"N": "3"}},
	}

	update := &expr.UpdateExpr{
		Actions: []expr.UpdateAction{
			{
				Type: expr.TokenADD,
				Items: []expr.UpdateItem{
					{
						Path:  &expr.PathExpr{Elements: []expr.PathElement{{Name: "count", Type: expr.ElementKey}}},
						Value: &expr.ValuePlaceholder{Name: ":inc"},
					},
				},
			},
		},
	}

	err := ev.ApplyUpdate(update)
	require.NoError(t, err)
	assert.Equal(t, "8", ev.UnwrapAttributeValue(ev.Item["count"]))
}

func TestEvaluator_MutateList(t *testing.T) {
	t.Parallel()

	listPath := []expr.PathElement{
		{Name: "vals", Type: expr.ElementKey},
		{Type: expr.ElementIndex, Index: 0},
	}

	t.Run("set list element", func(t *testing.T) {
		t.Parallel()
		ev := &expr.Evaluator{
			Item: map[string]any{
				"vals": map[string]any{"L": []any{
					map[string]any{"S": "old"},
					map[string]any{"S": "keep"},
				}},
			},
			AttrValues: map[string]any{":new": map[string]any{"S": "new"}},
		}
		update := &expr.UpdateExpr{
			Actions: []expr.UpdateAction{
				{
					Type: expr.TokenSET,
					Items: []expr.UpdateItem{
						{
							Path:  &expr.PathExpr{Elements: listPath},
							Value: &expr.ValuePlaceholder{Name: ":new"},
						},
					},
				},
			},
		}
		require.NoError(t, ev.ApplyUpdate(update))
	})

	t.Run("remove list element", func(t *testing.T) {
		t.Parallel()
		ev := &expr.Evaluator{
			Item: map[string]any{
				"vals": map[string]any{"L": []any{
					map[string]any{"S": "a"},
					map[string]any{"S": "b"},
				}},
			},
		}
		remove := &expr.UpdateExpr{
			Actions: []expr.UpdateAction{
				{
					Type:  expr.TokenREMOVE,
					Items: []expr.UpdateItem{{Path: &expr.PathExpr{Elements: listPath}}},
				},
			},
		}
		require.NoError(t, ev.ApplyUpdate(remove))
	})
}

func TestEvaluator_MutateMapNested(t *testing.T) {
	t.Parallel()

	t.Run("create intermediate map on set", func(t *testing.T) {
		t.Parallel()
		ev := &expr.Evaluator{
			Item:       map[string]any{},
			AttrValues: map[string]any{":v": map[string]any{"S": "leaf"}},
		}
		update := &expr.UpdateExpr{
			Actions: []expr.UpdateAction{
				{
					Type: expr.TokenSET,
					Items: []expr.UpdateItem{
						{
							Path: &expr.PathExpr{Elements: []expr.PathElement{
								{Name: "parent", Type: expr.ElementKey},
								{Name: "child", Type: expr.ElementKey},
							}},
							Value: &expr.ValuePlaceholder{Name: ":v"},
						},
					},
				},
			},
		}
		require.NoError(t, ev.ApplyUpdate(update))
	})

	t.Run("remove non-existent nested path", func(t *testing.T) {
		t.Parallel()
		ev := &expr.Evaluator{Item: map[string]any{}}
		update := &expr.UpdateExpr{
			Actions: []expr.UpdateAction{
				{
					Type: expr.TokenREMOVE,
					Items: []expr.UpdateItem{
						{
							Path: &expr.PathExpr{Elements: []expr.PathElement{
								{Name: "missing", Type: expr.ElementKey},
								{Name: "child", Type: expr.ElementKey},
							}},
						},
					},
				},
			},
		}
		require.NoError(t, ev.ApplyUpdate(update))
	})
}

func TestEvaluator_ApplyUpdate_DeleteAction(t *testing.T) {
	t.Parallel()

	ev := &expr.Evaluator{
		Item:       map[string]any{"a": map[string]any{"S": "v"}},
		AttrValues: map[string]any{":v": map[string]any{"S": "v"}},
	}

	// DELETE action is a no-op in current implementation — just verify it does not error.
	update := &expr.UpdateExpr{
		Actions: []expr.UpdateAction{
			{
				Type: expr.TokenDELETE,
				Items: []expr.UpdateItem{
					{
						Path:  &expr.PathExpr{Elements: []expr.PathElement{{Name: "a", Type: expr.ElementKey}}},
						Value: &expr.ValuePlaceholder{Name: ":v"},
					},
				},
			},
		},
	}
	require.NoError(t, ev.ApplyUpdate(update))
}

func TestEvaluator_ApplyUpdate_NonPathError(t *testing.T) {
	t.Parallel()

	ev := &expr.Evaluator{Item: map[string]any{}}

	update := &expr.UpdateExpr{
		Actions: []expr.UpdateAction{
			{
				Type: expr.TokenSET,
				Items: []expr.UpdateItem{
					{
						Path:  &expr.ValuePlaceholder{Name: ":v"},
						Value: &expr.ValuePlaceholder{Name: ":v"},
					},
				},
			},
		},
	}
	assert.ErrorIs(t, ev.ApplyUpdate(update), expr.ErrUpdatePathMustBePathExpr)
}

func TestEvaluator_LogicalShortCircuit(t *testing.T) {
	t.Parallel()

	item := map[string]any{"a": map[string]any{"S": "x"}}
	vals := map[string]any{
		":x": map[string]any{"S": "x"},
		":y": map[string]any{"S": "y"},
	}
	pathA := []expr.PathElement{{Name: "a", Type: expr.ElementKey}}

	tests := []struct {
		node     *expr.LogicalExpr
		name     string
		expected bool
	}{
		{
			name: "AND short-circuits on false left",
			node: &expr.LogicalExpr{
				Operator: expr.TokenAND,
				Left: &expr.ComparisonExpr{
					Left:     &expr.PathExpr{Elements: pathA},
					Operator: expr.TokenEqual,
					Right:    &expr.ValuePlaceholder{Name: ":y"},
				},
				Right: &expr.ComparisonExpr{
					Left:     &expr.PathExpr{Elements: pathA},
					Operator: expr.TokenEqual,
					Right:    &expr.ValuePlaceholder{Name: ":x"},
				},
			},
			expected: false,
		},
		{
			name: "OR short-circuits on true left",
			node: &expr.LogicalExpr{
				Operator: expr.TokenOR,
				Left: &expr.ComparisonExpr{
					Left:     &expr.PathExpr{Elements: pathA},
					Operator: expr.TokenEqual,
					Right:    &expr.ValuePlaceholder{Name: ":x"},
				},
				Right: &expr.ComparisonExpr{
					Left:     &expr.PathExpr{Elements: pathA},
					Operator: expr.TokenEqual,
					Right:    &expr.ValuePlaceholder{Name: ":y"},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ev := &expr.Evaluator{Item: item, AttrValues: vals}
			result, err := ev.Evaluate(tt.node)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluator_AttrNameResolution(t *testing.T) {
	t.Parallel()

	ev := &expr.Evaluator{
		Item:      map[string]any{"myKey": map[string]any{"S": "found"}},
		AttrNames: map[string]string{"#k": "myKey"},
		AttrValues: map[string]any{
			":v": map[string]any{"S": "found"},
		},
	}

	node := &expr.ComparisonExpr{
		Left:     &expr.PathExpr{Elements: []expr.PathElement{{Name: "#k", Type: expr.ElementKey}}},
		Operator: expr.TokenEqual,
		Right:    &expr.ValuePlaceholder{Name: ":v"},
	}

	result, err := ev.Evaluate(node)
	require.NoError(t, err)
	assert.Equal(t, true, result)
}
