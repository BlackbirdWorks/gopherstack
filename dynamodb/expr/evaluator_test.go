package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluator_UnwrapAttributeValue(t *testing.T) {
	t.Parallel()

	eval := &Evaluator{}
	tests := []struct {
		input    any
		expected any
		name     string
	}{
		{map[string]any{"S": "val"}, "val", "String"},
		{map[string]any{"N": "123"}, "123", "Number"},
		{map[string]any{"B": []byte("bin")}, []byte("bin"), "Binary"},
		{map[string]any{"BOOL": true}, true, "Bool"},
		{map[string]any{"NULL": true}, nil, "Null"},
		{map[string]any{"M": map[string]any{"k": "v"}}, map[string]any{"k": "v"}, "Map"},
		{map[string]any{"L": []any{"a"}}, []any{"a"}, "List"},
		{map[string]any{"SS": []string{"a"}}, []string{"a"}, "StringSet"},
		{map[string]any{"NS": []string{"1"}}, []string{"1"}, "NumberSet"},
		{map[string]any{"BS": [][]byte{[]byte("a")}}, [][]byte{[]byte("a")}, "BinarySet"},
		{"raw", "raw", "RawValue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, eval.unwrapAttributeValue(tt.input))
		})
	}
}

func TestEvaluator_CalculateSize(t *testing.T) {
	t.Parallel()

	eval := &Evaluator{}
	tests := []struct {
		input    any
		name     string
		expected float64
	}{
		{map[string]any{"S": "abc"}, "String", 3},
		{map[string]any{"B": []byte{1, 2}}, "Binary", 2},
		{map[string]any{"L": []any{1, 2, 3}}, "List", 3},
		{map[string]any{"M": map[string]any{"k": "v"}}, "Map", 1},
		{map[string]any{"SS": []string{"a", "b"}}, "StringSet", 2},
		{map[string]any{"BS": [][]byte{[]byte("a")}}, "BinarySet", 1},
		{map[string]any{"BOOL": true}, "Unsupported", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, eval.calculateSize(tt.input))
		})
	}
}

func TestEvaluator_Evaluate_Errors(t *testing.T) {
	t.Parallel()

	eval := &Evaluator{
		AttrValues: map[string]any{":v": map[string]any{"S": "val"}},
	}

	t.Run("ValuePlaceholderNotFound", func(t *testing.T) {
		t.Parallel()
		_, err := eval.Evaluate(&ValuePlaceholder{Name: ":missing"})
		assert.ErrorIs(t, err, ErrValuePlaceholderNotFound)
	})

	t.Run("UnsupportedNode", func(t *testing.T) {
		t.Parallel()
		_, err := eval.Evaluate(nil)
		assert.ErrorIs(t, err, ErrUnsupportedNodeType)
	})
}

func TestEvaluator_ApplyUpdate(t *testing.T) {
	t.Parallel()

	item := map[string]any{
		"a": map[string]any{"S": "val1"},
		"b": map[string]any{"N": "10"},
		"c": map[string]any{"M": map[string]any{"d": map[string]any{"S": "val2"}}},
	}

	eval := &Evaluator{
		Item: item,
		AttrValues: map[string]any{
			":newVal": map[string]any{"S": "new"},
			":inc":    map[string]any{"N": "5"},
		},
	}

	// SET a = :newVal, b = b + :inc
	update := &UpdateExpr{
		Actions: []UpdateAction{
			{
				Type: TokenSET,
				Items: []UpdateItem{
					{
						Path:  &PathExpr{Elements: []PathElement{{Name: "a", Type: ElementKey}}},
						Value: &ValuePlaceholder{Name: ":newVal"},
					},
				},
			},
		},
	}

	err := eval.ApplyUpdate(update)
	require.NoError(t, err)
	assert.Equal(t, "new", eval.unwrapAttributeValue(eval.Item["a"]))

	// REMOVE c.d
	remove := &UpdateExpr{
		Actions: []UpdateAction{
			{
				Type: TokenREMOVE,
				Items: []UpdateItem{
					{
						Path: &PathExpr{Elements: []PathElement{
							{Name: "c", Type: ElementKey},
							{Name: "d", Type: ElementKey},
						}},
					},
				},
			},
		},
	}

	err = eval.ApplyUpdate(remove)
	require.NoError(t, err)
	c := eval.unwrapAttributeValue(eval.Item["c"]).(map[string]any)
	_, exists := c["d"]
	assert.False(t, exists)
}

func TestEvaluator_ApplyAdd(t *testing.T) {
	t.Parallel()

	item := map[string]any{
		"num": map[string]any{"N": "10"},
	}

	eval := &Evaluator{
		Item: item,
		AttrValues: map[string]any{
			":inc": map[string]any{"N": "5"},
		},
	}

	err := eval.applyAdd([]PathElement{{Name: "num", Type: ElementKey}}, map[string]any{"N": "5"})
	require.NoError(t, err)
	assert.Equal(t, "15", eval.unwrapAttributeValue(eval.Item["num"]))

	// Test adding to non-existent
	err = eval.applyAdd([]PathElement{{Name: "newNum", Type: ElementKey}}, map[string]any{"N": "1"})
	require.NoError(t, err)
	assert.Equal(t, "1", eval.unwrapAttributeValue(eval.Item["newNum"]))
}

func TestEvaluator_FunctionErrors(t *testing.T) {
	t.Parallel()

	eval := &Evaluator{}

	tests := []struct {
		wantErr error
		fn      *FunctionExpr
		name    string
	}{
		{"size wrong args", &FunctionExpr{Name: "size", Args: []Node{&ValuePlaceholder{Name: ":v"}, &ValuePlaceholder{Name: ":v"}}}, ErrSizeExpectsOneArg},
		{"attribute_exists wrong args", &FunctionExpr{Name: "attribute_exists", Args: []Node{}}, ErrAttributeExistsExpectsOne},
		{"attribute_exists not path", &FunctionExpr{Name: "attribute_exists", Args: []Node{&ValuePlaceholder{Name: ":v"}}}, ErrAttributeExistsExpectsPath},
		{"attribute_not_exists wrong args", &FunctionExpr{Name: "attribute_not_exists", Args: []Node{}}, ErrAttributeNExistsExpectsOne},
		{"attribute_not_exists not path", &FunctionExpr{Name: "attribute_not_exists", Args: []Node{&ValuePlaceholder{Name: ":v"}}}, ErrAttributeNExistsExpectsPath},
		{"begins_with wrong args", &FunctionExpr{Name: "begins_with", Args: []Node{&ValuePlaceholder{Name: ":v"}}}, ErrBeginsWithExpectsTwo},
		{"contains wrong args", &FunctionExpr{Name: "contains", Args: []Node{&ValuePlaceholder{Name: ":v"}}}, ErrContainsExpectsTwo},
		{"unknown function", &FunctionExpr{Name: "unknown"}, ErrUnknownFunction},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := eval.evaluateFunction(tt.fn)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestEvaluator_Mutate_Errors(t *testing.T) {
	t.Parallel()

	eval := &Evaluator{}

	t.Run("ExpectedMapForKey", func(t *testing.T) {
		t.Parallel()
		_, err := eval.mutate([]any{}, []PathElement{{Name: "foo", Type: ElementKey}}, "val", false)
		assert.ErrorIs(t, err, ErrExpectedMapForKey)
	})

	t.Run("ExpectedListAtIndex", func(t *testing.T) {
		t.Parallel()
		_, err := eval.mutate(map[string]any{"foo": "bar"}, []PathElement{{Name: "foo", Type: ElementIndex, Index: 0}}, "val", false)
		assert.ErrorIs(t, err, ErrExpectedListAtIndex)
	})

	t.Run("ExpectedListForIndex", func(t *testing.T) {
		t.Parallel()
		_, err := eval.mutate("not a list", []PathElement{{Name: "foo", Type: ElementIndex, Index: 0}}, "val", false)
		assert.ErrorIs(t, err, ErrExpectedListForIndex)
	})

	t.Run("IndexOutOfRange", func(t *testing.T) {
		t.Parallel()
		_, err := eval.mutate([]any{}, []PathElement{{Name: "foo", Type: ElementIndex, Index: 0}}, "val", false)
		assert.ErrorIs(t, err, ErrIndexOutOfRange)
	})
}
