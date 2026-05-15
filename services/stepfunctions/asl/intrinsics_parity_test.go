package asl_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

func TestParityIntrinsics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   any
		want    any
		name    string
		expr    string
		wantErr bool
	}{
		{name: "string_concat", expr: "States.StringConcat('a', 'b', 'c')", want: "abc"},
		{name: "string_length", expr: "States.StringLength('héllo')", want: 5.0},
		{name: "string_to_lower", expr: "States.StringToLower('HeLLo')", want: "hello"},
		{name: "string_to_upper", expr: "States.StringToUpper('HeLLo')", want: "HELLO"},
		{name: "string_index_found", expr: "States.StringIndex('hello', 'll')", want: 2.0},
		{name: "string_index_missing", expr: "States.StringIndex('hello', 'zz')", want: -1.0},
		{
			name:  "array_slice",
			expr:  "States.ArraySlice($.a, 1, 3)",
			input: map[string]any{"a": []any{"x", "y", "z", "w"}},
			want:  []any{"y", "z"},
		},
		{
			name:  "array_slice_clamps_end",
			expr:  "States.ArraySlice($.a, 0, 99)",
			input: map[string]any{"a": []any{1.0, 2.0}},
			want:  []any{1.0, 2.0},
		},
		{
			name:  "array_flatten",
			expr:  "States.ArrayFlatten($.a)",
			input: map[string]any{"a": []any{[]any{1.0, 2.0}, []any{3.0}, 4.0}},
			want:  []any{1.0, 2.0, 3.0, 4.0},
		},
		{
			name:  "array_reverse",
			expr:  "States.ArrayReverse($.a)",
			input: map[string]any{"a": []any{"a", "b", "c"}},
			want:  []any{"c", "b", "a"},
		},
		{
			name:  "array_sort_numbers",
			expr:  "States.ArraySort($.a)",
			input: map[string]any{"a": []any{3.0, 1.0, 2.0}},
			want:  []any{1.0, 2.0, 3.0},
		},
		{
			name:  "array_sort_strings",
			expr:  "States.ArraySort($.a)",
			input: map[string]any{"a": []any{"c", "a", "b"}},
			want:  []any{"a", "b", "c"},
		},
		{name: "math_subtract", expr: "States.MathSubtract(10, 3)", want: 7.0},
		{name: "math_multiply", expr: "States.MathMultiply(4, 3)", want: 12.0},
		{name: "math_divide", expr: "States.MathDivide(10, 4)", want: 2.5},
		{name: "math_divide_by_zero", expr: "States.MathDivide(1, 0)", wantErr: true},
		{name: "math_mod", expr: "States.MathMod(10, 3)", want: 1.0},
		{name: "math_mod_by_zero", expr: "States.MathMod(1, 0)", wantErr: true},
		{name: "math_min", expr: "States.MathMin(2, 5)", want: 2.0},
		{name: "math_max", expr: "States.MathMax(2, 5)", want: 5.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := asl.EvaluateIntrinsicFunction(tt.expr, tt.input)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExecutorBuildContextObject(t *testing.T) {
	t.Parallel()

	exec := asl.NewExecutor(&asl.StateMachine{}, nil, nil)
	exec.SetExecutionContext(
		"arn:aws:states:us-east-1:000000000000:execution:sm:e1",
		"e1",
		"arn:aws:iam::000000000000:role/sfn",
		"2026-01-02T03:04:05Z",
		"arn:aws:states:us-east-1:000000000000:stateMachine:sm",
		"sm",
	)

	ctx := exec.BuildContextObject()
	exe, _ := ctx["Execution"].(map[string]any)
	require.NotNil(t, exe)
	assert.Equal(t, "e1", exe["Name"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/sfn", exe["RoleArn"])
	assert.Equal(t, "2026-01-02T03:04:05Z", exe["StartTime"])

	sm, _ := ctx["StateMachine"].(map[string]any)
	require.NotNil(t, sm)
	assert.Equal(t, "sm", sm["Name"])

	branch := exec.NewBranchExecutor(&asl.StateMachine{}, "Branch-2")
	bctx := branch.BuildContextObject()
	parallel, _ := bctx["Parallel"].(map[string]any)
	require.NotNil(t, parallel)
	assert.Equal(t, "Branch-2", parallel["BranchName"])

	bexe, _ := bctx["Execution"].(map[string]any)
	require.NotNil(t, bexe)
	assert.Equal(t, "Branch-2", bexe["BranchName"])
}
