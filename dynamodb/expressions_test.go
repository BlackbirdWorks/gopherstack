package dynamodb_test

import (
	"Gopherstack/dynamodb"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluateExpression(t *testing.T) {
	t.Parallel()
	item := map[string]any{
		"pk":     map[string]any{"S": "item1"},
		"val":    map[string]any{"N": "100"},
		"status": map[string]any{"S": "active"},
		"tags":   map[string]any{"SS": []string{"tag1", "tag2"}},
		"meta": map[string]any{
			"M": map[string]any{
				"ver": map[string]any{"N": "1"},
			},
		},
	}

	tests := []struct {
		vals      map[string]any
		name      string
		expr      string
		wantMatch bool
		wantErr   bool
	}{
		{
			name:      "Simple Equality",
			expr:      "status = :s",
			vals:      map[string]any{":s": map[string]any{"S": "active"}},
			wantMatch: true,
		},
		{
			name:      "Numeric Comparison (>)",
			expr:      "val > :v",
			vals:      map[string]any{":v": map[string]any{"N": "50"}},
			wantMatch: true,
		},
		{
			name:      "Numeric Comparison (<) - False",
			expr:      "val < :v",
			vals:      map[string]any{":v": map[string]any{"N": "50"}},
			wantMatch: false,
		},
		{
			name:      "Between Condition",
			expr:      "val BETWEEN :min AND :max",
			vals:      map[string]any{":min": map[string]any{"N": "50"}, ":max": map[string]any{"N": "150"}},
			wantMatch: true,
		},
		{
			name:      "Attribute Exists",
			expr:      "attribute_exists(pk)",
			wantMatch: true,
		},
		{
			name:      "Attribute Not Exists",
			expr:      "attribute_not_exists(missing)",
			wantMatch: true,
		},
		{
			name:      "Begins With",
			expr:      "begins_with(status, :prefix)",
			vals:      map[string]any{":prefix": map[string]any{"S": "act"}},
			wantMatch: true,
		},
		{
			name:      "Contains (String)",
			expr:      "contains(status, :sub)",
			vals:      map[string]any{":sub": map[string]any{"S": "tiv"}},
			wantMatch: true,
		},
		{
			name:      "Nested Attribute Path",
			expr:      "meta.ver = :v",
			vals:      map[string]any{":v": map[string]any{"N": "1"}},
			wantMatch: true,
		},
		{
			name:      "OR Condition",
			expr:      "val < :min OR val > :max",
			vals:      map[string]any{":min": map[string]any{"N": "10"}, ":max": map[string]any{"N": "90"}},
			wantMatch: true,
		},
		{
			name:      "AND Condition",
			expr:      "status = :s AND val = :v",
			vals:      map[string]any{":s": map[string]any{"S": "active"}, ":v": map[string]any{"N": "100"}},
			wantMatch: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			match, err := dynamodb.EvaluateExpression(tc.expr, item, tc.vals, nil)
			if tc.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantMatch, match)
		})
	}
}

func TestCompareValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		lhs  any
		rhs  any
		name string
		op   string
		want bool
	}{
		{name: "Number < True", lhs: map[string]any{"N": "10"}, op: "<", rhs: map[string]any{"N": "20"}, want: true},
		{name: "Number < False", lhs: map[string]any{"N": "20"}, op: "<", rhs: map[string]any{"N": "10"}, want: false},
		{name: "Number > True", lhs: map[string]any{"N": "20"}, op: ">", rhs: map[string]any{"N": "10"}, want: true},
		{name: "Number = True", lhs: map[string]any{"N": "10"}, op: "=", rhs: map[string]any{"N": "10"}, want: true},
		{name: "String < True", lhs: map[string]any{"S": "a"}, op: "<", rhs: map[string]any{"S": "b"}, want: true},
		{name: "String > True", lhs: map[string]any{"S": "b"}, op: ">", rhs: map[string]any{"S": "a"}, want: true},
		{name: "String = True", lhs: map[string]any{"S": "a"}, op: "=", rhs: map[string]any{"S": "a"}, want: true},
		{name: "String != True", lhs: map[string]any{"S": "a"}, op: "<>", rhs: map[string]any{"S": "b"}, want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := dynamodb.CompareValues(tc.lhs, tc.op, tc.rhs)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestUnwrapAttributeValue(t *testing.T) {
	t.Parallel()
	// Test unwrapping various attribute types
	sVal := map[string]any{"S": "test"}
	assert.Equal(t, "test", dynamodb.UnwrapAttributeValue(sVal))

	nVal := map[string]any{"N": "123"}
	assert.Equal(t, "123", dynamodb.UnwrapAttributeValue(nVal))

	boolVal := map[string]any{"BOOL": true}
	assert.Equal(t, true, dynamodb.UnwrapAttributeValue(boolVal))

	nullVal := map[string]any{"NULL": true}
	assert.Nil(t, dynamodb.UnwrapAttributeValue(nullVal))

	// Already unwrapped
	assert.Equal(t, "raw", dynamodb.UnwrapAttributeValue("raw"))
}

func TestExtractFunctionArgs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  []string
	}{
		{"func(a, b)", []string{"a", "b"}},
		{"func(a,b)", []string{"a", "b"}},
		{"func( a , b )", []string{"a", "b"}},
		{"func(single)", []string{"single"}},
		{"func()", nil},
		{"contains(status, :sub)", []string{"status", ":sub"}},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			got := dynamodb.ExtractFunctionArgs(tc.input)
			assert.Equal(t, tc.want, got, "Input: %s", tc.input)
		})
	}
}
