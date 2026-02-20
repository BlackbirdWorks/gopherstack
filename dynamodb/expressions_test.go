package dynamodb_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/dynamodb"

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
			name: "Between Condition",
			expr: "val BETWEEN :min AND :max",
			vals: map[string]any{
				":min": map[string]any{"N": "50"},
				":max": map[string]any{"N": "150"},
			},
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
			name: "OR Condition",
			expr: "val < :min OR val > :max",
			vals: map[string]any{
				":min": map[string]any{"N": "10"},
				":max": map[string]any{"N": "90"},
			},
			wantMatch: true,
		},
		{
			name: "AND Condition",
			expr: "status = :s AND val = :v",
			vals: map[string]any{
				":s": map[string]any{"S": "active"},
				":v": map[string]any{"N": "100"},
			},
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
		{
			name: "Number < True",
			lhs:  map[string]any{"N": "10"},
			op:   "<",
			rhs:  map[string]any{"N": "20"},
			want: true,
		},
		{
			name: "Number < False",
			lhs:  map[string]any{"N": "20"},
			op:   "<",
			rhs:  map[string]any{"N": "10"},
			want: false,
		},
		{
			name: "Number > True",
			lhs:  map[string]any{"N": "20"},
			op:   ">",
			rhs:  map[string]any{"N": "10"},
			want: true,
		},
		{
			name: "Number = True",
			lhs:  map[string]any{"N": "10"},
			op:   "=",
			rhs:  map[string]any{"N": "10"},
			want: true,
		},
		{
			name: "String < True",
			lhs:  map[string]any{"S": "a"},
			op:   "<",
			rhs:  map[string]any{"S": "b"},
			want: true,
		},
		{
			name: "String > True",
			lhs:  map[string]any{"S": "b"},
			op:   ">",
			rhs:  map[string]any{"S": "a"},
			want: true,
		},
		{
			name: "String = True",
			lhs:  map[string]any{"S": "a"},
			op:   "=",
			rhs:  map[string]any{"S": "a"},
			want: true,
		},
		{
			name: "String != True",
			lhs:  map[string]any{"S": "a"},
			op:   "<>",
			rhs:  map[string]any{"S": "b"},
			want: true,
		},
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

	tests := []struct {
		input any
		want  any
		name  string
	}{
		{
			name:  "String",
			input: map[string]any{"S": "test"},
			want:  "test",
		},
		{
			name:  "Number",
			input: map[string]any{"N": "123"},
			want:  "123",
		},
		{
			name:  "Boolean",
			input: map[string]any{"BOOL": true},
			want:  true,
		},
		{
			name:  "Null",
			input: map[string]any{"NULL": true},
			want:  nil,
		},
		{
			name:  "RawString",
			input: "raw",
			want:  "raw",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := dynamodb.UnwrapAttributeValue(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}
