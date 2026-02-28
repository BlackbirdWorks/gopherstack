package dynamoattr_test

import (
	"errors"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

type badJSON struct {
	A string
}

func (b badJSON) MarshalJSON() ([]byte, error) {
	return nil, errMarshalJSON
}

var errMarshalJSON = errors.New("nope")

func TestUnwrapAttributeValue(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   any
		want any
		name string
	}{
		{name: "string", in: map[string]any{"S": "v"}, want: "v"},
		{name: "number", in: map[string]any{"N": "1"}, want: "1"},
		{name: "binary", in: map[string]any{"B": "b"}, want: "b"},
		{name: "bool", in: map[string]any{"BOOL": true}, want: true},
		{name: "null", in: map[string]any{"NULL": true}, want: nil},
		{
			name: "map",
			in:   map[string]any{"M": map[string]any{"k": "v"}},
			want: map[string]any{"k": "v"},
		},
		{name: "list", in: map[string]any{"L": []any{"x"}}, want: []any{"x"}},
		{name: "ss", in: map[string]any{"SS": []string{"a"}}, want: []string{"a"}},
		{name: "ns", in: map[string]any{"NS": []string{"1"}}, want: []string{"1"}},
		{name: "bs", in: map[string]any{"BS": []string{"b"}}, want: []string{"b"}},
		{name: "plain", in: "x", want: "x"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := dynamoattr.UnwrapAttributeValue(tc.in)
			assert.Empty(t, cmp.Diff(tc.want, got), "UnwrapAttributeValue() mismatch")
		})
	}
}

func TestParseNumeric(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in     any
		name   string
		want   float64
		okWant bool
	}{
		{in: float64(1.5), name: "float64", want: 1.5, okWant: true},
		{in: int(2), name: "int", want: 2, okWant: true},
		{in: int64(3), name: "int64", want: 3, okWant: true},
		{in: "4.5", name: "string", want: 4.5, okWant: true},
		{in: map[string]any{"N": "6"}, name: "wrapped", want: 6, okWant: true},
		{in: "nope", name: "bad", want: 0, okWant: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := dynamoattr.ParseNumeric(tc.in)
			assert.Equal(t, tc.okWant, ok)
			if ok {
				assert.InDelta(t, tc.want, got, 1e-9)
			}
		})
	}
}

func TestResolveValue(t *testing.T) {
	t.Parallel()

	cases := []struct {
		attrValues map[string]any
		want       any
		name       string
		token      string
	}{
		{
			name:       "resolved",
			token:      ":a",
			attrValues: map[string]any{":a": "value"},
			want:       "value",
		},
		{
			name:       "missing",
			token:      ":missing",
			attrValues: map[string]any{":a": "value"},
			want:       ":missing",
		},
		{
			name:       "plain",
			token:      "plain",
			attrValues: map[string]any{":a": "value"},
			want:       "plain",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := dynamoattr.ResolveValue(tc.token, tc.attrValues)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCompareValues(t *testing.T) {
	t.Parallel()

	cases := []struct {
		lhs  any
		rhs  any
		name string
		op   string
		want bool
	}{
		{
			name: "numeric true",
			lhs:  map[string]any{"N": "2"},
			op:   "<",
			rhs:  map[string]any{"N": "3"},
			want: true,
		},
		{
			name: "numeric false",
			lhs:  map[string]any{"N": "3"},
			op:   "<",
			rhs:  map[string]any{"N": "2"},
			want: false,
		},
		{
			name: "numeric > true",
			lhs:  map[string]any{"N": "3"},
			op:   ">",
			rhs:  map[string]any{"N": "2"},
			want: true,
		},
		{
			name: "numeric <= true",
			lhs:  map[string]any{"N": "2"},
			op:   "<=",
			rhs:  map[string]any{"N": "2"},
			want: true,
		},
		{
			name: "numeric <= true 2",
			lhs:  map[string]any{"N": "1"},
			op:   "<=",
			rhs:  map[string]any{"N": "2"},
			want: true,
		},
		{
			name: "numeric >= true",
			lhs:  map[string]any{"N": "2"},
			op:   ">=",
			rhs:  map[string]any{"N": "2"},
			want: true,
		},
		{name: "string comparison false", lhs: "b", op: "<", rhs: "a", want: false},
		{name: "string comparison true", lhs: "a", op: "<", rhs: "b", want: true},
		{name: "string > true", lhs: "b", op: ">", rhs: "a", want: true},
		{name: "string <= true", lhs: "a", op: "<=", rhs: "a", want: true},
		{name: "string >= true", lhs: "a", op: ">=", rhs: "a", want: true},
		{name: "equality true", lhs: "a", op: "=", rhs: "a", want: true},
		{name: "inequality false", lhs: "a", op: "<>", rhs: "a", want: false},
		{name: "unknown operator false", lhs: "a", op: "?", rhs: "b", want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := dynamoattr.CompareValues(tc.lhs, tc.op, tc.rhs)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestSplitANDConditions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		expr string
		want []string
	}{
		{
			name: "basic and between",
			expr: "pk = :pk AND sk BETWEEN :a AND :b AND other = :c",
			want: []string{"pk = :pk", "sk BETWEEN :a AND :b", "other = :c"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := dynamoattr.SplitANDConditions(tc.expr)
			assert.Empty(t, cmp.Diff(tc.want, got), "SplitANDConditions() mismatch")
		})
	}
}

func TestToString(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   any
		want string
	}{
		{"bool true", true, "true"},
		{"float64", float64(1.25), "1.25"},
		{"int", int(2), "2"},
		{"int32", int32(3), "3"},
		{"nil", nil, ""},
		{"badJSON", badJSON{A: "bad"}, "{bad}"},
		{"generic map", map[string]string{"foo": "bar"}, `{"foo":"bar"}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := dynamoattr.ToString(tc.in)
			assert.Equal(t, tc.want, got)
		})
	}
}
