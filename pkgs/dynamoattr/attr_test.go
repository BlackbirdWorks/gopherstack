package dynamoattr_test

import (
	"errors"
	"reflect"
	"testing"

	"Gopherstack/pkgs/dynamoattr"
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
		{map[string]any{"S": "v"}, "v", "string"},
		{map[string]any{"N": "1"}, "1", "number"},
		{map[string]any{"B": "b"}, "b", "binary"},
		{map[string]any{"BOOL": true}, true, "bool"},
		{map[string]any{"NULL": true}, nil, "null"},
		{map[string]any{"M": map[string]any{"k": "v"}}, map[string]any{"k": "v"}, "map"},
		{map[string]any{"L": []any{"x"}}, []any{"x"}, "list"},
		{map[string]any{"SS": []string{"a"}}, []string{"a"}, "ss"},
		{map[string]any{"NS": []string{"1"}}, []string{"1"}, "ns"},
		{map[string]any{"BS": []string{"b"}}, []string{"b"}, "bs"},
		{"x", "x", "plain"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := dynamoattr.UnwrapAttributeValue(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
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
		{float64(1.5), "float64", 1.5, true},
		{int(2), "int", 2, true},
		{int64(3), "int64", 3, true},
		{"4.5", "string", 4.5, true},
		{map[string]any{"N": "6"}, "wrapped", 6, true},
		{"nope", "bad", 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := dynamoattr.ParseNumeric(tc.in)
			if ok != tc.okWant {
				t.Fatalf("expected ok=%v, got %v", tc.okWant, ok)
			}
			if ok && got != tc.want {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
		})
	}
}

func TestResolveValue(t *testing.T) {
	t.Parallel()

	attrs := map[string]any{":a": "value"}

	if got := dynamoattr.ResolveValue(":a", attrs); got != "value" {
		t.Fatalf("expected value, got %v", got)
	}

	if got := dynamoattr.ResolveValue(":missing", attrs); got != ":missing" {
		t.Fatalf("expected :missing, got %v", got)
	}

	if got := dynamoattr.ResolveValue("plain", attrs); got != "plain" {
		t.Fatalf("expected plain, got %v", got)
	}
}

func TestCompareValues(t *testing.T) {
	t.Parallel()

	if !dynamoattr.CompareValues(map[string]any{"N": "2"}, "<", map[string]any{"N": "3"}) {
		t.Fatalf("expected numeric comparison true")
	}

	if dynamoattr.CompareValues("b", "<", "a") {
		t.Fatalf("expected string comparison false")
	}

	if !dynamoattr.CompareValues("a", "=", "a") {
		t.Fatalf("expected equality true")
	}

	if dynamoattr.CompareValues("a", "<>", "a") {
		t.Fatalf("expected inequality false")
	}

	if dynamoattr.CompareValues("a", "?", "b") {
		t.Fatalf("expected unknown operator false")
	}
}

func TestSplitANDConditions(t *testing.T) {
	t.Parallel()

	expr := "pk = :pk AND sk BETWEEN :a AND :b AND other = :c"
	parts := dynamoattr.SplitANDConditions(expr)
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}

	if parts[1] != "sk BETWEEN :a AND :b" {
		t.Fatalf("expected BETWEEN clause, got %q", parts[1])
	}
}

func TestToString(t *testing.T) {
	t.Parallel()

	if got := dynamoattr.ToString(true); got != "true" {
		t.Fatalf("expected true, got %q", got)
	}
	if got := dynamoattr.ToString(float64(1.25)); got != "1.25" {
		t.Fatalf("expected 1.25, got %q", got)
	}
	if got := dynamoattr.ToString(int(2)); got != "2" {
		t.Fatalf("expected 2, got %q", got)
	}
	if got := dynamoattr.ToString(nil); got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
	if got := dynamoattr.ToString(badJSON{A: "bad"}); got != "{bad}" {
		t.Fatalf("expected {bad}, got %q", got)
	}
}
