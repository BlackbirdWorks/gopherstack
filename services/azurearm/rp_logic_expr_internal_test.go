package azurearm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func logicTestCtx() *logicEvalCtx {
	return &logicEvalCtx{now: func() time.Time { return time.Date(2025, 9, 25, 5, 18, 31, 338479700, time.UTC) }}
}

func TestLogicFunctionsDocExamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want any
		name string
		expr string
	}{
		{name: "concat", expr: "@concat('Sophia', 'Owen')", want: "SophiaOwen"},
		{name: "concat mixed", expr: "@concat('a', 1, true)", want: "a1True"},
		{name: "endsWith true", expr: "@endsWith('hello world', 'world')", want: true},
		{name: "endsWith false", expr: "@endsWith('hello world', 'universe')", want: false},
		{name: "startsWith", expr: "@startsWith('hello world', 'hello')", want: true},
		{name: "startsWith false", expr: "@startsWith('hello world', 'greetings')", want: false},
		{name: "contains string", expr: "@contains('hello world', 'world')", want: true},
		{name: "contains string false", expr: "@contains('hello world', 'universe')", want: false},
		{name: "createArray", expr: "@createArray('h', 'e', 'l', 'l', 'o')", want: []any{"h", "e", "l", "l", "o"}},
		{name: "empty empty", expr: "@empty('')", want: true},
		{name: "empty abc", expr: "@empty('abc')", want: false},
		{name: "first string", expr: "@first('hello')", want: "h"},
		{name: "first array", expr: "@first(createArray(0, 1, 2))", want: int64(0)},
		{name: "last string", expr: "@last('abcd')", want: "d"},
		{name: "last array", expr: "@last(createArray(0, 1, 2, 3))", want: int64(3)},
		{name: "and tt", expr: "@and(true, true)", want: true},
		{name: "and ft", expr: "@and(false, true)", want: false},
		{name: "and nested", expr: "@and(equals(1, 1), equals(2, 2))", want: true},
		{name: "and nested false", expr: "@and(equals(1, 1), equals(1, 2))", want: false},
		{name: "or tf", expr: "@or(true, false)", want: true},
		{name: "or ff", expr: "@or(false, false)", want: false},
		{name: "or nested", expr: "@or(equals(1, 1), equals(1, 2))", want: true},
		{name: "not", expr: "@not(false)", want: true},
		{name: "not equals", expr: "@not(equals(1, 1))", want: false},
		{name: "equals bool int", expr: "@equals(true, 1)", want: true},
		{name: "equals strings", expr: "@equals('abc', 'abcd')", want: false},
		{name: "greater", expr: "@greater(10, 5)", want: true},
		{name: "greater strings", expr: "@greater('apple', 'banana')", want: false},
		{name: "greaterOrEquals", expr: "@greaterOrEquals(5, 5)", want: true},
		{name: "less", expr: "@less(5, 10)", want: true},
		{name: "less strings", expr: "@less('banana', 'apple')", want: false},
		{name: "lessOrEquals", expr: "@lessOrEquals(10, 10)", want: true},
		{name: "lessOrEquals strings", expr: "@lessOrEquals('apply', 'apple')", want: false},
		{name: "if", expr: "@if(equals(1, 1), 'yes', 'no')", want: "yes"},
		{name: "coalesce string", expr: "@coalesce(null, 'hello', 'world')", want: "hello"},
		{name: "coalesce bool", expr: "@coalesce(null, true, false)", want: true},
		{name: "coalesce null", expr: "@coalesce(null, null, null)", want: nil},
		{name: "base64", expr: "@base64('hello')", want: "aGVsbG8="},
		{name: "base64ToString", expr: "@base64ToString('aGVsbG8=')", want: "hello"},
		{name: "bool 1", expr: "@bool(1)", want: true},
		{name: "bool 0", expr: "@bool(0)", want: false},
		{name: "bool -1", expr: "@bool(-1)", want: true},
		{name: "bool true", expr: "@bool('true')", want: true},
		{name: "bool false", expr: "@bool('false')", want: false},
		{name: "float", expr: "@float('10,000.333')", want: 10000.333},
		{name: "add", expr: "@add(1, 1.5)", want: 2.5},
		{name: "add ints", expr: "@add(1, 2)", want: int64(3)},
		{name: "div ints", expr: "@div(11, 5)", want: int64(2)},
		{name: "div int by float", expr: "@div(11, 5.0)", want: 2.2},
		{name: "sub", expr: "@sub(10.3, .3)", want: int64(10)},
		{name: "mul", expr: "@mul(1, 2)", want: int64(2)},
		{name: "mul float", expr: "@mul(1.5, 2)", want: int64(3)},
		{name: "mod", expr: "@mod(3, 2)", want: int64(1)},
		{name: "mod negative", expr: "@mod(-5, 2)", want: int64(-1)},
		{name: "mod negative divisor", expr: "@mod(4, -3)", want: int64(1)},
		{name: "min", expr: "@min(1, 2, 3)", want: int64(1)},
		{name: "min array", expr: "@min(createArray(1, 2, 3))", want: int64(1)},
		{name: "max", expr: "@max(1, 2, 3)", want: int64(3)},
		{name: "max array", expr: "@max(createArray(1, 2, 3))", want: int64(3)},
		{name: "range", expr: "@range(1, 4)", want: []any{int64(1), int64(2), int64(3), int64(4)}},
		{name: "pow", expr: "@pow(2, 10)", want: 1024.0},
		{name: "indexOf", expr: "@indexOf('hello world', 'world')", want: int64(6)},
		{name: "int", expr: "@int('10')", want: int64(10)},
		{name: "isInt", expr: "@isInt('10')", want: true},
		{name: "isFloat", expr: "@isFloat('10,000.00')", want: true},
		{name: "json array", expr: "@json('[1, 2, 3]')", want: []any{int64(1), int64(2), int64(3)}},
		{
			name: "json object",
			expr: `@json('{"fullName": "Sophia Owen"}')`,
			want: map[string]any{"fullName": "Sophia Owen"},
		},
		{
			name: "intersection",
			expr: "@intersection(createArray(1, 2, 3), createArray(101, 2, 1, 10), createArray(6, 8, 1, 2))",
			want: []any{int64(1), int64(2)},
		},
		{name: "join", expr: "@join(createArray('a', 'b', 'c'), '.')", want: "a.b.c"},
		{name: "length string", expr: "@length('abcd')", want: int64(4)},
		{name: "length array", expr: "@length(createArray(0, 1, 2, 3))", want: int64(4)},
		{
			name: "reverse",
			expr: "@reverse(createArray(0, 1, 2, 3))",
			want: []any{int64(3), int64(2), int64(1), int64(0)},
		},
		{name: "replace", expr: "@replace('the old string', 'old', 'new')", want: "the new string"},
		{name: "skip", expr: "@skip(createArray(0, 1, 2, 3), 1)", want: []any{int64(1), int64(2), int64(3)}},
		{name: "sort", expr: "@sort(createArray(2, 1, 0, 3))", want: []any{int64(0), int64(1), int64(2), int64(3)}},
		{name: "split", expr: "@split('a_b_c', '_')", want: []any{"a", "b", "c"}},
		{name: "split no match", expr: "@split('a_b_c', ' ')", want: []any{"a_b_c"}},
		{name: "string int", expr: "@string(10)", want: "10"},
		{name: "string object", expr: `@string(json('{"name":"Sophie Owen"}'))`, want: `{"name":"Sophie Owen"}`},
		{name: "substring", expr: "@substring('hello world', 6, 5)", want: "world"},
		{name: "take string", expr: "@take('abcde', 3)", want: "abc"},
		{name: "take array", expr: "@take(createArray(0, 1, 2, 3, 4), 3)", want: []any{int64(0), int64(1), int64(2)}},
		{name: "toLower", expr: "@toLower('Hello World')", want: "hello world"},
		{name: "toUpper", expr: "@toUpper('Hello World')", want: "HELLO WORLD"},
		{name: "trim", expr: "@trim(' Hello World  ')", want: "Hello World"},
		{
			name: "union",
			expr: "@union(createArray(1, 2, 3), createArray(1, 2, 10, 101))",
			want: []any{int64(1), int64(2), int64(3), int64(10), int64(101)},
		},
		{name: "uriComponent", expr: "@uriComponent('https://contoso.com')", want: "https%3A%2F%2Fcontoso.com"},
		{
			name: "uriComponentToString",
			expr: "@uriComponentToString('https%3A%2F%2Fcontoso.com')",
			want: "https://contoso.com",
		},
		{name: "uriHost", expr: "@uriHost('https://www.localhost.com:8080')", want: "www.localhost.com"},
		{
			name: "uriPath",
			expr: "@uriPath('https://www.contoso.com/catalog/shownew.htm?date=today')",
			want: "/catalog/shownew.htm",
		},
		{
			name: "uriPathAndQuery",
			expr: "@uriPathAndQuery('https://www.contoso.com/catalog/shownew.htm?date=today')",
			want: "/catalog/shownew.htm?date=today",
		},
		{name: "uriPort", expr: "@uriPort('https://www.localhost:8080')", want: int64(8080)},
		{
			name: "uriQuery",
			expr: "@uriQuery('https://www.contoso.com/catalog/shownew.htm?date=today')",
			want: "?date=today",
		},
		{name: "uriScheme", expr: "@uriScheme('http://www.contoso.com/catalog/shownew.htm?date=today')", want: "http"},
		{
			name: "mergeObjects",
			expr: `@mergeObjects(json('{"a":1}'), json('{"b":2}'))`,
			want: map[string]any{"a": int64(1), "b": int64(2)},
		},
		{name: "setProperty", expr: `@setProperty(json('{"a":1}'), 'a', 5)`, want: map[string]any{"a": int64(5)}},
		{
			name: "removeProperty",
			expr: `@removeProperty(json('{"a":1,"b":2}'), 'a')`,
			want: map[string]any{"b": int64(2)},
		},
		{name: "property access", expr: `@json('{"a":{"b":[10,20]}}').a.b[1]`, want: int64(20)},
		{name: "safe access", expr: `@json('{"a":1}')?['missing']`, want: nil},
		{name: "string literal escape", expr: "@concat('it''s')", want: "it's"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := evalLogicTemplate(tc.expr, logicTestCtx())
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestLogicDateFunctionsDocExamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
		want string
	}{
		{name: "addDays", expr: "@addDays('2018-03-15T00:00:00Z', 10)", want: "2018-03-25T00:00:00.0000000Z"},
		{name: "addDays negative", expr: "@addDays('2018-03-15T00:00:00Z', -5)", want: "2018-03-10T00:00:00.0000000Z"},
		{name: "addHours", expr: "@addHours('2018-03-15T00:00:00Z', 10)", want: "2018-03-15T10:00:00.0000000Z"},
		{
			name: "addHours negative",
			expr: "@addHours('2018-03-15T15:00:00Z', -5)",
			want: "2018-03-15T10:00:00.0000000Z",
		},
		{name: "formatDateTime default", expr: "@formatDateTime('03/15/2018')", want: "2018-03-15T00:00:00.0000000"},
		{
			name: "formatDateTime custom",
			expr: "@formatDateTime('03/15/2018 12:00:00', 'yyyy-MM-ddTHH:mm:ss')",
			want: "2018-03-15T12:00:00",
		},
		{
			name: "formatDateTime day month",
			expr: "@formatDateTime('01/31/2016', 'dddd MMMM d')",
			want: "Sunday January 31",
		},
		{
			name: "formatDateTime fr",
			expr: "@formatDateTime('01/31/2016', 'dddd MMMM d', 'fr-fr')",
			want: "dimanche janvier 31",
		},
		{
			name: "formatDateTime fr upper",
			expr: "@formatDateTime('01/31/2016', 'dddd MMMM d', 'fr-FR')",
			want: "dimanche janvier 31",
		},
		{
			name: "formatDateTime es",
			expr: "@formatDateTime('01/31/2016', 'dddd MMMM d', 'es-es')",
			want: "domingo enero 31",
		},
		{name: "startOfDay", expr: "@startOfDay('2018-03-15T13:30:30Z')", want: "2018-03-15T00:00:00.0000000Z"},
		{name: "startOfHour", expr: "@startOfHour('2018-03-15T13:30:30Z')", want: "2018-03-15T13:00:00.0000000Z"},
		{name: "startOfMonth", expr: "@startOfMonth('2018-03-15T13:30:30Z')", want: "2018-03-01T00:00:00.0000000Z"},
		{name: "startOfMonth format", expr: "@startOfMonth('2018-03-15T13:30:30Z', 'yyyy-MM-dd')", want: "2018-03-01"},
		{
			name: "subtractFromTime",
			expr: "@subtractFromTime('2018-01-02T00:00:00Z', 1, 'Day')",
			want: "2018-01-01T00:00:00.0000000Z",
		},
		{name: "utcNow default", expr: "@utcNow()", want: "2025-09-25T05:18:31.3384797Z"},
		{name: "utcNow u", expr: "@utcNow('u')", want: "2025-09-25 05:18:31Z"},
		{name: "utcNow U", expr: "@utcNow('U')", want: "Thursday, September 25, 2025 5:18:31 AM"},
		{name: "utcNow d", expr: "@utcNow('d')", want: "9/25/2025"},
		{name: "utcNow D", expr: "@utcNow('D')", want: "Thursday, September 25, 2025"},
		{name: "utcNow t", expr: "@utcNow('t')", want: "5:18 AM"},
		{name: "utcNow T", expr: "@utcNow('T')", want: "5:18:31 AM"},
		{name: "utcNow y", expr: "@utcNow('y')", want: "September 2025"},
		{name: "utcNow yyyy", expr: "@utcNow('yyyy')", want: "2025"},
		{name: "utcNow custom", expr: "@utcNow('ddd, d MMM yyyy')", want: "Thu, 25 Sep 2025"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := evalLogicTemplate(tc.expr, logicTestCtx())
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestLogicGUIDFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		expr   string
		length int
	}{
		{name: "default", expr: "@guid()", length: 36},
		{name: "N", expr: "@guid('N')", length: 32},
		{name: "P", expr: "@guid('P')", length: 38},
		{name: "B", expr: "@guid('B')", length: 38},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := evalLogicTemplate(tc.expr, logicTestCtx())
			require.NoError(t, err)
			assert.Len(t, got, tc.length)
		})
	}
}

func TestLogicTemplates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want any
		name string
		in   string
	}{
		{name: "plain", in: "hello", want: "hello"},
		{name: "escaped at", in: "@@hello", want: "@hello"},
		{name: "interpolation", in: "a-@{add(1, 2)}-b", want: "a-3-b"},
		{name: "interpolation only is string", in: "@{add(1, 2)}", want: "3"},
		{name: "whole expression is typed", in: "@add(1, 2)", want: int64(3)},
		{name: "email stays literal", in: "user@example.com", want: "user@example.com"},
		{name: "brace inside string literal", in: "@{concat('}', 'x')}", want: "}x"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := evalLogicTemplate(tc.in, logicTestCtx())
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestLogicExpressionErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
	}{
		{name: "unknown function", expr: "@nope()"},
		{name: "unterminated string", expr: "@concat('a"},
		{name: "bad arity", expr: "@toLower()"},
		{name: "type mismatch compare", expr: "@greater('a', 1)"},
		{name: "divide by zero", expr: "@div(1, 0)"},
		{name: "index out of range", expr: "@createArray(1)[3]"},
		{name: "missing property", expr: `@json('{"a":1}')['b']`},
		{name: "workflow function outside run", expr: "@variables('x')"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := evalLogicTemplate(tc.expr, logicTestCtx())
			require.Error(t, err)
		})
	}
}
