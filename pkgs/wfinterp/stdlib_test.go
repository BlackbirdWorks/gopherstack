package wfinterp_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpressions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want any
		name string
		expr string
	}{
		{int64(7), "precedence", `1 + 2 * 3`},
		{int64(9), "parentheses", `(1 + 2) * 3`},
		{2.5, "float division", `5 / 2`},
		{int64(2), "floor division", `5 // 2`},
		{int64(-3), "floor division negative", `-5 // 2`},
		{int64(1), "remainder", `7 % 3`},
		{int64(-4), "unary minus", `-(2 + 2)`},
		{"ab", "string concat", `"a" + "b"`},
		{true, "string compare", `"a" < "b"`},
		{true, "int float equality", `1 == 1.0`},
		{false, "not equal", `1 != 1`},
		{true, "and or", `true and (false or true)`},
		{true, "not", `not(false)`},
		{true, "in list", `2 in [1, 2, 3]`},
		{true, "in map", `"k" in {"k": 1}`},
		{true, "not in", `not("z" in [1])`},
		{"x", "if", `if(1 < 2, "x", "y")`},
		{int64(5), "default null", `default(null, 5)`},
		{int64(3), "default value", `default(3, 5)`},
		{int64(3), "len list", `len([1, 2, 3])`},
		{int64(3), "len string", `len("héé")`},
		{[]any{"a", "b"}, "keys sorted", `keys({"b": 1, "a": 2})`},
		{int64(42), "int from string", `int("42")`},
		{3.0, "double from int", `double(3)`},
		{"3", "string from int", `string(3)`},
		{"int", "get_type int", `get_type(1)`},
		{"map", "get_type map", `get_type({})`},
		{[]any{"a", "b", "c"}, "text.split", `text.split("a,b,c", ",")`},
		{"ll", "text.substring", `text.substring("hello", 2, 4)`},
		{"hello", "text.substring clamps", `text.substring("hello", -10, 10)`},
		{[]any{int64(0), int64(2)}, "text.find_all", `text.find_all("00000", "00")`},
		{true, "text.match_regex", `text.match_regex("abc123", "[0-9]+")`},
		{"Goodbye World", "text.replace_all", `text.replace_all("Hello World", "Hello", "Goodbye")`},
		{"a-b", "text.replace_all_regex", `text.replace_all_regex("a1b", "[0-9]", "-")`},
		{"HELLO", "text.to_upper", `text.to_upper("hello")`},
		{"a%2Fb%20c", "text.url_encode", `text.url_encode("a/b c")`},
		{"a+b", "text.url_encode_plus", `text.url_encode_plus("a b")`},
		{"a b", "text.url_decode", `text.url_decode("a+b")`},
		{
			[]any{
				map[string]any{"index": int64(0), "match": "Hello"},
				map[string]any{"index": int64(5), "match": "World"},
			},
			"text.find_all_regex",
			`text.find_all_regex("HelloWorld", "Hello|World")`,
		},
		{"hi", "text encode decode", `text.decode(text.encode("hi", "UTF-8"))`},
		{map[string]any{"a": int64(1)}, "json.decode", `json.decode("{\"a\":1}")`},
		{`{"a":[1,2]}`, "json.encode_to_string", `json.encode_to_string({"a": [1, 2]})`},
		{"SGk=", "base64.encode", `base64.encode(text.encode("Hi"))`},
		{"Hi", "base64.decode", `text.decode(base64.decode("SGk="))`},
		{"pZGm1Av0IEBKARczz7exkNYsZb8LzaMrV7J32a2fFG4=", "hash sha256",
			`base64.encode(hash.compute_checksum(text.encode("Hello World"), "SHA256"))`},
		{"QidPcoCDkuWLV9co1+PA5RY+rwfRZdwDEF9iq74afRE=", "hash hmac",
			`base64.encode(hash.compute_hmac(text.encode("key"), text.encode("Hello World"), "SHA256"))`},
		{[]any{"z", int64(1), int64(2)}, "list.prepend", `list.prepend([1, 2], "z")`},
		{[]any{int64(1), int64(2), int64(3)}, "list.concat", `list.concat([1, 2], 3)`},
		{"world", "map.get", `map.get({"k": "world"}, "k")`},
		{nil, "map.get missing", `map.get({"k": 1}, "x")`},
		{int64(2), "map.get nested", `map.get({"a": {"b": 2}}, ["a", "b"])`},
		{map[string]any{"a": int64(1)}, "map.delete", `map.delete({"a": 1, "b": 2}, "b")`},
		{map[string]any{"a": int64(1), "b": int64(2)}, "map.merge", `map.merge({"a": 1}, {"b": 2})`},
		{map[string]any{"a": map[string]any{"x": int64(1), "y": int64(2)}}, "map.merge_nested",
			`map.merge_nested({"a": {"x": 1}}, {"a": {"y": 2}})`},
		{int64(3), "math.abs", `math.abs(-3)`},
		{int64(4), "math.floor", `math.floor(4.9)`},
		{int64(9), "math.max ints", `math.max(3, 9)`},
		{3.5, "math.max float", `math.max(3.5, 1)`},
		{int64(1), "math.min", `math.min(3, 1)`},
		{"2026-01-02T03:04:05.000000Z", "time.format", `time.format(1767323045)`},
		{"2024-07-12T14:50:33.000000+02:00", "time.format zone", `time.format(1720788633, "Europe/Paris")`},
		{float64(1715169660), "time.parse", `time.parse("2024-05-08T12:01:00.000000Z")`},
		{float64(1767323045), "sys.now uses the clock", `sys.now()`},
		{int64(7), "member of list element", `[{"a": 7}][0].a`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := run(t, "main:\n  steps:\n    - r:\n        return: '${"+tt.expr+"}'\n", nil, nil)
			require.NoError(t, r.err)
			assert.Equal(t, tt.want, r.value)
		})
	}
}

func TestExpressionErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expr    string
		wantTag string
	}{
		{"divide by zero", `1 / 0`, "ZeroDivisionError"},
		{"remainder by zero", `1 % 0`, "ZeroDivisionError"},
		{"add string and int", `"a" + 1`, "TypeError"},
		{"compare mixed", `"a" < 1`, "TypeError"},
		{"missing key", `{"a": 1}.b`, "KeyError"},
		{"list index out of range", `[1][3]`, "IndexError"},
		{"undefined variable", `nothing`, "KeyError"},
		{"bad int", `int("x")`, "ValueError"},
		{"len of int", `len(1)`, "TypeError"},
		{"non boolean and", `1 and true`, "TypeError"},
		{"bad base64", `base64.decode("%%%")`, "ValueError"},
		{"bad regex", `text.match_regex("a", "(")`, "ValueError"},
		{"bad hash", `hash.compute_checksum(text.encode("a"), "NOPE")`, "ValueError"},
		{"bad json", `json.decode("{")`, "ValueError"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := "main:\n  steps:\n    - t:\n        try:\n          assign: [{x: '${" + tt.expr +
				"}'}]\n        except:\n          as: e\n          steps:\n            - r:\n                return: ${e.tags}\n"
			r := run(t, src, nil, nil)
			require.NoError(t, r.err)

			tags, ok := r.value.([]any)
			require.True(t, ok, "%v", r.value)
			assert.Contains(t, tags, tt.wantTag)
		})
	}
}
