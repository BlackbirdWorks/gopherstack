package iot

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	sqlTestPayload = `{"color":"red","temperature":50,"nested":{"a":{"b":7},"arr":[10,20,30]},` +
		`"e":[{"n":"temperature","v":22.5},{"n":"light","v":135}],"enc":"eyJ0IjoxfQ==","on":true,"nul":null}`
	sqlVersionLatest = sqlVersion2016
)

func runSQL(sql, version, topic, payload string) (string, bool, error) {
	parsed, err := ParseRuleSQLVersion(sql, version)
	if err != nil {
		return "", false, err
	}

	msg := &ruleMessage{
		received: time.UnixMilli(1700000000123), topic: topic, clientID: "dev-1", account: "123456789012",
		payload: []byte(payload), original: []byte(payload),
	}

	out, ok := parsed.apply(msg)

	return string(out), ok, nil
}

func TestSQLProjection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sql     string
		version string
		payload string
		want    string
	}{
		{name: "star", sql: "SELECT * FROM 't/x'", want: sqlTestPayload},
		{name: "field", sql: "SELECT color FROM 't/x'", want: `{"color":"red"}`},
		{
			name: "alias",
			sql:  "SELECT color AS my_color, temperature AS f FROM 't/x'",
			want: `{"my_color":"red","f":50}`,
		},
		{
			name:    "star_plus_literal",
			sql:     "SELECT *, 15 AS speed FROM 'a/b'",
			payload: `{"c":1}`,
			want:    `{"c":1,"speed":15}`,
		},
		{name: "nested_path_alias", sql: "SELECT nested.a.b AS bee FROM 't/x'", want: `{"bee":7}`},
		{name: "nested_path_keeps_shape", sql: "SELECT nested.a.b FROM 't/x'", want: `{"nested":{"a":{"b":7}}}`},
		{name: "array_index", sql: "SELECT nested.arr[1] AS second FROM 't/x'", want: `{"second":20}`},
		{name: "dotted_alias_nests", sql: "SELECT color AS x.y FROM 't/x'", want: `{"x":{"y":"red"}}`},
		{name: "undefined_omitted", sql: "SELECT color, missing, nested.nope AS n FROM 't/x'", want: `{"color":"red"}`},
		{name: "null_kept", sql: "SELECT nul FROM 't/x'", want: `{"nul":null}`},
		{name: "null_literal", sql: "SELECT NULL AS n FROM 't/x'", want: `{"n":null}`},
		{name: "arithmetic", sql: "SELECT (temperature - 32) * 5 / 9 AS celsius, upper(color) AS c FROM 't/x'",
			want: `{"celsius":10,"c":"RED"}`},
		{name: "string_concat_plus", sql: "SELECT color + '-' + temperature AS s FROM 't/x'", want: `{"s":"red-50"}`},
		{name: "select_value_scalar", sql: "SELECT VALUE color FROM 't/x'", want: `"red"`},
		{name: "select_value_array_2016", sql: "SELECT VALUE nested.arr FROM 't/x'", want: `[10,20,30]`},
		{
			name:    "select_value_array_2015_undefined",
			sql:     "SELECT VALUE nested.arr FROM 't/x'",
			version: sqlVersion2015,
			want:    ``,
		},
		{name: "field_named_value", sql: "SELECT value FROM 't/x'", payload: `{"value":3}`, want: `{"value":3}`},
		{name: "object_literal", sql: `SELECT {"key-with-hyphen": color, "n": temperature} AS o FROM 't/x'`,
			want: `{"o":{"key-with-hyphen":"red","n":50}}`},
		{name: "array_literal", sql: "SELECT [1, color] AS a FROM 't/x'", want: `{"a":[1,"red"]}`},
		{
			name: "case_match",
			sql:  "SELECT CASE color WHEN 'green' THEN 'go' WHEN 'red' THEN 'stop' ELSE 'x' END AS i FROM 't/x'",
			want: `{"i":"stop"}`,
		},
		{
			name: "case_else",
			sql:  "SELECT CASE color WHEN 'green' THEN 'go' ELSE 'other' END AS i FROM 't/x'",
			want: `{"i":"other"}`,
		},
		{
			name: "case_no_else_undefined",
			sql:  "SELECT CASE color WHEN 'green' THEN 'go' END AS i FROM 't/x'",
			want: `{}`,
		},
		{name: "nested_select_values", sql: "SELECT (SELECT VALUE n FROM e) AS sensors FROM 't/x'",
			want: `{"sensors":["temperature","light"]}`},
		{name: "nested_select_where", sql: "SELECT (SELECT v FROM e WHERE n = 'temperature') AS t FROM 't/x'",
			want: `{"t":[{"v":22.5}]}`},
		{name: "nested_select_flatten", sql: "SELECT get((SELECT v FROM e WHERE n = 'light'), 0).v AS t FROM 't/x'",
			want: `{"t":135}`},
		{name: "whole_decimal_2016_is_int", sql: "SELECT d FROM 't/x'", payload: `{"d":10.0}`, want: `{"d":10}`},
		{name: "whole_decimal_2015_stays_decimal", sql: "SELECT d FROM 't/x'", version: sqlVersion2015,
			payload: `{"d":10.0}`, want: `{"d":10.0}`},
		{name: "non_json_star_passthrough", sql: "SELECT * FROM 't/x'", payload: "\x01\x02raw", want: "\x01\x02raw"},
		{
			name:    "non_json_field_undefined",
			sql:     "SELECT color, 1 AS one FROM 't/x'",
			payload: "\x01\x02raw",
			want:    `{"one":1}`,
		},
		{
			name:    "non_json_encode_roundtrip",
			sql:     "SELECT encode(*, 'base64') AS b FROM 't/x'",
			payload: "hi",
			want:    `{"b":"aGk="}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			payload := tt.payload
			if payload == "" {
				payload = sqlTestPayload
			}

			version := tt.version
			if version == "" {
				version = sqlVersionLatest
			}

			got, ok, err := runSQL(tt.sql, version, "t/x", payload)
			require.NoError(t, err)
			require.True(t, ok)

			if tt.want != "" && tt.want[0] == '{' || tt.want != "" && tt.want[0] == '[' {
				assert.JSONEq(t, tt.want, got)

				return
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSQLFunctions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
		want string
	}{
		{name: "topic", expr: "topic()", want: `"t/x/y"`},
		{name: "topic_segment", expr: "topic(2)", want: `"x"`},
		{name: "topic_out_of_range", expr: "topic(9)", want: ``},
		{name: "timestamp", expr: "timestamp()", want: `1700000000123`},
		{name: "clientid", expr: "clientid()", want: `"dev-1"`},
		{name: "accountid", expr: "accountid()", want: `"123456789012"`},
		{name: "sql_version", expr: "sql_version()", want: `"2016-03-23"`},
		{name: "abs", expr: "abs(-5)", want: `5`},
		{name: "abs_string", expr: "abs('-5.5')", want: `5.5`},
		{name: "round_half_up", expr: "round(1.5)", want: `2`},
		{name: "round_negative", expr: "round(-1.5)", want: `-2`},
		{name: "floor", expr: "floor(1.7)", want: `1`},
		{name: "ceil", expr: "ceil(1.2)", want: `2`},
		{name: "trunc", expr: "trunc(2.888, 2)", want: `2.88`},
		{name: "power", expr: "power(2, 5)", want: `32`},
		{name: "mod", expr: "mod(8, 3)", want: `2`},
		{name: "sign", expr: "sign(-7)", want: `-1`},
		{name: "nanvl_undefined", expr: "nanvl(missing, 3)", want: `3`},
		{name: "nanvl_value", expr: "nanvl(8, 3)", want: `8`},
		{name: "lower", expr: "lower('ABC')", want: `"abc"`},
		{name: "concat_strings", expr: "concat('he', 'is', 'man')", want: `"heisman"`},
		{name: "concat_mixed", expr: "concat(1, 'hello')", want: `"1hello"`},
		{name: "concat_arrays", expr: "concat([1, 2, 3], 4)", want: `[1,2,3,4]`},
		{name: "concat_empty", expr: "concat()", want: ``},
		{name: "substring_from", expr: "substring('012345', 2)", want: `"2345"`},
		{name: "substring_range", expr: "substring('012345', 1, 3)", want: `"12"`},
		{name: "substring_clamped", expr: "substring('012345', -50, 50)", want: `"012345"`},
		{name: "substring_fraction", expr: "substring('012345', 2.745)", want: `"2345"`},
		{name: "substring_bool", expr: "substring(true, 1.2)", want: `"rue"`},
		{name: "substring_reversed", expr: "substring('012345', 3, 1)", want: `""`},
		{name: "indexof", expr: "indexof('abcd', 'bc')", want: `1`},
		{name: "length", expr: "length(false)", want: `5`},
		{name: "numbytes", expr: "numbytes('é')", want: `2`},
		{name: "trim", expr: "trim('  hi ')", want: `"hi"`},
		{name: "lpad", expr: "lpad('hello', 2)", want: `"  hello"`},
		{name: "replace", expr: "replace('abcdabcd', 'b', 'x')", want: `"axcdaxcd"`},
		{name: "regexp_matches", expr: "regexp_matches('aaaa', 'a{2,}')", want: `true`},
		{name: "regexp_replace", expr: "regexp_replace('abcd', 'b(.*)d', '$1')", want: `"ac"`},
		{name: "regexp_substr", expr: "regexp_substr('hihihello', '(hi)*')", want: `"hihi"`},
		{name: "startswith", expr: "startswith('ranger', 'ran')", want: `true`},
		{name: "endswith_null_undefined", expr: "endswith(nul, 'a')", want: ``},
		{name: "chr", expr: "chr(65)", want: `"A"`},
		{name: "cast_bool_to_int", expr: "cast(true AS Int)", want: `1`},
		{name: "cast_to_string", expr: "cast(12 AS String)", want: `"12"`},
		{name: "cast_to_boolean", expr: "cast(1 AS Boolean)", want: `true`},
		{name: "cast_string_to_decimal", expr: "cast('5E-1' AS Decimal)", want: `0.5`},
		{name: "get_array", expr: "get(nested.arr, 2)", want: `30`},
		{name: "get_object", expr: "get(nested, 'arr')", want: `[10,20,30]`},
		{name: "get_string", expr: "get('abc', 0)", want: `"a"`},
		{name: "get_out_of_range", expr: "get(nested.arr, 9)", want: ``},
		{name: "get_or_default", expr: "get_or_default(missing, 'd')", want: `"d"`},
		{name: "is_undefined_true", expr: "isUndefined(missing)", want: `true`},
		{name: "is_undefined_false", expr: "isUndefined(color)", want: `false`},
		{name: "is_null", expr: "isNull(nul)", want: `true`},
		{name: "encode_value", expr: "encode(color, 'base64')", want: `"cmVk"`},
		{name: "decode_json", expr: "decode(enc, 'base64').t", want: `1`},
		{name: "decode_string", expr: "decode('cmVk', 'base64')", want: `"red"`},
		{name: "decode_bad_is_null", expr: "decode('!!', 'base64')", want: `null`},
		{name: "md5", expr: "md5('hello')", want: `"5d41402abc4b2a76b9719d911017c592"`},
		{name: "sha256_len", expr: "length(sha256('hello'))", want: `64`},
		{name: "wrong_arity_undefined", expr: "abs()", want: ``},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok, err := runSQL("SELECT VALUE "+tt.expr+" FROM 't/#'", sqlVersionLatest, "t/x/y", sqlTestPayload)
			require.NoError(t, err)
			require.True(t, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSQLWhere(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		where   string
		version string
		want    bool
	}{
		{name: "gt_true", where: "temperature > 40", want: true},
		{name: "gt_false", where: "temperature > 60", want: false},
		{name: "lte", where: "temperature <= 50", want: true},
		{name: "neq_bang", where: "color != 'blue'", want: true},
		{name: "neq_angle", where: "color <> 'red'", want: false},
		{name: "and", where: "temperature > 40 AND color = 'red'", want: true},
		{name: "and_false", where: "temperature > 60 AND color = 'red'", want: false},
		{name: "or", where: "temperature > 60 OR color = 'red'", want: true},
		{name: "not", where: "NOT (temperature > 60)", want: true},
		{name: "paren_precedence", where: "(temperature > 60 OR color = 'red') AND on", want: true},
		{name: "bool_field", where: "on = true", want: true},
		{name: "arithmetic", where: "temperature * 2 = 100", want: true},
		{name: "nested_field", where: "nested.a.b = 7", want: true},
		{name: "array_index", where: "nested.arr[0] = 10", want: true},
		{name: "string_number_coercion", where: "'50' = temperature OR '50' >= temperature", want: true},
		{name: "in_array", where: "20 IN nested.arr", want: true},
		{name: "not_in_array", where: "21 IN nested.arr", want: false},
		{name: "exists_subquery", where: "exists (select * from nested.arr as a where a = 30)", want: true},
		{name: "exists_subquery_none", where: "exists (select * from nested.arr as a where a = 31)", want: false},
		{name: "function_in_where", where: "upper(color) = 'RED'", want: true},
		{name: "topic_in_where", where: "topic(2) = 'x'", want: true},
		{name: "undefined_field_is_false", where: "missing > 1", want: false},
		{name: "undefined_negated_still_false", where: "NOT (missing > 1)", want: false},
		{name: "missing_equals_is_false", where: "missing = 1", want: false},
		{name: "type_mismatch_equals_false", where: "color = 50", want: false},
		{name: "div_by_zero_undefined", where: "temperature / 0 > 1", want: false},
		{name: "2015_whole_decimal_equals", where: "temperature = 50.0", version: sqlVersion2015, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			version := tt.version
			if version == "" {
				version = sqlVersionLatest
			}

			_, ok, err := runSQL("SELECT * FROM 't/#' WHERE "+tt.where, version, "t/x", sqlTestPayload)
			require.NoError(t, err)
			assert.Equal(t, tt.want, ok)
		})
	}
}

func TestSQLWhereNonJSONPayload(t *testing.T) {
	t.Parallel()

	sql := "SELECT decode(encode(*, 'base64'), 'base64') AS value FROM 't/x' " +
		"WHERE decode(encode(*, 'base64'), 'base64') > 50"

	got, ok, err := runSQL(sql, sqlVersionLatest, "t/x", "80")
	require.NoError(t, err)
	require.True(t, ok)
	assert.JSONEq(t, `{"value":80}`, got)

	_, ok, err = runSQL(sql, sqlVersionLatest, "t/x", "10")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestSQLParseErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sql     string
		version string
	}{
		{name: "empty", sql: ""},
		{name: "junk_instead_of_from", sql: "SELECT * junk"},
		{name: "no_select", sql: "FROM 't/x'"},
		{name: "bad_topic_hash_middle", sql: "SELECT * FROM 'a/#/b'"},
		{name: "bad_topic_partial_wildcard", sql: "SELECT * FROM 'a/b+'"},
		{name: "unterminated_string", sql: "SELECT 'abc FROM 't'"},
		{name: "unknown_function", sql: "SELECT nosuchfn(1) FROM 't'"},
		{name: "unbalanced_paren", sql: "SELECT (1 + 2 FROM 't'"},
		{name: "trailing_garbage", sql: "SELECT * FROM 't' GROUP BY x"},
		{name: "dangling_operator", sql: "SELECT * FROM 't' WHERE a >"},
		{name: "case_without_when", sql: "SELECT CASE a END FROM 't'"},
		{name: "value_with_two_items", sql: "SELECT VALUE a, b FROM 't'"},
		{name: "bad_cast_type", sql: "SELECT cast(a AS blob) FROM 't'"},
		{name: "nested_select_2015", sql: "SELECT (SELECT VALUE n FROM e) AS s FROM 't'", version: sqlVersion2015},
		{name: "isundefined_2015", sql: "SELECT isUndefined(a) FROM 't'", version: sqlVersion2015},
		{name: "decimal_cast_2015", sql: "SELECT cast(a AS Decimal) FROM 't'", version: sqlVersion2015},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			version := tt.version
			if version == "" {
				version = sqlVersionLatest
			}

			_, err := ParseRuleSQLVersion(tt.sql, version)
			require.ErrorIs(t, err, ErrSQLParse)
		})
	}
}

func TestSQLTopicFilterForms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		sql   string
		topic string
		want  bool
	}{
		{name: "exact", sql: "SELECT * FROM 'a/b'", topic: "a/b", want: true},
		{name: "exact_other", sql: "SELECT * FROM 'a/b'", topic: "a/c", want: false},
		{name: "plus", sql: "SELECT * FROM 'a/+/c'", topic: "a/b/c", want: true},
		{name: "plus_one_level_only", sql: "SELECT * FROM 'a/+'", topic: "a/b/c", want: false},
		{name: "hash", sql: "SELECT * FROM 'a/#'", topic: "a/b/c/d", want: true},
		{name: "hash_all", sql: "SELECT * FROM '#'", topic: "x/y", want: true},
		{name: "bare", sql: "SELECT * FROM a/+/c WHERE 1 = 1", topic: "a/z/c", want: true},
		{name: "double_quoted", sql: `SELECT * FROM "a/b"`, topic: "a/b", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rule := &TopicRule{SQL: tt.sql, Enabled: true, AWSIoTSQLVersion: sqlVersion2016}
			assert.Equal(t, tt.want, EvaluateRule(rule, tt.topic, []byte(`{}`)))
		})
	}
}
