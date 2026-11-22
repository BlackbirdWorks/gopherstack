package athena_test

import (
	"testing"

	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

const seriesData = `{"g":"a","x":1,"y":3}
{"g":"a","x":2,"y":5}
{"g":"a","x":3,"y":7}
{"g":"b","x":4,"y":9}
{"g":"b","x":12,"y":25}
`

func seriesClient(t *testing.T) *athenasdk.Client {
	t.Helper()

	return sdkClientFor(t, map[string][]byte{"lake/series/a.json": []byte(seriesData)},
		map[string]*athena.GlueTableDef{"shop/series": {
			Name: "series", Location: "s3://lake/series/", SerdeLibrary: serdeJSON,
			Columns: []athena.Column{
				{Name: "g", Type: "string"}, {Name: "x", Type: "int"}, {Name: "y", Type: "int"},
			},
		}})
}

func TestSDK_TrinoCollectionFunctions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		typ  string
		want string
	}{
		{"sequence up", "SELECT sequence(1, 5)", "array", "[1, 2, 3, 4, 5]"},
		{"sequence down", "SELECT sequence(5, 1)", "array", "[5, 4, 3, 2, 1]"},
		{"sequence step", "SELECT sequence(1, 10, 3)", "array", "[1, 4, 7, 10]"},
		{"sequence negative step", "SELECT sequence(10, 1, -4)", "array", "[10, 6, 2]"},
		{"sequence single", "SELECT sequence(3, 3)", "array", "[3]"},
		{"sequence null", "SELECT sequence(1, NULL)", "varchar", ""},
		{
			"sequence dates", "SELECT sequence(DATE '2024-01-01', DATE '2024-01-03')", "array",
			"[2024-01-01, 2024-01-02, 2024-01-03]",
		},
		{
			"sequence month steps",
			"SELECT sequence(DATE '2024-01-31', DATE '2024-04-30', INTERVAL '1' MONTH)",
			"array",
			"[2024-01-31, 2024-02-29, 2024-03-31, 2024-04-30]",
		},
		{
			"sequence day interval", "SELECT sequence(DATE '2024-01-01', DATE '2024-01-07', INTERVAL '3' DAY)", "array",
			"[2024-01-01, 2024-01-04, 2024-01-07]",
		},
		{
			"sequence timestamps",
			"SELECT sequence(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 02:00:00', INTERVAL '1' HOUR)",
			"array", "[2024-01-01 00:00:00.000, 2024-01-01 01:00:00.000, 2024-01-01 02:00:00.000]",
		},
		{"repeat text", "SELECT repeat('a', 3)", "array", "[a, a, a]"},
		{"repeat zero", "SELECT repeat(1, 0)", "array", "[]"},
		{"repeat array", "SELECT repeat(ARRAY[1], 2)", "array", "[[1], [1]]"},
		{"repeat null count", "SELECT repeat(1, NULL)", "varchar", ""},
		{"zip pads", "SELECT zip(ARRAY[1, 2], ARRAY['a', 'b', 'c'])", "array", "[[1, a], [2, b], [null, c]]"},
		{"zip three", "SELECT zip(ARRAY[1], ARRAY[2], ARRAY[3])", "array", "[[1, 2, 3]]"},
		{"array_union", "SELECT array_union(ARRAY[1, 2, 3], ARRAY[3, 4, 4])", "array", "[1, 2, 3, 4]"},
		{"array_union dedupes", "SELECT array_union(ARRAY[1, 1], ARRAY[2, 2])", "array", "[1, 2]"},
		{"array_intersect", "SELECT array_intersect(ARRAY[1, 2, 2, 3], ARRAY[2, 3, 4])", "array", "[2, 3]"},
		{"array_except", "SELECT array_except(ARRAY[1, 2, 3, 3], ARRAY[2])", "array", "[1, 3]"},
		{"array_except text", "SELECT array_except(ARRAY['a', 'b'], ARRAY['b'])", "array", "[a]"},
		{"arrays_overlap true", "SELECT arrays_overlap(ARRAY[1, 2], ARRAY[2, 3])", "boolean", "true"},
		{"arrays_overlap false", "SELECT arrays_overlap(ARRAY[1], ARRAY[2])", "boolean", "false"},
		{"arrays_overlap null element", "SELECT arrays_overlap(ARRAY[1, NULL], ARRAY[2])", "boolean", ""},
		{"shuffle keeps elements", "SELECT array_sort(shuffle(ARRAY[3, 1, 2, 4]))", "array", "[1, 2, 3, 4]"},
		{"shuffle cardinality", "SELECT cardinality(shuffle(ARRAY[1, 2, 3]))", "bigint", "3"},
		{"array_first", "SELECT array_first(ARRAY[7, 8])", "bigint", "7"},
		{"array_last", "SELECT array_last(ARRAY[7, 8])", "bigint", "8"},
		{"flatten", "SELECT flatten(ARRAY[ARRAY[1, 2], ARRAY[3]])", "array", "[1, 2, 3]"},
		{"trim_array", "SELECT trim_array(ARRAY[1, 2, 3, 4], 2)", "array", "[1, 2]"},
		{"contains_sequence", "SELECT contains_sequence(ARRAY[1, 2, 3, 4], ARRAY[2, 3])", "boolean", "true"},
		{"contains_sequence gap", "SELECT contains_sequence(ARRAY[1, 2, 3, 4], ARRAY[2, 4])", "boolean", "false"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := sdkRun(t, docsClient(t), tc.sql)
			assert.Equal(t, [][]string{{tc.want}}, got.rows)
			assert.Equal(t, []string{tc.typ}, got.types)
		})
	}
}

func TestSDK_TrinoNumericFunctions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		typ  string
		want string
	}{
		{"from_base hex", "SELECT from_base('ff', 16)", "bigint", "255"},
		{"from_base upper", "SELECT from_base('FF', 16)", "bigint", "255"},
		{"from_base binary", "SELECT from_base('1011', 2)", "bigint", "11"},
		{"from_base negative", "SELECT from_base('-z', 36)", "bigint", "-35"},
		{"to_base hex", "SELECT to_base(255, 16)", "varchar", "ff"},
		{"to_base binary", "SELECT to_base(5, 2)", "varchar", "101"},
		{"to_base negative", "SELECT to_base(-255, 16)", "varchar", "-ff"},
		{"to_base null", "SELECT to_base(NULL, 2)", "varchar", ""},
		{"bit_count", "SELECT bit_count(9, 64)", "bigint", "2"},
		{"bit_count negative", "SELECT bit_count(-7, 64)", "bigint", "62"},
		{"bit_count narrow", "SELECT bit_count(-7, 8)", "bigint", "6"},
		{"bitwise_not", "SELECT bitwise_not(0)", "bigint", "-1"},
		{"bitwise_not value", "SELECT bitwise_not(5)", "bigint", "-6"},
		{"bitwise_left_shift", "SELECT bitwise_left_shift(1, 4)", "bigint", "16"},
		{"bitwise_right_shift", "SELECT bitwise_right_shift(16, 2)", "bigint", "4"},
		{"bitwise_right_shift negative", "SELECT bitwise_right_shift(-1, 63)", "bigint", "1"},
		{"bitwise_right_shift_arithmetic", "SELECT bitwise_right_shift_arithmetic(-16, 2)", "bigint", "-4"},
		{"hamming_distance", "SELECT hamming_distance('karolin', 'kathrin')", "bigint", "3"},
		{"hamming_distance equal", "SELECT hamming_distance('abc', 'abc')", "bigint", "0"},
		{"infinity", "SELECT infinity()", "double", "Infinity"},
		{"negative infinity", "SELECT -infinity()", "double", "-Infinity"},
		{"e", "SELECT e()", "double", "2.718281828459045"},
		{"is_infinite", "SELECT is_infinite(infinity())", "boolean", "true"},
		{"is_infinite finite", "SELECT is_infinite(1.5)", "boolean", "false"},
		{"is_nan of nan", "SELECT is_nan(nan())", "boolean", "true"},
		{"year_of_week", "SELECT year_of_week(DATE '2021-01-03')", "bigint", "2020"},
		{"yow", "SELECT yow(DATE '2024-12-30')", "bigint", "2025"},
		{"yow timestamp", "SELECT yow(TIMESTAMP '2024-12-30 10:00:00')", "bigint", "2025"},
		{"width_bucket", "SELECT width_bucket(5.5, 0, 10, 4)", "bigint", "3"},
		{"width_bucket below", "SELECT width_bucket(-1, 0, 10, 4)", "bigint", "0"},
		{"width_bucket above", "SELECT width_bucket(10, 0, 10, 4)", "bigint", "5"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := sdkRun(t, docsClient(t), tc.sql)
			assert.Equal(t, [][]string{{tc.want}}, got.rows)
			assert.Equal(t, []string{tc.typ}, got.types)
		})
	}
}

func TestSDK_TrinoStringFunctions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		typ  string
		want string
	}{
		{"ends_with", "SELECT ends_with('hello', 'llo')", "boolean", "true"},
		{"ends_with false", "SELECT ends_with('hello', 'he')", "boolean", "false"},
		{"translate", "SELECT translate('abcd', 'ac', 'xy')", "varchar", "xbyd"},
		{"translate delete", "SELECT translate('abcd', 'abc', 'x')", "varchar", "xd"},
		{"url_encode", "SELECT url_encode('a b&c')", "varchar", "a+b%26c"},
		{"url_decode", "SELECT url_decode('a+b%26c')", "varchar", "a b&c"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := sdkRun(t, docsClient(t), tc.sql)
			assert.Equal(t, [][]string{{tc.want}}, got.rows)
			assert.Equal(t, []string{tc.typ}, got.types)
		})
	}
}

func TestSDK_TrinoStatisticalAggregates(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		typ  string
		want [][]string
	}{
		{"bitwise_and_agg", "SELECT bitwise_and_agg(x) FROM series", "bigint", [][]string{{"0"}}},
		{"bitwise_or_agg", "SELECT bitwise_or_agg(x) FROM series", "bigint", [][]string{{"15"}}},
		{"bitwise_xor_agg", "SELECT bitwise_xor_agg(x) FROM series", "bigint", [][]string{{"8"}}},
		{
			"bitwise group",
			"SELECT bitwise_or_agg(x) FROM series GROUP BY g ORDER BY g",
			"bigint",
			[][]string{{"3"}, {"12"}},
		},
		{
			"bitwise filter", "SELECT bitwise_and_agg(x) FILTER (WHERE g = 'b') FROM series", "bigint",
			[][]string{{"4"}},
		},
		{"bitwise distinct", "SELECT bitwise_xor_agg(DISTINCT x) FROM series", "bigint", [][]string{{"8"}}},
		{"bitwise empty", "SELECT bitwise_or_agg(x) FROM series WHERE g = 'z'", "varchar", [][]string{{""}}},
		{"regr_slope", "SELECT round(regr_slope(y, x), 6) FROM series WHERE g = 'a'", "double", [][]string{{"2.0"}}},
		{
			"regr_intercept",
			"SELECT round(regr_intercept(y, x), 6) FROM series WHERE g = 'a'",
			"double",
			[][]string{{"1.0"}},
		},
		{
			"regr group", "SELECT round(regr_slope(y, x), 6) FROM series GROUP BY g ORDER BY g", "double",
			[][]string{{"2.0"}, {"2.0"}},
		},
		{"regr no variance", "SELECT regr_slope(y, x) FROM series WHERE x = 1", "varchar", [][]string{{""}}},
		{"skewness", "SELECT round(skewness(x), 6) FROM series", "double", [][]string{{"1.882335"}}},
		{"skewness too few", "SELECT skewness(x) FROM series WHERE g = 'b'", "varchar", [][]string{{""}}},
		{"kurtosis", "SELECT round(kurtosis(x), 6) FROM series", "double", [][]string{{"3.768423"}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := sdkRun(t, seriesClient(t), tc.sql)
			assert.Equal(t, tc.want, got.rows)
			assert.Equal(t, []string{tc.typ}, got.types)
		})
	}
}

func TestSDK_TrinoFunctionErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		sql    string
		reason string
	}{
		{"sequence zero step", "SELECT sequence(1, 5, 0)", "step must not be zero"},
		{
			"sequence wrong direction", "SELECT sequence(1, 5, -1)",
			"sequence stop value should be greater than or equal to start value if step is greater than zero " +
				"otherwise stop should be less than or equal to start",
		},
		{
			"sequence too long", "SELECT sequence(1, 10001)",
			"result of sequence function must not have more than 10000 entries",
		},
		{
			"sequence date hour step", "SELECT sequence(DATE '2024-01-01', DATE '2024-01-03', INTERVAL '1' HOUR)",
			"sequence step must be a day interval if start and end values are dates",
		},
		{
			"repeat negative",
			"SELECT repeat(1, -1)",
			"count argument of repeat function must be greater than or equal to 0",
		},
		{
			"repeat too many", "SELECT repeat(1, 10001)",
			"count argument of repeat function must be less than or equal to 10000",
		},
		{"from_base invalid", "SELECT from_base('zz', 16)", "Not a valid base-16 number: zz"},
		{"from_base radix", "SELECT from_base('1', 37)", "Radix must be between 2 and 36"},
		{"to_base radix", "SELECT to_base(1, 1)", "Radix must be between 2 and 36"},
		{"bit_count bits", "SELECT bit_count(1, 1)", "Bits specified in bit_count must be between 2 and 64, got 1"},
		{
			"bit_count range", "SELECT bit_count(7, 3)",
			"Number must be representable with the bits specified. 7 can not be represented with 3 bits",
		},
		{
			"hamming length", "SELECT hamming_distance('a', 'bb')",
			"The input strings to hamming_distance function must have the same length",
		},
		{"zip arity", "SELECT zip(ARRAY[1])", "Unexpected parameters for function zip"},
		{"trim_array too large", "SELECT trim_array(ARRAY[1], 2)", "size must not exceed array length"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reason := sdkFailure(t, docsClient(t), tc.sql)
			assert.Contains(t, reason, tc.reason)
		})
	}
}

func TestSDK_TimestreamOnlyFunctionsAreNotRegistered(t *testing.T) {
	t.Parallel()

	fns := []string{
		"strrpos('a', 'a')",
		"from_milliseconds(1)",
		"from_nanoseconds(1)",
		"to_nanoseconds(1)",
		"nanosecond(1)",
	}

	for _, fn := range fns {
		t.Run(fn, func(t *testing.T) {
			t.Parallel()

			assert.Contains(t, sdkFailure(t, docsClient(t), "SELECT "+fn), "not registered")
		})
	}
}

func TestSDK_TrinoNaN(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		want string
	}{
		{"nan renders", "SELECT nan()", "NaN"},
		{"is_finite nan", "SELECT is_finite(nan())", "false"},
		{"is_infinite nan", "SELECT is_infinite(nan())", "false"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, [][]string{{tc.want}}, sdkRun(t, docsClient(t), tc.sql).rows)
		})
	}
}
