package timestreamquery_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	tqsdk "github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDKQuery_CheckedValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		wantTypes string
		wantRow   string
	}{
		{"int division truncates", `SELECT -7 / 2, 7 / -2, -7 / -2`, "INTEGER,INTEGER,INTEGER", "-3|-3|3"},
		{
			"modulo follows dividend", `SELECT -7 % 2, 7 % -2, -7 % -2, mod(-7, 2)`,
			"INTEGER,INTEGER,INTEGER,INTEGER", "-1|1|-1|-1",
		},
		{"bigint division", `SELECT bigint '-9' / bigint '4', bigint '-9' % bigint '4'`, "BIGINT,BIGINT", "-2|-1"},
		{
			"double division by zero", `SELECT 1.0 / 0, -1.0 / 0, 0.0 / 0`,
			"DOUBLE,DOUBLE,DOUBLE", "Infinity|-Infinity|NaN",
		},
		{"double division of ints", `SELECT 7 / 2.0, CAST(7 AS double) / 2`, "DOUBLE,DOUBLE", "3.5|3.5"},
		{
			"cast rounds half away",
			`SELECT CAST(2.5 AS bigint), CAST(-2.5 AS bigint), CAST(2.4 AS integer), CAST(-2.6 AS integer)`,
			"BIGINT,BIGINT,INTEGER,INTEGER",
			"3|-3|2|-3",
		},
		{"try cast nulls", `SELECT TRY_CAST('x' AS bigint), TRY_CAST('x' AS integer), TRY_CAST('x' AS double), ` +
			`TRY_CAST('x' AS boolean), TRY_CAST('x' AS timestamp), TRY_CAST('x' AS date), TRY_CAST('1.5' AS bigint), ` +
			`TRY_CAST(3000000000 AS integer), TRY_CAST(1e30 AS bigint)`,
			"BIGINT,INTEGER,DOUBLE,BOOLEAN,TIMESTAMP,DATE,BIGINT,INTEGER,BIGINT",
			"null|null|null|null|null|null|null|null|null"},
		{
			"cast ok",
			`SELECT CAST('12' AS bigint), CAST('-5' AS integer), CAST('1.5' AS double), CAST('t' AS boolean), ` +
				`CAST('FALSE' AS boolean), CAST('2020-01-02' AS date)`,
			"BIGINT,INTEGER,DOUBLE,BOOLEAN,BOOLEAN,DATE",
			"12|-5|1.5|true|false|2020-01-02",
		},
		{
			"cast to varchar",
			`SELECT CAST(true AS varchar), CAST(1.0 AS varchar), CAST(1e10 AS varchar), CAST(5 AS varchar)`,
			"VARCHAR,VARCHAR,VARCHAR,VARCHAR",
			"true|1.0|1.0E10|5",
		},
		{"try swallows overflow", `SELECT try(9223372036854775807 + 1), try(1 / 0), try(abs(-9223372036854775807 - 1))`,
			"BIGINT,INTEGER,BIGINT", "null|null|null"},
		{
			"checked inside parens",
			`SELECT (2 + 3) * (4 - 1), -(5), abs(-5), abs(-5.5)`,
			"INTEGER,INTEGER,INTEGER,DOUBLE",
			"15|-5|5|5.5",
		},
		{
			"sum of bigint",
			`SELECT sum(n), sum(DISTINCT n % 2), sum(n) FILTER (WHERE n > 3) FROM UNNEST(SEQUENCE(1, 5)) AS t(n)`,
			"BIGINT,BIGINT,BIGINT", "15|1|9",
		},
		{"e and random", `SELECT e(), random(1)`, "DOUBLE,INTEGER", "2.718281828459045|0"},
		{"random bigint", `SELECT random(bigint '1')`, "BIGINT", "0"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			types, row := queryLabels(t, newSDKQueryClient(t), tc.query)
			assert.Equal(t, tc.wantTypes, types)
			assert.Equal(t, tc.wantRow, row)
		})
	}
}

func TestSDKQuery_CheckedErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"bigint add", `SELECT 9223372036854775807 + 1`, "bigint addition overflow: 9223372036854775807 + 1"},
		{"bigint sub", `SELECT -9223372036854775807 - 2`, "bigint subtraction overflow: -9223372036854775807 - 2"},
		{
			"bigint mul",
			`SELECT bigint '4611686018427387904' * 2`,
			"bigint multiplication overflow: 4611686018427387904 * 2",
		},
		{"bigint neg", `SELECT -(-9223372036854775807 - 1)`, "bigint negation overflow: -9223372036854775808"},
		{"bigint abs", `SELECT abs(-9223372036854775807 - 1)`, "out of range"},
		{"integer add", `SELECT 2147483647 + 1`, "integer addition overflow: 2147483647 + 1"},
		{"integer mul", `SELECT 65536 * 65536`, "integer multiplication overflow: 65536 * 65536"},
		{
			"sum overflow",
			`SELECT sum(n * 1152921504606846976) FROM UNNEST(SEQUENCE(1, 5)) AS t(n)`,
			"bigint addition overflow",
		},
		{"int div zero", `SELECT 1 / 0`, "Division by zero"},
		{"int mod zero", `SELECT 1 % 0`, "Division by zero"},
		{"cast text", `SELECT CAST('x' AS bigint)`, "Cannot cast 'x' to BIGINT"},
		{"cast text integer", `SELECT CAST('3000000000' AS integer)`, "Cannot cast '3000000000' to INTEGER"},
		{"cast double text", `SELECT CAST('x' AS double)`, "Cannot cast 'x' to DOUBLE"},
		{"cast bool text", `SELECT CAST('x' AS boolean)`, "Cannot cast 'x' to BOOLEAN"},
		{"cast ts text", `SELECT CAST('x' AS timestamp)`, "Value cannot be cast to timestamp: x"},
		{"cast bigint range", `SELECT CAST(3000000000 AS integer)`, "Out of range for integer: 3000000000"},
		{"cast double range", `SELECT CAST(1e30 AS bigint)`, "Out of range for bigint: 1.0E30"},
		{"cast nan", `SELECT CAST(0.0 / 0 AS bigint)`, "NaN"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := newSDKQueryClient(t).Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(tc.query)})
			require.Error(t, err)

			var ve *types.ValidationException

			require.ErrorAs(t, err, &ve)
			assert.Contains(t, aws.ToString(ve.Message), tc.want)
		})
	}
}

func TestSDKQuery_StaticLambdaAndFunctionTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		wantTypes string
		wantRow   string
	}{
		{
			"zip with", `SELECT zip_with(ARRAY[1, 2], ARRAY[3, 4], (a, b) -> a + b)`,
			"ARRAY(INTEGER)", "[4,6]",
		},
		{
			"zip with mixed", `SELECT zip_with(ARRAY[1], ARRAY['x'], (a, b) -> CAST(a AS varchar) || b)`,
			"ARRAY(VARCHAR)", "[1x]",
		},
		{"transform", `SELECT transform(ARRAY[1, 2], x -> x * 2.5)`, "ARRAY(DOUBLE)", "[2.5,5.0]"},
		{"transform cast", `SELECT transform(ARRAY[1, 2], x -> CAST(x AS bigint))`, "ARRAY(BIGINT)", "[1,2]"},
		{
			"match functions",
			`SELECT any_match(ARRAY[1, 2], x -> x > 1), all_match(ARRAY[1, 2], x -> x > 1), ` +
				`none_match(ARRAY[1], x -> x > 5)`,
			"BOOLEAN,BOOLEAN,BOOLEAN", "true|false|true",
		},
		{
			"reduce output function", `SELECT reduce(ARRAY[1, 2, 3], 0, (s, x) -> s + x, s -> s * 2)`,
			"INTEGER", "12",
		},
		{"codepoint", `SELECT codepoint('a')`, "INTEGER", "97"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			types, row := queryLabels(t, newSDKQueryClient(t), tc.query)
			assert.Equal(t, tc.wantTypes, types)
			assert.Equal(t, tc.wantRow, row)
		})
	}
}

func TestSDKQuery_RowSubscriptTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		wantTypes string
		wantRow   string
	}{
		{"anonymous row", `SELECT ROW(1, 'a')[2]`, "VARCHAR", "a"},
		{"first field", `SELECT ROW(1, 'a')[1], ROW(2.5, true)[2]`, "INTEGER,BOOLEAN", "1|true"},
		{"named row cast", `SELECT CAST(ROW(1, 2) AS ROW(a bigint, b bigint))[2]`, "BIGINT", "2"},
		{"array of rows", `SELECT ARRAY[ROW(1, 'a')][1][2]`, "VARCHAR", "a"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			types, row := queryLabels(t, newSDKQueryClient(t), tc.query)
			assert.Equal(t, tc.wantTypes, types)
			assert.Equal(t, tc.wantRow, row)
		})
	}
}
