package athena_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	tyYM = "interval year to month"
	tyDT = "interval day to second"
)

func TestSDK_IntervalValues(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		typ  string
		want string
	}{
		{"year to month", "SELECT INTERVAL '1-2' YEAR TO MONTH", tyYM, "1-2"},
		{"months only", "SELECT INTERVAL '3' MONTH", tyYM, "0-3"},
		{"years", "SELECT INTERVAL '2' YEAR", tyYM, "2-0"},
		{"negative year to month", "SELECT INTERVAL '-1-2' YEAR TO MONTH", tyYM, "-1-2"},
		{"days", "SELECT INTERVAL '2' DAY", tyDT, "2 00:00:00.000"},
		{"day to second", "SELECT INTERVAL '1 02:03:04' DAY TO SECOND", tyDT, "1 02:03:04.000"},
		{"fractional seconds", "SELECT INTERVAL '1 02:03:04.5' DAY TO SECOND", tyDT, "1 02:03:04.500"},
		{"hour to minute", "SELECT INTERVAL '5:30' HOUR TO MINUTE", tyDT, "0 05:30:00.000"},
		{"minute to second", "SELECT INTERVAL '2:15.25' MINUTE TO SECOND", tyDT, "0 00:02:15.250"},
		{"negative minutes", "SELECT INTERVAL '-90' MINUTE", tyDT, "-0 01:30:00.000"},
		{"sign outside literal", "SELECT INTERVAL - '1' DAY", tyDT, "-1 00:00:00.000"},
		{"sum", "SELECT INTERVAL '1' DAY + INTERVAL '2' HOUR", tyDT, "1 02:00:00.000"},
		{"difference", "SELECT INTERVAL '1' YEAR - INTERVAL '3' MONTH", tyYM, "0-9"},
		{"times integer", "SELECT INTERVAL '1' DAY * 2", tyDT, "2 00:00:00.000"},
		{"integer times", "SELECT 2 * INTERVAL '1' HOUR", tyDT, "0 02:00:00.000"},
		{"times double", "SELECT INTERVAL '1-0' YEAR TO MONTH * 1.5", tyYM, "1-6"},
		{"divided", "SELECT INTERVAL '1' DAY / 2", tyDT, "0 12:00:00.000"},
		{"precedence", "SELECT INTERVAL '1' HOUR + INTERVAL '30' MINUTE * 2", tyDT, "0 02:00:00.000"},
		{"to_milliseconds", "SELECT to_milliseconds(INTERVAL '1' DAY + INTERVAL '1' SECOND)", "bigint", "86401000"},
		{"parse_duration", "SELECT parse_duration('1.5h')", tyDT, "0 01:30:00.000"},
		{"parse_duration unit", "SELECT parse_duration('250ms')", tyDT, "0 00:00:00.250"},
		{"cast to varchar", "SELECT CAST(INTERVAL '1' DAY AS varchar)", "varchar", "1 00:00:00.000"},
		{"cast from varchar ym", "SELECT CAST('2-3' AS INTERVAL YEAR TO MONTH)", tyYM, "2-3"},
		{"cast from varchar dt", "SELECT CAST('1 02:03:04.000' AS INTERVAL DAY TO SECOND)", tyDT, "1 02:03:04.000"},
		{"extract day", "SELECT extract(day FROM INTERVAL '3 04:00:00' DAY TO SECOND)", "bigint", "3"},
		{"extract hour", "SELECT extract(hour FROM INTERVAL '3 04:00:00' DAY TO SECOND)", "bigint", "4"},
		{"hour function", "SELECT hour(INTERVAL '3 04:00:00' DAY TO SECOND)", "bigint", "4"},
		{"year function", "SELECT year(INTERVAL '26' MONTH)", "bigint", "2"},
		{"month function", "SELECT month(INTERVAL '26' MONTH)", "bigint", "2"},
		{"second function", "SELECT second(INTERVAL '75' SECOND)", "bigint", "15"},
		{
			"plain arithmetic alongside",
			"SELECT (7 - 2 * 3) = 1 AND INTERVAL '1' DAY < INTERVAL '2' DAY",
			"boolean",
			"true",
		},
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

func TestSDK_IntervalComparison(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		want string
	}{
		{"day vs hours equal", "SELECT INTERVAL '1' DAY = INTERVAL '24' HOUR", "true"},
		{"less than", "SELECT INTERVAL '23' HOUR < INTERVAL '1' DAY", "true"},
		{"greater than negative", "SELECT INTERVAL '-1' DAY > INTERVAL '1' HOUR", "false"},
		{"year month ordering", "SELECT INTERVAL '1-1' YEAR TO MONTH > INTERVAL '12' MONTH", "true"},
		{"year equals months", "SELECT INTERVAL '1' YEAR = INTERVAL '12' MONTH", "true"},
		{"between", "SELECT INTERVAL '2' HOUR BETWEEN INTERVAL '1' HOUR AND INTERVAL '3' HOUR", "true"},
		{"in list", "SELECT INTERVAL '1' DAY IN (INTERVAL '2' DAY, INTERVAL '24' HOUR)", "true"},
		{"not equal", "SELECT INTERVAL '1' DAY <> INTERVAL '2' DAY", "true"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := sdkRun(t, docsClient(t), tc.sql)
			assert.Equal(t, [][]string{{tc.want}}, got.rows)
		})
	}
}

const intervalRows = "SELECT INTERVAL '2' DAY AS d UNION ALL SELECT INTERVAL '-1' DAY " +
	"UNION ALL SELECT INTERVAL '36' HOUR"

func TestSDK_IntervalOrderingAndAggregates(t *testing.T) {
	t.Parallel()

	got := sdkRun(t, docsClient(t),
		"SELECT d FROM ("+intervalRows+") ORDER BY d")
	assert.Equal(t, [][]string{{"-1 00:00:00.000"}, {"1 12:00:00.000"}, {"2 00:00:00.000"}}, got.rows)

	got = sdkRun(t, docsClient(t),
		"SELECT max(d), min(d) FROM ("+intervalRows+")")
	assert.Equal(t, [][]string{{"2 00:00:00.000", "-1 00:00:00.000"}}, got.rows)
	assert.Equal(t, []string{tyDT, tyDT}, got.types)
}

func TestSDK_IntervalTemporalArithmetic(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		want string
		typ  string
	}{
		{"date plus months clamps", "SELECT DATE '2024-01-31' + INTERVAL '1' MONTH", "2024-02-29", "date"},
		{"interval first", "SELECT INTERVAL '1' DAY + DATE '2024-01-01'", "2024-01-02", "date"},
		{"date minus years", "SELECT DATE '2024-02-29' - INTERVAL '1' YEAR", "2023-02-28", "date"},
		{"date plus whole days", "SELECT DATE '2024-01-01' + INTERVAL '48' HOUR", "2024-01-03", "date"},
		{"timestamp plus dt", "SELECT TIMESTAMP '2024-01-01 23:30:00' + INTERVAL '1 00:45:30.250' DAY TO SECOND",
			"2024-01-03 00:15:30.250", "timestamp"},
		{"timestamp minus ym", "SELECT TIMESTAMP '2024-03-31 10:00:00' - INTERVAL '1-1' YEAR TO MONTH",
			"2023-02-28 10:00:00.000", "timestamp"},
		{"interval expression operand", "SELECT DATE '2024-01-01' + INTERVAL '1' DAY * 3", "2024-01-04", "date"},
		{"chained", "SELECT DATE '2024-01-01' + INTERVAL '1' MONTH + INTERVAL '1' DAY", "2024-02-02", "date"},
		{
			"through a cte",
			"WITH s AS (SELECT INTERVAL '2' DAY AS d) SELECT DATE '2024-01-01' + d AS x FROM s",
			"2024-01-03",
			"date",
		},
		{
			"matches date_add",
			"SELECT DATE '2024-01-31' + INTERVAL '1' MONTH = date_add('month', 1, DATE '2024-01-31')",
			"true", "boolean",
		},
		{"date_add result type", "SELECT date_add('day', 1, DATE '2024-01-31')", "2024-02-01", "date"},
		{
			"diff of sum",
			"SELECT date_diff('day', DATE '2024-01-01', DATE '2024-01-01' + INTERVAL '1' YEAR)",
			"366",
			"bigint",
		},
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

func TestSDK_IntervalFailures(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		want string
	}{
		{
			"hours onto a date",
			"SELECT DATE '2024-01-01' + INTERVAL '3' HOUR",
			"Cannot add hour, minutes or seconds to a date",
		},
		{
			"hours off a date", "SELECT DATE '2024-01-01' - INTERVAL '3' HOUR",
			"Cannot subtract hour, minutes or seconds from a date",
		},
		{"mixed kinds", "SELECT INTERVAL '1' DAY + INTERVAL '1' MONTH", "Cannot apply operator"},
		{"interval minus date", "SELECT INTERVAL '1' DAY - DATE '2024-01-01'", "Cannot apply operator"},
		{"divide by zero", "SELECT INTERVAL '1' DAY / 0", "Division by zero"},
		{"bad literal", "SELECT INTERVAL 'x' DAY", "Invalid INTERVAL value"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Contains(t, sdkFailure(t, docsClient(t), tc.sql), tc.want)
		})
	}
}

func TestSDK_IntervalCTASIsRejected(t *testing.T) {
	t.Parallel()

	client := docsClient(t)
	sdkRun(t, client, "CREATE DATABASE shop")

	reason := sdkFailure(t, client, "CREATE TABLE shop.iv WITH (format = 'JSON', external_location = 's3://lake/iv/') "+
		"AS SELECT INTERVAL '1' DAY AS d")
	assert.Contains(t, reason, "Unsupported Hive type: interval day to second")
}
