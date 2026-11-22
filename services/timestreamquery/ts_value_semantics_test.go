package timestreamquery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuery_NonFiniteDoubles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"sqrt negative", `SELECT sqrt(-1)`, "NaN"},
		{"ln negative", `SELECT ln(-1)`, "NaN"},
		{"acos range", `SELECT acos(2)`, "NaN"},
		{"power negative base", `SELECT power(-1, 0.5)`, "NaN"},
		{"cast text nan", `SELECT CAST('NaN' AS double)`, "NaN"},
		{"zero over zero", `SELECT 0e0/0e0`, "NaN"},
		{"nan function", `SELECT nan()`, "NaN"},
		{"is_nan sqrt", `SELECT is_nan(sqrt(-1))`, "true"},
		{"is_nan cast", `SELECT is_nan(CAST('NaN' AS double))`, "true"},
		{"is_nan nan", `SELECT is_nan(nan())`, "true"},
		{"is_nan finite", `SELECT is_nan(1.5)`, "false"},
		{"ln zero", `SELECT ln(0)`, "-Infinity"},
		{"log10 zero", `SELECT log10(0)`, "-Infinity"},
		{"negated ln zero", `SELECT -ln(0)`, "Infinity"},
		{"is_infinite ln zero", `SELECT is_infinite(ln(0))`, "true"},
		{"is_finite division", `SELECT is_finite(1e0/0e0)`, "false"},
		{"infinity minus infinity", `SELECT infinity() - infinity()`, "NaN"},
		{"infinity times zero", `SELECT infinity() * 0e0`, "NaN"},
		{"nan plus", `SELECT nan() + 1`, "NaN"},
		{"nan chain", `SELECT nan() + 1 + 2`, "NaN"},
		{"division chain", `SELECT 0e0/0e0 + 1`, "NaN"},
		{"is_nan arithmetic", `SELECT is_nan(nan() * 2)`, "true"},
		{"nan equals nan", `SELECT nan() = nan()`, "false"},
		{"nan differs", `SELECT nan() <> nan()`, "true"},
		{"nan less", `SELECT nan() < 1`, "false"},
		{"nan greater equal", `SELECT nan() >= 1`, "false"},
		{"infinity greater", `SELECT infinity() > 1e308`, "true"},
		{"cast text infinity compare", `SELECT CAST('Infinity' AS double) > 1`, "true"},
		{"negative infinity", `SELECT -infinity()`, "-Infinity"},
		{"nan to varchar", `SELECT CAST(CAST('NaN' AS double) AS varchar)`, "NaN"},
		{"finite sum", `SELECT 1.5 + 2.5`, "4.0"},
		{"null sqrt", `SELECT sqrt(NULL)`, "<null>"},
		{"filter drops nan", `SELECT count(*) FROM db.t WHERE nan() > 1`, "0"},
		{"filter keeps infinity", `SELECT count(*) FROM db.t WHERE infinity() > 1`, "6"},
		{"try cast nan", `SELECT TRY_CAST('abc' AS double)`, "<null>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, _ := runFn(t, tt.query)
			require.Len(t, rows, 1)
			assert.Equal(t, tt.want, rows[0][0])
		})
	}
}

func TestQuery_IntervalArithmetic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
		typ   string
	}{
		{"add", `SELECT 1h + 30m`, "0 01:30:00.000000000", "INTERVAL_DAY_TO_SECOND"},
		{"add days", `SELECT 1d + 1h`, "1 01:00:00.000000000", "INTERVAL_DAY_TO_SECOND"},
		{"sub", `SELECT 1h - 30m`, "0 00:30:00.000000000", "INTERVAL_DAY_TO_SECOND"},
		{"sub negative", `SELECT 1h - 2h`, "-0 01:00:00.000000000", "INTERVAL_DAY_TO_SECOND"},
		{"mul", `SELECT 1h * 2`, "0 02:00:00.000000000", "INTERVAL_DAY_TO_SECOND"},
		{"mul left", `SELECT 2 * 1h`, "0 02:00:00.000000000", "INTERVAL_DAY_TO_SECOND"},
		{"mul double", `SELECT 1h * 1.5`, "0 01:30:00.000000000", "INTERVAL_DAY_TO_SECOND"},
		{"div", `SELECT 1h / 2`, "0 00:30:00.000000000", "INTERVAL_DAY_TO_SECOND"},
		{"negate", `SELECT -1h`, "-0 01:00:00.000000000", "INTERVAL_DAY_TO_SECOND"},
		{"typeof", `SELECT typeof(1h + 30m)`, "interval day to second", "VARCHAR"},
		{"parse_duration add", `SELECT parse_duration('1h') + 1m`, "0 01:01:00.000000000", "INTERVAL_DAY_TO_SECOND"},
		{
			"sql literal add",
			`SELECT interval '1' hour + interval '30' minute`,
			"0 01:30:00.000000000",
			"INTERVAL_DAY_TO_SECOND",
		},
		{"mixed literal add", `SELECT interval '1' hour + 30m`, "0 01:30:00.000000000", "INTERVAL_DAY_TO_SECOND"},
		{"nested", `SELECT (1h + 30m) * 2 - 1h`, "0 02:00:00.000000000", "INTERVAL_DAY_TO_SECOND"},
		{"null operand", `SELECT 1h + CAST(NULL AS interval day to second)`, "<null>", "INTERVAL_DAY_TO_SECOND"},
		{"compare sum", `SELECT 1h + 30m > 80m`, "true", "BOOLEAN"},
		{"compare equal", `SELECT 1h + 30m = 90m`, "true", "BOOLEAN"},
		{"compare less", `SELECT 1h - 30m < 1h`, "true", "BOOLEAN"},
		{"between", `SELECT 1h BETWEEN 30m AND 2h`, "true", "BOOLEAN"},
		{"ago sum", `SELECT ago(2m + 30s)`, "2026-10-31 11:57:30.000000000", "TIMESTAMP"},
		{"ago product", `SELECT ago(2 * 1h)`, "2026-10-31 10:00:00.000000000", "TIMESTAMP"},
		{"ago sql literal sum", `SELECT ago(interval '1' hour + 30m)`, "2026-10-31 10:30:00.000000000", "TIMESTAMP"},
		{"bin computed", `SELECT bin(now(), 5m + 5m)`, "2026-10-31 12:00:00.000000000", "TIMESTAMP"},
		{"timestamp plus sum", `SELECT now() + (1h + 30m)`, "2026-10-31 13:30:00.000000000", "TIMESTAMP"},
		{"timestamp chain", `SELECT now() + 1h + 30m`, "2026-10-31 13:30:00.000000000", "TIMESTAMP"},
		{"to_milliseconds sum", `SELECT to_milliseconds(1h + 30m)`, "5400000", "BIGINT"},
		{"to_nanoseconds sum", `SELECT to_nanoseconds(1m + 1s)`, "61000000000", "BIGINT"},
		{"date_add amount", `SELECT date_add('second', 2 * 30, now())`, "2026-10-31 12:01:00.000000000", "TIMESTAMP"},
		{
			"date_add column amount",
			`SELECT date_add('minute', length('abc') + 1, now())`,
			"2026-10-31 12:04:00.000000000",
			"TIMESTAMP",
		},
		{
			"date_add interval ratio",
			`SELECT date_add('second', to_milliseconds(1h + 30m) / 1000, now())`,
			"2026-10-31 13:30:00.000000000",
			"TIMESTAMP",
		},
		{"year month add", `SELECT 1year + 2month`, "1-2", "INTERVAL_YEAR_TO_MONTH"},
		{"year month mul", `SELECT 1year * 2`, "2-0", "INTERVAL_YEAR_TO_MONTH"},
		{"months literal", `SELECT 12month`, "1-0", "INTERVAL_YEAR_TO_MONTH"},
		{"eight months", `SELECT 8month`, "0-8", "INTERVAL_YEAR_TO_MONTH"},
		{"year literal", `SELECT 1year`, "1-0", "INTERVAL_YEAR_TO_MONTH"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, types := runFn(t, tt.query)
			require.Len(t, rows, 1)
			assert.Equal(t, tt.want, rows[0][0])
			assert.Equal(t, tt.typ, types[0])
		})
	}
}

func TestQuery_IntervalArithmeticErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"divide by zero", `SELECT 1h / 0`, "Division by zero"},
		{"mul overflow", `SELECT 100000000d * 100000000`, "out of range"},
		{"add overflow", `SELECT 70000d + 70000d`, "overflow"},
		{"day and year month", `SELECT 1h + 1year`, "TYPE_MISMATCH"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := runFnErr(t, tt.query)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestQuery_GuardedConstantsAreNotEvaluatedEarly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"and guard", `SELECT count(*) FROM db.t WHERE host = 'zzz' AND 1/0 > 1`, "0"},
		{"and false literal", `SELECT count(*) FROM db.t WHERE false AND 1/0 > 1`, "0"},
		{"and cast guard", `SELECT count(*) FROM db.t WHERE host = 'zzz' AND CAST('x' AS bigint) > 1`, "0"},
		{"and overflow guard", `SELECT count(*) FROM db.t WHERE host = 'zzz' AND 9223372036854775807 + 1 > 1`, "0"},
		{"and mod guard", `SELECT count(*) FROM db.t WHERE host = 'zzz' AND mod(1, 0) > 1`, "0"},
		{"and abs guard", `SELECT count(*) FROM db.t WHERE host = 'zzz' AND abs(-9223372036854775807 - 1) > 1`, "0"},
		{"case false", `SELECT CASE WHEN false THEN 1/0 ELSE 0 END`, "0"},
		{"case row guard", `SELECT CASE WHEN host = 'zzz' THEN 1/0 ELSE 5 END FROM db.t LIMIT 1`, "5"},
		{"case cast guard", `SELECT CASE WHEN false THEN CAST('x' AS bigint) ELSE 0 END`, "0"},
		{"coalesce guard", `SELECT coalesce(1, 1/0)`, "1"},
		{"if guard", `SELECT if(false, 1/0, 3)`, "3"},
		{"try guard", `SELECT try(1/0)`, "<null>"},
		{"nested case", `SELECT CASE WHEN 1 = 2 THEN 1/0 WHEN 2 = 2 THEN 7 ELSE 1/0 END`, "7"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, _ := runFn(t, tt.query)
			require.NotEmpty(t, rows)
			assert.Equal(t, tt.want, rows[0][0])
		})
	}
}

func TestQuery_ReachedConstantsStillRaise(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"select list", `SELECT 1/0`, "Division by zero"},
		{"or reaches", `SELECT count(*) FROM db.t WHERE host = 'a' OR 1/0 > 1`, "Division by zero"},
		{"where reaches", `SELECT count(*) FROM db.t WHERE 1/0 > 1`, "Division by zero"},
		{"and reaches", `SELECT count(*) FROM db.t WHERE host = 'a' AND 1/0 > 1`, "Division by zero"},
		{"cast reaches", `SELECT CAST('x' AS bigint)`, "Cannot cast 'x' to BIGINT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := runFnErr(t, tt.query)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}
