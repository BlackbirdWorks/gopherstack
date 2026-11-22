package timestreamquery

import (
	"database/sql/driver"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunWithFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		plain   string
		want    string
		wantErr string
	}{
		{"unknown function falls back", "SELECT ts_nosuch(1)", "SELECT 7", "7", ""},
		{"syntax error falls back", "SELECT ts_add64(1, 2", "SELECT 8", "8", ""},
		{"operand type mismatch falls back", "SELECT ts_add64('x', 1)", "SELECT 2", "2", ""},
		{
			"overflow is not swallowed",
			"SELECT ts_add64(9223372036854775807, 1)",
			"SELECT 1",
			"",
			"bigint addition overflow",
		},
		{"division by zero is not swallowed", "SELECT ts_div32(1, 0)", "SELECT NULL", "", "Division by zero"},
		{
			"both fail reports the plain error",
			"SELECT ts_nosuch(1)",
			"SELECT nosuch FROM nowhere",
			"",
			"Schema must be",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ann := annotation{query: tc.query, plain: tc.plain}
			rs, err := runWithFallback(t.Context(), ann, time.Time{}, nil, loadedSet{})

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				assert.NotContains(t, err.Error(), evalMarker)

				return
			}

			require.NoError(t, err)
			require.Len(t, rs.rows, 1)
			require.NotNil(t, rs.rows[0].Data[0].ScalarValue)
			assert.Equal(t, tc.want, *rs.rows[0].Data[0].ScalarValue)
		})
	}
}

func TestCheckedRewriteMatchesNativeForValidInputs(t *testing.T) {
	t.Parallel()

	tests := []string{
		"SELECT CAST('2020-01-02 03:04:05' AS timestamp)",
		"SELECT CAST('2020-01-02T03:04:05Z' AS timestamp)",
		"SELECT CAST('2020-01-02' AS timestamp)",
		"SELECT CAST('2020-01-02 03:04:05+01:00' AS timestamp), CAST('2020-01-02 03:04:05Z' AS timestamp)",
		"SELECT CAST('2020-01-02 03:04' AS timestamp), CAST('2020-01-02T03:04' AS timestamp)",
		"SELECT CAST('2020-01-02 03:04:05+01:00' AS date)",
		"SELECT CAST(5 AS varchar), CAST(2.5 AS varchar), CAST(-3 AS varchar)",
		"SELECT bigint '5', integer '6', double '1.5', date '2020-01-02', timestamp '2020-01-02 03:04:05'",
		"SELECT CAST('2020-01-02' AS date)",
		"SELECT CAST('2020-01-02 03:04:05' AS date)",
		"SELECT CAST('12' AS bigint), CAST('-12' AS integer), CAST('+7' AS bigint), CAST(' 8 ' AS bigint)",
		"SELECT CAST('1.5' AS double), CAST('-2' AS double), CAST('1e3' AS double), CAST(' 2.5 ' AS double)",
		"SELECT CAST(7 AS double), CAST(bigint '7' AS integer), CAST(2.0 AS bigint), CAST(0 AS boolean)",
		"SELECT TRY_CAST('12' AS bigint), TRY_CAST('2020-01-02' AS date), TRY_CAST('1.5' AS double)",
		"SELECT 7 / 2, -7 / 2, 7 % 3, -7 % 3, 2 * 3 + 4, 10 - 2 - 3, -(4), abs(-4), mod(9, 4)",
		"SELECT bigint '7' / 2, bigint '9' % 4, 2147483647 + 1.0, 7 / 2.0",
		"SELECT sum(n), sum(n * 2) FROM UNNEST(SEQUENCE(1, 5)) AS t(n)",
	}

	for _, q := range tests {
		t.Run(q, func(t *testing.T) {
			t.Parallel()

			ann := buildAnnotation(q, loadedSet{}, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), nil)
			require.True(t, ann.ok)
			require.NotEqual(t, ann.query, ann.plain, "the query must exercise a checked rewrite")

			checked, err := runAnnotated(t.Context(), ann.query, time.Time{}, nil, loadedSet{}, ann)
			require.NoError(t, err)

			native, err := runAnnotated(t.Context(), ann.plain, time.Time{}, nil, loadedSet{}, ann)
			require.NoError(t, err)

			assert.Equal(t, rowStrings(native), rowStrings(checked))
		})
	}
}

func rowStrings(rs *resultSet) []string {
	var out []string

	for _, r := range rs.rows {
		for _, d := range r.Data {
			switch {
			case d.ScalarValue != nil:
				out = append(out, *d.ScalarValue)
			default:
				out = append(out, "null")
			}
		}
	}

	return out
}

func TestRegisteredFunctionsTolerateAnyArgument(t *testing.T) {
	t.Parallel()

	values := []driver.Value{
		nil,
		int64(0),
		int64(-1),
		int64(math.MinInt64),
		int64(math.MaxInt64),
		1.5,
		math.NaN(),
		math.Inf(1),
		math.Inf(-1),
		"",
		"x",
		"NaN",
		"9223372036854775808",
		[]byte{0xff, 0},
		true,
	}

	for _, f := range tsArithFuncs() {
		t.Run(f.name, func(t *testing.T) {
			t.Parallel()

			for _, a := range values {
				for _, b := range values {
					args := []driver.Value{a, b}[:max(f.nargs, 0)]
					if f.nargs == 0 {
						args = nil
					}

					assert.NotPanics(t, func() { _, _ = f.impl(nil, args) }, "%v %v", a, b)
				}
			}
		})
	}

	t.Run("casts", func(t *testing.T) {
		t.Parallel()

		for _, v := range values {
			for _, src := range []string{"varchar", "double", "bigint", "integer", "boolean", ""} {
				for _, dst := range []string{"varchar", "double", "bigint", "integer", "boolean", "timestamp", "date", "time"} {
					assert.NotPanics(t, func() {
						_, _ = castValue(v, src, dst, false)
						_, _ = castValue(v, src, dst, true)
					}, "%v %s %s", v, src, dst)
				}
			}
		}
	})
}
