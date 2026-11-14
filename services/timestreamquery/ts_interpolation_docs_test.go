package timestreamquery_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Query shapes are the Developer Guide "Interpolation functions" examples
// (binned CTE, INTERPOLATE_* over CREATE_TIME_SERIES + SEQUENCE, CROSS JOIN UNNEST).
// The guide prints no outputs, so expected values are derived from the function definitions.
func TestQuery_InterpolationDocExamples(t *testing.T) {
	t.Parallel()

	const tmpl = `WITH binned_timeseries AS (
SELECT host, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization
FROM db.t
WHERE measure_name = 'cpu' AND host = 'a' AND time > ago(2h)
GROUP BY host, BIN(time, 30s)
), interpolated_timeseries AS (
SELECT host,
    %s(
        CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization),
            SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization
FROM binned_timeseries
GROUP BY host
)
SELECT time, ROUND(value, 2) AS interpolated_cpu
FROM interpolated_timeseries
CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)
WHERE time IN (TIMESTAMP '2026-10-31 11:40:00.000000000', TIMESTAMP '2026-10-31 11:40:15.000000000',
  TIMESTAMP '2026-10-31 11:45:00.000000000',
  TIMESTAMP '2026-10-31 11:54:45.000000000', TIMESTAMP '2026-10-31 11:55:00.000000000')
ORDER BY time`

	tests := []struct {
		name string
		fn   string
		want []string
	}{
		{"linear", "INTERPOLATE_LINEAR", []string{"10.0", "10.25", "15.0", "29.5", "30.0"}},
		{"locf", "INTERPOLATE_LOCF", []string{"10.0", "10.0", "10.0", "20.0", "30.0"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			q := sprintfQuery(tmpl, tt.fn)
			rows, types := runFn(t, q)

			got := make([]string, 0, len(rows))
			for _, r := range rows {
				got = append(got, r[1])
			}

			assert.Equal(t, tt.want, got)
			assert.Equal(t, []string{"TIMESTAMP", "DOUBLE"}, types)
		})
	}
}

func sprintfQuery(tmpl, fn string) string {
	return strings.Replace(tmpl, "%s", fn, 1)
}

// Query shapes are the Developer Guide "Derivatives functions" examples; the guide prints no outputs.
func TestQuery_DerivativeDocExamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			"derivative_linear group by",
			`SELECT DERIVATIVE_LINEAR(CREATE_TIME_SERIES(time, measure_value::double), 5m) AS result
FROM db.t WHERE measure_name = 'cpu' AND host = 'a' AND time > ago(1h) GROUP BY host, measure_name`,
			"ts[2026-10-31 11:50:00.000000000=5.0, 2026-10-31 11:55:00.000000000=10.0]",
		},
		{
			"non_negative_derivative_linear binned",
			`WITH binned_view AS (
SELECT bin(time, 5m) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS value
FROM db.t WHERE host = 'a' AND time > ago(1h) AND measure_name = 'cpu' GROUP BY bin(time, 5m)
)
SELECT non_negative_derivative_linear(CREATE_TIME_SERIES(binned_timestamp, value), 1m) AS rateOfErrorIncrease
FROM binned_view`,
			"ts[2026-10-31 11:50:00.000000000=1.0, 2026-10-31 11:55:00.000000000=2.0]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, types := runFn(t, tt.query)
			assert.Equal(t, [][]string{{tt.want}}, rows)
			assert.Equal(t, []string{"timeseries(DOUBLE)"}, types)
		})
	}
}
