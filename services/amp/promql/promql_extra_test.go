package promql_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

func histFixture() memStore {
	b := func(le string, v float64) promql.Series {
		return promql.Series{
			Labels:  promql.Labels{promql.NameLabel: "lat_bucket", "job": "api", "le": le},
			Samples: []promql.Sample{{T: 0, V: v}},
		}
	}

	return memStore{b("0.1", 10), b("0.5", 60), b("1", 90), b("+Inf", 100)}
}

func TestExtraFunctions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want  map[string]float64
		name  string
		query string
		by    string
		sec   int64
	}{
		{name: "label_replace", sec: 60, by: "env", query: `label_replace(up, "env", "$1-x", "job", "(a.*)")`,
			want: map[string]float64{"api-x": 1}},
		{name: "label_replace nomatch", sec: 60, by: "env", query: `label_replace(up, "env", "z", "job", "web")`,
			want: map[string]float64{"": 1}},
		{name: "label_join", sec: 60, by: "k", query: `label_join(up, "k", "-", "job", "__name__")`,
			want: map[string]float64{"api-up": 1}},
		{name: "subquery", sec: 60, by: "code", query: `max_over_time(http_requests_total{job="api"}[30s:10s])`,
			want: map[string]float64{"200": 60, "500": 6}},
		{name: "subquery rate", sec: 60, by: "code",
			query: `rate(http_requests_total{job="api",code="200"}[30s:10s])`, want: map[string]float64{"200": 1}},
		{name: "at modifier", sec: 60, by: "code", query: `http_requests_total{job="api",code="200"} @ 20`,
			want: map[string]float64{"200": 20}},
		{
			name:  "at with offset",
			sec:   60,
			by:    "code",
			query: `http_requests_total{job="api",code="200"} @ 40 offset 10s`,
			want:  map[string]float64{"200": 30},
		},
		{name: "group_left", sec: 60, by: "code", query: `http_requests_total{job="api"} * on(job) group_left up`,
			want: map[string]float64{"200": 60, "500": 6}},
		{name: "group_right", sec: 60, by: "code", query: `up * on(job) group_right http_requests_total{job="api"}`,
			want: map[string]float64{"200": 60, "500": 6}},
		{name: "group_left include", sec: 60, by: "inst",
			query: `up * on(job) group_left(inst) label_replace(up, "inst", "i1", "job", ".*")`,
			want:  map[string]float64{"i1": 1}},
		{name: "deriv", sec: 60, by: "code", query: `deriv(http_requests_total{job="api",code="200"}[1m])`,
			want: map[string]float64{"200": 1}},
		{name: "predict_linear", sec: 60, by: "code",
			query: `predict_linear(http_requests_total{job="api",code="200"}[1m], 10)`,
			want:  map[string]float64{"200": 70}},
		{name: "count_values", sec: 60, by: "v", query: `count_values("v", http_requests_total)`,
			want: map[string]float64{"60": 1, "6": 1, "100": 1}},
		{name: "stddev_over_time", sec: 60, by: "job", query: `stddev_over_time(up[1m])`,
			want: map[string]float64{"api": 0}},
		{name: "year", sec: 60, by: "x", query: `year(vector(86400*366))`, want: map[string]float64{"": 1971}},
		{name: "day_of_week", sec: 60, by: "x", query: `day_of_week(vector(0))`, want: map[string]float64{"": 4}},
		{name: "days_in_month", sec: 60, by: "x", query: `days_in_month(vector(0))`, want: map[string]float64{"": 31}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := vectorMap(t, instantAt(t, tt.query, tt.sec), tt.by)
			require.Len(t, got, len(tt.want))

			for k, v := range tt.want {
				assert.InDelta(t, v, got[k], 1e-6, k)
			}
		})
	}
}

func TestHistogramQuantile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  float64
	}{
		{name: "median", query: `histogram_quantile(0.5, lat_bucket)`, want: 0.1 + 0.4*(40.0/50.0)},
		{name: "p95", query: `histogram_quantile(0.95, lat_bucket)`, want: 1},
		{name: "above", query: `histogram_quantile(1.5, lat_bucket)`, want: math.Inf(1)},
		{name: "below", query: `histogram_quantile(-1, lat_bucket)`, want: math.Inf(-1)},
		{name: "sum rate", query: `histogram_quantile(0.5, sum by (le) (lat_bucket))`, want: 0.1 + 0.4*(40.0/50.0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			v, err := (&promql.Engine{Store: histFixture()}).Instant(tt.query, time.UnixMilli(0))
			require.NoError(t, err)

			vec, ok := v.(promql.Vector)
			require.True(t, ok)
			require.Len(t, vec, 1)
			assert.InDelta(t, tt.want, vec[0].V, 1e-9)
		})
	}
}

func TestSubqueryAndAtErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		parse bool
	}{
		{name: "subquery on matrix", query: `up[5m][5m:1m]`, parse: true},
		{name: "at on call", query: `abs(up) @ 5`, parse: true},
		{name: "huge subquery", query: `max_over_time(up[10y:1s])`},
		{name: "label_replace bad dst", query: `label_replace(up, "1x", "a", "job", ".*")`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := promql.Parse(tt.query)
			if tt.parse {
				require.ErrorIs(t, err, promql.ErrSyntax)

				return
			}

			require.NoError(t, err)

			_, err = (&promql.Engine{Store: fixture()}).Instant(tt.query, time.UnixMilli(60_000))
			require.ErrorIs(t, err, promql.ErrEval)
		})
	}
}

func TestEngineHonoursContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := (&promql.Engine{Ctx: ctx, Store: fixture()}).Instant(`up`, time.UnixMilli(60_000))
	require.ErrorIs(t, err, promql.ErrTimeout)
}

func TestStaleMarkerEndsSeries(t *testing.T) {
	t.Parallel()

	st := memStore{{
		Labels:  promql.Labels{promql.NameLabel: "m"},
		Samples: []promql.Sample{{T: 0, V: 1}, {T: 10_000, V: promql.StaleNaN()}},
	}}
	e := &promql.Engine{Store: st}

	v, err := e.Instant(`m`, time.UnixMilli(5_000))
	require.NoError(t, err)
	assert.Len(t, v, 1)

	v, err = e.Instant(`m`, time.UnixMilli(20_000))
	require.NoError(t, err)
	assert.Empty(t, v)
}
