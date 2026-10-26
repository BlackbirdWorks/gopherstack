package promql_test

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

func gaugeStore(vals ...float64) memStore {
	s := promql.Series{Labels: promql.Labels{promql.NameLabel: "g", "job": "x"}}
	for i, v := range vals {
		s.Samples = append(s.Samples, promql.Sample{T: int64(i) * 10_000, V: v})
	}

	return memStore{s}
}

func instantOn(t *testing.T, st promql.Storage, q string, ms int64) (promql.Vector, error) {
	t.Helper()

	v, err := (&promql.Engine{Store: st}).Instant(q, time.UnixMilli(ms))
	if err != nil {
		return nil, err
	}

	vec, ok := v.(promql.Vector)
	require.True(t, ok)

	return vec, nil
}

func TestDoubleExponentialSmoothing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		vals    []float64
		want    float64
		wantNil bool
		wantErr bool
	}{
		{name: "holt_winters", query: `holt_winters(g[35s], 0.5, 0.5)`, vals: []float64{1, 3, 2, 5}, want: 4.875},
		{
			name:  "renamed",
			query: `double_exponential_smoothing(g[35s], 0.5, 0.5)`,
			vals:  []float64{1, 3, 2, 5},
			want:  4.875,
		},
		{name: "linear", query: `holt_winters(g[25s], 0.3, 0.7)`, vals: []float64{0, 10, 20}, want: 20},
		{name: "one sample", query: `holt_winters(g[5s], 0.5, 0.5)`, vals: []float64{1, 3, 2, 5}, wantNil: true},
		{name: "sf zero", query: `holt_winters(g[35s], 0, 0.5)`, vals: []float64{1, 2, 3, 4}, wantErr: true},
		{name: "tf one", query: `holt_winters(g[35s], 0.5, 1)`, vals: []float64{1, 2, 3, 4}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			end := int64(len(tt.vals)-1) * 10_000
			vec, err := instantOn(t, gaugeStore(tt.vals...), tt.query, end)

			if tt.wantErr {
				require.ErrorIs(t, err, promql.ErrEval)

				return
			}

			require.NoError(t, err)

			if tt.wantNil {
				assert.Empty(t, vec)

				return
			}

			require.Len(t, vec, 1)
			assert.InDelta(t, tt.want, vec[0].V, 1e-9)
		})
	}
}

func TestAbsentOverTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		wantJob string
		wantLen int
	}{
		{name: "missing", query: `absent_over_time(nope{job="x", env="p"}[1m])`, wantLen: 1, wantJob: "x"},
		{name: "present", query: `absent_over_time(up[1m])`},
		{name: "outside_window", query: `absent_over_time(up{job="api"}[1s] offset 10m)`, wantLen: 1, wantJob: "api"},
		{name: "subquery_missing", query: `absent_over_time(nope[1m:10s])`, wantLen: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vec, err := instantOn(t, fixture(), tt.query, 60_000)
			require.NoError(t, err)
			require.Len(t, vec, tt.wantLen)

			if tt.wantLen > 0 {
				assert.InDelta(t, 1, vec[0].V, 0)
				assert.Equal(t, tt.wantJob, vec[0].Metric["job"])
			}
		})
	}
}

func TestSortByLabel(t *testing.T) {
	t.Parallel()

	mk := func(inst, zone string) promql.Series {
		return promql.Series{
			Labels:  promql.Labels{promql.NameLabel: "m", "instance": inst, "zone": zone},
			Samples: []promql.Sample{{T: 0, V: 1}},
		}
	}
	st := memStore{mk("i10", "b"), mk("i2", "b"), mk("i1", "a"), mk("i2", "a")}

	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{"asc", `sort_by_label(m, "instance")`, []string{"i1/a", "i2/a", "i2/b", "i10/b"}},
		{"desc", `sort_by_label_desc(m, "instance")`, []string{"i10/b", "i2/a", "i2/b", "i1/a"}},
		{"two_labels", `sort_by_label(m, "zone", "instance")`, []string{"i1/a", "i2/a", "i2/b", "i10/b"}},
		{"two_labels_desc", `sort_by_label_desc(m, "zone", "instance")`, []string{"i10/b", "i2/b", "i2/a", "i1/a"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vec, err := instantOn(t, st, tt.query, 0)
			require.NoError(t, err)

			got := make([]string, len(vec))
			for i, s := range vec {
				got[i] = s.Metric["instance"] + "/" + s.Metric["zone"]
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLimitAggregations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		wantLen int
		wantErr bool
	}{
		{"limitk_one", `limitk(1, http_requests_total)`, 1, false},
		{"limitk_two", `limitk(2, http_requests_total)`, 2, false},
		{"limitk_over", `limitk(10, http_requests_total)`, 3, false},
		{"limitk_zero", `limitk(0, http_requests_total)`, 0, false},
		{"limitk_by", `limitk by (job) (1, http_requests_total)`, 2, false},
		{"ratio_all", `limit_ratio(1, http_requests_total)`, 3, false},
		{"ratio_none", `limit_ratio(0, http_requests_total)`, 0, false},
		{"ratio_neg_all", `limit_ratio(-1, http_requests_total)`, 3, false},
		{"ratio_clamped", `limit_ratio(5, http_requests_total)`, 3, false},
		{"ratio_nan", `limit_ratio(NaN, http_requests_total)`, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vec, err := instantOn(t, fixture(), tt.query, 60_000)
			if tt.wantErr {
				require.ErrorIs(t, err, promql.ErrEval)

				return
			}

			require.NoError(t, err)
			assert.Len(t, vec, tt.wantLen)
		})
	}

	t.Run("ratio_complement", func(t *testing.T) {
		t.Parallel()

		pos, err := instantOn(t, fixture(), `limit_ratio(0.5, http_requests_total)`, 60_000)
		require.NoError(t, err)

		neg, err := instantOn(t, fixture(), `limit_ratio(-0.5, http_requests_total)`, 60_000)
		require.NoError(t, err)

		seen := map[string]bool{}
		for _, s := range append(pos, neg...) {
			assert.False(t, seen[promql.Fingerprint(s.Metric)], "series sampled twice")
			seen[promql.Fingerprint(s.Metric)] = true
		}

		assert.Len(t, seen, 3)
	})
}

func TestHistogramFraction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  float64
	}{
		{"first_bucket", `histogram_fraction(0, 0.1, lat_bucket)`, 0.1},
		{"whole_bucket", `histogram_fraction(0.1, 0.5, lat_bucket)`, 0.5},
		{"interpolated", `histogram_fraction(0.1, 0.3, lat_bucket)`, 0.25},
		{"everything", `histogram_fraction(-Inf, +Inf, lat_bucket)`, 1},
		{"tail", `histogram_fraction(1, +Inf, lat_bucket)`, 0.1},
		{"empty_range", `histogram_fraction(0.5, 0.1, lat_bucket)`, 0},
		{"nan_bound", `histogram_fraction(NaN, 1, lat_bucket)`, math.NaN()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vec, err := instantOn(t, histFixture(), tt.query, 0)
			require.NoError(t, err)
			require.Len(t, vec, 1)

			if math.IsNaN(tt.want) {
				assert.True(t, math.IsNaN(vec[0].V))

				return
			}

			assert.InDelta(t, tt.want, vec[0].V, 1e-9)
			assert.NotContains(t, vec[0].Metric, "le")
		})
	}
}

func TestNativeHistogramAccessorsIgnoreFloats(t *testing.T) {
	t.Parallel()

	for _, fn := range []string{
		"histogram_count", "histogram_sum", "histogram_avg", "histogram_stddev", "histogram_stdvar",
	} {
		t.Run(fn, func(t *testing.T) {
			t.Parallel()

			vec, err := instantOn(t, fixture(), fn+`(http_requests_total)`, 60_000)
			require.NoError(t, err)
			assert.Empty(t, vec)

			_, err = instantOn(t, fixture(), fn+`(http_requests_total[1m])`, 60_000)
			require.ErrorIs(t, err, promql.ErrEval)
		})
	}
}

func TestAtStartEnd(t *testing.T) {
	t.Parallel()

	const sel = `http_requests_total{job="api",code="200"}`

	tests := []struct {
		name  string
		query string
		want  []float64
	}{
		{"start", sel + ` @ start()`, []float64{20, 20, 20}},
		{"end", sel + ` @ end()`, []float64{40, 40, 40}},
		{"matrix_end", `max_over_time(` + sel + `[15s] @ end())`, []float64{40, 40, 40}},
		{"subquery_start", `max_over_time(` + sel + `[15s:5s] @ start())`, []float64{20, 20, 20}},
		{"in_binary", sel + ` @ end() - ` + sel + ` @ start()`, []float64{20, 20, 20}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, err := (&promql.Engine{Store: fixture()}).Range(
				tt.query, time.UnixMilli(20_000), time.UnixMilli(40_000), 10*time.Second)
			require.NoError(t, err)
			require.Len(t, m, 1)

			got := make([]float64, len(m[0].Samples))
			for i, s := range m[0].Samples {
				got[i] = s.V
			}

			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("instant", func(t *testing.T) {
		t.Parallel()

		vec, err := instantOn(t, fixture(), sel+` @ end()`, 30_000)
		require.NoError(t, err)
		require.Len(t, vec, 1)
		assert.InDelta(t, 30, vec[0].V, 0)
	})

	t.Run("negative_rejected", func(t *testing.T) {
		t.Parallel()

		_, err := promql.Parse(sel + ` @ -start()`)
		require.ErrorIs(t, err, promql.ErrSyntax)
	})
}
