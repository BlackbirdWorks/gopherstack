package promql_test

import (
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

type memStore []promql.Series

func (m memStore) Select(ms []*promql.Matcher, minT, maxT int64) []promql.Series {
	var out []promql.Series

	for _, s := range m {
		if !promql.MatchesAll(s.Labels, ms) {
			continue
		}

		var win []promql.Sample

		for _, p := range s.Samples {
			if p.T >= minT && p.T <= maxT {
				win = append(win, p)
			}
		}

		out = append(out, promql.Series{Labels: s.Labels, Samples: win})
	}

	return out
}

func series(name, job string, extra map[string]string, vals ...float64) promql.Series {
	l := promql.Labels{promql.NameLabel: name, "job": job}
	maps.Copy(l, extra)

	s := promql.Series{Labels: l}
	for i, v := range vals {
		s.Samples = append(s.Samples, promql.Sample{T: int64(i) * 10_000, V: v})
	}

	return s
}

func fixture() memStore {
	return memStore{
		series("http_requests_total", "api", map[string]string{"code": "200"}, 0, 10, 20, 30, 40, 50, 60),
		series("http_requests_total", "api", map[string]string{"code": "500"}, 0, 1, 2, 3, 4, 5, 6),
		series("http_requests_total", "web", map[string]string{"code": "200"}, 100, 100, 100, 100, 100, 100, 100),
		series("up", "api", nil, 1, 1, 1, 1, 1, 1, 1),
	}
}

func instantAt(t *testing.T, q string, sec int64) promql.Value {
	t.Helper()

	e := &promql.Engine{Store: fixture()}
	v, err := e.Instant(q, time.UnixMilli(sec*1000))
	require.NoError(t, err)

	return v
}

func vectorMap(t *testing.T, v promql.Value, label string) map[string]float64 {
	t.Helper()

	vec, ok := v.(promql.Vector)
	require.True(t, ok, "want vector, got %T", v)

	out := map[string]float64{}
	for _, s := range vec {
		out[s.Metric[label]] = s.V
	}

	return out
}

func TestInstantQueries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want  map[string]float64
		name  string
		query string
		by    string
	}{
		{
			name:  "selector",
			query: `http_requests_total{job="api",code="200"}`,
			by:    "code",
			want:  map[string]float64{"200": 60},
		},
		{name: "neq", query: `http_requests_total{code!="200"}`, by: "code", want: map[string]float64{"500": 6}},
		{
			name:  "regex",
			query: `http_requests_total{job=~"a.*"}`,
			by:    "code",
			want:  map[string]float64{"200": 60, "500": 6},
		},
		{name: "nregex", query: `http_requests_total{job!~"a.*"}`, by: "job", want: map[string]float64{"web": 100}},
		{
			name:  "sum by",
			query: `sum by (job) (http_requests_total)`,
			by:    "job",
			want:  map[string]float64{"api": 66, "web": 100},
		},
		{
			name:  "sum by suffix",
			query: `sum(http_requests_total) by (code)`,
			by:    "code",
			want:  map[string]float64{"200": 160, "500": 6},
		},
		{name: "avg", query: `avg(http_requests_total)`, by: "x", want: map[string]float64{"": 166.0 / 3}},
		{
			name:  "count",
			query: `count(http_requests_total) by (job)`,
			by:    "job",
			want:  map[string]float64{"api": 2, "web": 1},
		},
		{
			name:  "min max",
			query: `max(http_requests_total) - min(http_requests_total)`,
			by:    "x",
			want:  map[string]float64{"": 94},
		},
		{
			name:  "rate",
			query: `rate(http_requests_total{job="api",code="200"}[1m])`,
			by:    "code",
			want:  map[string]float64{"200": 1},
		},
		{
			name:  "increase",
			query: `increase(http_requests_total{job="api",code="200"}[1m])`,
			by:    "code",
			want:  map[string]float64{"200": 60},
		},
		{
			name:  "arith scalar",
			query: `http_requests_total{code="500"} * 2 + 1`,
			by:    "code",
			want:  map[string]float64{"500": 13},
		},
		{
			name:  "vector ratio",
			query: `http_requests_total{code="500"} / ignoring(code) http_requests_total{code="200",job="api"}`,
			by:    "code",
			want:  map[string]float64{"": 0.1},
		},
		{
			name:  "compare filter",
			query: `http_requests_total > 50`,
			by:    "job",
			want:  map[string]float64{"api": 60, "web": 100},
		},
		{
			name:  "compare bool",
			query: `http_requests_total{job="api"} > bool 50`,
			by:    "code",
			want:  map[string]float64{"200": 1, "500": 0},
		},
		{
			name:  "and",
			query: `http_requests_total and on(job) up`,
			by:    "code",
			want:  map[string]float64{"200": 60, "500": 6},
		},
		{
			name:  "unless",
			query: `http_requests_total unless on(job) up`,
			by:    "job",
			want:  map[string]float64{"web": 100},
		},
		{name: "topk", query: `topk(1, http_requests_total)`, by: "job", want: map[string]float64{"web": 100}},
		{
			name:  "offset",
			query: `http_requests_total{code="500"} offset 30s`,
			by:    "code",
			want:  map[string]float64{"500": 3},
		},
		{
			name:  "avg over time",
			query: `avg_over_time(http_requests_total{code="500"}[30s])`,
			by:    "code",
			want:  map[string]float64{"500": 5},
		},
		{
			name:  "abs neg",
			query: `abs(-http_requests_total{code="500"})`,
			by:    "code",
			want:  map[string]float64{"500": 6},
		},
		{name: "absent", query: `absent(nonexistent{job="x"})`, by: "job", want: map[string]float64{"x": 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := vectorMap(t, instantAt(t, tt.query, 60), tt.by)
			require.Len(t, got, len(tt.want))

			for k, want := range tt.want {
				assert.InDelta(t, want, got[k], 1e-9, k)
			}
		})
	}
}

func TestScalarsAndLiterals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  float64
	}{
		{name: "add", query: `1 + 2 * 3`, want: 7},
		{name: "pow right assoc", query: `2 ^ 3 ^ 2`, want: 512},
		{name: "unary pow", query: `-2 ^ 2`, want: -4},
		{name: "parens", query: `(1 + 2) * 3`, want: 9},
		{name: "mod", query: `7 % 4`, want: 3},
		{name: "bool cmp", query: `3 > bool 2`, want: 1},
		{name: "time", query: `time()`, want: 60},
		{name: "scalar fn", query: `scalar(up)`, want: 1},
		{name: "exponent", query: `1e2`, want: 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sc, ok := instantAt(t, tt.query, 60).(promql.Scalar)
			require.True(t, ok)
			assert.InDelta(t, tt.want, sc.V, 1e-9)
		})
	}
}

func TestParseErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
	}{
		{name: "empty selector", query: `{}`},
		{name: "unterminated", query: `foo{job="a"`},
		{name: "unknown func", query: `bogus(up)`},
		{name: "bad regex", query: `up{job=~"("}`},
		{name: "trailing", query: `up up`},
		{name: "bad arity", query: `rate(up[1m], 3)`},
		{name: "dangling op", query: `up +`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := promql.Parse(tt.query)
			require.ErrorIs(t, err, promql.ErrSyntax)
		})
	}
}

func TestEvalErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
	}{
		{name: "duplicate match", query: `http_requests_total + on(job) http_requests_total`},
		{name: "scalar compare no bool", query: `1 > 2`},
		{name: "rate on vector", query: `rate(up)`},
		{name: "set op on scalar", query: `1 and 2`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := &promql.Engine{Store: fixture()}
			_, err := e.Instant(tt.query, time.UnixMilli(60_000))
			require.ErrorIs(t, err, promql.ErrEval)
		})
	}
}

func TestRangeQuery(t *testing.T) {
	t.Parallel()

	e := &promql.Engine{Store: fixture()}
	m, err := e.Range(`sum by (job) (http_requests_total)`, time.UnixMilli(0), time.UnixMilli(60_000), 20*time.Second)
	require.NoError(t, err)
	require.Len(t, m, 2)

	byJob := map[string][]promql.Sample{}
	for _, s := range m {
		byJob[s.Labels["job"]] = s.Samples
	}

	assert.Equal(
		t,
		[]promql.Sample{{T: 0, V: 0}, {T: 20_000, V: 22}, {T: 40_000, V: 44}, {T: 60_000, V: 66}},
		byJob["api"],
	)
	assert.Len(t, byJob["web"], 4)

	_, err = e.Range(`up`, time.UnixMilli(0), time.UnixMilli(60_000), 0)
	require.ErrorIs(t, err, promql.ErrEval)

	_, err = e.Range(`up`, time.UnixMilli(0), time.UnixMilli(1_000_000_000), time.Second)
	require.ErrorIs(t, err, promql.ErrEval)

	_, err = e.Range(`up[1m]`, time.UnixMilli(0), time.UnixMilli(60_000), time.Second)
	require.ErrorIs(t, err, promql.ErrEval)
}

func TestParseDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{in: "5m", want: 5 * time.Minute},
		{in: "1h30m", want: 90 * time.Minute},
		{in: "250ms", want: 250 * time.Millisecond},
		{in: "2d", want: 48 * time.Hour},
		{in: "1w", want: 7 * 24 * time.Hour},
		{in: "", wantErr: true},
		{in: "5x", wantErr: true},
		{in: "m", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()

			got, err := promql.ParseDuration(tt.in)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMatcherAnchoring(t *testing.T) {
	t.Parallel()

	m, err := promql.NewMatcher(promql.MatchRegexp, "a", "ab|cd")
	require.NoError(t, err)
	assert.True(t, m.Matches("ab"))
	assert.False(t, m.Matches("xabx"))
}
