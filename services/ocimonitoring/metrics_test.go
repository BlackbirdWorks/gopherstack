package ocimonitoring_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/monitoring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ns     = "custom_ns"
	metric = "CpuUtilization"
)

func min1(n int) time.Duration { return time.Duration(n) * time.Minute }

// seeded posts host a: 10@-5m 20@-4m 30(x2)@-3m 40@-2m and host b: 100@-4m.
func seeded(t *testing.T) *env {
	t.Helper()

	e := newEnv(t, envOpts{})
	e.post(t, ns, metric, map[string]string{"host": "a", "az": "1"},
		pt{-min1(5), 10, 0}, pt{-min1(4), 20, 0}, pt{-min1(3), 30, 2}, pt{-min1(2), 40, 0})
	e.post(t, ns, metric, map[string]string{"host": "b", "az": "1"}, pt{-min1(4), 100, 0})

	return e
}

func (e *env) summarize(t *testing.T, query, resolution string) ([]monitoring.MetricData, error) {
	t.Helper()

	d := monitoring.SummarizeMetricsDataDetails{
		Namespace: new(ns), Query: new(query),
		StartTime: &common.SDKTime{Time: e.clock.Now().Add(-min1(5))}, EndTime: &common.SDKTime{Time: e.clock.Now()},
	}
	if resolution != "" {
		d.Resolution = new(resolution)
	}

	resp, err := e.mon.SummarizeMetricsData(t.Context(), monitoring.SummarizeMetricsDataRequest{
		CompartmentId: new(testCompartment), SummarizeMetricsDataDetails: d,
	})

	return resp.Items, err
}

func values(m monitoring.MetricData) []float64 {
	out := make([]float64, 0, len(m.AggregatedDatapoints))
	for _, d := range m.AggregatedDatapoints {
		out = append(out, *d.Value)
	}

	return out
}

func TestSummarizeStatistics(t *testing.T) {
	t.Parallel()

	e := seeded(t)

	tests := []struct {
		name       string
		query      string
		resolution string
		want       []float64
	}{
		{"mean per minute", metric + `[1m]{host = "a"}.mean()`, "", []float64{10, 20, 30, 40}},
		{"avg alias", metric + `[1m]{host = "a"}.avg()`, "", []float64{10, 20, 30, 40}},
		{"sum counts weights", metric + `[1m]{host = "a"}.sum()`, "", []float64{10, 20, 60, 40}},
		{"count", metric + `[1m]{host = "a"}.count()`, "", []float64{1, 1, 2, 1}},
		{"max", metric + `[1m]{host = "a"}.max()`, "", []float64{10, 20, 30, 40}},
		{"min whole range", metric + `[5m]{host = "a"}.min()`, "5m", []float64{10}},
		{"max whole range", metric + `[5m]{host = "a"}.max()`, "5m", []float64{40}},
		{"sum whole range", metric + `[5m]{host = "a"}.sum()`, "5m", []float64{130}},
		{"mean whole range", metric + `[5m]{host = "a"}.mean()`, "5m", []float64{26}},
		{"count whole range", metric + `[5m]{host = "a"}.count()`, "5m", []float64{5}},
		{"first", metric + `[5m]{host = "a"}.first()`, "5m", []float64{10}},
		{"last", metric + `[5m]{host = "a"}.last()`, "5m", []float64{40}},
		{"increment", metric + `[5m]{host = "a"}.increment()`, "5m", []float64{30}},
		{"rate per second", metric + `[5m]{host = "a"}.rate()`, "5m", []float64{30.0 / 180}},
		{"percentile 50", metric + `[5m]{host = "a"}.percentile(0.5)`, "5m", []float64{30}},
		{"percentile 90", metric + `[5m]{host = "a"}.percentile(0.9)`, "5m", []float64{40}},
		{"sliding five minute window", metric + `[2m]{host = "a"}.sum()`, "1m",
			[]float64{30, 80, 100, 40}},
		{"absent over data", metric + `[1m]{host = "a"}.absent()`, "", []float64{0, 0, 0, 0, 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			items, err := e.summarize(t, tt.query, tt.resolution)
			require.NoError(t, err)
			require.Len(t, items, 1)
			assert.InDeltaSlice(t, tt.want, values(items[0]), 1e-9)
			assert.Equal(t, ns, *items[0].Namespace)
			assert.Equal(t, metric, *items[0].Name)
			assert.Equal(t, map[string]string{"host": "a", "az": "1"}, items[0].Dimensions)
		})
	}
}

func TestSummarizeGroupingAndFilters(t *testing.T) {
	t.Parallel()

	e := seeded(t)

	tests := []struct {
		name  string
		query string
		dims  []map[string]string
		first []float64
	}{
		{"per stream", metric + `[5m].max()`, []map[string]string{{"host": "a", "az": "1"}, {"host": "b", "az": "1"}},
			[]float64{40}},
		{
			"group by host",
			metric + `[5m].groupBy(host).max()`,
			[]map[string]string{{"host": "a"}, {"host": "b"}},
			[]float64{40},
		},
		{"group by az", metric + `[5m].groupBy(az).max()`, []map[string]string{{"az": "1"}}, []float64{100}},
		{"group by nothing", metric + `[5m].groupBy().sum()`, []map[string]string{{}}, []float64{230}},
		{
			"not equal",
			metric + `[5m]{host != "a"}.max()`,
			[]map[string]string{{"host": "b", "az": "1"}},
			[]float64{100},
		},
		{
			"fuzzy or",
			metric + `[5m]{host =~ "a|b"}.groupBy(host).max()`,
			[]map[string]string{{"host": "a"}, {"host": "b"}},
			[]float64{40},
		},
		{
			"fuzzy wildcard",
			metric + `[5m]{host =~ "b*"}.max()`,
			[]map[string]string{{"host": "b", "az": "1"}},
			[]float64{100},
		},
		{
			"not fuzzy",
			metric + `[5m]{host !=~ "a*"}.max()`,
			[]map[string]string{{"host": "b", "az": "1"}},
			[]float64{100},
		},
		{
			"two filters",
			metric + `[5m]{host = "a", az = "1"}.max()`,
			[]map[string]string{{"host": "a", "az": "1"}},
			[]float64{40},
		},
		{"no match", metric + `[5m]{host =~ "z*"}.max()`, nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			items, err := e.summarize(t, tt.query, "5m")
			require.NoError(t, err)
			require.Len(t, items, len(tt.dims))

			for i, want := range tt.dims {
				assert.Equal(t, want, items[i].Dimensions)
			}

			if len(items) > 0 {
				assert.Equal(t, tt.first, values(items[0]))
			}
		})
	}
}

func TestSummarizeErrors(t *testing.T) {
	t.Parallel()

	e := seeded(t)

	tests := []struct {
		name       string
		query      string
		resolution string
	}{
		{"no interval", metric + `.mean()`, ""},
		{"bad interval", metric + `[7x].mean()`, ""},
		{"interval 61m", metric + `[61m].mean()`, ""},
		{"interval 2d", metric + `[2d].mean()`, ""},
		{"no statistic", metric + `[1m]`, ""},
		{"unknown statistic", metric + `[1m].median()`, ""},
		{"two statistics", metric + `[1m].mean().sum()`, ""},
		{"bad percentile", metric + `[1m].percentile(1.5)`, ""},
		{"threshold in summarize", metric + `[1m].mean() > 5`, ""},
		{"join unsupported", metric + `[1m].mean() > 5 && ` + metric + `[1m].max() > 1`, ""},
		{"grouping unsupported", metric + `[1m].grouping().mean()`, ""},
		{"unquoted value", metric + `[1m]{host = a}.mean()`, ""},
		{"resolution above interval", metric + `[1m].mean()`, "5m"},
		{"bad resolution", metric + `[5m].mean()`, "zz"},
		{"missing metric", `[1m].mean()`, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.summarize(t, tt.query, tt.resolution)
			se := svcErr(t, err)
			assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
			assert.Equal(t, "InvalidParameter", se.GetCode())
		})
	}

	status, body := rawDo(t, http.MethodPost, e.base+"/20180401/metrics/actions/summarizeMetricsData",
		`{"namespace":"x","query":"m[1m].mean()"}`)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body, "MissingParameter")

	d := monitoring.SummarizeMetricsDataDetails{
		Namespace: new(ns), Query: new(metric + "[1m].mean()"),
		StartTime: &common.SDKTime{Time: e.clock.Now()}, EndTime: &common.SDKTime{Time: e.clock.Now().Add(-time.Hour)},
	}
	_, err := e.mon.SummarizeMetricsData(t.Context(), monitoring.SummarizeMetricsDataRequest{
		CompartmentId: new(testCompartment), SummarizeMetricsDataDetails: d,
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
}

func TestSummarizeDefaultRange(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.post(t, ns, metric, map[string]string{"h": "1"}, pt{-2 * time.Hour, 5, 0}, pt{-min1(1), 7, 0})

	resp, err := e.mon.SummarizeMetricsData(t.Context(), monitoring.SummarizeMetricsDataRequest{
		CompartmentId: new(testCompartment),
		Namespace:     new(ns), Query: new(metric + "[1h].sum()"), Resolution: new("1h"),
	})
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)
	assert.InDeltaSlice(t, []float64{5, 7}, values(resp.Items[0]), 1e-9, "3 hour default range covers both")
	assert.Equal(t, "1h", *resp.Items[0].Resolution)
}

func TestPostMetricDataValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod  func(*monitoring.MetricDataDetails)
		name string
		ok   bool
	}{
		{nil, "valid", true},
		{func(m *monitoring.MetricDataDetails) { m.Namespace = new("oci_custom") }, "reserved oci_ prefix", false},
		{func(m *monitoring.MetricDataDetails) { m.Namespace = new("Oracle_x") }, "reserved oracle_ prefix", false},
		{func(m *monitoring.MetricDataDetails) { m.Namespace = new("my-ns") }, "namespace with dash", false},
		{func(m *monitoring.MetricDataDetails) { m.Namespace = new("1ns") }, "namespace digit first", false},
		{func(m *monitoring.MetricDataDetails) { m.Name = new("bad name") }, "name with space", false},
		{
			func(m *monitoring.MetricDataDetails) { m.Name = new("my_app.success-rate$1") },
			"name with dots and dollar",
			true,
		},
		{func(m *monitoring.MetricDataDetails) { m.Dimensions = map[string]string{} }, "no dimensions", false},
		{func(m *monitoring.MetricDataDetails) {
			m.Dimensions = map[string]string{}
			for i := range 21 {
				m.Dimensions["k"+string(rune('a'+i))] = "v"
			}
		}, "21 dimensions", false},
		{
			func(m *monitoring.MetricDataDetails) { m.Dimensions = map[string]string{"k": ""} },
			"empty dimension value",
			false,
		},
		{
			func(m *monitoring.MetricDataDetails) { m.Dimensions = map[string]string{"a b": "v"} },
			"dimension key with space",
			false,
		},
		{func(m *monitoring.MetricDataDetails) {
			m.Dimensions = map[string]string{"k": string(make([]byte, 513))}
		}, "dimension value too long", false},
		{func(m *monitoring.MetricDataDetails) { m.Datapoints = []monitoring.Datapoint{} }, "no datapoints", false},
		{func(m *monitoring.MetricDataDetails) {
			m.Datapoints[0].Timestamp = &common.SDKTime{Time: epoch().Add(-2*time.Hour - time.Second)}
		}, "too old", false},
		{func(m *monitoring.MetricDataDetails) {
			m.Datapoints[0].Timestamp = &common.SDKTime{Time: epoch().Add(-2*time.Hour + time.Second)}
		}, "just inside past limit", true},
		{func(m *monitoring.MetricDataDetails) {
			m.Datapoints[0].Timestamp = &common.SDKTime{Time: epoch().Add(10 * time.Minute)}
		}, "too far in future", false},
		{func(m *monitoring.MetricDataDetails) {
			m.Datapoints[0].Timestamp = &common.SDKTime{Time: epoch().Add(9 * time.Minute)}
		}, "just inside future limit", true},
		{func(m *monitoring.MetricDataDetails) { m.Datapoints[0].Count = new(0) }, "zero count", false},
		{func(m *monitoring.MetricDataDetails) { m.ResourceGroup = new("1bad") }, "bad resource group", false},
		{
			func(m *monitoring.MetricDataDetails) { m.ResourceGroup = new("frontend-fleet") },
			"good resource group",
			true,
		},
		{func(m *monitoring.MetricDataDetails) {
			m.Metadata = map[string]string{"unit": string(make([]byte, 257))}
		}, "long metadata", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			m := e.metric(ns, metric, map[string]string{"host": "a"}, pt{0, 1, 0})

			if tt.mod != nil {
				tt.mod(&m)
			}

			resp, err := e.mon.PostMetricData(t.Context(), monitoring.PostMetricDataRequest{
				MetricData: []monitoring.MetricDataDetails{m},
			})
			if tt.ok {
				require.NoError(t, err)
				assert.Zero(t, *resp.FailedMetricsCount)

				return
			}

			se := svcErr(t, err)
			assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
			assert.Equal(t, "InvalidParameter", se.GetCode())
		})
	}
}

func TestPostMetricDataBatches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		atomicity  monitoring.PostMetricDataDetailsBatchAtomicityEnum
		wantFailed int
		wantErr    bool
		wantStored int
	}{
		{"non atomic partial", monitoring.PostMetricDataDetailsBatchAtomicityNonAtomic, 1, false, 1},
		{"default is non atomic", "", 1, false, 1},
		{"atomic rejects all", monitoring.PostMetricDataDetailsBatchAtomicityAtomic, 0, true, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			good := e.metric(ns, "good", map[string]string{"h": "1"}, pt{0, 1, 0})
			bad := e.metric("oci_reserved", "bad", map[string]string{"h": "1"}, pt{0, 1, 0})

			resp, err := e.mon.PostMetricData(t.Context(), monitoring.PostMetricDataRequest{
				MetricData: []monitoring.MetricDataDetails{good, bad}, BatchAtomicity: tt.atomicity,
			})
			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantFailed, *resp.FailedMetricsCount)
				require.Len(t, resp.FailedMetrics, 1)
				assert.Equal(t, "oci_reserved", *resp.FailedMetrics[0].MetricData.Namespace)
				assert.NotEmpty(t, *resp.FailedMetrics[0].Message)
			}

			list, lerr := e.mon.ListMetrics(t.Context(), monitoring.ListMetricsRequest{
				CompartmentId: new(testCompartment), ListMetricsDetails: monitoring.ListMetricsDetails{},
			})
			require.NoError(t, lerr)
			assert.Len(t, list.Items, tt.wantStored)
		})
	}

	e := newEnv(t, envOpts{})
	many := make([]monitoring.MetricDataDetails, 0, 51)

	for i := range 51 {
		many = append(many, e.metric(ns, "m", map[string]string{"i": string(rune('A' + i))}, pt{0, 1, 0}))
	}

	_, err := e.mon.PostMetricData(t.Context(), monitoring.PostMetricDataRequest{
		MetricData: many,
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "more than 50 streams")

	status, body := rawDo(t, http.MethodPost, e.base+"/20180401/metrics", `{}`)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body, "MissingParameter")

	status, body = rawDo(t, http.MethodPost, e.base+"/20180401/metrics", `{"batchAtomicity":"MAYBE","metricData":[{}]}`)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body, "InvalidParameter")
}

func TestListMetrics(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.post(t, ns, "cpu.user", map[string]string{"host": "a"}, pt{0, 1, 0})
	e.post(t, ns, "cpu.system", map[string]string{"host": "a"}, pt{0, 1, 0})
	e.post(t, ns, "disk", map[string]string{"host": "b"}, pt{0, 1, 0})
	e.post(t, "other_ns", "cpu.user", map[string]string{"host": "c"}, pt{0, 1, 0})

	tests := []struct {
		name string
		d    monitoring.ListMetricsDetails
		want []string
	}{
		{"all", monitoring.ListMetricsDetails{}, []string{"cpu.system", "cpu.user", "disk", "cpu.user"}},
		{
			"name wildcard",
			monitoring.ListMetricsDetails{Name: new("cpu*")},
			[]string{"cpu.system", "cpu.user", "cpu.user"},
		},
		{"namespace", monitoring.ListMetricsDetails{Namespace: new("other_ns")}, []string{"cpu.user"}},
		{
			"dimension filter",
			monitoring.ListMetricsDetails{DimensionFilters: map[string]string{"host": "b"}},
			[]string{"disk"},
		},
		{"sort by name desc", monitoring.ListMetricsDetails{
			SortBy: monitoring.ListMetricsDetailsSortByName, SortOrder: monitoring.ListMetricsDetailsSortOrderDesc,
		}, []string{"disk", "cpu.user", "cpu.user", "cpu.system"}},
		{
			"group by name",
			monitoring.ListMetricsDetails{GroupBy: []string{"name"}},
			[]string{"cpu.system", "cpu.user", "disk"},
		},
		{"group by namespace", monitoring.ListMetricsDetails{GroupBy: []string{"namespace"}}, []string{"", ""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := e.mon.ListMetrics(t.Context(), monitoring.ListMetricsRequest{
				CompartmentId: new(testCompartment), ListMetricsDetails: tt.d,
			})
			require.NoError(t, err)

			got := make([]string, 0, len(resp.Items))
			for _, m := range resp.Items {
				got = append(got, deref(m.Name))
			}

			assert.Equal(t, tt.want, got)
		})
	}

	_, err := e.mon.ListMetrics(t.Context(), monitoring.ListMetricsRequest{
		CompartmentId: new(testCompartment), GroupBy: []string{"bogus"},
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	first, err := e.mon.ListMetrics(t.Context(), monitoring.ListMetricsRequest{
		CompartmentId: new(testCompartment), Limit: new(3),
	})
	require.NoError(t, err)
	require.NotNil(t, first.OpcNextPage)

	rest, err := e.mon.ListMetrics(t.Context(), monitoring.ListMetricsRequest{
		CompartmentId: new(testCompartment), Limit: new(3), Page: first.OpcNextPage,
	})
	require.NoError(t, err)
	assert.Len(t, first.Items, 3)
	assert.Len(t, rest.Items, 1)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}

func TestMetricRetention(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.post(t, ns, metric, map[string]string{"h": "1"}, pt{0, 1, 0})

	list := func() int {
		resp, err := e.mon.ListMetrics(t.Context(), monitoring.ListMetricsRequest{CompartmentId: new(testCompartment)})
		require.NoError(t, err)

		return len(resp.Items)
	}

	assert.Equal(t, 1, list())

	e.clock.Advance(89 * 24 * time.Hour)
	assert.Equal(t, 1, list())

	e.clock.Advance(2 * 24 * time.Hour)
	assert.Equal(t, 0, list(), "90 day metric retention elapsed")
}
