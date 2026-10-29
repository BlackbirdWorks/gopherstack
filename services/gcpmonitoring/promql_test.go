package gcpmonitoring_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	distpb "google.golang.org/genproto/googleapis/api/distribution"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

const (
	promQueueDepth = "custom_googleapis_com:test_queue_depth"
	promBase       = "/v1/projects/" + testProject + "/location/global/prometheus/api/v1/"
)

type promReply struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
	Data      struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []any             `json:"value"`
			Values [][]any           `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func (e *env) promGet(t *testing.T, path string, params url.Values) (int, []byte) {
	t.Helper()

	host := strings.Replace(e.handler.Addr(), "[::]", "127.0.0.1", 1)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+host+path+"?"+params.Encode(), nil)
	require.NoError(t, err)
	// no keep-alive: an idle pooled connection outlives the test and trips goleak
	req.Close = true

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, body
}

func (e *env) promQuery(t *testing.T, q string) promReply {
	t.Helper()

	code, body := e.promGet(t, promBase+"query", url.Values{"query": {q}, "time": {tsParam(e.clk.now())}})
	require.Equal(t, http.StatusOK, code, string(body))

	var r promReply
	require.NoError(t, json.Unmarshal(body, &r))

	return r
}

func tsParam(t time.Time) string { return strconv.FormatInt(t.Unix(), 10) }

func (r promReply) byLabel(label string) map[string]string {
	out := map[string]string{}

	for _, s := range r.Data.Result {
		v, _ := s.Value[1].(string)
		out[s.Metric[label]] = v
	}

	return out
}

func TestPromQLInstantQuery(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.writeAt(t, epoch(), "gold", map[string]float64{"inst-1": 3, "inst-2": 5})

	tests := []struct {
		want  map[string]string
		name  string
		query string
		label string
		typ   string
	}{
		{
			name: "selector", query: promQueueDepth + `{monitored_resource="gce_instance"}`, label: "instance_id",
			want: map[string]string{"inst-1": "3", "inst-2": "5"}, typ: "vector",
		},
		{
			name: "label matcher", query: promQueueDepth + `{instance_id="inst-2",tier="gold"}`, label: "zone",
			want: map[string]string{zoneA: "5"}, typ: "vector",
		},
		{
			name: "regex matcher", query: promQueueDepth + `{instance_id=~"inst-[12]"}`, label: "instance_id",
			want: map[string]string{"inst-1": "3", "inst-2": "5"}, typ: "vector",
		},
		{
			name: "sum by", query: `sum by (tier) (` + promQueueDepth + `)`, label: "tier",
			want: map[string]string{"gold": "8"}, typ: "vector",
		},
		{
			name: "arithmetic", query: promQueueDepth + `{instance_id="inst-1"} * 2`, label: "instance_id",
			want: map[string]string{"inst-1": "6"}, typ: "vector",
		},
		{
			name: "no match", query: promQueueDepth + `{instance_id="nope"}`, label: "instance_id",
			want: map[string]string{}, typ: "vector",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := e.promQuery(t, tt.query)
			assert.Equal(t, "success", r.Status)
			assert.Equal(t, tt.typ, r.Data.ResultType)

			if tt.typ == "vector" {
				assert.Equal(t, tt.want, r.byLabel(tt.label))
			}
		})
	}
}

func TestPromQLScalarQuery(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	code, body := e.promGet(t, promBase+"query", url.Values{"query": {"1 + 2"}, "time": {tsParam(epoch())}})
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, string(body), `"resultType":"scalar"`)
	assert.Contains(t, string(body), `"3"]`)
}

func TestPromQLRangeQuery(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	for m := range 3 {
		e.writeAt(t, epoch().Add(time.Duration(m)*time.Minute), "gold", map[string]float64{"inst-1": float64(m + 1)})
	}

	code, body := e.promGet(t, promBase+"query_range", url.Values{
		"query": {promQueueDepth}, "start": {tsParam(epoch())},
		"end": {tsParam(epoch().Add(2 * time.Minute))}, "step": {"60"},
	})
	require.Equal(t, http.StatusOK, code, string(body))

	var r promReply
	require.NoError(t, json.Unmarshal(body, &r))
	require.Equal(t, "matrix", r.Data.ResultType)
	require.Len(t, r.Data.Result, 1)

	got := make([]string, 0, len(r.Data.Result[0].Values))
	for _, p := range r.Data.Result[0].Values {
		got = append(got, p[1].(string))
	}

	assert.Equal(t, []string{"1", "2", "3"}, got)
	assert.Equal(t, "inst-1", r.Data.Result[0].Metric["instance_id"])
}

func counterSeries(typ string, kind metricpb.MetricDescriptor_MetricKind, start, end time.Time, v int64,
) *monitoringpb.TimeSeries {
	return intSeries(typ, kind, &monitoredres.MonitoredResource{
		Type: "global", Labels: map[string]string{"project_id": testProject},
	}, start, end, v)
}

func TestPromQLMetricKinds(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	t0 := epoch()

	for i := range 3 {
		end := t0.Add(time.Duration(i) * time.Minute)
		e.clk.set(end)
		e.write(
			t,
			counterSeries(
				"custom.googleapis.com/jobs/done",
				metricpb.MetricDescriptor_CUMULATIVE,
				t0.Add(-time.Hour),
				end,
				int64(60*i),
			),
		)
		e.write(
			t,
			counterSeries(
				"custom.googleapis.com/jobs/delta",
				metricpb.MetricDescriptor_DELTA,
				end.Add(-time.Minute),
				end,
				5,
			),
		)
		e.write(t, counterSeries("prometheus.googleapis.com/http_requests_total/counter",
			metricpb.MetricDescriptor_CUMULATIVE, t0.Add(-time.Hour), end, int64(120*i)))
	}

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"cumulative rate", `rate(custom_googleapis_com:jobs_done[2m])`, "1"},
		{"cumulative increase", `increase(custom_googleapis_com:jobs_done[2m])`, "120"},
		{"delta read as running total", `custom_googleapis_com:jobs_delta`, "15"},
		{"prometheus metric name", `http_requests_total`, "240"},
		{"prometheus rate", `rate(http_requests_total[2m])`, "2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := e.promQuery(t, tt.query)
			require.Len(t, r.Data.Result, 1, tt.query)
			assert.Equal(t, tt.want, r.Data.Result[0].Value[1])
		})
	}
}

func TestPromQLDistribution(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	now := epoch()
	dist := &distpb.Distribution{
		Count: 6, Mean: 2, BucketCounts: []int64{1, 2, 3},
		BucketOptions: &distpb.Distribution_BucketOptions{Options: &distpb.Distribution_BucketOptions_ExplicitBuckets{
			ExplicitBuckets: &distpb.Distribution_BucketOptions_Explicit{Bounds: []float64{1, 5}},
		}},
	}

	e.write(t, &monitoringpb.TimeSeries{
		Metric: &metricpb.Metric{Type: "custom.googleapis.com/test/latency"},
		Resource: &monitoredres.MonitoredResource{
			Type:   "global",
			Labels: map[string]string{"project_id": testProject},
		},
		MetricKind: metricpb.MetricDescriptor_CUMULATIVE,
		ValueType:  metricpb.MetricDescriptor_DISTRIBUTION,
		Points: []*monitoringpb.Point{
			{
				Interval: &monitoringpb.TimeInterval{
					StartTime: timestamppb.New(now.Add(-time.Hour)),
					EndTime:   timestamppb.New(now),
				},
				Value: &monitoringpb.TypedValue{
					Value: &monitoringpb.TypedValue_DistributionValue{DistributionValue: dist},
				},
			},
		},
	})

	const base = "custom_googleapis_com:test_latency"

	assert.Equal(t, "6", e.promQuery(t, base+"_count").Data.Result[0].Value[1])
	assert.Equal(t, "12", e.promQuery(t, base+"_sum").Data.Result[0].Value[1])
	assert.Equal(t, map[string]string{"1": "1", "5": "3", "+Inf": "6"}, e.promQuery(t, base+"_bucket").byLabel("le"))
	assert.Equal(t, "5", e.promQuery(t, "histogram_quantile(0.5, "+base+"_bucket)").Data.Result[0].Value[1])
}

func TestPromQLProjectIsolation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.writeAt(t, epoch(), "gold", map[string]float64{"inst-1": 3})

	code, body := e.promGet(t, "/v1/projects/other/location/global/prometheus/api/v1/query", url.Values{
		"query": {promQueueDepth}, "time": {tsParam(epoch())},
	})
	require.Equal(t, http.StatusOK, code)

	var r promReply
	require.NoError(t, json.Unmarshal(body, &r))
	assert.Empty(t, r.Data.Result)
}

func TestPromQLMetadataEndpoints(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.writeAt(t, epoch(), "gold", map[string]float64{"inst-1": 3, "inst-2": 5})
	sel := url.Values{"match[]": {promQueueDepth}}

	tests := []struct {
		params   url.Values
		name     string
		path     string
		contains []string
	}{
		{sel, "series", "series", []string{`"instance_id":"inst-1"`, `"instance_id":"inst-2"`, `"tier":"gold"`}},
		{nil, "label names", "labels", []string{`"__name__"`, `"instance_id"`, `"monitored_resource"`, `"tier"`}},
		{sel, "label values", "label/instance_id/values", []string{`["inst-1","inst-2"]`}},
		{nil, "name values", "label/__name__/values", []string{`["` + promQueueDepth + `"]`}},
		{nil, "metadata", "metadata", []string{`"` + promQueueDepth + `":[{"type":"gauge"`}},
		{url.Values{"metric": {"nope"}}, "metadata filtered", "metadata", []string{`"data":{}`}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, body := e.promGet(t, promBase+tt.path, tt.params)
			require.Equal(t, http.StatusOK, code, string(body))

			for _, c := range tt.contains {
				assert.Contains(t, string(body), c)
			}
		})
	}
}

func TestPromQLErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	tests := []struct {
		params  url.Values
		name    string
		path    string
		errType string
		status  int
	}{
		{url.Values{"query": {"sum("}}, "syntax", "query", "bad_data", http.StatusBadRequest},
		{nil, "missing query", "query", "bad_data", http.StatusBadRequest},
		{url.Values{"query": {"1"}, "time": {"yesterday"}}, "bad time", "query", "bad_data", http.StatusBadRequest},
		{url.Values{"query": {"1"}}, "range without start", "query_range", "bad_data", http.StatusBadRequest},
		{nil, "series without match", "series", "bad_data", http.StatusBadRequest},
		{url.Values{"match[]": {"sum(x)"}}, "series non selector", "series", "bad_data", http.StatusBadRequest},
		{nil, "bad label name", "label/1bad/values", "bad_data", http.StatusBadRequest},
		{nil, "unknown endpoint", "nonsense", "not_found", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, body := e.promGet(t, promBase+tt.path, tt.params)
			assert.Equal(t, tt.status, code, string(body))

			var r promReply
			require.NoError(t, json.Unmarshal(body, &r))
			assert.Equal(t, "error", r.Status)
			assert.Equal(t, tt.errType, r.ErrorType)
		})
	}
}
