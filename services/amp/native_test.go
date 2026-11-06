package amp_test

import (
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ampsdk "github.com/aws/aws-sdk-go-v2/service/amp"
	"github.com/klauspost/compress/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/blackbirdworks/gopherstack/services/amp"
)

type histSpec struct {
	labels     map[string]string
	exemplar   *exemplarSpec
	posSpans   [][2]int64
	posDeltas  []int64
	posCounts  []float64
	count      uint64
	zeroCount  uint64
	sum        float64
	t          int64
	floatCount float64
	schema     int32
	floatHist  bool
	staleSum   bool
}

type exemplarSpec struct {
	labels map[string]string
	v      float64
	t      int64
}

func labelBytes(k, v string) []byte {
	var b []byte

	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendString(b, k)
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendString(b, v)

	return b
}

func histogramBytes(h histSpec) []byte {
	var b []byte

	if h.floatHist {
		b = protowire.AppendTag(b, 2, protowire.Fixed64Type)
		b = protowire.AppendFixed64(b, math.Float64bits(h.floatCount))
	} else {
		b = protowire.AppendTag(b, 1, protowire.VarintType)
		b = protowire.AppendVarint(b, h.count)
	}

	sum := h.sum
	if h.staleSum {
		sum = math.Float64frombits(0x7ff0000000000002)
	}

	b = protowire.AppendTag(b, 3, protowire.Fixed64Type)
	b = protowire.AppendFixed64(b, math.Float64bits(sum))
	b = protowire.AppendTag(b, 4, protowire.VarintType)
	b = protowire.AppendVarint(b, protowire.EncodeZigZag(int64(h.schema)))

	if !h.floatHist {
		b = protowire.AppendTag(b, 6, protowire.VarintType)
		b = protowire.AppendVarint(b, h.zeroCount)
	}

	for _, sp := range h.posSpans {
		var sb []byte

		sb = protowire.AppendTag(sb, 1, protowire.VarintType)
		sb = protowire.AppendVarint(sb, protowire.EncodeZigZag(sp[0]))
		sb = protowire.AppendTag(sb, 2, protowire.VarintType)
		sb = protowire.AppendVarint(sb, uint64(sp[1]))
		b = protowire.AppendTag(b, 11, protowire.BytesType)
		b = protowire.AppendBytes(b, sb)
	}

	if len(h.posDeltas) > 0 {
		var pk []byte
		for _, d := range h.posDeltas {
			pk = protowire.AppendVarint(pk, protowire.EncodeZigZag(d))
		}

		b = protowire.AppendTag(b, 12, protowire.BytesType)
		b = protowire.AppendBytes(b, pk)
	}

	if len(h.posCounts) > 0 {
		var pk []byte
		for _, c := range h.posCounts {
			pk = protowire.AppendFixed64(pk, math.Float64bits(c))
		}

		b = protowire.AppendTag(b, 13, protowire.BytesType)
		b = protowire.AppendBytes(b, pk)
	}

	b = protowire.AppendTag(b, 15, protowire.VarintType)
	b = protowire.AppendVarint(b, uint64(h.t))

	return b
}

func exemplarBytes(e exemplarSpec) []byte {
	var b []byte

	for k, v := range e.labels {
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendBytes(b, labelBytes(k, v))
	}

	b = protowire.AppendTag(b, 2, protowire.Fixed64Type)
	b = protowire.AppendFixed64(b, math.Float64bits(e.v))
	b = protowire.AppendTag(b, 3, protowire.VarintType)
	b = protowire.AppendVarint(b, uint64(e.t))

	return b
}

func (f *fixture) writeRaw(t *testing.T, ws string, hs ...histSpec) httpResp {
	t.Helper()

	var raw []byte

	for _, h := range hs {
		var ts []byte

		for k, v := range h.labels {
			ts = protowire.AppendTag(ts, 1, protowire.BytesType)
			ts = protowire.AppendBytes(ts, labelBytes(k, v))
		}

		if h.exemplar != nil {
			ts = protowire.AppendTag(ts, 3, protowire.BytesType)
			ts = protowire.AppendBytes(ts, exemplarBytes(*h.exemplar))
		}

		if h.count > 0 || h.floatHist || h.staleSum {
			ts = protowire.AppendTag(ts, 4, protowire.BytesType)
			ts = protowire.AppendBytes(ts, histogramBytes(h))
		}

		raw = protowire.AppendTag(raw, 1, protowire.BytesType)
		raw = protowire.AppendBytes(raw, ts)
	}

	return f.do(t, http.MethodPost, "/workspaces/"+ws+"/api/v1/remote_write",
		strings.NewReader(string(snappy.Encode(nil, raw))),
		map[string]string{"Content-Encoding": "snappy", "Authorization": signedAPS})
}

func TestNativeHistogramIngestAndQuery(t *testing.T) {
	t.Parallel()

	lbl := map[string]string{"__name__": "lat", "job": "api"}
	// schema 0: buckets (1,2] (2,4] (4,8] each holding 2 observations.
	intHist := histSpec{
		labels: lbl, posSpans: [][2]int64{{1, 3}}, posDeltas: []int64{2, 0, 0}, count: 6, sum: 20, t: 60_000,
	}
	floatHist := histSpec{
		labels: lbl, floatHist: true, floatCount: 6, sum: 20, t: 60_000,
		posSpans: [][2]int64{{1, 3}}, posCounts: []float64{2, 2, 2},
	}

	tests := []struct {
		name  string
		query string
		hist  histSpec
		want  float64
	}{
		{name: "count int", query: "histogram_count(lat)", want: 6, hist: intHist},
		{name: "count float", query: "histogram_count(lat)", want: 6, hist: floatHist},
		{name: "sum", query: "histogram_sum(lat)", want: 20, hist: intHist},
		{name: "avg", query: "histogram_avg(lat)", want: 20.0 / 6, hist: intHist},
		{name: "quantile", query: "histogram_quantile(0.5, lat)", want: math.Pow(2, 1.5), hist: intHist},
		{name: "fraction", query: "histogram_fraction(0, 4, lat)", want: 4.0 / 6, hist: intHist},
		{name: "stdvar", query: "histogram_stdvar(lat)", want: stdvar(20.0/6, []float64{2, 8, 32}), hist: intHist},
		{
			name:  "stddev",
			query: "histogram_stddev(lat)",
			want:  math.Sqrt(stdvar(20.0/6, []float64{2, 8, 32})),
			hist:  intHist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ws := f.workspace(t)
			r := f.writeRaw(t, ws, tt.hist)
			require.Equal(t, http.StatusOK, r.status, string(r.body))
			assert.Equal(t, 1, f.backend.SampleCount(ws))

			_, pr := f.query(t, ws, "query", url.Values{"query": {tt.query}, "time": {"60"}})
			require.Equal(t, "success", pr.Status)
			assert.Contains(t, string(pr.Data.Result), fmt.Sprintf(`"%s"]`, fmtFloat(tt.want)))
		})
	}
}

func fmtFloat(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

// stdvar of a histogram whose three buckets each hold count/3 observations at squared midpoints sq.
func stdvar(mean float64, sq []float64) float64 {
	v := 0.0
	for _, s := range sq {
		d := math.Sqrt(s) - mean
		v += 2 * d * d
	}

	return v / 6
}

func TestNativeHistogramRendering(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ws := f.workspace(t)
	h := histSpec{
		labels: map[string]string{"__name__": "lat"}, posSpans: [][2]int64{{1, 2}}, posDeltas: []int64{1, 1},
		count: 3, sum: 7, t: 60_000, zeroCount: 0,
	}
	require.Equal(t, http.StatusOK, f.writeRaw(t, ws, h).status)

	_, pr := f.query(t, ws, "query", url.Values{"query": {"lat"}, "time": {"60"}})
	assert.JSONEq(
		t,
		`[{"metric":{"__name__":"lat"},"histogram":[60,{"count":"3","sum":"7",`+
			`"buckets":[[0,"1","2","1"],[0,"2","4","2"]]}]}]`,
		string(pr.Data.Result),
	)

	_, pr = f.query(t, ws, "query_range", url.Values{"query": {"lat"}, "start": {"60"}, "end": {"60"}, "step": {"1"}})
	assert.Contains(t, string(pr.Data.Result), `"histograms":[[60,{"count":"3"`)

	_, pr = f.query(t, ws, "query", url.Values{"query": {"lat + 1"}, "time": {"60"}})
	assert.JSONEq(t, `[]`, string(pr.Data.Result), "histograms are not float operands")
}

func TestNativeHistogramRejects(t *testing.T) {
	t.Parallel()

	lbl := map[string]string{"__name__": "lat"}
	tests := []struct {
		name string
		hist histSpec
	}{
		{name: "span bucket mismatch", hist: histSpec{
			labels: lbl, posSpans: [][2]int64{{1, 3}}, posDeltas: []int64{1}, count: 1, sum: 1, t: 1,
		}},
		{name: "schema out of range", hist: histSpec{
			labels: lbl, posSpans: [][2]int64{{1, 1}}, posDeltas: []int64{1}, count: 1, sum: 1, t: 1, schema: 20,
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ws := f.workspace(t)
			r := f.writeRaw(t, ws, tt.hist)
			assert.Equal(t, http.StatusBadRequest, r.status)
			assert.Equal(t, 0, f.backend.SampleCount(ws))
		})
	}
}

func TestNativeHistogramStaleMarkerDropped(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ws := f.workspace(t)
	lbl := map[string]string{"__name__": "lat"}
	require.Equal(t, http.StatusOK, f.writeRaw(t, ws, histSpec{
		labels: lbl, posSpans: [][2]int64{{1, 1}}, posDeltas: []int64{1}, count: 1, sum: 1, t: 10_000,
	}).status)
	require.Equal(t, http.StatusOK, f.writeRaw(t, ws, histSpec{labels: lbl, staleSum: true, t: 20_000}).status)

	assert.Equal(t, 1, f.backend.SampleCount(ws), "remote-write stale markers are dropped like float ones")
}

func TestExemplars(t *testing.T) {
	t.Parallel()

	lbl := map[string]string{"__name__": "req_seconds_bucket", "le": "1"}
	ex := &exemplarSpec{labels: map[string]string{"trace_id": "abc"}, v: 0.7, t: 55_000}
	long := &exemplarSpec{labels: map[string]string{"trace_id": strings.Repeat("x", 200)}, v: 1, t: 56_000}

	tests := []struct {
		query url.Values
		name  string
		want  string
		write []histSpec
	}{
		{name: "returned", write: []histSpec{{labels: lbl, exemplar: ex}},
			query: url.Values{"query": {"req_seconds_bucket"}, "start": {"0"}, "end": {"100"}},
			want: `[{"seriesLabels":{"__name__":"req_seconds_bucket","le":"1"},` +
				`"exemplars":[{"labels":{"trace_id":"abc"},"value":"0.7","timestamp":55}]}]`},
		{name: "outside range", write: []histSpec{{labels: lbl, exemplar: ex}},
			query: url.Values{"query": {"req_seconds_bucket"}, "start": {"0"}, "end": {"10"}}, want: `[]`},
		{name: "other selector", write: []histSpec{{labels: lbl, exemplar: ex}},
			query: url.Values{"query": {"other"}, "start": {"0"}, "end": {"100"}}, want: `[]`},
		{name: "oversized dropped", write: []histSpec{{labels: lbl, exemplar: long}},
			query: url.Values{"query": {"req_seconds_bucket"}, "start": {"0"}, "end": {"100"}}, want: `[]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ws := f.workspace(t)
			require.Equal(t, http.StatusOK, f.writeRaw(t, ws, tt.write...).status)

			r := f.do(t, http.MethodGet, "/workspaces/"+ws+"/api/v1/query_exemplars?"+tt.query.Encode(), nil, nil)
			require.Equal(t, http.StatusOK, r.status, string(r.body))
			assert.JSONEq(t, `{"status":"success","data":`+tt.want+`}`, string(r.body))
		})
	}
}

func TestOutOfOrderWindow(t *testing.T) {
	t.Parallel()

	lbl := map[string]string{"__name__": "m"}
	tests := []struct {
		window     *int32
		name       string
		second     int64
		wantStatus int
		wantCount  int
	}{
		{name: "default rejects older", second: 59_000, wantStatus: http.StatusBadRequest, wantCount: 1},
		{
			name:       "zero rejects older",
			window:     aws.Int32(0),
			second:     59_000,
			wantStatus: http.StatusBadRequest,
			wantCount:  1,
		},
		{name: "inside window", window: aws.Int32(30), second: 40_000, wantStatus: http.StatusOK, wantCount: 2},
		{name: "on window edge", window: aws.Int32(30), second: 30_000, wantStatus: http.StatusOK, wantCount: 2},
		{
			name:       "outside window",
			window:     aws.Int32(30),
			second:     29_999,
			wantStatus: http.StatusBadRequest,
			wantCount:  1,
		},
		{name: "newer always fine", second: 61_000, wantStatus: http.StatusOK, wantCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ws := f.workspace(t)

			if tt.window != nil {
				_, err := f.client.UpdateWorkspaceConfiguration(t.Context(), &ampsdk.UpdateWorkspaceConfigurationInput{
					WorkspaceId: aws.String(ws), OutOfOrderTimeWindowInSeconds: tt.window,
				})
				require.NoError(t, err)
				f.settle()
			}

			require.Equal(t, http.StatusOK, f.write(t, ws, newSeries(lbl, 60_000, 1)).status)

			r := f.write(t, ws, newSeries(lbl, tt.second, 2))
			assert.Equal(t, tt.wantStatus, r.status, string(r.body))
			assert.Equal(t, tt.wantCount, f.backend.SampleCount(ws))
		})
	}
}

func newSeries(lbl map[string]string, ts int64, v float64) amp.WriteSeries {
	return amp.NewSeries(lbl, sample(ts, v))
}

func TestNativeHistogramAndExemplarPersist(t *testing.T) {
	t.Parallel()

	src := newFixture(t)
	ws := src.workspace(t)
	lbl := map[string]string{"__name__": "lat"}
	require.Equal(t, http.StatusOK, src.writeRaw(t, ws, histSpec{
		labels: lbl, posSpans: [][2]int64{{1, 2}}, posDeltas: []int64{1, 1}, count: 3, sum: 7, t: 60_000,
		exemplar: &exemplarSpec{labels: map[string]string{"trace_id": "t1"}, v: 2, t: 59_000},
	}).status)

	snap := src.backend.Snapshot(t.Context())

	dst := newFixture(t)
	require.NoError(t, dst.backend.Restore(t.Context(), snap))
	dst.settle()

	_, pr := dst.query(t, ws, "query", url.Values{"query": {"histogram_sum(lat)"}, "time": {"60"}})
	assert.Contains(t, string(pr.Data.Result), `"7"`)

	r := dst.do(t, http.MethodGet, "/workspaces/"+ws+"/api/v1/query_exemplars?query=lat&start=0&end=100", nil, nil)
	assert.Contains(t, string(r.body), `"trace_id":"t1"`)
}
