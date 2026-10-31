package amp_test

import (
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ampsdk "github.com/aws/aws-sdk-go-v2/service/amp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amp"
	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

type httpResp struct {
	body   []byte
	status int
}

func (f *fixture) do(t *testing.T, method, path string, body io.Reader, hdr map[string]string) httpResp {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, f.url+path, body)
	require.NoError(t, err)

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	res, err := f.http.Do(req)
	require.NoError(t, err)

	defer res.Body.Close()

	b, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	return httpResp{status: res.StatusCode, body: b}
}

const signedAPS = "AWS4-HMAC-SHA256 Credential=AKID/20260101/us-east-1/aps/aws4_request, " +
	"SignedHeaders=host, Signature=00"

func sample(ms int64, v float64) promql.Sample { return promql.Sample{T: ms, V: v} }

func (f *fixture) write(t *testing.T, ws string, series ...amp.WriteSeries) httpResp {
	t.Helper()

	return f.do(
		t,
		http.MethodPost,
		"/workspaces/"+ws+"/api/v1/remote_write",
		strings.NewReader(string(amp.EncodeWriteRequest(series))),
		map[string]string{
			"Content-Encoding": "snappy",
			"Content-Type":     "application/x-protobuf",
			"Authorization":    signedAPS,
		},
	)
}

type promResult struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Data      struct {
		ResultType string          `json:"resultType"`
		Result     json.RawMessage `json:"result"`
	} `json:"data"`
}

func (f *fixture) query(t *testing.T, ws, path string, vals url.Values) (httpResp, promResult) {
	t.Helper()

	r := f.do(t, http.MethodGet, "/workspaces/"+ws+"/api/v1/"+path+"?"+vals.Encode(), nil, nil)

	var pr promResult
	_ = json.Unmarshal(r.body, &pr)

	return r, pr
}

func seedMetrics(t *testing.T, f *fixture, ws string) {
	t.Helper()

	lbl := func(name, job, code string) map[string]string {
		return map[string]string{"__name__": name, "job": job, "code": code}
	}

	s20, s500 := make([]promql.Sample, 0, 7), make([]promql.Sample, 0, 7)

	for i := range 7 {
		s20 = append(s20, sample(int64(i)*10_000, float64(i)*10))
		s500 = append(s500, sample(int64(i)*10_000, float64(i)))
	}

	r := f.write(t, ws,
		amp.NewSeries(lbl("http_requests_total", "api", "200"), s20...),
		amp.NewSeries(lbl("http_requests_total", "api", "500"), s500...),
		amp.NewSeries(map[string]string{"__name__": "up", "job": "api"}, sample(60_000, 1)),
	)
	require.Equal(t, http.StatusOK, r.status, string(r.body))
}

func TestRemoteWriteAndInstantQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		query  string
		want   string
		status int
	}{
		{name: "selector", query: `http_requests_total{code="200"}`, want: `"value":[60,"60"]`, status: 200},
		{name: "rate", query: `rate(http_requests_total{code="200"}[1m])`, want: `"value":[60,"1"]`, status: 200},
		{
			name:   "sum by",
			query:  `sum by (job) (http_requests_total)`,
			want:   `"metric":{"job":"api"},"value":[60,"66"]`,
			status: 200,
		},
		{name: "scalar", query: `1+1`, want: `"resultType":"scalar"`, status: 200},
		{name: "syntax error", query: `sum(`, want: `"errorType":"bad_data"`, status: 400},
		{name: "exec error", query: `1 > 2`, want: `"errorType":"execution"`, status: 422},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ws := f.workspace(t)
			seedMetrics(t, f, ws)

			r, _ := f.query(t, ws, "query", url.Values{"query": {tt.query}, "time": {"60"}})
			assert.Equal(t, tt.status, r.status, string(r.body))
			assert.Contains(t, string(r.body), tt.want)
		})
	}
}

func TestQueryRangeSeriesLabels(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ws := f.workspace(t)
	seedMetrics(t, f, ws)

	t.Run("range", func(t *testing.T) {
		t.Parallel()

		r, pr := f.query(t, ws, "query_range", url.Values{
			"query": {`sum(http_requests_total)`}, "start": {"0"}, "end": {"60"}, "step": {"30s"},
		})
		require.Equal(t, http.StatusOK, r.status, string(r.body))
		assert.Equal(t, "matrix", pr.Data.ResultType)
		assert.Contains(t, string(pr.Data.Result), `"values":[[0,"0"],[30,"33"],[60,"66"]]`)
	})

	t.Run("range needs params", func(t *testing.T) {
		t.Parallel()

		r, _ := f.query(t, ws, "query_range", url.Values{"query": {"up"}})
		assert.Equal(t, http.StatusBadRequest, r.status)

		r, _ = f.query(t, ws, "query_range", url.Values{"query": {"up"}, "start": {"0"}, "end": {"60"}, "step": {"0"}})
		assert.Equal(t, http.StatusUnprocessableEntity, r.status)
	})

	t.Run("series", func(t *testing.T) {
		t.Parallel()

		r, pr := f.query(t, ws, "series", url.Values{"match[]": {`http_requests_total{code="500"}`}})
		require.Equal(t, http.StatusOK, r.status)
		assert.Equal(t, "success", pr.Status)
		assert.Contains(t, string(r.body), `"code":"500"`)
		assert.NotContains(t, string(r.body), `"code":"200"`)

		r, _ = f.query(t, ws, "series", url.Values{})
		assert.Equal(t, http.StatusBadRequest, r.status)

		r, _ = f.query(t, ws, "series", url.Values{"match[]": {`up`}, "start": {"100"}, "end": {"200"}})
		assert.JSONEq(t, `{"status":"success","data":[]}`, string(r.body))
	})

	t.Run("labels and values", func(t *testing.T) {
		t.Parallel()

		r, _ := f.query(t, ws, "labels", url.Values{})
		assert.JSONEq(t, `{"status":"success","data":["__name__","code","job"]}`, string(r.body))

		r, _ = f.query(t, ws, "label/code/values", url.Values{})
		assert.JSONEq(t, `{"status":"success","data":["200","500"]}`, string(r.body))

		r, _ = f.query(t, ws, "label/__name__/values", url.Values{})
		assert.JSONEq(t, `{"status":"success","data":["http_requests_total","up"]}`, string(r.body))
	})

	t.Run("post form", func(t *testing.T) {
		t.Parallel()

		form := url.Values{"query": {"up"}, "time": {"60"}}.Encode()
		r := f.do(t, http.MethodPost, "/workspaces/"+ws+"/api/v1/query", strings.NewReader(form),
			map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
		assert.Equal(t, http.StatusOK, r.status)
		assert.Contains(t, string(r.body), `"value":[60,"1"]`)
	})
}

func TestRemoteWriteRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture, ws string)
		name string
	}{
		{name: "duplicates and out of order", run: func(t *testing.T, f *fixture, ws string) {
			t.Helper()

			lbl := map[string]string{"__name__": "m"}
			require.Equal(t, http.StatusOK, f.write(t, ws, amp.NewSeries(lbl, sample(2000, 2), sample(1000, 1))).status)
			require.Equal(
				t,
				http.StatusOK,
				f.write(t, ws, amp.NewSeries(lbl, sample(2000, 99), sample(3000, 3))).status,
			)
			assert.Equal(t, 3, f.backend.SampleCount(ws))

			_, pr := f.query(t, ws, "query", url.Values{"query": {"m[10s]"}, "time": {"4"}})
			assert.Equal(t, "matrix", pr.Data.ResultType)
			assert.Contains(t, string(pr.Data.Result), `"values":[[1,"1"],[2,"2"],[3,"3"]]`)
		}},
		{name: "non finite values dropped", run: func(t *testing.T, f *fixture, ws string) {
			t.Helper()

			nan := promql.Sample{T: 1000, V: math.NaN()}
			require.Equal(
				t,
				http.StatusOK,
				f.write(t, ws, amp.NewSeries(map[string]string{"__name__": "m"}, nan, sample(2000, 1))).status,
			)
			assert.Equal(t, 1, f.backend.SampleCount(ws))
		}},
		{name: "invalid label name", run: func(t *testing.T, f *fixture, ws string) {
			t.Helper()

			r := f.write(t, ws, amp.NewSeries(map[string]string{"__name__": "m", "bad-name": "x"}, sample(1, 1)))
			assert.Equal(t, http.StatusBadRequest, r.status)
		}},
		{name: "garbage body", run: func(t *testing.T, f *fixture, ws string) {
			t.Helper()

			r := f.do(t, http.MethodPost, "/workspaces/"+ws+"/api/v1/remote_write", strings.NewReader("not snappy"),
				map[string]string{"Authorization": signedAPS})
			assert.Equal(t, http.StatusBadRequest, r.status)
		}},
		{name: "unknown workspace", run: func(t *testing.T, f *fixture, _ string) {
			t.Helper()

			r := f.write(t, "ws-missing", amp.NewSeries(map[string]string{"__name__": "m"}, sample(1, 1)))
			assert.Equal(t, http.StatusNotFound, r.status)

			r2, _ := f.query(t, "ws-missing", "query", url.Values{"query": {"up"}})
			assert.Equal(t, http.StatusNotFound, r2.status)
		}},
		{name: "data removed with workspace", run: func(t *testing.T, f *fixture, ws string) {
			t.Helper()

			require.Equal(
				t,
				http.StatusOK,
				f.write(t, ws, amp.NewSeries(map[string]string{"__name__": "m"}, sample(1, 1))).status,
			)
			require.Equal(t, 1, f.backend.SampleCount(ws))

			_, err := f.client.DeleteWorkspace(t.Context(), &ampsdk.DeleteWorkspaceInput{WorkspaceId: aws.String(ws)})
			require.NoError(t, err)
			f.settle()

			f.workspace(t)
			assert.Equal(t, 0, f.backend.SampleCount(ws))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			tt.run(t, f, f.workspace(t))
		})
	}
}

func TestDataPlaneRoutingWithoutSignature(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ws := f.workspace(t)

	r := f.do(t, http.MethodGet, "/workspaces/"+ws+"/api/v1/query?query=vector(1)&time=60", nil, nil)
	assert.Equal(t, http.StatusOK, r.status, string(r.body))

	r = f.do(
		t,
		http.MethodGet,
		"/workspaces/"+ws+"/api/v1/query?query=vector(1)",
		nil,
		map[string]string{
			"Authorization": "AWS4-HMAC-SHA256 Credential=AKID/20260101/us-east-1/grafana/aws4_request, Signature=00",
		},
	)
	assert.NotEqual(t, http.StatusOK, r.status)
}

func TestDecodeWriteRequestRoundTrip(t *testing.T) {
	t.Parallel()

	in := []amp.WriteSeries{
		amp.NewSeries(map[string]string{"__name__": "a", "x": "y"}, sample(1, 1.5), sample(2, -2)),
		amp.NewSeries(map[string]string{"__name__": "b"}),
	}

	out, err := amp.DecodeWriteRequest(amp.EncodeWriteRequest(in))
	require.NoError(t, err)
	assert.Equal(t, in, out)

	_, err = amp.DecodeWriteRequest([]byte{0xff, 0xff, 0xff})
	require.Error(t, err)
}
