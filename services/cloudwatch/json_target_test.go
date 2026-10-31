package cloudwatch_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func doJSONTarget(t *testing.T, url, op, body string) (*http.Response, []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(
		context.Background(), http.MethodPost, url+"/", bytes.NewReader([]byte(body)),
	)
	require.NoError(t, err)
	req.Header.Set("X-Amz-Target", "GraniteServiceVersion20100801."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp, out
}

func TestJSONTarget_RoundTrip(t *testing.T) {
	t.Parallel()

	srv := newTargetRequestTestServer(t)
	ts := time.Now().Truncate(time.Minute).Add(-5 * time.Minute).Unix()

	putResp, _ := doJSONTarget(t, srv.URL, "PutMetricData",
		`{"Namespace":"N","MetricData":[{"MetricName":"M","Value":-2.5,"Timestamp":`+epochStr(ts)+`}]}`)
	require.Equal(t, http.StatusOK, putResp.StatusCode)

	tests := []struct {
		check func(t *testing.T, resp *http.Response, body []byte)
		name  string
		op    string
		body  string
	}{
		{
			name: "list metrics",
			op:   "ListMetrics",
			body: `{}`,
			check: func(t *testing.T, resp *http.Response, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, resp.StatusCode)
				assert.Contains(t, resp.Header.Get("Content-Type"), "application/x-amz-json-1.0")

				var out struct {
					Metrics []struct {
						MetricName string `json:"MetricName"`
					} `json:"Metrics"`
				}
				require.NoError(t, json.Unmarshal(body, &out))
				require.Len(t, out.Metrics, 1)
				assert.Equal(t, "M", out.Metrics[0].MetricName)
			},
		},
		{
			name: "get metric data",
			op:   "GetMetricData",
			body: `{"StartTime":` + epochStr(ts-60) + `,"EndTime":` + epochStr(ts+600) +
				`,"MetricDataQueries":[{"Id":"m","MetricStat":{"Metric":` +
				`{"Namespace":"N","MetricName":"M"},"Period":60,"Stat":"Sum"}}]}`,
			check: func(t *testing.T, resp *http.Response, body []byte) {
				t.Helper()
				require.Equal(t, http.StatusOK, resp.StatusCode)

				var out struct {
					MetricDataResults []struct {
						Timestamps []float64 `json:"Timestamps"`
						Values     []float64 `json:"Values"`
					} `json:"MetricDataResults"`
				}
				require.NoError(t, json.Unmarshal(body, &out))
				require.Len(t, out.MetricDataResults, 1)
				assert.Equal(t, []float64{-2.5}, out.MetricDataResults[0].Values)
				assert.Equal(t, []float64{float64(ts)}, out.MetricDataResults[0].Timestamps)
			},
		},
		{
			name: "malformed json",
			op:   "ListMetrics",
			body: `{`,
			check: func(t *testing.T, resp *http.Response, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
				assert.Contains(t, string(body), "SerializationException")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, body := doJSONTarget(t, srv.URL, tt.op, tt.body)
			tt.check(t, resp, body)
		})
	}
}

func epochStr(n int64) string {
	b, _ := json.Marshal(n)

	return string(b)
}

func TestGetMetricData_EndToEnd(t *testing.T) {
	t.Parallel()

	base := time.Now().Truncate(5 * time.Minute).Add(-15 * time.Minute)
	dims := []types.Dimension{{Name: aws.String("Svc"), Value: aws.String("a")}}

	stat := func(id, s string, period int32, ret bool) types.MetricDataQuery {
		return types.MetricDataQuery{
			Id: aws.String(id), ReturnData: aws.Bool(ret),
			MetricStat: &types.MetricStat{
				Metric: &types.Metric{
					Namespace: aws.String("E2E"), MetricName: aws.String("Lat"), Dimensions: dims,
				},
				Period: aws.Int32(period), Stat: aws.String(s),
			},
		}
	}
	expr := func(id, e string) types.MetricDataQuery {
		return types.MetricDataQuery{Id: aws.String(id), Expression: aws.String(e)}
	}

	tests := []struct {
		want    map[string][]float64
		name    string
		queries []types.MetricDataQuery
	}{
		{
			name:    "return data false hides metric stat",
			queries: []types.MetricDataQuery{stat("a", "Sum", 60, false), expr("e", "a*2")},
			want:    map[string][]float64{"e": {10, 30, 50, 70}},
		},
		{
			name:    "percentile stat",
			queries: []types.MetricDataQuery{stat("p", "p100", 300, true)},
			want:    map[string][]float64{"p": {35}},
		},
		{
			name:    "nested arithmetic",
			queries: []types.MetricDataQuery{stat("a", "Sum", 60, false), expr("e", "(a+10)/5*-1")},
			want:    map[string][]float64{"e": {-3, -5, -7, -9}},
		},
		{
			name:    "running sum",
			queries: []types.MetricDataQuery{stat("a", "Sum", 60, false), expr("e", "RUNNING_SUM(a)")},
			want:    map[string][]float64{"e": {5, 20, 45, 80}},
		},
		{
			name:    "if comparison",
			queries: []types.MetricDataQuery{stat("a", "Sum", 60, false), expr("e", "IF(a>10, a, 0)")},
			want:    map[string][]float64{"e": {0, 15, 25, 35}},
		},
		{
			name:    "diff abs",
			queries: []types.MetricDataQuery{stat("a", "Sum", 60, false), expr("e", "ABS(DIFF(a)*-1)")},
			want:    map[string][]float64{"e": {10, 10, 10}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)

			for i := range 4 {
				_, err := client.PutMetricData(t.Context(), &cwsdk.PutMetricDataInput{
					Namespace: aws.String("E2E"),
					MetricData: []types.MetricDatum{{
						MetricName: aws.String("Lat"), Dimensions: dims,
						Timestamp: aws.Time(base.Add(time.Duration(i) * time.Minute)),
						Value:     aws.Float64(float64(5 + 10*i)),
					}},
				})
				require.NoError(t, err)
			}

			out, err := client.GetMetricData(t.Context(), &cwsdk.GetMetricDataInput{
				StartTime:         aws.Time(base.Add(-time.Minute)),
				EndTime:           aws.Time(base.Add(10 * time.Minute)),
				MetricDataQueries: tt.queries,
			})
			require.NoError(t, err)
			require.Len(t, out.MetricDataResults, len(tt.want))

			for _, r := range out.MetricDataResults {
				assert.Equal(t, tt.want[aws.ToString(r.Id)], r.Values, aws.ToString(r.Id))
			}
		})
	}
}
