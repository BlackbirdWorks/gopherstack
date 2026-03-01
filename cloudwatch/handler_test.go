package cloudwatch_test

import (
	"encoding/xml"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/cloudwatch"
)

// postForm sends a form-encoded POST to the CloudWatch handler.
func postForm(t *testing.T, h *cloudwatch.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, req.ParseForm())
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func newCWHandler() *cloudwatch.Handler {
	return cloudwatch.NewHandler(cloudwatch.NewInMemoryBackend(), slog.Default())
}

func TestCloudWatchHandler(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup func(t *testing.T, h *cloudwatch.Handler)
		body  string
		want  func(t *testing.T, rec *httptest.ResponseRecorder)
		run   func(t *testing.T)
	}{
		{
			name: "Name",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				assert.Equal(t, "CloudWatch", h.Name())
			},
		},
		{
			name: "MatchPriority",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				assert.Equal(t, 80, h.MatchPriority())
			},
		},
		{
			name: "GetSupportedOperations",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				ops := h.GetSupportedOperations()
				assert.Contains(t, ops, "PutMetricData")
				assert.Contains(t, ops, "GetMetricStatistics")
				assert.Contains(t, ops, "ListMetrics")
				assert.Contains(t, ops, "PutMetricAlarm")
				assert.Contains(t, ops, "DescribeAlarms")
				assert.Contains(t, ops, "DeleteAlarms")
			},
		},
		{
			name: "ExtractOperation",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=ListMetrics"))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				assert.Equal(t, "ListMetrics", h.ExtractOperation(e.NewContext(req, httptest.NewRecorder())))
			},
		},
		{
			name: "ExtractResource",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=ListMetrics&Namespace=AWS/EC2"))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				assert.Equal(t, "AWS/EC2", h.ExtractResource(e.NewContext(req, httptest.NewRecorder())))
			},
		},
		{
			name: "PutMetricData/success",
			body: "Action=PutMetricData&Version=2010-08-01&Namespace=Test" +
				"&MetricData.member.1.MetricName=Requests" +
				"&MetricData.member.1.Value=42" +
				"&MetricData.member.1.Unit=Count",
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "PutMetricDataResponse")
			},
		},
		{
			name: "PutMetricData/missing namespace",
			body: "Action=PutMetricData&MetricData.member.1.MetricName=CPU&MetricData.member.1.Value=10",
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "ListMetrics",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				postForm(t, h,
					"Action=PutMetricData&Namespace=MyNS&MetricData.member.1.MetricName=M1&MetricData.member.1.Value=1")
			},
			body: "Action=ListMetrics&Namespace=MyNS",
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "ListMetricsResponse")
				assert.Contains(t, rec.Body.String(), "M1")
			},
		},
		{
			name: "ListMetrics/empty",
			body: "Action=ListMetrics",
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "GetMetricStatistics/success",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				postForm(t, h,
					"Action=PutMetricData&Namespace=NS"+
						"&MetricData.member.1.MetricName=CPU&MetricData.member.1.Value=50"+
						"&MetricData.member.1.Timestamp=2024-06-01T12:00:00Z")
			},
			body: "Action=GetMetricStatistics&Namespace=NS&MetricName=CPU" +
				"&StartTime=2024-06-01T11:00:00Z" +
				"&EndTime=2024-06-01T13:00:00Z" +
				"&Period=60" +
				"&Statistics.member.1=Average" +
				"&Statistics.member.2=Sum",
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "GetMetricStatisticsResponse")
			},
		},
		{
			name: "GetMetricStatistics/bad start time",
			body: "Action=GetMetricStatistics&Namespace=NS&MetricName=CPU&StartTime=bad&EndTime=bad&Period=60",
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "GetMetricStatistics/bad end time",
			body: "Action=GetMetricStatistics&Namespace=NS&MetricName=CPU" +
				"&StartTime=2024-01-01T00:00:00Z&EndTime=bad&Period=60",
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "GetMetricStatistics/bad period",
			body: "Action=GetMetricStatistics&Namespace=NS&MetricName=CPU" +
				"&StartTime=2024-01-01T00:00:00Z&EndTime=2024-01-01T01:00:00Z&Period=0",
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "PutMetricAlarm/success",
			body: "Action=PutMetricAlarm&AlarmName=high-cpu&Namespace=AWS/EC2&MetricName=CPUUtilization" +
				"&ComparisonOperator=GreaterThanThreshold&Threshold=80&EvaluationPeriods=1&Period=60&Statistic=Average",
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "PutMetricAlarmResponse")
			},
		},
		{
			name: "PutMetricAlarm/missing name",
			body: "Action=PutMetricAlarm&Namespace=NS",
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "DescribeAlarms/all",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=a1&Namespace=NS&MetricName=M")
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=a2&Namespace=NS&MetricName=M")
			},
			body: "Action=DescribeAlarms",
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "DescribeAlarmsResponse")
				assert.Contains(t, rec.Body.String(), "a1")
			},
		},
		{
			name: "DescribeAlarms/by name",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=alpha&Namespace=NS&MetricName=M")
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=beta&Namespace=NS&MetricName=M")
			},
			body: "Action=DescribeAlarms&AlarmNames.member.1=alpha",
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "alpha")
				assert.NotContains(t, rec.Body.String(), "beta")
			},
		},
		{
			name: "DeleteAlarms",
			run: func(t *testing.T) {
				h := newCWHandler()
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=to-del&Namespace=NS&MetricName=M")
				rec := postForm(t, h, "Action=DeleteAlarms&AlarmNames.member.1=to-del")
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "DeleteAlarmsResponse")

				rec2 := postForm(t, h, "Action=DescribeAlarms")
				assert.Equal(t, http.StatusOK, rec2.Code)

				type descResp struct {
					XMLName xml.Name `xml:"DescribeAlarmsResponse"`
					Result  struct {
						MetricAlarms []struct {
							AlarmName string `xml:"AlarmName"`
						} `xml:"MetricAlarms>member"`
					} `xml:"DescribeAlarmsResult"`
				}
				var resp descResp
				require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
				assert.Empty(t, resp.Result.MetricAlarms)
			},
		},
		{
			name: "UnknownAction",
			body: "Action=UnknownOp",
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "GetMetricData",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				postForm(t, h,
					"Action=PutMetricData&Namespace=MyNS&MetricData.member.1.MetricName=Latency"+
						"&MetricData.member.1.Value=100&MetricData.member.1.Timestamp=2024-01-01T00:00:00Z")
				postForm(t, h,
					"Action=PutMetricData&Namespace=MyNS&MetricData.member.1.MetricName=Latency"+
						"&MetricData.member.1.Value=200&MetricData.member.1.Timestamp=2024-01-01T00:01:00Z")
			},
			body: "Action=GetMetricData" +
				"&StartTime=2024-01-01T00:00:00Z" +
				"&EndTime=2024-01-01T00:10:00Z" +
				"&MetricDataQueries.member.1.Id=latency1" +
				"&MetricDataQueries.member.1.MetricStat.Metric.Namespace=MyNS" +
				"&MetricDataQueries.member.1.MetricStat.Metric.MetricName=Latency" +
				"&MetricDataQueries.member.1.MetricStat.Stat=Sum" +
				"&MetricDataQueries.member.1.MetricStat.Period=60",
			want: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "GetMetricDataResponse")
				assert.Contains(t, rec.Body.String(), "latency1")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.run != nil {
				tt.run(t)
				return
			}
			h := newCWHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}
			if tt.body != "" {
				rec := postForm(t, h, tt.body)
				if tt.want != nil {
					tt.want(t, rec)
				}
			}
		})
	}
}

func TestCloudWatchHandler_RouteMatcher(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		method string
		body   string
		ctype  string
		want   bool
	}{
		{
			name:   "match/correct request",
			method: http.MethodPost,
			body:   "Action=PutMetricData",
			ctype:  "application/x-www-form-urlencoded",
			want:   true,
		},
		{
			name:   "no match/wrong method",
			method: http.MethodGet,
			ctype:  "application/x-www-form-urlencoded",
			want:   false,
		},
		{
			name:   "no match/wrong content-type",
			method: http.MethodPost,
			body:   "Action=PutMetricData",
			ctype:  "application/json",
			want:   false,
		},
		{
			name:   "no match/unknown action",
			method: http.MethodPost,
			body:   "Action=UnknownAction",
			ctype:  "application/x-www-form-urlencoded",
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newCWHandler()
			e := echo.New()
			var req *http.Request
			if tt.body != "" {
				req = httptest.NewRequest(tt.method, "/", strings.NewReader(tt.body))
			} else {
				req = httptest.NewRequest(tt.method, "/", nil)
			}
			req.Header.Set("Content-Type", tt.ctype)
			assert.Equal(t, tt.want, h.RouteMatcher()(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}
