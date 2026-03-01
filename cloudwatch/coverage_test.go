package cloudwatch_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/cloudwatch"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// mockCWConfigProvider implements config.Provider for testing.
type mockCWConfigProvider struct{}

func (m *mockCWConfigProvider) GetGlobalConfig() config.GlobalConfig {
	return config.GlobalConfig{AccountID: "111111111111", Region: "eu-west-1"}
}

// cwServer creates a test HTTP server backed by a real CloudWatch handler.
func cwServer(t *testing.T) *httptest.Server {
	t.Helper()
	h := cloudwatch.NewHandler(cloudwatch.NewInMemoryBackend(), slog.Default())
	e := echo.New()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := httptest.NewRecorder()
		c := e.NewContext(r, rec)
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}
		if herr := h.Handler()(c); herr != nil {
			http.Error(w, herr.Error(), http.StatusInternalServerError)

			return
		}
		for k, vals := range rec.Result().Header {
			for _, v := range vals {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	}))
	t.Cleanup(ts.Close)

	return ts
}

func cwPost(t *testing.T, ts *httptest.Server, body string) *http.Response {
	t.Helper()
	resp, err := ts.Client().Post(ts.URL+"/", "application/x-www-form-urlencoded", strings.NewReader(body))
	require.NoError(t, err)

	return resp
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &cloudwatch.Provider{}
	assert.Equal(t, "CloudWatch", p.Name())
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   config.Provider
		wantName string
	}{
		{
			name:     "with_config",
			config:   &mockCWConfigProvider{},
			wantName: "CloudWatch",
		},
		{
			name: "without_config",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &cloudwatch.Provider{}
			ctx := &service.AppContext{Logger: slog.Default(), Config: tt.config}
			svc, err := p.Init(ctx)
			require.NoError(t, err)
			require.NotNil(t, svc)
			if tt.wantName != "" {
				assert.Equal(t, tt.wantName, svc.Name())
			}
		})
	}
}

func TestCoverage_PutAndDescribeAlarms(t *testing.T) {
	t.Parallel()

	ts := cwServer(t)
	resp := cwPost(t, ts,
		"Action=PutMetricAlarm&AlarmName=cov-alarm&Namespace=NS&MetricName=M"+
			"&ComparisonOperator=GreaterThanThreshold&Threshold=90&EvaluationPeriods=2&Period=300&Statistic=Average"+
			"&AlarmDescription=Test+alarm")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	resp = cwPost(t, ts, "Action=DescribeAlarms&AlarmNames.member.1=cov-alarm")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	resp = cwPost(t, ts, "Action=DescribeAlarms&StateValue=INSUFFICIENT_DATA")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}

func TestCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(t *testing.T, ts *httptest.Server)
		body     string
		wantCode int
	}{
		{
			name:     "PutMetricData",
			body:     "Action=PutMetricData&Namespace=Coverage&MetricData.member.1.MetricName=Hits&MetricData.member.1.Value=1",
			wantCode: http.StatusOK,
		},
		{
			name: "ListMetrics",
			setup: func(t *testing.T, ts *httptest.Server) {
				t.Helper()
				cwPost(t, ts,
					"Action=PutMetricData&Namespace=Coverage&MetricData.member.1.MetricName=Hits&MetricData.member.1.Value=1",
				).Body.Close()
			},
			body:     "Action=ListMetrics&Namespace=Coverage",
			wantCode: http.StatusOK,
		},
		{
			name: "GetMetricStatistics",
			setup: func(t *testing.T, ts *httptest.Server) {
				t.Helper()
				cwPost(t, ts,
					"Action=PutMetricData&Namespace=Coverage&MetricData.member.1.MetricName=Hits&MetricData.member.1.Value=5"+
						"&MetricData.member.1.Timestamp=2024-06-01T12:00:00Z",
				).Body.Close()
			},
			body: "Action=GetMetricStatistics&Namespace=Coverage&MetricName=Hits" +
				"&StartTime=2024-06-01T11:00:00Z&EndTime=2024-06-01T13:00:00Z&Period=60" +
				"&Statistics.member.1=Average&Statistics.member.2=Sum&Statistics.member.3=Minimum" +
				"&Statistics.member.4=Maximum&Statistics.member.5=SampleCount",
			wantCode: http.StatusOK,
		},
		{
			name: "DeleteAlarms",
			setup: func(t *testing.T, ts *httptest.Server) {
				t.Helper()
				cwPost(t, ts, "Action=PutMetricAlarm&AlarmName=del-me&Namespace=NS&MetricName=M").Body.Close()
			},
			body:     "Action=DeleteAlarms&AlarmNames.member.1=del-me",
			wantCode: http.StatusOK,
		},
		{
			name:     "ErrorPaths/missing_namespace",
			body:     "Action=PutMetricData&MetricData.member.1.MetricName=M&MetricData.member.1.Value=1",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "ErrorPaths/bad_start_time",
			body:     "Action=GetMetricStatistics&Namespace=NS&MetricName=M&StartTime=bad&EndTime=bad&Period=60",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "ErrorPaths/bad_period",
			body: "Action=GetMetricStatistics&Namespace=NS&MetricName=M" +
				"&StartTime=2024-01-01T00:00:00Z&EndTime=2024-01-01T01:00:00Z&Period=0",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "ErrorPaths/missing_alarm_name",
			body:     "Action=PutMetricAlarm&Namespace=NS",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := cwServer(t)
			if tt.setup != nil {
				tt.setup(t, ts)
			}
			resp := cwPost(t, ts, tt.body)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}
