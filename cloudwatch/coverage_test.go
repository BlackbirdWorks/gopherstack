package cloudwatch_test

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

func TestProvider_Name(t *testing.T) {
	t.Parallel()
	p := &cloudwatch.Provider{}
	assert.Equal(t, "CloudWatch", p.Name())
}

func TestProvider_Init_WithConfig(t *testing.T) {
	t.Parallel()
	p := &cloudwatch.Provider{}
	ctx := &service.AppContext{
		Logger: slog.Default(),
		Config: &mockCWConfigProvider{},
	}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, svc)
	assert.Equal(t, "CloudWatch", svc.Name())
}

func TestProvider_Init_WithoutConfig(t *testing.T) {
	t.Parallel()
	p := &cloudwatch.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, svc)
}

// cwServer creates a test HTTP server backed by a real CloudWatch handler.
func cwServer(t *testing.T) (*httptest.Server, *cloudwatch.Handler) {
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
	return ts, h
}

func cwPost(t *testing.T, ts *httptest.Server, body string) *http.Response {
	t.Helper()
	resp, err := ts.Client().Post(ts.URL+"/", "application/x-www-form-urlencoded", strings.NewReader(body))
	require.NoError(t, err)
	return resp
}

func TestCoverage_PutMetricData(t *testing.T) {
	t.Parallel()
	ts, _ := cwServer(t)
	resp := cwPost(t, ts,
		"Action=PutMetricData&Namespace=Coverage&MetricData.member.1.MetricName=Hits&MetricData.member.1.Value=1")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}

func TestCoverage_ListMetrics(t *testing.T) {
	t.Parallel()
	ts, _ := cwServer(t)
	cwPost(t, ts,
		"Action=PutMetricData&Namespace=Coverage&MetricData.member.1.MetricName=Hits&MetricData.member.1.Value=1").Body.Close()
	resp := cwPost(t, ts, "Action=ListMetrics&Namespace=Coverage")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}

func TestCoverage_GetMetricStatistics(t *testing.T) {
	t.Parallel()
	ts, _ := cwServer(t)
	cwPost(t, ts,
		"Action=PutMetricData&Namespace=Coverage&MetricData.member.1.MetricName=Hits&MetricData.member.1.Value=5").Body.Close()

	start := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	end := time.Now().Add(time.Minute).UTC().Format(time.RFC3339)
	body := fmt.Sprintf(
		"Action=GetMetricStatistics&Namespace=Coverage&MetricName=Hits&StartTime=%s&EndTime=%s&Period=60"+
			"&Statistics.member.1=Average&Statistics.member.2=Sum&Statistics.member.3=Minimum"+
			"&Statistics.member.4=Maximum&Statistics.member.5=SampleCount",
		start, end,
	)
	resp := cwPost(t, ts, body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}

func TestCoverage_PutAndDescribeAlarms(t *testing.T) {
	t.Parallel()
	ts, _ := cwServer(t)
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

func TestCoverage_DeleteAlarms(t *testing.T) {
	t.Parallel()
	ts, _ := cwServer(t)
	cwPost(t, ts, "Action=PutMetricAlarm&AlarmName=del-me&Namespace=NS&MetricName=M").Body.Close()
	resp := cwPost(t, ts, "Action=DeleteAlarms&AlarmNames.member.1=del-me")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}

func TestCoverage_ErrorPaths(t *testing.T) {
	t.Parallel()
	ts, _ := cwServer(t)

	// Missing namespace for PutMetricData
	resp := cwPost(t, ts, "Action=PutMetricData&MetricData.member.1.MetricName=M&MetricData.member.1.Value=1")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	resp.Body.Close()

	// Bad StartTime for GetMetricStatistics
	resp = cwPost(t, ts, "Action=GetMetricStatistics&Namespace=NS&MetricName=M&StartTime=bad&EndTime=bad&Period=60")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	resp.Body.Close()

	// Bad Period (zero)
	start := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	end := time.Now().UTC().Format(time.RFC3339)
	resp = cwPost(t, ts,
		"Action=GetMetricStatistics&Namespace=NS&MetricName=M&StartTime="+start+"&EndTime="+end+"&Period=0")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	resp.Body.Close()

	// Missing AlarmName
	resp = cwPost(t, ts, "Action=PutMetricAlarm&Namespace=NS")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	resp.Body.Close()
}
