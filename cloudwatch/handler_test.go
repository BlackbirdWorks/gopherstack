package cloudwatch_test

import (
	"encoding/xml"
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

func TestHandler_Name(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	assert.Equal(t, "CloudWatch", h.Name())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	assert.Equal(t, 80, h.MatchPriority())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "PutMetricData")
	assert.Contains(t, ops, "GetMetricStatistics")
	assert.Contains(t, ops, "ListMetrics")
	assert.Contains(t, ops, "PutMetricAlarm")
	assert.Contains(t, ops, "DescribeAlarms")
	assert.Contains(t, ops, "DeleteAlarms")
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	e := echo.New()
	matcher := h.RouteMatcher()

	// Match: correct method, content-type, action
	body := strings.NewReader("Action=PutMetricData")
	req := httptest.NewRequest(http.MethodPost, "/", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	assert.True(t, matcher(e.NewContext(req, httptest.NewRecorder())))

	// No match: wrong method
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	assert.False(t, matcher(e.NewContext(req2, httptest.NewRecorder())))

	// No match: wrong content-type
	req3 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=PutMetricData"))
	req3.Header.Set("Content-Type", "application/json")
	assert.False(t, matcher(e.NewContext(req3, httptest.NewRecorder())))

	// No match: unknown action
	req4 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=UnknownAction"))
	req4.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	assert.False(t, matcher(e.NewContext(req4, httptest.NewRecorder())))
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=ListMetrics"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	assert.Equal(t, "ListMetrics", h.ExtractOperation(e.NewContext(req, httptest.NewRecorder())))
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=ListMetrics&Namespace=AWS/EC2"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	assert.Equal(t, "AWS/EC2", h.ExtractResource(e.NewContext(req, httptest.NewRecorder())))
}

func TestHandler_PutMetricData_Success(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	body := "Action=PutMetricData&Version=2010-08-01&Namespace=Test" +
		"&MetricData.member.1.MetricName=Requests" +
		"&MetricData.member.1.Value=42" +
		"&MetricData.member.1.Unit=Count"
	rec := postForm(t, h, body)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "PutMetricDataResponse")
}

func TestHandler_PutMetricData_MissingNamespace(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	rec := postForm(t, h, "Action=PutMetricData&MetricData.member.1.MetricName=CPU&MetricData.member.1.Value=10")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListMetrics(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	// First put some data
	postForm(t, h,
		"Action=PutMetricData&Namespace=MyNS&MetricData.member.1.MetricName=M1&MetricData.member.1.Value=1")

	rec := postForm(t, h, "Action=ListMetrics&Namespace=MyNS")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListMetricsResponse")
	assert.Contains(t, rec.Body.String(), "M1")
}

func TestHandler_ListMetrics_Empty(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	rec := postForm(t, h, "Action=ListMetrics")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_GetMetricStatistics(t *testing.T) {
	t.Parallel()
	h := cloudwatch.NewHandler(cloudwatch.NewInMemoryBackend(), slog.Default())

	// Seed data
	now := time.Now().UTC()
	postForm(t, h,
		"Action=PutMetricData&Namespace=NS&MetricData.member.1.MetricName=CPU&MetricData.member.1.Value=50")

	startStr := now.Add(-time.Minute).Format(time.RFC3339)
	endStr := now.Add(time.Minute).Format(time.RFC3339)
	body := "Action=GetMetricStatistics&Namespace=NS&MetricName=CPU" +
		"&StartTime=" + startStr +
		"&EndTime=" + endStr +
		"&Period=60" +
		"&Statistics.member.1=Average" +
		"&Statistics.member.2=Sum"

	rec := postForm(t, h, body)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetMetricStatisticsResponse")
}

func TestHandler_GetMetricStatistics_BadStartTime(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	rec := postForm(t, h,
		"Action=GetMetricStatistics&Namespace=NS&MetricName=CPU&StartTime=bad&EndTime=bad&Period=60")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetMetricStatistics_BadEndTime(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	start := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	rec := postForm(t, h,
		"Action=GetMetricStatistics&Namespace=NS&MetricName=CPU&StartTime="+start+"&EndTime=bad&Period=60")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetMetricStatistics_BadPeriod(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	start := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	end := time.Now().UTC().Format(time.RFC3339)
	rec := postForm(t, h,
		"Action=GetMetricStatistics&Namespace=NS&MetricName=CPU&StartTime="+start+"&EndTime="+end+"&Period=0")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_PutMetricAlarm_Success(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	body := "Action=PutMetricAlarm&AlarmName=high-cpu&Namespace=AWS/EC2&MetricName=CPUUtilization" +
		"&ComparisonOperator=GreaterThanThreshold&Threshold=80&EvaluationPeriods=1&Period=60&Statistic=Average"
	rec := postForm(t, h, body)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "PutMetricAlarmResponse")
}

func TestHandler_PutMetricAlarm_MissingName(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	rec := postForm(t, h, "Action=PutMetricAlarm&Namespace=NS")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DescribeAlarms_All(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	postForm(t, h, "Action=PutMetricAlarm&AlarmName=a1&Namespace=NS&MetricName=M")
	postForm(t, h, "Action=PutMetricAlarm&AlarmName=a2&Namespace=NS&MetricName=M")
	rec := postForm(t, h, "Action=DescribeAlarms")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeAlarmsResponse")
	assert.Contains(t, rec.Body.String(), "a1")
}

func TestHandler_DescribeAlarms_ByName(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	postForm(t, h, "Action=PutMetricAlarm&AlarmName=alpha&Namespace=NS&MetricName=M")
	postForm(t, h, "Action=PutMetricAlarm&AlarmName=beta&Namespace=NS&MetricName=M")
	rec := postForm(t, h, "Action=DescribeAlarms&AlarmNames.member.1=alpha")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "alpha")
	assert.NotContains(t, rec.Body.String(), "beta")
}

func TestHandler_DeleteAlarms(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	postForm(t, h, "Action=PutMetricAlarm&AlarmName=to-del&Namespace=NS&MetricName=M")
	rec := postForm(t, h, "Action=DeleteAlarms&AlarmNames.member.1=to-del")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteAlarmsResponse")

	// Verify deleted
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
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	rec := postForm(t, h, "Action=UnknownOp")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetMetricData(t *testing.T) {
	t.Parallel()
	h := newCWHandler()

	// Put some metric data
	postForm(
		t,
		h,
		"Action=PutMetricData&Namespace=MyNS&MetricData.member.1.MetricName=Latency"+
			"&MetricData.member.1.Value=100&MetricData.member.1.Timestamp=2024-01-01T00:00:00Z",
	)
	postForm(
		t,
		h,
		"Action=PutMetricData&Namespace=MyNS&MetricData.member.1.MetricName=Latency"+
			"&MetricData.member.1.Value=200&MetricData.member.1.Timestamp=2024-01-01T00:01:00Z",
	)

	// GetMetricData
	rec := postForm(t, h, "Action=GetMetricData"+
		"&StartTime=2024-01-01T00:00:00Z"+
		"&EndTime=2024-01-01T00:10:00Z"+
		"&MetricDataQueries.member.1.Id=latency1"+
		"&MetricDataQueries.member.1.MetricStat.Metric.Namespace=MyNS"+
		"&MetricDataQueries.member.1.MetricStat.Metric.MetricName=Latency"+
		"&MetricDataQueries.member.1.MetricStat.Stat=Sum"+
		"&MetricDataQueries.member.1.MetricStat.Period=60")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetMetricDataResponse")
	assert.Contains(t, rec.Body.String(), "latency1")
}
