package cloudwatch_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
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
	return cloudwatch.NewHandler(cloudwatch.NewInMemoryBackend())
}

func TestCloudWatchHandler_Name(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	assert.Equal(t, "CloudWatch", h.Name())
}

func TestCloudWatchHandler_MatchPriority(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	assert.Equal(t, 80, h.MatchPriority())
}

func TestCloudWatchHandler_GetSupportedOperations(t *testing.T) {
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

func TestCloudWatchHandler_ExtractOperation(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=ListMetrics"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	assert.Equal(t, "ListMetrics", h.ExtractOperation(e.NewContext(req, httptest.NewRecorder())))
}

func TestCloudWatchHandler_ExtractResource(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=ListMetrics&Namespace=AWS/EC2"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	assert.Equal(t, "AWS/EC2", h.ExtractResource(e.NewContext(req, httptest.NewRecorder())))
}

func TestCloudWatchHandler_DeleteAlarms(t *testing.T) {
	t.Parallel()
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
}

func TestCloudWatchHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(t *testing.T, h *cloudwatch.Handler)
		name            string
		body            string
		wantContains    []string
		wantNotContains []string
		wantCode        int
	}{
		{
			name: "PutMetricData/success",
			body: "Action=PutMetricData&Version=2010-08-01&Namespace=Test" +
				"&MetricData.member.1.MetricName=Requests" +
				"&MetricData.member.1.Value=42" +
				"&MetricData.member.1.Unit=Count",
			wantCode:     http.StatusOK,
			wantContains: []string{"PutMetricDataResponse"},
		},
		{
			name:     "PutMetricData/missing namespace",
			body:     "Action=PutMetricData&MetricData.member.1.MetricName=CPU&MetricData.member.1.Value=10",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "ListMetrics",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h,
					"Action=PutMetricData&Namespace=MyNS&MetricData.member.1.MetricName=M1&MetricData.member.1.Value=1")
			},
			body:         "Action=ListMetrics&Namespace=MyNS",
			wantCode:     http.StatusOK,
			wantContains: []string{"ListMetricsResponse", "M1"},
		},
		{
			name:     "ListMetrics/empty",
			body:     "Action=ListMetrics",
			wantCode: http.StatusOK,
		},
		{
			name: "GetMetricStatistics/success",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
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
			wantCode:     http.StatusOK,
			wantContains: []string{"GetMetricStatisticsResponse"},
		},
		{
			name:     "GetMetricStatistics/bad start time",
			body:     "Action=GetMetricStatistics&Namespace=NS&MetricName=CPU&StartTime=bad&EndTime=bad&Period=60",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "GetMetricStatistics/bad end time",
			body: "Action=GetMetricStatistics&Namespace=NS&MetricName=CPU" +
				"&StartTime=2024-01-01T00:00:00Z&EndTime=bad&Period=60",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "GetMetricStatistics/bad period",
			body: "Action=GetMetricStatistics&Namespace=NS&MetricName=CPU" +
				"&StartTime=2024-01-01T00:00:00Z&EndTime=2024-01-01T01:00:00Z&Period=0",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "PutMetricAlarm/success",
			body: "Action=PutMetricAlarm&AlarmName=high-cpu&Namespace=AWS/EC2&MetricName=CPUUtilization" +
				"&ComparisonOperator=GreaterThanThreshold&Threshold=80&EvaluationPeriods=1&Period=60&Statistic=Average",
			wantCode:     http.StatusOK,
			wantContains: []string{"PutMetricAlarmResponse"},
		},
		{
			name:     "PutMetricAlarm/missing name",
			body:     "Action=PutMetricAlarm&Namespace=NS",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "DescribeAlarms/all",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=a1&Namespace=NS&MetricName=M")
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=a2&Namespace=NS&MetricName=M")
			},
			body:         "Action=DescribeAlarms",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAlarmsResponse", "a1"},
		},
		{
			name: "DescribeAlarms/by name",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=alpha&Namespace=NS&MetricName=M")
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=beta&Namespace=NS&MetricName=M")
			},
			body:            "Action=DescribeAlarms&AlarmNames.member.1=alpha",
			wantCode:        http.StatusOK,
			wantContains:    []string{"alpha"},
			wantNotContains: []string{"beta"},
		},
		{
			name: "TagResource/success",
			body: "Action=TagResource&ResourceARN=arn:aws:cloudwatch:us-east-1:123456789:alarm:test" +
				"&Tags.member.1.Key=env&Tags.member.1.Value=prod" +
				"&Tags.member.2.Key=team&Tags.member.2.Value=backend",
			wantCode:     http.StatusOK,
			wantContains: []string{"TagResourceResponse"},
		},
		{
			name:         "ListTagsForResource/empty",
			body:         "Action=ListTagsForResource&ResourceARN=arn:aws:cloudwatch:us-east-1:123456789:alarm:none",
			wantCode:     http.StatusOK,
			wantContains: []string{"ListTagsForResourceResponse"},
		},
		{
			name: "ListTagsForResource/with tags",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, "Action=TagResource&ResourceARN=arn:aws:cloudwatch:us-east-1:123456789:alarm:tagged"+
					"&Tags.member.1.Key=env&Tags.member.1.Value=prod")
			},
			body:         "Action=ListTagsForResource&ResourceARN=arn:aws:cloudwatch:us-east-1:123456789:alarm:tagged",
			wantCode:     http.StatusOK,
			wantContains: []string{"ListTagsForResourceResponse", "env", "prod"},
		},
		{
			name: "UntagResource/success",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, "Action=TagResource&ResourceARN=arn:aws:cloudwatch:us-east-1:123456789:alarm:untag"+
					"&Tags.member.1.Key=env&Tags.member.1.Value=prod")
			},
			body: "Action=UntagResource&ResourceARN=arn:aws:cloudwatch:us-east-1:123456789:alarm:untag" +
				"&TagKeys.member.1=env",
			wantCode:     http.StatusOK,
			wantContains: []string{"UntagResourceResponse"},
		},
		{
			name:     "UnknownAction",
			body:     "Action=UnknownOp",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "GetMetricData",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
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
			wantCode:     http.StatusOK,
			wantContains: []string{"GetMetricDataResponse", "latency1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newCWHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
			for _, s := range tt.wantNotContains {
				assert.NotContains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestCloudWatchHandler_TagLifecycle(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	arn := "arn:aws:cloudwatch:us-east-1:123456789:alarm:lifecycle"

	// Tag the resource with two tags.
	rec := postForm(t, h, "Action=TagResource&ResourceARN="+arn+
		"&Tags.member.1.Key=env&Tags.member.1.Value=prod"+
		"&Tags.member.2.Key=team&Tags.member.2.Value=backend")
	assert.Equal(t, http.StatusOK, rec.Code)

	// List tags and verify both are present.
	rec = postForm(t, h, "Action=ListTagsForResource&ResourceARN="+arn)
	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "env")
	assert.Contains(t, body, "prod")
	assert.Contains(t, body, "team")
	assert.Contains(t, body, "backend")

	// Untag one key.
	rec = postForm(t, h, "Action=UntagResource&ResourceARN="+arn+"&TagKeys.member.1=env")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify only the untagged key was removed.
	rec = postForm(t, h, "Action=ListTagsForResource&ResourceARN="+arn)
	assert.Equal(t, http.StatusOK, rec.Code)

	type listResp struct {
		XMLName xml.Name `xml:"ListTagsForResourceResponse"`
		Tags    []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"ListTagsForResourceResult>Tags>member"`
	}
	var resp listResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Tags, 1)
	assert.Equal(t, "team", resp.Tags[0].Key)
	assert.Equal(t, "backend", resp.Tags[0].Value)
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

func TestCloudWatchHandler_NewOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(t *testing.T, h *cloudwatch.Handler)
		name            string
		body            string
		wantContains    []string
		wantNotContains []string
		wantCode        int
	}{
		// PutCompositeAlarm
		{
			name: "PutCompositeAlarm/success",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=child-alarm&Namespace=NS&MetricName=M")
			},
			body: `Action=PutCompositeAlarm&AlarmName=parent-alarm` +
				`&AlarmRule=ALARM("child-alarm")` +
				`&AlarmDescription=test+composite` +
				`&AlarmActions.member.1=arn:aws:sns:us-east-1:123:topic-1` +
				`&ActionsEnabled=true`,
			wantCode:     http.StatusOK,
			wantContains: []string{"PutCompositeAlarmResponse"},
		},
		{
			name:     "PutCompositeAlarm/missing name",
			body:     `Action=PutCompositeAlarm&AlarmRule=ALARM("x")`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "PutCompositeAlarm/missing rule",
			body:     "Action=PutCompositeAlarm&AlarmName=x",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "PutCompositeAlarm/actions_disabled",
			body: `Action=PutCompositeAlarm&AlarmName=comp-disabled` +
				`&AlarmRule=ALARM("x")&ActionsEnabled=false`,
			wantCode:     http.StatusOK,
			wantContains: []string{"PutCompositeAlarmResponse"},
		},
		// DescribeAlarms with composite
		{
			name: "DescribeAlarms/with_composite",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=child-m&Namespace=NS&MetricName=M")
				postForm(t, h, `Action=PutCompositeAlarm&AlarmName=parent-c&AlarmRule=ALARM("child-m")`)
			},
			body:         "Action=DescribeAlarms",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAlarmsResponse", "child-m", "parent-c"},
		},
		{
			name: "DescribeAlarms/filter_composite_type",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=metric-only&Namespace=NS&MetricName=M")
				postForm(t, h, `Action=PutCompositeAlarm&AlarmName=comp-only&AlarmRule=ALARM("x")`)
			},
			body:            "Action=DescribeAlarms&AlarmTypes.member.1=CompositeAlarm",
			wantCode:        http.StatusOK,
			wantContains:    []string{"comp-only"},
			wantNotContains: []string{"metric-only"},
		},
		// DescribeAlarmsForMetric
		{
			name: "DescribeAlarmsForMetric/success",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=cpu-alarm&Namespace=AWS/EC2&MetricName=CPUUtilization")
				postForm(
					t,
					h,
					"Action=PutMetricAlarm&AlarmName=mem-alarm&Namespace=AWS/EC2&MetricName=MemoryUtilization",
				)
			},
			body:            "Action=DescribeAlarmsForMetric&Namespace=AWS/EC2&MetricName=CPUUtilization",
			wantCode:        http.StatusOK,
			wantContains:    []string{"DescribeAlarmsForMetricResponse", "cpu-alarm"},
			wantNotContains: []string{"mem-alarm"},
		},
		{
			name:         "DescribeAlarmsForMetric/empty",
			body:         "Action=DescribeAlarmsForMetric&Namespace=AWS/EC2&MetricName=NotExist",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAlarmsForMetricResponse"},
		},
		// DescribeAlarmHistory
		{
			name: "DescribeAlarmHistory/success",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=hist-alarm&Namespace=NS&MetricName=M")
				postForm(t, h, "Action=SetAlarmState&AlarmName=hist-alarm&StateValue=ALARM&StateReason=test")
			},
			body:         "Action=DescribeAlarmHistory&AlarmName=hist-alarm",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAlarmHistoryResponse", "hist-alarm"},
		},
		{
			name:         "DescribeAlarmHistory/empty",
			body:         "Action=DescribeAlarmHistory&AlarmName=nonexistent-alarm",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAlarmHistoryResponse"},
		},
		{
			name: "DescribeAlarmHistory/with_dates",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=date-alarm&Namespace=NS&MetricName=M")
				postForm(t, h, "Action=SetAlarmState&AlarmName=date-alarm&StateValue=ALARM&StateReason=test")
			},
			body: "Action=DescribeAlarmHistory&AlarmName=date-alarm" +
				"&StartDate=2020-01-01T00:00:00Z&EndDate=2099-01-01T00:00:00Z",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAlarmHistoryResponse"},
		},
		// SetAlarmState
		{
			name: "SetAlarmState/success",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=state-alarm&Namespace=NS&MetricName=M")
			},
			body:         "Action=SetAlarmState&AlarmName=state-alarm&StateValue=ALARM&StateReason=manual",
			wantCode:     http.StatusOK,
			wantContains: []string{"SetAlarmStateResponse"},
		},
		{
			name:     "SetAlarmState/missing name",
			body:     "Action=SetAlarmState&StateValue=ALARM",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "SetAlarmState/not found",
			body:     "Action=SetAlarmState&AlarmName=does-not-exist&StateValue=ALARM&StateReason=test",
			wantCode: http.StatusBadRequest,
		},
		// EnableAlarmActions
		{
			name: "EnableAlarmActions/success",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=enable-alarm&Namespace=NS&MetricName=M")
			},
			body:         "Action=EnableAlarmActions&AlarmNames.member.1=enable-alarm",
			wantCode:     http.StatusOK,
			wantContains: []string{"EnableAlarmActionsResponse"},
		},
		// DisableAlarmActions
		{
			name: "DisableAlarmActions/success",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=disable-alarm&Namespace=NS&MetricName=M")
			},
			body:         "Action=DisableAlarmActions&AlarmNames.member.1=disable-alarm",
			wantCode:     http.StatusOK,
			wantContains: []string{"DisableAlarmActionsResponse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newCWHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
			for _, s := range tt.wantNotContains {
				assert.NotContains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestCloudWatchHandler_Dashboards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(t *testing.T, h *cloudwatch.Handler)
		name            string
		body            string
		wantContains    []string
		wantNotContains []string
		wantCode        int
	}{
		{
			name:         "PutDashboard/success",
			body:         `Action=PutDashboard&DashboardName=MyDash&DashboardBody={"widgets":[]}`,
			wantCode:     http.StatusOK,
			wantContains: []string{"PutDashboardResponse"},
		},
		{
			name:     "PutDashboard/missing name",
			body:     `Action=PutDashboard&DashboardBody={}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "GetDashboard/success",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, `Action=PutDashboard&DashboardName=FetchMe&DashboardBody={"widgets":[]}`)
			},
			body:         "Action=GetDashboard&DashboardName=FetchMe",
			wantCode:     http.StatusOK,
			wantContains: []string{"GetDashboardResponse", "FetchMe"},
		},
		{
			name:     "GetDashboard/not found",
			body:     "Action=GetDashboard&DashboardName=Ghost",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "GetDashboard/missing name",
			body:     "Action=GetDashboard",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "ListDashboards/success",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, `Action=PutDashboard&DashboardName=prod-web&DashboardBody={}`)
				postForm(t, h, `Action=PutDashboard&DashboardName=prod-api&DashboardBody={}`)
			},
			body:         "Action=ListDashboards",
			wantCode:     http.StatusOK,
			wantContains: []string{"ListDashboardsResponse", "prod-web", "prod-api"},
		},
		{
			name: "ListDashboards/with prefix",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, `Action=PutDashboard&DashboardName=prod-web&DashboardBody={}`)
				postForm(t, h, `Action=PutDashboard&DashboardName=staging-web&DashboardBody={}`)
			},
			body:            "Action=ListDashboards&DashboardNamePrefix=prod-",
			wantCode:        http.StatusOK,
			wantContains:    []string{"prod-web"},
			wantNotContains: []string{"staging-web"},
		},
		{
			name:         "ListDashboards/empty",
			body:         "Action=ListDashboards",
			wantCode:     http.StatusOK,
			wantContains: []string{"ListDashboardsResponse"},
		},
		{
			name: "DeleteDashboards/success",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, `Action=PutDashboard&DashboardName=to-delete&DashboardBody={}`)
			},
			body:         "Action=DeleteDashboards&DashboardNames.member.1=to-delete",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteDashboardsResponse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newCWHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
			for _, s := range tt.wantNotContains {
				assert.NotContains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestCloudWatchHandler_GetSupportedOperations_DashboardOps(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "PutDashboard")
	assert.Contains(t, ops, "GetDashboard")
	assert.Contains(t, ops, "ListDashboards")
	assert.Contains(t, ops, "DeleteDashboards")
}

// newCWHandlerWithBackend creates a handler and returns it alongside the typed backend for seeding.
func newCWHandlerWithBackend() (*cloudwatch.Handler, *cloudwatch.InMemoryBackend) {
	backend := cloudwatch.NewInMemoryBackend()

	return cloudwatch.NewHandler(backend), backend
}

func TestCloudWatchHandler_AlarmMuteRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(t *testing.T, h *cloudwatch.Handler, b *cloudwatch.InMemoryBackend)
		name            string
		body            string
		wantContains    []string
		wantNotContains []string
		wantCode        int
	}{
		{
			name: "PutAlarmMuteRule/success",
			body: "Action=PutAlarmMuteRule&MuteName=my-mute-rule" +
				"&Description=suppress+noisy+alerts" +
				"&AlarmNames.member.1=alarm-a" +
				"&MuteDuration=3600",
			wantCode:     http.StatusOK,
			wantContains: []string{"PutAlarmMuteRuleResponse"},
		},
		{
			name:     "PutAlarmMuteRule/missing name",
			body:     "Action=PutAlarmMuteRule",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "UpdateAlarmMuteRule/success",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutAlarmMuteRuleInternal(&cloudwatch.AlarmMuteRule{MuteName: "update-mute"})
			},
			body:         "Action=UpdateAlarmMuteRule&MuteName=update-mute&Description=updated",
			wantCode:     http.StatusOK,
			wantContains: []string{"UpdateAlarmMuteRuleResponse"},
		},
		{
			name:     "UpdateAlarmMuteRule/not found",
			body:     "Action=UpdateAlarmMuteRule&MuteName=missing-mute",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "GetAlarmMuteRule/success",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutAlarmMuteRuleInternal(&cloudwatch.AlarmMuteRule{
					MuteName:    "my-mute-rule",
					Description: "suppress noisy alerts",
				})
			},
			body:         "Action=GetAlarmMuteRule&MuteName=my-mute-rule",
			wantCode:     http.StatusOK,
			wantContains: []string{"GetAlarmMuteRuleResponse", "my-mute-rule", "suppress noisy alerts"},
		},
		{
			name:     "GetAlarmMuteRule/not found",
			body:     "Action=GetAlarmMuteRule&MuteName=ghost-rule",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "GetAlarmMuteRule/missing name",
			body:     "Action=GetAlarmMuteRule",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "DeleteAlarmMuteRule/success",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutAlarmMuteRuleInternal(&cloudwatch.AlarmMuteRule{MuteName: "delete-me"})
			},
			body:         "Action=DeleteAlarmMuteRule&MuteName=delete-me",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteAlarmMuteRuleResponse"},
		},
		{
			name:     "DeleteAlarmMuteRule/not found",
			body:     "Action=DeleteAlarmMuteRule&MuteName=ghost-rule",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DeleteAlarmMuteRule/missing name",
			body:     "Action=DeleteAlarmMuteRule",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newCWHandlerWithBackend()
			if tt.setup != nil {
				tt.setup(t, h, b)
			}

			rec := postForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
			for _, s := range tt.wantNotContains {
				assert.NotContains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestCloudWatchHandler_AnomalyDetector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(t *testing.T, h *cloudwatch.Handler, b *cloudwatch.InMemoryBackend)
		name            string
		body            string
		wantContains    []string
		wantNotContains []string
		wantCode        int
	}{
		{
			name: "DescribeAnomalyDetectors/success",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutAnomalyDetectorInternal(&cloudwatch.AnomalyDetector{
					Namespace:  "AWS/EC2",
					MetricName: "CPUUtilization",
					Stat:       "Average",
				})
			},
			body:         "Action=DescribeAnomalyDetectors&Namespace=AWS/EC2",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAnomalyDetectorsResponse", "CPUUtilization"},
		},
		{
			name:         "DescribeAnomalyDetectors/empty",
			body:         "Action=DescribeAnomalyDetectors",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAnomalyDetectorsResponse"},
		},
		{
			name: "DescribeAnomalyDetectors/namespace filter",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutAnomalyDetectorInternal(&cloudwatch.AnomalyDetector{
					Namespace:  "AWS/EC2",
					MetricName: "CPUUtilization",
					Stat:       "Average",
				})
				b.PutAnomalyDetectorInternal(&cloudwatch.AnomalyDetector{
					Namespace:  "AWS/RDS",
					MetricName: "DatabaseConnections",
					Stat:       "Sum",
				})
			},
			body:            "Action=DescribeAnomalyDetectors&Namespace=AWS/EC2",
			wantCode:        http.StatusOK,
			wantContains:    []string{"CPUUtilization"},
			wantNotContains: []string{"DatabaseConnections"},
		},
		{
			name: "DeleteAnomalyDetector/success",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutAnomalyDetectorInternal(&cloudwatch.AnomalyDetector{
					Namespace:  "NS",
					MetricName: "Metric",
					Stat:       "Sum",
				})
			},
			body: "Action=DeleteAnomalyDetector" +
				"&SingleMetricAnomalyDetector.Namespace=NS" +
				"&SingleMetricAnomalyDetector.MetricName=Metric" +
				"&SingleMetricAnomalyDetector.Stat=Sum",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteAnomalyDetectorResponse"},
		},
		{
			name: "DeleteAnomalyDetector/not found",
			body: "Action=DeleteAnomalyDetector" +
				"&SingleMetricAnomalyDetector.Namespace=NS" +
				"&SingleMetricAnomalyDetector.MetricName=Ghost" +
				"&SingleMetricAnomalyDetector.Stat=Sum",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DeleteAnomalyDetector/missing namespace",
			body:     "Action=DeleteAnomalyDetector&SingleMetricAnomalyDetector.MetricName=M",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newCWHandlerWithBackend()
			if tt.setup != nil {
				tt.setup(t, h, b)
			}

			rec := postForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
			for _, s := range tt.wantNotContains {
				assert.NotContains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestCloudWatchHandler_InsightRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(t *testing.T, h *cloudwatch.Handler, b *cloudwatch.InMemoryBackend)
		name            string
		body            string
		wantContains    []string
		wantNotContains []string
		wantCode        int
	}{
		{
			name: "PutInsightRule/success",
			body: "Action=PutInsightRule&RuleName=rule-created&RuleState=ENABLED" +
				"&RuleDefinition=%7B%22Schema%22%3A%22CloudWatchLogRule%22%7D",
			wantCode:     http.StatusOK,
			wantContains: []string{"PutInsightRuleResponse"},
		},
		{
			name:     "PutInsightRule/missing rule name",
			body:     "Action=PutInsightRule",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "UpdateInsightRule/success",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutInsightRuleInternal(&cloudwatch.InsightRule{Name: "rule-update", State: "ENABLED"})
			},
			body:         "Action=UpdateInsightRule&RuleName=rule-update&RuleState=DISABLED",
			wantCode:     http.StatusOK,
			wantContains: []string{"UpdateInsightRuleResponse"},
		},
		{
			name:     "UpdateInsightRule/not found",
			body:     "Action=UpdateInsightRule&RuleName=missing-rule",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "DescribeInsightRules/success",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutInsightRuleInternal(&cloudwatch.InsightRule{Name: "rule-alpha", State: "ENABLED"})
				b.PutInsightRuleInternal(&cloudwatch.InsightRule{Name: "rule-beta", State: "DISABLED"})
			},
			body:         "Action=DescribeInsightRules",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeInsightRulesResponse", "rule-alpha", "rule-beta"},
		},
		{
			name:         "DescribeInsightRules/empty",
			body:         "Action=DescribeInsightRules",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeInsightRulesResponse"},
		},
		{
			name: "DeleteInsightRules/success",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutInsightRuleInternal(&cloudwatch.InsightRule{Name: "del-rule"})
			},
			body:         "Action=DeleteInsightRules&RuleNames.member.1=del-rule",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteInsightRulesResponse"},
		},
		{
			name:         "DeleteInsightRules/not found returns failure entry",
			body:         "Action=DeleteInsightRules&RuleNames.member.1=ghost-rule",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteInsightRulesResponse", "ResourceNotFoundException"},
		},
		{
			name:     "DeleteInsightRules/missing rule names",
			body:     "Action=DeleteInsightRules",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "DisableInsightRules/success",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutInsightRuleInternal(&cloudwatch.InsightRule{Name: "enabled-rule", State: "ENABLED"})
			},
			body:         "Action=DisableInsightRules&RuleNames.member.1=enabled-rule",
			wantCode:     http.StatusOK,
			wantContains: []string{"DisableInsightRulesResponse"},
		},
		{
			name:         "DisableInsightRules/not found returns failure entry",
			body:         "Action=DisableInsightRules&RuleNames.member.1=ghost-rule",
			wantCode:     http.StatusOK,
			wantContains: []string{"DisableInsightRulesResponse", "ResourceNotFoundException"},
		},
		{
			name:     "DisableInsightRules/missing rule names",
			body:     "Action=DisableInsightRules",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "EnableInsightRules/success",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutInsightRuleInternal(&cloudwatch.InsightRule{Name: "disabled-rule", State: "DISABLED"})
			},
			body:         "Action=EnableInsightRules&RuleNames.member.1=disabled-rule",
			wantCode:     http.StatusOK,
			wantContains: []string{"EnableInsightRulesResponse"},
		},
		{
			name:         "EnableInsightRules/not found returns failure entry",
			body:         "Action=EnableInsightRules&RuleNames.member.1=ghost-rule",
			wantCode:     http.StatusOK,
			wantContains: []string{"EnableInsightRulesResponse", "ResourceNotFoundException"},
		},
		{
			name:     "EnableInsightRules/missing rule names",
			body:     "Action=EnableInsightRules",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newCWHandlerWithBackend()
			if tt.setup != nil {
				tt.setup(t, h, b)
			}

			rec := postForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
			for _, s := range tt.wantNotContains {
				assert.NotContains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestCloudWatchHandler_MetricStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(t *testing.T, h *cloudwatch.Handler, b *cloudwatch.InMemoryBackend)
		name            string
		body            string
		wantContains    []string
		wantNotContains []string
		wantCode        int
	}{
		{
			name: "PutMetricStream/success",
			body: "Action=PutMetricStream&Name=my-stream" +
				"&FirehoseArn=arn%3Aaws%3Afirehose%3Aus-east-1%3A123456789012%3Adeliverystream%2Fmain" +
				"&RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fstream-role",
			wantCode:     http.StatusOK,
			wantContains: []string{"PutMetricStreamResponse"},
		},
		{
			name:     "PutMetricStream/missing name",
			body:     "Action=PutMetricStream",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "UpdateMetricStream/success",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutMetricStreamInternal(&cloudwatch.MetricStream{Name: "my-stream"})
			},
			body:         "Action=UpdateMetricStream&Name=my-stream&OutputFormat=opentelemetry0.7",
			wantCode:     http.StatusOK,
			wantContains: []string{"UpdateMetricStreamResponse"},
		},
		{
			name:     "UpdateMetricStream/not found",
			body:     "Action=UpdateMetricStream&Name=missing-stream",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "DeleteMetricStream/success",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutMetricStreamInternal(&cloudwatch.MetricStream{Name: "my-stream"})
			},
			body:         "Action=DeleteMetricStream&Name=my-stream",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteMetricStreamResponse"},
		},
		{
			name:     "DeleteMetricStream/not found",
			body:     "Action=DeleteMetricStream&Name=ghost-stream",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DeleteMetricStream/missing name",
			body:     "Action=DeleteMetricStream",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newCWHandlerWithBackend()
			if tt.setup != nil {
				tt.setup(t, h, b)
			}

			rec := postForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
			for _, s := range tt.wantNotContains {
				assert.NotContains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestCloudWatchHandler_DescribeAlarmContributors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(t *testing.T, h *cloudwatch.Handler)
		name            string
		body            string
		wantContains    []string
		wantNotContains []string
		wantCode        int
	}{
		{
			name: "DescribeAlarmContributors/success",
			setup: func(t *testing.T, h *cloudwatch.Handler) {
				t.Helper()
				postForm(t, h, "Action=PutMetricAlarm&AlarmName=alarm-with-contrib&Namespace=NS&MetricName=M")
			},
			body:         "Action=DescribeAlarmContributors&AlarmName=alarm-with-contrib",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAlarmContributorsResponse"},
		},
		{
			name:     "DescribeAlarmContributors/alarm not found",
			body:     "Action=DescribeAlarmContributors&AlarmName=ghost-alarm",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DescribeAlarmContributors/missing alarm name",
			body:     "Action=DescribeAlarmContributors",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newCWHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
			for _, s := range tt.wantNotContains {
				assert.NotContains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestCloudWatchHandler_InsightRulesStateLifecycle(t *testing.T) {
	t.Parallel()

	h, b := newCWHandlerWithBackend()

	// Seed an enabled insight rule.
	b.PutInsightRuleInternal(&cloudwatch.InsightRule{Name: "lifecycle-rule", State: "ENABLED"})

	// Describe: should see ENABLED.
	rec := postForm(t, h, "Action=DescribeInsightRules")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ENABLED")
	assert.Contains(t, rec.Body.String(), "lifecycle-rule")

	// Disable it.
	rec = postForm(t, h, "Action=DisableInsightRules&RuleNames.member.1=lifecycle-rule")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DisableInsightRulesResponse")

	// Describe: should see DISABLED now.
	rec = postForm(t, h, "Action=DescribeInsightRules")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DISABLED")

	// Re-enable it.
	rec = postForm(t, h, "Action=EnableInsightRules&RuleNames.member.1=lifecycle-rule")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "EnableInsightRulesResponse")

	// Describe: should see ENABLED again.
	rec = postForm(t, h, "Action=DescribeInsightRules")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ENABLED")

	// Delete it.
	rec = postForm(t, h, "Action=DeleteInsightRules&RuleNames.member.1=lifecycle-rule")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteInsightRulesResponse")

	// Describe: rule should be gone, no failures in describe.
	rec = postForm(t, h, "Action=DescribeInsightRules")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "lifecycle-rule")
}

func TestCloudWatchHandler_NewOpsInSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	ops := h.GetSupportedOperations()

	require.Contains(t, ops, "DeleteAlarmMuteRule")
	require.Contains(t, ops, "PutAlarmMuteRule")
	require.Contains(t, ops, "UpdateAlarmMuteRule")
	require.Contains(t, ops, "DeleteAnomalyDetector")
	require.Contains(t, ops, "DeleteInsightRules")
	require.Contains(t, ops, "PutInsightRule")
	require.Contains(t, ops, "UpdateInsightRule")
	require.Contains(t, ops, "DeleteMetricStream")
	require.Contains(t, ops, "PutMetricStream")
	require.Contains(t, ops, "UpdateMetricStream")
	require.Contains(t, ops, "DescribeAlarmContributors")
	require.Contains(t, ops, "DescribeAnomalyDetectors")
	require.Contains(t, ops, "DescribeInsightRules")
	require.Contains(t, ops, "DisableInsightRules")
	require.Contains(t, ops, "EnableInsightRules")
	require.Contains(t, ops, "GetAlarmMuteRule")
}

func TestCloudWatchHandler_InsightRules_SortedOutput(t *testing.T) {
	t.Parallel()

	h, b := newCWHandlerWithBackend()

	// Seed rules in reverse alphabetical order.
	b.PutInsightRuleInternal(&cloudwatch.InsightRule{Name: "rule-zz"})
	b.PutInsightRuleInternal(&cloudwatch.InsightRule{Name: "rule-aa"})
	b.PutInsightRuleInternal(&cloudwatch.InsightRule{Name: "rule-mm"})

	rec := postForm(t, h, "Action=DescribeInsightRules")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()

	// Verify alphabetical order by checking position of each rule name.
	aaPos := strings.Index(body, "rule-aa")
	mmPos := strings.Index(body, "rule-mm")
	zzPos := strings.Index(body, "rule-zz")
	require.Positive(t, aaPos)
	require.Positive(t, mmPos)
	require.Positive(t, zzPos)
	assert.Less(t, aaPos, mmPos, "rule-aa should appear before rule-mm")
	assert.Less(t, mmPos, zzPos, "rule-mm should appear before rule-zz")
}

func TestCloudWatchHandler_InsightRules_ArnInResponse(t *testing.T) {
	t.Parallel()

	h, b := newCWHandlerWithBackend()
	b.PutInsightRuleInternal(&cloudwatch.InsightRule{Name: "arn-rule"})

	rec := postForm(t, h, "Action=DescribeInsightRules")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "RuleArn")
	assert.Contains(t, body, "insight-rule/arn-rule")
}

func TestCloudWatchHandler_InsightRules_FailureDescription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
	}{
		{name: "Delete", action: "DeleteInsightRules"},
		{name: "Disable", action: "DisableInsightRules"},
		{name: "Enable", action: "EnableInsightRules"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newCWHandler()
			rec := postForm(t, h, "Action="+tt.action+"&RuleNames.member.1=nonexistent-rule")
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, "ResourceNotFoundException")
			assert.Contains(t, body, "FailureDescription")
			assert.Contains(t, body, "nonexistent-rule")
		})
	}
}

func TestCloudWatchHandler_AnomalyDetector_MetricNameFilter(t *testing.T) {
	t.Parallel()

	h, b := newCWHandlerWithBackend()
	b.PutAnomalyDetectorInternal(
		&cloudwatch.AnomalyDetector{Namespace: "AWS/EC2", MetricName: "CPUUtilization", Stat: "Average"},
	)
	b.PutAnomalyDetectorInternal(
		&cloudwatch.AnomalyDetector{Namespace: "AWS/EC2", MetricName: "NetworkIn", Stat: "Sum"},
	)

	rec := postForm(t, h, "Action=DescribeAnomalyDetectors&MetricName=CPUUtilization")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "CPUUtilization")
	assert.NotContains(t, body, "NetworkIn")
}

func TestCloudWatchHandler_AnomalyDetector_DefaultState(t *testing.T) {
	t.Parallel()

	_, b := newCWHandlerWithBackend()
	b.PutAnomalyDetectorInternal(&cloudwatch.AnomalyDetector{Namespace: "NS", MetricName: "M", Stat: "Sum"})

	p, err := b.DescribeAnomalyDetectors("NS", "", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "TRAINED_INSUFFICIENT_DATA", p.Data[0].StateValue)
}

func TestCloudWatchHandler_AlarmMuteRule_AlarmNames(t *testing.T) {
	t.Parallel()

	h, b := newCWHandlerWithBackend()
	b.PutAlarmMuteRuleInternal(&cloudwatch.AlarmMuteRule{
		MuteName:   "mute-with-alarms",
		AlarmNames: []string{"alarm-1", "alarm-2", "alarm-3"},
	})

	rec := postForm(t, h, "Action=GetAlarmMuteRule&MuteName=mute-with-alarms")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "alarm-1")
	assert.Contains(t, body, "alarm-2")
	assert.Contains(t, body, "alarm-3")
	assert.Contains(t, body, "AlarmNames")
}

func TestCloudWatchHandler_DescribeAlarmContributors_CompositeAlarm(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	// Create a child metric alarm and a composite alarm referencing it.
	rec := postForm(t, h, "Action=PutMetricAlarm&AlarmName=child-for-contrib2&Namespace=NS&MetricName=M")
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postForm(
		t,
		h,
		`Action=PutCompositeAlarm&AlarmName=composite-for-contrib2&AlarmRule=ALARM("child-for-contrib2")`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeAlarmContributors on the composite alarm should succeed.
	rec = postForm(t, h, "Action=DescribeAlarmContributors&AlarmName=composite-for-contrib2")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeAlarmContributorsResponse")
}

func TestCloudWatchRefinement1_ErrValidation(t *testing.T) {
	t.Parallel()

	// ErrValidation should be a distinct error from resource-not-found sentinels.
	require.NotEqual(t, cloudwatch.ErrValidation, cloudwatch.ErrAlarmNotFound)
	require.NotEqual(t, cloudwatch.ErrValidation, cloudwatch.ErrAlarmMuteRuleNotFound)
	require.NotEqual(t, cloudwatch.ErrValidation, cloudwatch.ErrAnomalyDetectorNotFound)
	require.NotEqual(t, cloudwatch.ErrValidation, cloudwatch.ErrMetricStreamNotFound)
}

func TestCloudWatchRefinement1_InsightRuleArnSetInPutInternal(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	b.PutInsightRuleInternal(&cloudwatch.InsightRule{Name: "arn-check-rule"})

	p, err := b.DescribeInsightRules("", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)

	assert.NotEmpty(t, p.Data[0].Arn, "Arn should be set by PutInsightRuleInternal")
	assert.Contains(t, p.Data[0].Arn, "arn-check-rule")
	assert.False(t, p.Data[0].CreatedAt.IsZero(), "CreatedAt should be set by PutInsightRuleInternal")
}

func TestCloudWatchRefinement1_DescribeAnomalyDetectors_SortedOutput(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	b.PutAnomalyDetectorInternal(&cloudwatch.AnomalyDetector{Namespace: "ZNS", MetricName: "M", Stat: "Sum"})
	b.PutAnomalyDetectorInternal(&cloudwatch.AnomalyDetector{Namespace: "ANS", MetricName: "M", Stat: "Sum"})
	b.PutAnomalyDetectorInternal(&cloudwatch.AnomalyDetector{Namespace: "MNS", MetricName: "M", Stat: "Sum"})

	p, err := b.DescribeAnomalyDetectors("", "", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 3)
	assert.Equal(t, "ANS", p.Data[0].Namespace)
	assert.Equal(t, "MNS", p.Data[1].Namespace)
	assert.Equal(t, "ZNS", p.Data[2].Namespace)
}

func TestCloudWatchHandler_PutMetricFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		body           string
		wantStatusCode int
	}{
		{
			name: "valid",
			body: "Action=PutMetricFilter&FilterName=my-filter&LogGroupName=%2Faws%2Flambda%2Ffn&FilterPattern=%5B*%5D" +
				"&MetricTransformations.member.1.MetricName=Count" +
				"&MetricTransformations.member.1.MetricNamespace=MyApp" +
				"&MetricTransformations.member.1.MetricValue=1",
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "missing_filter_name",
			body:           "Action=PutMetricFilter&LogGroupName=%2Faws%2Flambda%2Ffn",
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name:           "missing_log_group",
			body:           "Action=PutMetricFilter&FilterName=my-filter",
			wantStatusCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newCWHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)
		})
	}
}

func TestCloudWatchHandler_DescribeMetricFilters(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	postForm(t, h, "Action=PutMetricFilter&FilterName=alpha&LogGroupName=%2Faws%2Flambda%2Ffn1&FilterPattern=a")
	postForm(t, h, "Action=PutMetricFilter&FilterName=beta&LogGroupName=%2Faws%2Flambda%2Ffn1&FilterPattern=b")
	postForm(t, h, "Action=PutMetricFilter&FilterName=gamma&LogGroupName=%2Faws%2Fec2&FilterPattern=c")

	rec := postForm(t, h, "Action=DescribeMetricFilters&LogGroupName=%2Faws%2Flambda%2Ffn1")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "alpha")
	assert.Contains(t, rec.Body.String(), "beta")
	assert.NotContains(t, rec.Body.String(), "gamma")
}

func TestCloudWatchHandler_DeleteMetricFilter(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	postForm(t, h, "Action=PutMetricFilter&FilterName=del-filter&LogGroupName=%2Faws%2Flambda%2Ffn&FilterPattern=x")

	rec := postForm(t, h, "Action=DeleteMetricFilter&FilterName=del-filter&LogGroupName=%2Faws%2Flambda%2Ffn")
	assert.Equal(t, http.StatusOK, rec.Code)

	// second delete should 400
	rec2 := postForm(t, h, "Action=DeleteMetricFilter&FilterName=del-filter&LogGroupName=%2Faws%2Flambda%2Ffn")
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestCloudWatchHandler_PutAnomalyDetector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		body           string
		wantStatusCode int
	}{
		{
			name: "valid",
			body: "Action=PutAnomalyDetector" +
				"&SingleMetricAnomalyDetector.Namespace=AWS%2FEC2" +
				"&SingleMetricAnomalyDetector.MetricName=CPUUtilization" +
				"&SingleMetricAnomalyDetector.Stat=Average",
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "missing_namespace",
			body:           "Action=PutAnomalyDetector&SingleMetricAnomalyDetector.MetricName=CPUUtilization",
			wantStatusCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newCWHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)
		})
	}
}

func TestCloudWatchHandler_ListMetricStreams(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	b.PutMetricStreamInternal(&cloudwatch.MetricStream{Name: "stream1", State: "running", OutputFormat: "json"})
	b.PutMetricStreamInternal(&cloudwatch.MetricStream{Name: "stream2", State: "running", OutputFormat: "json"})
	h := cloudwatch.NewHandler(b)

	rec := postForm(t, h, "Action=ListMetricStreams")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "stream1")
	assert.Contains(t, rec.Body.String(), "stream2")
}

func TestCloudWatchHandler_GetMetricStream(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	b.PutMetricStreamInternal(&cloudwatch.MetricStream{Name: "mystream", State: "running", OutputFormat: "json"})
	h := cloudwatch.NewHandler(b)

	rec := postForm(t, h, "Action=GetMetricStream&Name=mystream")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "mystream")
}

func TestCloudWatchHandler_GetMetricStream_NotFound(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	rec := postForm(t, h, "Action=GetMetricStream&Name=nonexistent")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCloudWatchHandler_GetInsightRuleReport(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	b.PutInsightRuleInternal(&cloudwatch.InsightRule{Name: "report-rule", State: "ENABLED", Definition: "{}"})
	h := cloudwatch.NewHandler(b)

	const reportBody = "Action=GetInsightRuleReport&RuleName=report-rule" +
		"&StartTime=2023-01-01T00%3A00%3A00Z&EndTime=2023-01-02T00%3A00%3A00Z&Period=3600"
	rec := postForm(t, h, reportBody)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetInsightRuleReportResponse")
}

func TestCloudWatchHandler_GetInsightRuleReport_NotFound(t *testing.T) {
	t.Parallel()
	h := newCWHandler()
	rec := postForm(t, h, "Action=GetInsightRuleReport&RuleName=nonexistent")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCloudWatchHandler_DescribeAlarms_AlarmNamePrefix(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	for _, name := range []string{"prod-cpu", "prod-mem", "staging-cpu"} {
		postForm(t, h, "Action=PutMetricAlarm&AlarmName="+name+
			"&Namespace=AWS%2FEC2&MetricName=CPU&ComparisonOperator=GreaterThanThreshold"+
			"&EvaluationPeriods=1&Period=60&Threshold=80&Statistic=Average")
	}

	rec := postForm(t, h, "Action=DescribeAlarms&AlarmNamePrefix=prod-")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "prod-cpu")
	assert.Contains(t, rec.Body.String(), "prod-mem")
	assert.NotContains(t, rec.Body.String(), "staging-cpu")
}

func TestCloudWatchHandler_PutMetricAlarm_NewFields(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	rec := postForm(t, h, "Action=PutMetricAlarm&AlarmName=test-alarm"+
		"&Namespace=AWS%2FEC2&MetricName=CPU&ComparisonOperator=GreaterThanThreshold"+
		"&EvaluationPeriods=3&Period=60&Threshold=80&Statistic=Average"+
		"&DatapointsToAlarm=2&TreatMissingData=notBreaching"+
		"&Dimensions.member.1.Name=InstanceId&Dimensions.member.1.Value=i-1234567890abcdef0")
	assert.Equal(t, http.StatusOK, rec.Code)

	type alarm struct {
		TreatMissingData  string `xml:"TreatMissingData"`
		DatapointsToAlarm int    `xml:"DatapointsToAlarm"`
	}
	type resp struct {
		XMLName      xml.Name `xml:"DescribeAlarmsResponse"`
		MetricAlarms []alarm  `xml:"DescribeAlarmsResult>MetricAlarms>member"`
	}

	rec2 := postForm(t, h, "Action=DescribeAlarms&AlarmNames.member.1=test-alarm")
	require.Equal(t, http.StatusOK, rec2.Code)

	var out resp
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &out))
	require.Len(t, out.MetricAlarms, 1)
	assert.Equal(t, 2, out.MetricAlarms[0].DatapointsToAlarm)
	assert.Equal(t, "notBreaching", out.MetricAlarms[0].TreatMissingData)
}

// ---------------------------------------------------------------------------
// Accuracy audit handler tests — gaps from issue #1686
// ---------------------------------------------------------------------------

func TestCloudWatchHandler_PutMetricData_WithStatisticSet(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	// Put a StatisticSet datum.
	body := strings.Join([]string{
		"Action=PutMetricData",
		"Namespace=App%2FMetrics",
		"MetricData.member.1.MetricName=Latency",
		"MetricData.member.1.Unit=Milliseconds",
		"MetricData.member.1.StatisticValues.SampleCount=10",
		"MetricData.member.1.StatisticValues.Sum=250",
		"MetricData.member.1.StatisticValues.Minimum=20",
		"MetricData.member.1.StatisticValues.Maximum=35",
	}, "&")
	rec := postForm(t, h, body)
	require.Equal(t, http.StatusOK, rec.Code)

	// GetMetricStatistics should reflect the pre-aggregated values.
	statsBody := strings.Join([]string{
		"Action=GetMetricStatistics",
		"Namespace=App%2FMetrics",
		"MetricName=Latency",
		"StartTime=2000-01-01T00%3A00%3A00Z",
		"EndTime=2100-01-01T00%3A00%3A00Z",
		"Period=600",
		"Statistics.member.1=Sum",
		"Statistics.member.2=SampleCount",
		"Statistics.member.3=Minimum",
		"Statistics.member.4=Maximum",
	}, "&")
	statsRec := postForm(t, h, statsBody)
	require.Equal(t, http.StatusOK, statsRec.Code)

	type dp struct {
		Sum         *float64 `xml:"Sum"`
		SampleCount *float64 `xml:"SampleCount"`
		Minimum     *float64 `xml:"Minimum"`
		Maximum     *float64 `xml:"Maximum"`
	}
	type resp struct {
		Datapoints []dp `xml:"GetMetricStatisticsResult>Datapoints>member"`
	}
	var out resp
	require.NoError(t, xml.Unmarshal(statsRec.Body.Bytes(), &out))
	require.Len(t, out.Datapoints, 1)
	require.NotNil(t, out.Datapoints[0].Sum)
	assert.InDelta(t, 250.0, *out.Datapoints[0].Sum, 1e-9)
	require.NotNil(t, out.Datapoints[0].SampleCount)
	assert.InDelta(t, 10.0, *out.Datapoints[0].SampleCount, 1e-9)
}

func TestCloudWatchHandler_PutMetricData_WithDimensions(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	body := strings.Join([]string{
		"Action=PutMetricData",
		"Namespace=AWS%2FEC2",
		"MetricData.member.1.MetricName=CPUUtilization",
		"MetricData.member.1.Value=75",
		"MetricData.member.1.Dimensions.member.1.Name=InstanceId",
		"MetricData.member.1.Dimensions.member.1.Value=i-123",
	}, "&")
	rec := postForm(t, h, body)
	require.Equal(t, http.StatusOK, rec.Code)

	listBody := "Action=ListMetrics&Namespace=AWS%2FEC2&MetricName=CPUUtilization"
	listRec := postForm(t, h, listBody)
	require.Equal(t, http.StatusOK, listRec.Code)

	type dim struct {
		Name  string `xml:"Name"`
		Value string `xml:"Value"`
	}
	type metric struct {
		MetricName string `xml:"MetricName"`
		Dimensions []dim  `xml:"Dimensions>member"`
	}
	type resp struct {
		Metrics []metric `xml:"ListMetricsResult>Metrics>member"`
	}
	var out resp
	require.NoError(t, xml.Unmarshal(listRec.Body.Bytes(), &out))
	require.Len(t, out.Metrics, 1)
	require.Len(t, out.Metrics[0].Dimensions, 1)
	assert.Equal(t, "InstanceId", out.Metrics[0].Dimensions[0].Name)
	assert.Equal(t, "i-123", out.Metrics[0].Dimensions[0].Value)
}

func TestCloudWatchHandler_GetMetricStatistics_WithDimensions(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	// Put metrics for two different instance IDs.
	for _, id := range []string{"i-001", "i-002"} {
		body := strings.Join([]string{
			"Action=PutMetricData",
			"Namespace=AWS%2FEC2",
			"MetricData.member.1.MetricName=NetworkIn",
			"MetricData.member.1.Value=100",
			"MetricData.member.1.Dimensions.member.1.Name=InstanceId",
			"MetricData.member.1.Dimensions.member.1.Value=" + id,
		}, "&")
		postForm(t, h, body)
	}

	// Query with the dimension filter for i-001 only.
	statsBody := strings.Join([]string{
		"Action=GetMetricStatistics",
		"Namespace=AWS%2FEC2",
		"MetricName=NetworkIn",
		"Dimensions.member.1.Name=InstanceId",
		"Dimensions.member.1.Value=i-001",
		"StartTime=2000-01-01T00%3A00%3A00Z",
		"EndTime=2100-01-01T00%3A00%3A00Z",
		"Period=600",
		"Statistics.member.1=Sum",
	}, "&")
	rec := postForm(t, h, statsBody)
	require.Equal(t, http.StatusOK, rec.Code)

	type dp struct {
		Sum *float64 `xml:"Sum"`
	}
	type resp struct {
		Datapoints []dp `xml:"GetMetricStatisticsResult>Datapoints>member"`
	}
	var out resp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Datapoints, 1)
	assert.InDelta(t, 100.0, *out.Datapoints[0].Sum, 1e-9)
}

func TestCloudWatchHandler_ListMetrics_DimensionFilter(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	for _, env := range []string{"prod", "staging"} {
		body := strings.Join([]string{
			"Action=PutMetricData",
			"Namespace=App",
			"MetricData.member.1.MetricName=RPM",
			"MetricData.member.1.Value=50",
			"MetricData.member.1.Dimensions.member.1.Name=Env",
			"MetricData.member.1.Dimensions.member.1.Value=" + env,
		}, "&")
		postForm(t, h, body)
	}

	// Filter to prod only.
	listBody := strings.Join([]string{
		"Action=ListMetrics",
		"Namespace=App",
		"Dimensions.member.1.Name=Env",
		"Dimensions.member.1.Value=prod",
	}, "&")
	rec := postForm(t, h, listBody)
	require.Equal(t, http.StatusOK, rec.Code)

	type metric struct {
		MetricName string `xml:"MetricName"`
	}
	type resp struct {
		Metrics []metric `xml:"ListMetricsResult>Metrics>member"`
	}
	var out resp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.Metrics, 1)
}

func TestCloudWatchHandler_GetMetricData_ScanByDescending(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	h := cloudwatch.NewHandler(b)

	// Put data via backend to avoid timestamp URL-encoding issues.
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 1; i <= 3; i++ {
		ts := base.Add(time.Duration(i) * time.Minute)
		_, _ = b.PutMetricData("NS", []cloudwatch.MetricDatum{
			{
				MetricName: "Counter", Value: float64(i), Count: 1,
				Sum: float64(i), Min: float64(i), Max: float64(i), Timestamp: ts,
			},
		})
	}

	queryBody := strings.Join([]string{
		"Action=GetMetricData",
		"StartTime=2020-01-01T00%3A00%3A00Z",
		"EndTime=2020-01-01T00%3A10%3A00Z",
		"ScanBy=TimestampDescending",
		"MetricDataQueries.member.1.Id=m1",
		"MetricDataQueries.member.1.MetricStat.Metric.Namespace=NS",
		"MetricDataQueries.member.1.MetricStat.Metric.MetricName=Counter",
		"MetricDataQueries.member.1.MetricStat.Stat=Sum",
		"MetricDataQueries.member.1.MetricStat.Period=60",
	}, "&")
	rec := postForm(t, h, queryBody)
	require.Equal(t, http.StatusOK, rec.Code)

	type result struct {
		ID         string   `xml:"Id"`
		Timestamps []string `xml:"Timestamps>member"`
	}
	type resp struct {
		Results []result `xml:"GetMetricDataResult>member"`
	}
	var out resp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Results, 1)
	if len(out.Results[0].Timestamps) >= 2 {
		// Descending: later timestamps first.
		assert.Greater(t, out.Results[0].Timestamps[0], out.Results[0].Timestamps[1])
	}
}

func TestCloudWatchHandler_PutMetricData_UnprocessedOnCap(t *testing.T) {
	t.Parallel()

	h := cloudwatch.NewHandler(cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1"))

	// Fill up the namespace to the cap first.
	b := h.Backend.(*cloudwatch.InMemoryBackend)
	for i := range cloudwatch.CwMaxMetricNamesPerNamespace {
		_, _ = b.PutMetricData("FullNS", []cloudwatch.MetricDatum{
			{
				MetricName: strings.Repeat("x", 1) + strings.Repeat("y", i%10) + strings.Repeat("z", i/10),
				Value:      1, Count: 1, Sum: 1, Min: 1, Max: 1,
			},
		})
	}

	body := "Action=PutMetricData&Namespace=FullNS&MetricData.member.1.MetricName=Overflow&MetricData.member.1.Value=1"
	rec := postForm(t, h, body)
	require.Equal(t, http.StatusOK, rec.Code)

	// Response should contain an UnprocessedMetricData entry.
	assert.Contains(t, rec.Body.String(), "Overflow")
}

func TestCloudWatchHandler_PutMetricStream_WithFilters(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	body := strings.Join([]string{
		"Action=PutMetricStream",
		"Name=filtered-stream",
		"FirehoseArn=arn%3Aaws%3Afirehose%3Aus-east-1%3A123%3Adeliverystream%2Fs",
		"RoleArn=arn%3Aaws%3Aiam%3A%3A123%3Arole%2Frole",
		"OutputFormat=json",
		"IncludeFilters.member.1.Namespace=AWS%2FEC2",
		"IncludeFilters.member.1.MetricNames.member.1=CPUUtilization",
		"ExcludeFilters.member.1.Namespace=AWS%2FLambda",
	}, "&")
	rec := postForm(t, h, body)
	require.Equal(t, http.StatusOK, rec.Code)

	// Retrieve and verify the filters were persisted.
	b := h.Backend.(*cloudwatch.InMemoryBackend)
	stream, err := b.GetMetricStream("filtered-stream")
	require.NoError(t, err)
	require.Len(t, stream.IncludeFilters, 1)
	assert.Equal(t, "AWS/EC2", stream.IncludeFilters[0].Namespace)
	assert.Equal(t, []string{"CPUUtilization"}, stream.IncludeFilters[0].MetricNames)
	require.Len(t, stream.ExcludeFilters, 1)
	assert.Equal(t, "AWS/Lambda", stream.ExcludeFilters[0].Namespace)
}

func TestCloudWatchHandler_GetInsightRuleReport_WithData(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	b := h.Backend.(*cloudwatch.InMemoryBackend)

	require.NoError(t, b.PutInsightRule(&cloudwatch.InsightRule{
		Name: "rule-test", Definition: `{}`, Schema: "CloudWatchLogRule",
	}))

	_, _ = b.PutMetricData("App", []cloudwatch.MetricDatum{
		{
			MetricName: "Hits", Value: 100, Count: 100, Sum: 1000, Min: 5, Max: 15,
			Dimensions: []cloudwatch.Dimension{{Name: "Host", Value: "h1"}},
		},
	})

	body := strings.Join([]string{
		"Action=GetInsightRuleReport",
		"RuleName=rule-test",
		"StartTime=2000-01-01T00%3A00%3A00Z",
		"EndTime=2100-01-01T00%3A00%3A00Z",
		"MaxContributorCount=5",
		"OrderBy=Sum",
	}, "&")
	rec := postForm(t, h, body)
	require.Equal(t, http.StatusOK, rec.Code)
}
