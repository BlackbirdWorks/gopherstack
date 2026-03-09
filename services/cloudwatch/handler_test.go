package cloudwatch_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
			wantCode: http.StatusNotFound,
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
