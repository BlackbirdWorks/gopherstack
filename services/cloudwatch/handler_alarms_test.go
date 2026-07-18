package cloudwatch_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_SetAlarmState_AllStates(t *testing.T) {
	t.Parallel()

	tests := []string{"ALARM", "OK", "INSUFFICIENT_DATA"}
	for _, state := range tests {
		t.Run(state, func(t *testing.T) {
			t.Parallel()

			h := newCWHandler()
			postForm(
				t, h,
				"Action=PutMetricAlarm&AlarmName=a&Namespace=NS&MetricName=M"+
					"&ComparisonOperator=GreaterThanThreshold&Threshold=50&EvaluationPeriods=1",
			)

			rec := postForm(
				t, h,
				"Action=SetAlarmState&AlarmName=a&StateValue="+state+"&StateReason=test",
			)
			assert.Equal(t, 200, rec.Code, "state=%s", state)
		})
	}
}

// ---------------------------------------------------------------------------
// Handler: DescribeAlarms with AlarmTypes filter
// ---------------------------------------------------------------------------

func TestHandler_DescribeAlarms_AlarmTypeMetricOnly(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	postForm(t, h, "Action=PutMetricAlarm&AlarmName=ma&Namespace=NS&MetricName=M"+
		"&ComparisonOperator=GreaterThanThreshold&Threshold=50&EvaluationPeriods=1")
	postForm(t, h, "Action=PutCompositeAlarm&AlarmName=ca&AlarmRule=ALARM%28ma%29")

	rec := postForm(t, h, "Action=DescribeAlarms&AlarmTypes.member.1=MetricAlarm")
	assert.Equal(t, 200, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "ma")
}

// ---------------------------------------------------------------------------
// Fix 1: DescribeAlarms drops StateReasonData from MetricAlarm response
// ---------------------------------------------------------------------------

func TestDescribeAlarms_MetricAlarm_StateReasonData_Returned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		stateReasonData string
		wantPresent     bool
	}{
		{
			name:            "StateReasonData set — returned in response",
			stateReasonData: `{"version":"1.0","queryDate":"2026-01-01T00:00:00Z"}`,
			wantPresent:     true,
		},
		{
			name:            "StateReasonData empty — omitted from response",
			stateReasonData: "",
			wantPresent:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newCWHandler()
			postForm(
				t,
				h,
				"Action=PutMetricAlarm&AlarmName=srd-alarm&Namespace=NS&MetricName=M"+
					"&ComparisonOperator=GreaterThanThreshold&EvaluationPeriods=1"+
					"&Period=60&Threshold=10&Statistic=Average",
			)

			postForm(t, h, url.Values{
				"Action":          []string{"SetAlarmState"},
				"AlarmName":       []string{"srd-alarm"},
				"StateValue":      []string{"ALARM"},
				"StateReason":     []string{"manual"},
				"StateReasonData": []string{tc.stateReasonData},
			}.Encode())

			rec := postForm(t, h, "Action=DescribeAlarms&AlarmNames.member.1=srd-alarm")
			require.Equal(t, 200, rec.Code)

			type alarm struct {
				AlarmName       string `xml:"AlarmName"`
				StateReasonData string `xml:"StateReasonData"`
			}
			type resp struct {
				XMLName xml.Name `xml:"DescribeAlarmsResponse"`
				Alarms  []alarm  `xml:"DescribeAlarmsResult>MetricAlarms>member"`
			}
			var out resp
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
			require.Len(t, out.Alarms, 1)

			if tc.wantPresent {
				assert.Equal(t, tc.stateReasonData, out.Alarms[0].StateReasonData)
			} else {
				assert.Empty(t, out.Alarms[0].StateReasonData)
			}
		})
	}
}

func TestDescribeAlarmsForMetric_DimensionFilter_FormHandler(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	postForm(t, h, "Action=PutMetricAlarm&AlarmName=prod-alarm&Namespace=NS&MetricName=M"+
		"&ComparisonOperator=GreaterThanThreshold&EvaluationPeriods=1&Period=60&Threshold=1"+
		"&Dimensions.member.1.Name=Env&Dimensions.member.1.Value=prod")
	postForm(t, h, "Action=PutMetricAlarm&AlarmName=staging-alarm&Namespace=NS&MetricName=M"+
		"&ComparisonOperator=GreaterThanThreshold&EvaluationPeriods=1&Period=60&Threshold=1"+
		"&Dimensions.member.1.Name=Env&Dimensions.member.1.Value=staging")

	rec := postForm(t, h, url.Values{
		"Action":                    []string{"DescribeAlarmsForMetric"},
		"Namespace":                 []string{"NS"},
		"MetricName":                []string{"M"},
		"Dimensions.member.1.Name":  []string{"Env"},
		"Dimensions.member.1.Value": []string{"prod"},
	}.Encode())
	require.Equal(t, 200, rec.Code)

	type alarm struct {
		AlarmName string `xml:"AlarmName"`
	}
	type resp struct {
		XMLName xml.Name `xml:"DescribeAlarmsForMetricResponse"`
		Alarms  []alarm  `xml:"DescribeAlarmsForMetricResult>MetricAlarms>member"`
	}
	var out resp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Alarms, 1)
	assert.Equal(t, "prod-alarm", out.Alarms[0].AlarmName,
		"DescribeAlarmsForMetric with Dimensions filter must return only matching alarm")
}

func TestCloudWatchHandler_HandleDeleteAlarms_Error(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(t *testing.T, ts *httptest.Server)
		body     string
		wantCode int
	}{
		{
			name:     "delete_alarms_not_found",
			body:     "Action=DeleteAlarms&AlarmNames.member.1=nonexistent",
			wantCode: http.StatusOK,
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
