package cloudwatch_test

import (
	"encoding/xml"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

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

// ---------------------------------------------------------------------------
// Fix 2: SetAlarmState ignores StateReasonData form parameter
// ---------------------------------------------------------------------------

func TestSetAlarmState_StateReasonData_StoredAndReturned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		stateReasonData string
	}{
		{
			name:            "JSON blob stored and returned",
			stateReasonData: `{"version":"1.0","queryDate":"2026-06-01T00:00:00Z","statistic":"Average","period":300}`,
		},
		{
			name:            "empty string — field absent",
			stateReasonData: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
				AlarmName:          "a",
				Namespace:          "NS",
				MetricName:         "M",
				ComparisonOperator: "GreaterThanThreshold",
				EvaluationPeriods:  1,
				Period:             60,
				Statistic:          "Average",
			}))

			require.NoError(t, b.SetAlarmState(t.Context(), "a", "ALARM", "manual", tc.stateReasonData))

			p, _, err := b.DescribeAlarms([]string{"a"}, nil, "", "", "", 0)
			require.NoError(t, err)
			require.Len(t, p.Data, 1)
			assert.Equal(t, tc.stateReasonData, p.Data[0].StateReasonData)
		})
	}
}

// ---------------------------------------------------------------------------
// Fix 3: CompositeAlarm missing StateTransitionedTimestamp
// ---------------------------------------------------------------------------

func TestCompositeAlarm_StateTransitionedTimestamp_SetAlarmState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stateValue string
	}{
		{name: "transition to ALARM", stateValue: "ALARM"},
		{name: "transition to OK", stateValue: "OK"},
		{name: "transition to INSUFFICIENT_DATA", stateValue: "INSUFFICIENT_DATA"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
				AlarmName: "comp",
				AlarmRule: "FALSE",
			}))

			require.NoError(t, b.SetAlarmState(t.Context(), "comp", tc.stateValue, "manual", ""))

			_, p, err := b.DescribeAlarms([]string{"comp"}, nil, "", "", "", 0)
			require.NoError(t, err)
			require.Len(t, p.Data, 1)

			assert.False(t, p.Data[0].StateTransitionedTimestamp.IsZero(),
				"StateTransitionedTimestamp must be set after SetAlarmState")
		})
	}
}

func TestCompositeAlarm_StateTransitionedTimestamp_InXMLResponse(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	postForm(t, h, "Action=PutCompositeAlarm&AlarmName=comp&AlarmRule=FALSE")
	postForm(t, h, "Action=SetAlarmState&AlarmName=comp&StateValue=ALARM&StateReason=manual")

	rec := postForm(t, h, "Action=DescribeAlarms&AlarmNames.member.1=comp")
	require.Equal(t, 200, rec.Code)

	type alarm struct {
		AlarmName                  string `xml:"AlarmName"`
		StateTransitionedTimestamp string `xml:"StateTransitionedTimestamp"`
	}
	type resp struct {
		XMLName xml.Name `xml:"DescribeAlarmsResponse"`
		Alarms  []alarm  `xml:"DescribeAlarmsResult>CompositeAlarms>member"`
	}
	var out resp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Alarms, 1)
	assert.NotEmpty(t, out.Alarms[0].StateTransitionedTimestamp,
		"StateTransitionedTimestamp must appear in DescribeAlarms XML for composite alarms")
}

func TestCompositeAlarm_StateTransitionedTimestamp_ReEvalOnChildChange(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:          "child",
		Namespace:          "NS",
		MetricName:         "M",
		ComparisonOperator: "GreaterThanThreshold",
		EvaluationPeriods:  1,
		Period:             60,
		Statistic:          "Average",
	}))
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "parent",
		AlarmRule: "ALARM(child)",
	}))

	_, before, err := b.DescribeAlarms([]string{"parent"}, nil, "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, before.Data, 1)
	assert.False(t, before.Data[0].StateTransitionedTimestamp.IsZero(),
		"StateTransitionedTimestamp must be initialized on creation")

	// Trigger composite re-evaluation by setting child to ALARM.
	require.NoError(t, b.SetAlarmState(t.Context(), "child", "ALARM", "test", ""))

	_, after, err := b.DescribeAlarms([]string{"parent"}, nil, "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, after.Data, 1)
	assert.Equal(t, "ALARM", after.Data[0].StateValue)
	assert.False(t, after.Data[0].StateTransitionedTimestamp.IsZero(),
		"StateTransitionedTimestamp must be updated after composite re-evaluation")
	assert.True(t, after.Data[0].StateTransitionedTimestamp.After(before.Data[0].StateTransitionedTimestamp),
		"StateTransitionedTimestamp must advance when state changes via re-evaluation")
}
