package cloudwatch_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/aws/smithy-go/encoding/cbor"
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

// ---------------------------------------------------------------------------
// Fix 4: buildCompositeAlarmCBOR missing StateTransitionedTimestamp
// ---------------------------------------------------------------------------

func TestCBOR_DescribeAlarms_CompositeAlarm_StateTransitionedTimestamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stateValue string
	}{
		{name: "ALARM state", stateValue: "ALARM"},
		{name: "OK state", stateValue: "OK"},
		{name: "INSUFFICIENT_DATA state", stateValue: "INSUFFICIENT_DATA"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newCBORHandler()
			postCBOR(t, h, "PutCompositeAlarm", cbor.Map{
				"AlarmName": cbor.String("comp-cbor"),
				"AlarmRule": cbor.String("FALSE"),
			})
			postCBOR(t, h, "SetAlarmState", cbor.Map{
				"AlarmName":   cbor.String("comp-cbor"),
				"StateValue":  cbor.String(tc.stateValue),
				"StateReason": cbor.String("manual"),
			})

			rec := postCBOR(t, h, "DescribeAlarms", cbor.Map{
				"AlarmNames": cbor.List{cbor.String("comp-cbor")},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := decodeCBORResponse(t, rec)
			compList, ok := resp["CompositeAlarms"].(cbor.List)
			require.True(t, ok, "CompositeAlarms must be a list")
			require.Len(t, compList, 1)

			alarmMap, ok := compList[0].(cbor.Map)
			require.True(t, ok)

			_, hasTimestamp := alarmMap["StateTransitionedTimestamp"]
			assert.True(t, hasTimestamp,
				"CBOR DescribeAlarms must include StateTransitionedTimestamp for composite alarms")
		})
	}
}

// ---------------------------------------------------------------------------
// Fix 5: cborPutMetricAlarm drops Dimensions
// ---------------------------------------------------------------------------

func TestCBOR_PutMetricAlarm_Dimensions_Stored(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		dims cbor.List
		want []cloudwatch.Dimension
	}{
		{
			name: "single dimension stored",
			dims: cbor.List{
				cbor.Map{"Name": cbor.String("Env"), "Value": cbor.String("prod")},
			},
			want: []cloudwatch.Dimension{{Name: "Env", Value: "prod"}},
		},
		{
			name: "multiple dimensions stored",
			dims: cbor.List{
				cbor.Map{"Name": cbor.String("Env"), "Value": cbor.String("prod")},
				cbor.Map{"Name": cbor.String("Region"), "Value": cbor.String("us-east-1")},
			},
			want: []cloudwatch.Dimension{
				{Name: "Env", Value: "prod"},
				{Name: "Region", Value: "us-east-1"},
			},
		},
		{
			name: "no dimensions",
			dims: nil,
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			h := cloudwatch.NewHandler(b)

			body := cbor.Map{
				"AlarmName":          cbor.String("dim-alarm"),
				"Namespace":          cbor.String("NS"),
				"MetricName":         cbor.String("M"),
				"ComparisonOperator": cbor.String("GreaterThanThreshold"),
				"Threshold":          cbor.Float64(1.0),
				"EvaluationPeriods":  cbor.Uint(1),
				"Period":             cbor.Uint(60),
			}
			if tc.dims != nil {
				body["Dimensions"] = tc.dims
			}

			rec := postCBOR(t, h, "PutMetricAlarm", body)
			require.Equal(t, http.StatusOK, rec.Code)

			page, _, err := b.DescribeAlarms([]string{"dim-alarm"}, nil, "", "", "", 0)
			require.NoError(t, err)
			require.Len(t, page.Data, 1)

			got := page.Data[0].Dimensions
			if len(tc.want) == 0 {
				assert.Empty(t, got, "no dimensions should be stored")
			} else {
				require.Len(t, got, len(tc.want))
				for _, wantDim := range tc.want {
					found := false
					for _, gotDim := range got {
						if gotDim.Name == wantDim.Name && gotDim.Value == wantDim.Value {
							found = true

							break
						}
					}
					assert.True(t, found, "dimension %s=%s must be stored", wantDim.Name, wantDim.Value)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Fix 6: DescribeAlarmsForMetric ignores Dimensions filter
// ---------------------------------------------------------------------------

func TestDescribeAlarmsForMetric_DimensionFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filterDims []cloudwatch.Dimension
		wantNames  []string
	}{
		{
			name:       "filter by prod dimension — returns only prod alarm",
			filterDims: []cloudwatch.Dimension{{Name: "Env", Value: "prod"}},
			wantNames:  []string{"prod-alarm"},
		},
		{
			name:       "filter by staging dimension — returns only staging alarm",
			filterDims: []cloudwatch.Dimension{{Name: "Env", Value: "staging"}},
			wantNames:  []string{"staging-alarm"},
		},
		{
			name:       "no dimension filter — returns both alarms",
			filterDims: nil,
			wantNames:  []string{"prod-alarm", "staging-alarm"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
				AlarmName:          "prod-alarm",
				Namespace:          "NS",
				MetricName:         "M",
				ComparisonOperator: "GreaterThanThreshold",
				EvaluationPeriods:  1,
				Period:             60,
				Dimensions:         []cloudwatch.Dimension{{Name: "Env", Value: "prod"}},
			}))
			require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
				AlarmName:          "staging-alarm",
				Namespace:          "NS",
				MetricName:         "M",
				ComparisonOperator: "GreaterThanThreshold",
				EvaluationPeriods:  1,
				Period:             60,
				Dimensions:         []cloudwatch.Dimension{{Name: "Env", Value: "staging"}},
			}))

			p, err := b.DescribeAlarmsForMetric("NS", "M", tc.filterDims, nil, "", 0)
			require.NoError(t, err)

			gotNames := make([]string, 0, len(p.Data))
			for _, a := range p.Data {
				gotNames = append(gotNames, a.AlarmName)
			}
			assert.ElementsMatch(t, tc.wantNames, gotNames,
				"DescribeAlarmsForMetric must filter by Dimensions when provided")
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
