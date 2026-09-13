package cloudwatch_test

import (
	"encoding/xml"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_PutMetricAlarm_Unit_XMLPath covers gopherstack-l4ywn on the legacy
// query/XML wire (handler_alarms.go): PutMetricAlarm's Unit form field was
// dropped and DescribeAlarms never emitted it.
func TestHandler_PutMetricAlarm_Unit_XMLPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		unit    string
		wantErr bool
	}{
		{name: "valid unit Count", unit: "Count"},
		{name: "valid unit Bytes/Second", unit: "Bytes/Second"},
		{name: "unit omitted", unit: ""},
		{name: "invalid unit rejected", unit: "NotAUnit", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newCWHandler()
			alarmName := "xml-unit-" + tc.name

			form := url.Values{
				"Action":             {"PutMetricAlarm"},
				"AlarmName":          {alarmName},
				"Namespace":          {"NS"},
				"MetricName":         {"M"},
				"ComparisonOperator": {"GreaterThanThreshold"},
				"Threshold":          {"1"},
				"EvaluationPeriods":  {"1"},
				"Period":             {"60"},
			}
			if tc.unit != "" {
				form.Set("Unit", tc.unit)
			}

			rec := postForm(t, h, form.Encode())

			if tc.wantErr {
				assert.Equal(t, 400, rec.Code)

				return
			}
			require.Equal(t, 200, rec.Code, rec.Body.String())

			descRec := postForm(t, h, url.Values{
				"Action":              {"DescribeAlarms"},
				"AlarmNames.member.1": {alarmName},
			}.Encode())
			require.Equal(t, 200, descRec.Code)

			var resp struct {
				XMLName xml.Name `xml:"DescribeAlarmsResponse"`
				Result  struct {
					MetricAlarms []struct {
						AlarmName string `xml:"AlarmName"`
						Unit      string `xml:"Unit"`
					} `xml:"MetricAlarms>member"`
				} `xml:"DescribeAlarmsResult"`
			}
			require.NoError(t, xml.Unmarshal(descRec.Body.Bytes(), &resp))
			require.Len(t, resp.Result.MetricAlarms, 1)
			assert.Equal(t, tc.unit, resp.Result.MetricAlarms[0].Unit)
		})
	}
}
