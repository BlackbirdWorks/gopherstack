package cloudwatch_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormPutOps_CreationTags(t *testing.T) {
	t.Parallel()

	tagQS := "&Tags.member.1.Key=env&Tags.member.1.Value=prod"
	dash := url.QueryEscape(`{"widgets":[]}`)

	tests := []struct {
		name string
		body string
		arn  string
	}{
		{
			name: "metric_alarm",
			body: "Action=PutMetricAlarm&AlarmName=a&Namespace=NS&MetricName=M" +
				"&ComparisonOperator=GreaterThanThreshold&Threshold=1&EvaluationPeriods=1" + tagQS,
			arn: "arn:aws:cloudwatch:us-east-1:000000000000:alarm:a",
		},
		{
			name: "composite_alarm",
			body: "Action=PutCompositeAlarm&AlarmName=c&AlarmRule=ALARM%28a%29" + tagQS,
			arn:  "arn:aws:cloudwatch:us-east-1:000000000000:alarm:c",
		},
		{
			name: "dashboard",
			body: "Action=PutDashboard&DashboardName=d&DashboardBody=" + dash + tagQS,
			arn:  "arn:aws:cloudwatch:us-east-1:000000000000:dashboard/d",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newCWHandler()
			rec := postForm(t, h, tt.body)
			require.Equal(t, 200, rec.Code, rec.Body.String())

			list := postForm(t, h, "Action=ListTagsForResource&ResourceARN="+url.QueryEscape(tt.arn))
			require.Equal(t, 200, list.Code)
			assert.Contains(t, list.Body.String(), "<Key>env</Key>")
			assert.Contains(t, list.Body.String(), "<Value>prod</Value>")
		})
	}
}
