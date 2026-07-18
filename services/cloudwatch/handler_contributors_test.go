package cloudwatch_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// ---------------------------------------------------------------------------
// DescribeAlarmContributors
// ---------------------------------------------------------------------------

func TestHandler_DescribeAlarmContributors_NotFound(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	rec := postForm(t, h, "Action=DescribeAlarmContributors&AlarmName=nonexistent")
	assert.Equal(t, 400, rec.Code)
}

func TestHandler_DescribeAlarmContributors_Existing(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	postForm(t, h, "Action=PutMetricAlarm&AlarmName=a&Namespace=NS&MetricName=M"+
		"&ComparisonOperator=GreaterThanThreshold&Threshold=50&EvaluationPeriods=1")

	rec := postForm(t, h, "Action=DescribeAlarmContributors&AlarmName=a")
	assert.Equal(t, 200, rec.Code)
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
