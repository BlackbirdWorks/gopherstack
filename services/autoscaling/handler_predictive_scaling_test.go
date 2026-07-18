package autoscaling_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAutoscalingHandler_GetPredictiveScalingForecastNonEmpty verifies the response includes the
// required UpdateTime/CapacityForecast/LoadForecast fields with real, non-empty data
// derived from the group's current DesiredCapacity, instead of an entirely empty
// (required-field-violating) response.
func TestAutoscalingHandler_GetPredictiveScalingForecastNonEmpty(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "forecast-asg"

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":       {asgName},
		"MinSize":                    {"0"},
		"MaxSize":                    {"10"},
		"DesiredCapacity":            {"3"},
		"AvailabilityZones.member.1": {"us-east-1a"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "GetPredictiveScalingForecast", url.Values{
		"AutoScalingGroupName": {asgName},
		"PolicyName":           {"some-policy"},
		"StartTime":            {"2030-01-01T00:00:00Z"},
		"EndTime":              {"2030-01-02T00:00:00Z"},
	})
	require.Equal(t, 200, code, body)

	assert.Contains(t, body, "<UpdateTime>", "UpdateTime is a required output field; got: %s", body)
	assert.Contains(t, body, "<LoadForecast>", "LoadForecast is a required output field; got: %s", body)
	assert.Contains(t, body, "<Values><member>3</member>", "forecast values must reflect current DesiredCapacity")
}
