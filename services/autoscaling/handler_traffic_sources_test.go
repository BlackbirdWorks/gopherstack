package autoscaling_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// TestAutoscalingHandler_CreateASGWithTrafficSources verifies TrafficSources specified at
// CreateAutoScalingGroup time are stored and returned (previously only
// Attach/DetachTrafficSources touched this field; Create silently dropped it).
func TestAutoscalingHandler_CreateASGWithTrafficSources(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "ts-create-asg"

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":               {asgName},
		"MinSize":                            {"0"},
		"MaxSize":                            {"5"},
		"AvailabilityZones.member.1":         {"us-east-1a"},
		"TrafficSources.member.1.Identifier": {"arn:aws:vpc-lattice:us-east-1:000000000000:targetgroup/tg-123"},
		"TrafficSources.member.1.Type":       {"vpc-lattice"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {asgName},
	})
	require.Equal(t, 200, code, body)

	parsed := describeASGInstances(t, body)
	require.Len(t, parsed.Result.AutoScalingGroups.Members[0].TrafficSources.Members, 1)
	assert.Equal(t, "vpc-lattice", parsed.Result.AutoScalingGroups.Members[0].TrafficSources.Members[0].Type)
}

func TestAutoscalingHandler_AttachTrafficSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "attach_traffic_sources_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=ts-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=AttachTrafficSources&Version=2011-01-01" +
				"&AutoScalingGroupName=ts-asg" +
				"&TrafficSources.member.1.Identifier=arn:aws:vpc-lattice:us-east-1:123:targetgroup/tg-abc" +
				"&TrafficSources.member.1.Type=vpc-lattice",
			wantStatus: http.StatusOK,
		},
		{
			name: "attach_traffic_sources_group_not_found",
			body: "Action=AttachTrafficSources&Version=2011-01-01" +
				"&AutoScalingGroupName=no-such" +
				"&TrafficSources.member.1.Identifier=arn:abc&TrafficSources.member.1.Type=vpc-lattice",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
