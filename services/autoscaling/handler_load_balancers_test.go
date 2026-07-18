package autoscaling_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// TestAutoscalingHandler_AttachLoadBalancersStateInService verifies that AttachLoadBalancers results in
// "InService" state rather than "Added" (the old incorrect value).
func TestAutoscalingHandler_AttachLoadBalancersStateInService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		lbName string
	}{
		{"single lb", "my-lb"},
		{"second lb", "other-lb"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			asgName := "lb-test-" + tc.lbName

			code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
				"AutoScalingGroupName":       {asgName},
				"MinSize":                    {"0"},
				"MaxSize":                    {"5"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			})
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "AttachLoadBalancers", url.Values{
				"AutoScalingGroupName":       {asgName},
				"LoadBalancerNames.member.1": {tc.lbName},
			})
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "DescribeLoadBalancers", url.Values{
				"AutoScalingGroupName": {asgName},
			})
			require.Equal(t, 200, code)

			assert.Contains(t, body, "<State>InService</State>",
				"AttachLoadBalancers must produce InService state, got: %s", body)
			assert.NotContains(t, body, "<State>Added</State>",
				"state must not be 'Added'")
		})
	}
}

func TestAutoscalingHandler_AttachLoadBalancerTargetGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "attach_tgs_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=tg-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=AttachLoadBalancerTargetGroups&Version=2011-01-01" +
				"&AutoScalingGroupName=tg-asg" +
				"&TargetGroupARNs.member.1=arn:aws:elasticloadbalancing:us-east-1:123:targetgroup/tg/abc",
			wantStatus: http.StatusOK,
		},
		{
			name: "attach_tgs_group_not_found",
			body: "Action=AttachLoadBalancerTargetGroups&Version=2011-01-01" +
				"&AutoScalingGroupName=no-such" +
				"&TargetGroupARNs.member.1=arn:aws:elasticloadbalancing:us-east-1:123:targetgroup/tg/abc",
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

func TestAutoscalingHandler_AttachLoadBalancers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "attach_lbs_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=lb-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=AttachLoadBalancers&Version=2011-01-01" +
				"&AutoScalingGroupName=lb-asg" +
				"&LoadBalancerNames.member.1=my-elb",
			wantStatus: http.StatusOK,
		},
		{
			name: "attach_lbs_group_not_found",
			body: "Action=AttachLoadBalancers&Version=2011-01-01" +
				"&AutoScalingGroupName=no-such&LoadBalancerNames.member.1=elb",
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
