package autoscaling_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

func TestAutoscalingHandler_UpdateAllFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "update_all_optional_fields",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=full-upd-asg&MinSize=1&MaxSize=5",
				)
			},
			body: "Action=UpdateAutoScalingGroup&Version=2011-01-01" +
				"&AutoScalingGroupName=full-upd-asg" +
				"&MinSize=2&MaxSize=8&DesiredCapacity=3" +
				"&DefaultCooldown=120&HealthCheckGracePeriod=60" +
				"&LaunchConfigurationName=my-lc" +
				"&HealthCheckType=ELB" +
				"&AvailabilityZones.member.1=us-east-1b",
			wantStatus: http.StatusOK,
		},
		{
			name: "update_invalid_min_size",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=inv-upd-asg&MinSize=1&MaxSize=5",
				)
			},
			body:       "Action=UpdateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=inv-upd-asg&MinSize=bad",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "update_invalid_max_size",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=inv-max-asg&MinSize=1&MaxSize=5",
				)
			},
			body:       "Action=UpdateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=inv-max-asg&MaxSize=bad",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "update_invalid_desired",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=inv-des-asg&MinSize=1&MaxSize=5",
				)
			},
			body:       "Action=UpdateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=inv-des-asg&DesiredCapacity=bad",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "update_invalid_cooldown",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=inv-cool-asg&MinSize=1&MaxSize=5",
				)
			},
			body:       "Action=UpdateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=inv-cool-asg&DefaultCooldown=bad",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "update_invalid_health_grace",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=inv-hgp-asg&MinSize=1&MaxSize=5",
				)
			},
			body: "Action=UpdateAutoScalingGroup&Version=2011-01-01" +
				"&AutoScalingGroupName=inv-hgp-asg&HealthCheckGracePeriod=bad",
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

func TestAutoscalingHandler_CreateWithTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "create_with_tags_and_lc",
			body: "Action=CreateAutoScalingGroup&Version=2011-01-01" +
				"&AutoScalingGroupName=tagged-asg&MinSize=1&MaxSize=5" +
				"&LaunchConfigurationName=my-lc" +
				"&HealthCheckType=EC2&HealthCheckGracePeriod=300" +
				"&DefaultCooldown=300" +
				"&LoadBalancerNames.member.1=my-elb" +
				"&TargetGroupARNs.member.1=arn:aws:elasticloadbalancing:us-east-1:123:targetgroup/tg/abc" +
				"&Tags.member.1.Key=env&Tags.member.1.Value=test",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_min_size",
			body:       "Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=inv-asg&MinSize=bad",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_max_size",
			body:       "Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=inv-asg2&MinSize=1&MaxSize=bad",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid_desired_capacity",
			body: "Action=CreateAutoScalingGroup&Version=2011-01-01" +
				"&AutoScalingGroupName=inv-asg3&MinSize=1&MaxSize=5&DesiredCapacity=bad",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid_cooldown",
			body: "Action=CreateAutoScalingGroup&Version=2011-01-01" +
				"&AutoScalingGroupName=inv-asg4&MinSize=1&MaxSize=5&DefaultCooldown=bad",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid_health_grace_period",
			body: "Action=CreateAutoScalingGroup&Version=2011-01-01" +
				"&AutoScalingGroupName=inv-asg5&MinSize=1&MaxSize=5&HealthCheckGracePeriod=bad",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_SetDesiredCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *autoscaling.Handler)
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "set_desired_capacity_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=sdc-asg&MinSize=1&MaxSize=10&DesiredCapacity=2",
				)
			},
			body:       "Action=SetDesiredCapacity&Version=2011-01-01&AutoScalingGroupName=sdc-asg&DesiredCapacity=5",
			wantStatus: http.StatusOK,
		},
		{
			name:       "set_desired_capacity_group_not_found",
			body:       "Action=SetDesiredCapacity&Version=2011-01-01&AutoScalingGroupName=no-such&DesiredCapacity=3",
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

func TestAutoscalingHandler_ForceDeleteAutoScalingGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *autoscaling.Handler)
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "delete_with_instances_requires_force",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=force-asg&MinSize=1&MaxSize=5&DesiredCapacity=2",
				)
			},
			body:       "Action=DeleteAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=force-asg",
			wantStatus: http.StatusBadRequest, // ForceDelete not set
		},
		{
			name: "delete_with_force_succeeds",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=force-asg2&MinSize=1&MaxSize=5&DesiredCapacity=2",
				)
			},
			body:       "Action=DeleteAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=force-asg2&ForceDelete=true",
			wantStatus: http.StatusOK,
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

func TestAutoscalingHandler_CapacityValidation(t *testing.T) {
	t.Parallel()

	const createASGFmt = "Action=CreateAutoScalingGroup&Version=2011-01-01"

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "create_desired_less_than_min",
			body: createASGFmt +
				"&AutoScalingGroupName=cap-asg&MinSize=3&MaxSize=10&DesiredCapacity=1",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "create_desired_exceeds_max",
			body: createASGFmt +
				"&AutoScalingGroupName=cap-asg2&MinSize=1&MaxSize=5&DesiredCapacity=10",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create_min_greater_than_max",
			body:       createASGFmt + "&AutoScalingGroupName=cap-asg3&MinSize=10&MaxSize=5",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "create_valid_capacity",
			body: createASGFmt +
				"&AutoScalingGroupName=cap-asg4&MinSize=1&MaxSize=10&DesiredCapacity=3",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
