package autoscaling_test

import (
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// TestAutoscalingHandler_HTTPActions tests the HTTP handler dispatch for
// 0%-covered operations via the HTTP layer.
func TestAutoscalingHandler_HTTPActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *autoscaling.Handler)
		name       string
		body       string
		wantStatus int
	}{
		// ResumeProcesses
		{
			name: "resume_processes_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=rp-http-asg&MinSize=1&MaxSize=5",
				)
				postAutoscalingForm(
					t,
					h,
					"Action=SuspendProcesses&Version=2011-01-01&AutoScalingGroupName=rp-http-asg&ScalingProcesses.member.1=Launch",
				)
			},
			body:       "Action=ResumeProcesses&Version=2011-01-01&AutoScalingGroupName=rp-http-asg&ScalingProcesses.member.1=Launch", //nolint:lll // existing issue.
			wantStatus: http.StatusOK,
		},
		{
			name:       "resume_processes_group_not_found",
			body:       "Action=ResumeProcesses&Version=2011-01-01&AutoScalingGroupName=no-such",
			wantStatus: http.StatusBadRequest,
		},
		// EnterStandby
		{
			name: "enter_standby_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=es-http-asg&MinSize=1&MaxSize=5&DesiredCapacity=2", //nolint:lll // existing issue.
				)
			},
			body:       "Action=EnterStandby&Version=2011-01-01&AutoScalingGroupName=es-http-asg&ShouldDecrementDesiredCapacity=true&InstanceIds.member.1=dummy", //nolint:lll // existing issue.
			wantStatus: http.StatusOK,
		},
		{
			name:       "enter_standby_group_not_found",
			body:       "Action=EnterStandby&Version=2011-01-01&AutoScalingGroupName=no-such&ShouldDecrementDesiredCapacity=false", //nolint:lll // existing issue.
			wantStatus: http.StatusBadRequest,
		},
		// ExitStandby
		{
			name: "exit_standby_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=xs-http-asg&MinSize=1&MaxSize=5&DesiredCapacity=2", //nolint:lll // existing issue.
				)
			},
			body:       "Action=ExitStandby&Version=2011-01-01&AutoScalingGroupName=xs-http-asg&InstanceIds.member.1=dummy",
			wantStatus: http.StatusOK,
		},
		{
			name:       "exit_standby_group_not_found",
			body:       "Action=ExitStandby&Version=2011-01-01&AutoScalingGroupName=no-such",
			wantStatus: http.StatusBadRequest,
		},
		// SetInstanceProtection
		{
			name: "set_instance_protection_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=sip-http-asg&MinSize=0&MaxSize=5",
				)
			},
			body:       "Action=SetInstanceProtection&Version=2011-01-01&AutoScalingGroupName=sip-http-asg&ProtectedFromScaleIn=true", //nolint:lll // existing issue.
			wantStatus: http.StatusOK,
		},
		{
			name:       "set_instance_protection_group_not_found",
			body:       "Action=SetInstanceProtection&Version=2011-01-01&AutoScalingGroupName=no-such&ProtectedFromScaleIn=true",
			wantStatus: http.StatusBadRequest,
		},
		// RecordLifecycleActionHeartbeat
		{
			name: "record_lifecycle_heartbeat_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=lhb-http-asg&MinSize=0&MaxSize=5",
				)
				postAutoscalingForm(
					t,
					h,
					"Action=PutLifecycleHook&Version=2011-01-01&AutoScalingGroupName=lhb-http-asg&LifecycleHookName=my-hook",
				)
			},
			body:       "Action=RecordLifecycleActionHeartbeat&Version=2011-01-01&AutoScalingGroupName=lhb-http-asg&LifecycleHookName=my-hook", //nolint:lll // existing issue.
			wantStatus: http.StatusOK,
		},
		{
			name:       "record_lifecycle_heartbeat_group_not_found",
			body:       "Action=RecordLifecycleActionHeartbeat&Version=2011-01-01&AutoScalingGroupName=no-such&LifecycleHookName=h", //nolint:lll // existing issue.
			wantStatus: http.StatusBadRequest,
		},
		// ExecutePolicy
		{
			name: "execute_policy_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=ep-http-asg&MinSize=1&MaxSize=10&DesiredCapacity=2", //nolint:lll // existing issue.
				)
				postAutoscalingForm(
					t,
					h,
					"Action=PutScalingPolicy&Version=2011-01-01&AutoScalingGroupName=ep-http-asg&PolicyName=scale-up&AdjustmentType=ChangeInCapacity&ScalingAdjustment=1", //nolint:lll // existing issue.
				)
			},
			body:       "Action=ExecutePolicy&Version=2011-01-01&AutoScalingGroupName=ep-http-asg&PolicyName=scale-up",
			wantStatus: http.StatusOK,
		},
		{
			name:       "execute_policy_group_not_found",
			body:       "Action=ExecutePolicy&Version=2011-01-01&AutoScalingGroupName=no-such&PolicyName=p",
			wantStatus: http.StatusBadRequest,
		},
		// GetPredictiveScalingForecast
		{
			name: "get_predictive_scaling_forecast_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=psf-http-asg&MinSize=0&MaxSize=5",
				)
			},
			body:       "Action=GetPredictiveScalingForecast&Version=2011-01-01&AutoScalingGroupName=psf-http-asg",
			wantStatus: http.StatusOK,
		},
		{
			name:       "get_predictive_scaling_forecast_not_found",
			body:       "Action=GetPredictiveScalingForecast&Version=2011-01-01&AutoScalingGroupName=no-such",
			wantStatus: http.StatusBadRequest,
		},
		// DeleteNotificationConfiguration
		{
			name: "delete_notification_configuration_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=dnc-http-asg&MinSize=0&MaxSize=5",
				)
				postAutoscalingForm(t, h,
					"Action=PutNotificationConfiguration&Version=2011-01-01&AutoScalingGroupName=dnc-http-asg"+
						"&TopicARN=arn:aws:sns:us-east-1:000000000000:t&NotificationTypes.member.1=autoscaling:EC2_INSTANCE_LAUNCH")
			},
			body:       "Action=DeleteNotificationConfiguration&Version=2011-01-01&AutoScalingGroupName=dnc-http-asg&TopicARN=arn:aws:sns:us-east-1:000000000000:t", //nolint:lll // existing issue.
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete_notification_configuration_group_not_found",
			body:       "Action=DeleteNotificationConfiguration&Version=2011-01-01&AutoScalingGroupName=no-such&TopicARN=arn:aws:sns:us-east-1:x", //nolint:lll // existing issue.
			wantStatus: http.StatusBadRequest,
		},
		// DeletePolicy
		{
			name: "delete_policy_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=dp-http-asg&MinSize=0&MaxSize=5",
				)
				postAutoscalingForm(
					t,
					h,
					"Action=PutScalingPolicy&Version=2011-01-01&AutoScalingGroupName=dp-http-asg&PolicyName=my-pol&AdjustmentType=ChangeInCapacity&ScalingAdjustment=1", //nolint:lll // existing issue.
				)
			},
			body:       "Action=DeletePolicy&Version=2011-01-01&AutoScalingGroupName=dp-http-asg&PolicyName=my-pol",
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete_policy_not_found",
			body:       "Action=DeletePolicy&Version=2011-01-01&AutoScalingGroupName=no-such&PolicyName=p",
			wantStatus: http.StatusBadRequest,
		},
		// PutScheduledUpdateGroupAction
		{
			name: "put_scheduled_update_group_action_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=psuga-http-asg&MinSize=0&MaxSize=10",
				)
			},
			body:       "Action=PutScheduledUpdateGroupAction&Version=2011-01-01&AutoScalingGroupName=psuga-http-asg&ScheduledActionName=sa1", //nolint:lll // existing issue.
			wantStatus: http.StatusOK,
		},
		{
			name:       "put_scheduled_update_group_action_group_not_found",
			body:       "Action=PutScheduledUpdateGroupAction&Version=2011-01-01&AutoScalingGroupName=no-such&ScheduledActionName=a", //nolint:lll // existing issue.
			wantStatus: http.StatusBadRequest,
		},
		// DeleteScheduledAction
		{
			name: "delete_scheduled_action_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=dsa-http-asg&MinSize=0&MaxSize=10",
				)
				postAutoscalingForm(
					t,
					h,
					"Action=PutScheduledUpdateGroupAction&Version=2011-01-01&AutoScalingGroupName=dsa-http-asg&ScheduledActionName=sa-del", //nolint:lll // existing issue.
				)
			},
			body:       "Action=DeleteScheduledAction&Version=2011-01-01&AutoScalingGroupName=dsa-http-asg&ScheduledActionName=sa-del", //nolint:lll // existing issue.
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete_scheduled_action_not_found",
			body:       "Action=DeleteScheduledAction&Version=2011-01-01&AutoScalingGroupName=no-such&ScheduledActionName=a",
			wantStatus: http.StatusBadRequest,
		},
		// DeleteWarmPool
		{
			name: "delete_warm_pool_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=dwp-http-asg&MinSize=0&MaxSize=10",
				)
				postAutoscalingForm(t, h,
					"Action=PutWarmPool&Version=2011-01-01&AutoScalingGroupName=dwp-http-asg")
			},
			body:       "Action=DeleteWarmPool&Version=2011-01-01&AutoScalingGroupName=dwp-http-asg",
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete_warm_pool_group_not_found",
			body:       "Action=DeleteWarmPool&Version=2011-01-01&AutoScalingGroupName=no-such",
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

// TestAutoscalingHandler_DescribeStaticTypeOps covers all the simple Describe* operations that return static data.
func TestAutoscalingHandler_DescribeStaticTypeOps(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()

	describeOps := []string{
		"DescribeAccountLimits",
		"DescribeAdjustmentTypes",
		"DescribeAutoScalingNotificationTypes",
		"DescribeLifecycleHookTypes",
		"DescribeMetricCollectionTypes",
		"DescribeScalingProcessTypes",
		"DescribeTerminationPolicyTypes",
	}

	for _, action := range describeOps {
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			rec := postAutoscalingForm(t, h, "Action="+action+"&Version=2011-01-01")
			assert.Equal(t, http.StatusOK, rec.Code, "action %s should succeed", action)
		})
	}
}

// TestAutoscalingHandler_LoadBalancerTrafficAndMetricsOps covers DescribeLoadBalancers,
// DescribeLoadBalancerTargetGroups, DescribeTrafficSources, DetachInstances,
// DetachLoadBalancerTargetGroups, DetachLoadBalancers,
// DetachTrafficSources, EnableMetricsCollection, DisableMetricsCollection.
func TestAutoscalingHandler_LoadBalancerTrafficAndMetricsOps(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()

	// Create a launch configuration.
	postAutoscalingForm(t, h, "Action=CreateLaunchConfiguration&Version=2011-01-01"+
		"&LaunchConfigurationName=lb-lc&ImageId=ami-12345&InstanceType=t3.micro")

	// Create an ASG.
	rec := postAutoscalingForm(t, h, "Action=CreateAutoScalingGroup&Version=2011-01-01"+
		"&AutoScalingGroupName=lb-asg&MinSize=0&MaxSize=5"+
		"&LaunchConfigurationName=lb-lc")
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeLoadBalancers.
	rec = postAutoscalingForm(t, h, "Action=DescribeLoadBalancers&Version=2011-01-01"+
		"&AutoScalingGroupName=lb-asg")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeLoadBalancersResponse")

	// DescribeLoadBalancerTargetGroups.
	rec = postAutoscalingForm(t, h, "Action=DescribeLoadBalancerTargetGroups&Version=2011-01-01"+
		"&AutoScalingGroupName=lb-asg")
	assert.Equal(t, http.StatusOK, rec.Code)

	// DescribeTrafficSources.
	rec = postAutoscalingForm(t, h, "Action=DescribeTrafficSources&Version=2011-01-01"+
		"&AutoScalingGroupName=lb-asg")
	assert.Equal(t, http.StatusOK, rec.Code)

	// DetachLoadBalancers.
	rec = postAutoscalingForm(t, h, "Action=DetachLoadBalancers&Version=2011-01-01"+
		"&AutoScalingGroupName=lb-asg"+
		"&LoadBalancerNames.member.1=my-lb")
	assert.Equal(t, http.StatusOK, rec.Code)

	// DetachLoadBalancerTargetGroups.
	rec = postAutoscalingForm(t, h, "Action=DetachLoadBalancerTargetGroups&Version=2011-01-01"+
		"&AutoScalingGroupName=lb-asg"+
		"&TargetGroupARNs.member.1=arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/test/12345")
	assert.Equal(t, http.StatusOK, rec.Code)

	// DetachTrafficSources.
	rec = postAutoscalingForm(t, h, "Action=DetachTrafficSources&Version=2011-01-01"+
		"&AutoScalingGroupName=lb-asg")
	assert.Equal(t, http.StatusOK, rec.Code)

	// EnableMetricsCollection.
	rec = postAutoscalingForm(t, h, "Action=EnableMetricsCollection&Version=2011-01-01"+
		"&AutoScalingGroupName=lb-asg&Granularity=1Minute"+
		"&Metrics.member.1=GroupMinSize")
	assert.Equal(t, http.StatusOK, rec.Code)

	// DisableMetricsCollection.
	rec = postAutoscalingForm(t, h, "Action=DisableMetricsCollection&Version=2011-01-01"+
		"&AutoScalingGroupName=lb-asg"+
		"&Metrics.member.1=GroupMinSize")
	assert.Equal(t, http.StatusOK, rec.Code)
}

// doAS posts an autoscaling form action on the given handler.
func doAS(t *testing.T, h *autoscaling.Handler, action string, extra url.Values) (int, string) {
	t.Helper()

	vals := url.Values{"Action": {action}, "Version": {"2011-01-01"}}
	maps.Copy(vals, extra)

	rec := postAutoscalingForm(t, h, vals.Encode())

	return rec.Code, rec.Body.String()
}

func newAutoscalingHandler() *autoscaling.Handler {
	return autoscaling.NewHandler(autoscaling.NewInMemoryBackend())
}

func postAutoscalingForm(t *testing.T, h *autoscaling.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestAutoscalingHandler_Name(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	assert.Equal(t, "Autoscaling", h.Name())
}

func TestAutoscalingHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateAutoScalingGroup")
	assert.Contains(t, ops, "DescribeAutoScalingGroups")
	assert.Contains(t, ops, "UpdateAutoScalingGroup")
	assert.Contains(t, ops, "DeleteAutoScalingGroup")
	assert.Contains(t, ops, "CreateLaunchConfiguration")
	assert.Contains(t, ops, "DescribeLaunchConfigurations")
	assert.Contains(t, ops, "DeleteLaunchConfiguration")
	assert.Contains(t, ops, "DescribeScalingActivities")
}

func TestAutoscalingHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	assert.Equal(t, 80, h.MatchPriority())
}

func TestAutoscalingHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	matcher := h.RouteMatcher()

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		want   bool
	}{
		{
			name:   "valid autoscaling request",
			method: http.MethodPost,
			path:   "/",
			body:   "Version=2011-01-01&Action=DescribeAutoScalingGroups",
			want:   true,
		},
		{
			name:   "wrong version",
			method: http.MethodPost,
			path:   "/",
			body:   "Version=2014-10-31&Action=DescribeAutoScalingGroups",
			want:   false,
		},
		{
			name:   "GET method",
			method: http.MethodGet,
			path:   "/",
			body:   "Version=2011-01-01&Action=DescribeAutoScalingGroups",
			want:   false,
		},
		{
			name:   "dashboard path excluded",
			method: http.MethodPost,
			path:   "/dashboard/autoscaling",
			body:   "Version=2011-01-01&Action=DescribeAutoScalingGroups",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestAutoscalingHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "create_asg",
			body: "Action=CreateAutoScalingGroup&Version=2011-01-01",
			want: "CreateAutoScalingGroup",
		},
		{
			name: "describe_asg",
			body: "Action=DescribeAutoScalingGroups&Version=2011-01-01",
			want: "DescribeAutoScalingGroups",
		},
		{
			name: "missing_action",
			body: "Version=2011-01-01",
			want: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestAutoscalingProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "provider_name",
			run: func(t *testing.T) {
				t.Helper()

				p := &autoscaling.Provider{}
				assert.Equal(t, "Autoscaling", p.Name())
			},
		},
		{
			name: "provider_init",
			run: func(t *testing.T) {
				t.Helper()

				p := &autoscaling.Provider{}
				svc, err := p.Init(nil)
				require.NoError(t, err)
				require.NotNil(t, svc)
				assert.Equal(t, "Autoscaling", svc.Name())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t)
		})
	}
}

func TestAutoscalingHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "with_group_name",
			body: "Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=my-resource-asg",
			want: "my-resource-asg",
		},
		{
			name: "without_group_name",
			body: "Action=DescribeAutoScalingGroups&Version=2011-01-01",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestAutoscalingHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *autoscaling.Handler)
		name string
	}{
		{
			name: "chaos_service_name",
			run: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				assert.Equal(t, "autoscaling", h.ChaosServiceName())
			},
		},
		{
			name: "chaos_operations",
			run: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				ops := h.ChaosOperations()
				assert.NotEmpty(t, ops)
				assert.Contains(t, ops, "CreateAutoScalingGroup")
			},
		},
		{
			name: "chaos_regions",
			run: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				regions := h.ChaosRegions()
				assert.NotEmpty(t, regions)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			tt.run(t, h)
		})
	}
}
