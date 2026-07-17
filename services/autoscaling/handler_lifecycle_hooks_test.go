package autoscaling_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// TestAutoscalingHandler_LifecycleHookGlobalTimeout verifies that DescribeLifecycleHooks returns
// GlobalTimeout equal to HeartbeatTimeout (numberOfRetries=1 is the default).
func TestAutoscalingHandler_LifecycleHookGlobalTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		heartbeatTimeout string
		wantGlobal       string
	}{
		{"default timeout", "", "<GlobalTimeout>3600</GlobalTimeout>"},
		{"custom 600", "600", "<GlobalTimeout>600</GlobalTimeout>"},
		{"custom 7200", "7200", "<GlobalTimeout>7200</GlobalTimeout>"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			asgName := "hook-asg-" + strings.ReplaceAll(tc.name, " ", "-")

			code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
				"AutoScalingGroupName":       {asgName},
				"MinSize":                    {"0"},
				"MaxSize":                    {"5"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			})
			require.Equal(t, 200, code, body)

			hookVals := url.Values{
				"AutoScalingGroupName": {asgName},
				"LifecycleHookName":    {"my-hook"},
				"LifecycleTransition":  {"autoscaling:EC2_INSTANCE_LAUNCHING"},
			}
			if tc.heartbeatTimeout != "" {
				hookVals.Set("HeartbeatTimeout", tc.heartbeatTimeout)
			}

			code, body = doAS(t, h, "PutLifecycleHook", hookVals)
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "DescribeLifecycleHooks", url.Values{
				"AutoScalingGroupName": {asgName},
			})
			require.Equal(t, 200, code)

			assert.Contains(t, body, tc.wantGlobal,
				"GlobalTimeout must equal HeartbeatTimeout; got: %s", body)
		})
	}
}

// xmlASGInstancesResponse is a minimal struct for parsing DescribeAutoScalingGroups
// focused on instance lifecycle state and capacity fields.
type xmlASGInstancesResponse struct {
	XMLName xml.Name `xml:"DescribeAutoScalingGroupsResponse"`
	Result  struct {
		AutoScalingGroups struct {
			Members []struct {
				AutoScalingGroupName string `xml:"AutoScalingGroupName"`
				MixedInstancesPolicy struct {
					LaunchTemplate struct {
						LaunchTemplateSpecification struct {
							LaunchTemplateID string `xml:"LaunchTemplateId"`
						} `xml:"LaunchTemplateSpecification"`
						Overrides struct {
							Members []struct {
								InstanceType     string `xml:"InstanceType"`
								WeightedCapacity string `xml:"WeightedCapacity"`
							} `xml:"member"`
						} `xml:"Overrides"`
					} `xml:"LaunchTemplate"`
					InstancesDistribution struct {
						SpotAllocationStrategy string `xml:"SpotAllocationStrategy"`
						OnDemandBaseCapacity   int32  `xml:"OnDemandBaseCapacity"`
					} `xml:"InstancesDistribution"`
				} `xml:"MixedInstancesPolicy"`
				TrafficSources struct {
					Members []struct {
						Identifier string `xml:"Identifier"`
						Type       string `xml:"Type"`
					} `xml:"member"`
				} `xml:"TrafficSources"`
				Instances struct {
					Members []struct {
						InstanceID     string `xml:"InstanceId"`
						LifecycleState string `xml:"LifecycleState"`
					} `xml:"member"`
				} `xml:"Instances"`
				DesiredCapacity int32 `xml:"DesiredCapacity"`
				MinSize         int32 `xml:"MinSize"`
			} `xml:"member"`
		} `xml:"AutoScalingGroups"`
	} `xml:"DescribeAutoScalingGroupsResult"`
}

func describeASGInstances(t *testing.T, body string) xmlASGInstancesResponse {
	t.Helper()

	var parsed xmlASGInstancesResponse
	require.NoError(t, xml.Unmarshal([]byte(body), &parsed))
	require.Len(t, parsed.Result.AutoScalingGroups.Members, 1)

	return parsed
}

// TestAutoscalingHandler_LifecycleHookGatesLaunch verifies that an EC2_INSTANCE_LAUNCHING lifecycle
// hook pauses newly-launched instances in Pending:Wait, and that
// CompleteLifecycleAction resolves the wait (CONTINUE -> InService, ABANDON ->
// instance removed). Before this fix, PutLifecycleHook/CompleteLifecycleAction never
// touched real instance state -- hooks were pure bookkeeping with no effect.
func TestAutoscalingHandler_LifecycleHookGatesLaunch(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		result         string
		wantState      string
		wantRemoved    bool
		wantInstanceOK bool
	}{
		{name: "continue resolves to InService", result: "CONTINUE", wantState: "InService", wantInstanceOK: true},
		{name: "abandon removes the instance", result: "ABANDON", wantRemoved: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			asgName := "launch-hook-asg-" + tc.result

			code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
				"AutoScalingGroupName":       {asgName},
				"MinSize":                    {"0"},
				"MaxSize":                    {"5"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			})
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "PutLifecycleHook", url.Values{
				"AutoScalingGroupName": {asgName},
				"LifecycleHookName":    {"launch-hook"},
				"LifecycleTransition":  {"autoscaling:EC2_INSTANCE_LAUNCHING"},
				"HeartbeatTimeout":     {"60"},
			})
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "SetDesiredCapacity", url.Values{
				"AutoScalingGroupName": {asgName},
				"DesiredCapacity":      {"1"},
			})
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
				"AutoScalingGroupNames.member.1": {asgName},
			})
			require.Equal(t, 200, code, body)

			parsed := describeASGInstances(t, body)
			require.Len(t, parsed.Result.AutoScalingGroups.Members[0].Instances.Members, 1,
				"gated instance must still appear in the group while Pending:Wait")

			inst := parsed.Result.AutoScalingGroups.Members[0].Instances.Members[0]
			assert.Equal(t, "Pending:Wait", inst.LifecycleState,
				"new instance behind an active launch hook must start Pending:Wait, not InService")

			code, body = doAS(t, h, "CompleteLifecycleAction", url.Values{
				"AutoScalingGroupName":  {asgName},
				"LifecycleHookName":     {"launch-hook"},
				"InstanceId":            {inst.InstanceID},
				"LifecycleActionResult": {tc.result},
			})
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
				"AutoScalingGroupNames.member.1": {asgName},
			})
			require.Equal(t, 200, code, body)

			parsed = describeASGInstances(t, body)
			gotInstances := parsed.Result.AutoScalingGroups.Members[0].Instances.Members

			if tc.wantRemoved {
				assert.Empty(t, gotInstances, "ABANDON on a launch hook must remove the instance")

				return
			}

			require.Len(t, gotInstances, 1)
			assert.Equal(t, tc.wantState, gotInstances[0].LifecycleState,
				"CompleteLifecycleAction(CONTINUE) must move the instance to InService")
		})
	}
}

// TestAutoscalingHandler_LifecycleHookGatesTermination verifies that an EC2_INSTANCE_TERMINATING
// lifecycle hook defers instance removal: TerminateInstanceInAutoScalingGroup pauses
// the instance in Terminating:Wait (with an InProgress activity) instead of removing
// it immediately, and CompleteLifecycleAction finishes the termination and applies
// the originally-requested desired-capacity decrement.
func TestAutoscalingHandler_LifecycleHookGatesTermination(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "terminate-hook-asg"

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":       {asgName},
		"MinSize":                    {"0"},
		"MaxSize":                    {"5"},
		"DesiredCapacity":            {"1"},
		"AvailabilityZones.member.1": {"us-east-1a"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {asgName},
	})
	require.Equal(t, 200, code, body)

	parsed := describeASGInstances(t, body)
	require.Len(t, parsed.Result.AutoScalingGroups.Members[0].Instances.Members, 1)
	instanceID := parsed.Result.AutoScalingGroups.Members[0].Instances.Members[0].InstanceID

	code, body = doAS(t, h, "PutLifecycleHook", url.Values{
		"AutoScalingGroupName": {asgName},
		"LifecycleHookName":    {"terminate-hook"},
		"LifecycleTransition":  {"autoscaling:EC2_INSTANCE_TERMINATING"},
		"HeartbeatTimeout":     {"60"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "TerminateInstanceInAutoScalingGroup", url.Values{
		"InstanceId":                     {instanceID},
		"ShouldDecrementDesiredCapacity": {"true"},
	})
	require.Equal(t, 200, code, body)
	assert.Contains(t, body, "<StatusCode>InProgress</StatusCode>",
		"a deferred termination must report InProgress while waiting on the hook; got: %s", body)

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {asgName},
	})
	require.Equal(t, 200, code, body)

	parsed = describeASGInstances(t, body)
	group := parsed.Result.AutoScalingGroups.Members[0]
	require.Len(t, group.Instances.Members, 1, "instance must remain present during Terminating:Wait")
	assert.Equal(t, "Terminating:Wait", group.Instances.Members[0].LifecycleState)
	assert.Equal(t, int32(1), group.DesiredCapacity, "capacity decrement must be deferred until the hook resolves")

	code, body = doAS(t, h, "CompleteLifecycleAction", url.Values{
		"AutoScalingGroupName":  {asgName},
		"LifecycleHookName":     {"terminate-hook"},
		"InstanceId":            {instanceID},
		"LifecycleActionResult": {"CONTINUE"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {asgName},
	})
	require.Equal(t, 200, code, body)

	parsed = describeASGInstances(t, body)
	group = parsed.Result.AutoScalingGroups.Members[0]
	assert.Empty(t, group.Instances.Members, "instance must be removed once the terminating hook resolves")
	assert.Equal(t, int32(0), group.DesiredCapacity, "ShouldDecrementDesiredCapacity must apply once resolved")
}

// TestAutoscalingHandler_LifecycleHookGatesDesiredCapacityScaleIn verifies that an
// EC2_INSTANCE_TERMINATING lifecycle hook also gates the desired-capacity-driven
// scale-in path (SetDesiredCapacity), not just TerminateInstanceInAutoScalingGroup.
// Before this fix, applyDesiredCapacityChange's scale-in branch removed instances
// immediately regardless of any registered terminating hook.
func TestAutoscalingHandler_LifecycleHookGatesDesiredCapacityScaleIn(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "scale-in-hook-asg"

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":       {asgName},
		"MinSize":                    {"0"},
		"MaxSize":                    {"5"},
		"DesiredCapacity":            {"1"},
		"AvailabilityZones.member.1": {"us-east-1a"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {asgName},
	})
	require.Equal(t, 200, code, body)

	parsed := describeASGInstances(t, body)
	require.Len(t, parsed.Result.AutoScalingGroups.Members[0].Instances.Members, 1)
	instanceID := parsed.Result.AutoScalingGroups.Members[0].Instances.Members[0].InstanceID

	code, body = doAS(t, h, "PutLifecycleHook", url.Values{
		"AutoScalingGroupName": {asgName},
		"LifecycleHookName":    {"scale-in-terminate-hook"},
		"LifecycleTransition":  {"autoscaling:EC2_INSTANCE_TERMINATING"},
		"HeartbeatTimeout":     {"60"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "SetDesiredCapacity", url.Values{
		"AutoScalingGroupName": {asgName},
		"DesiredCapacity":      {"0"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {asgName},
	})
	require.Equal(t, 200, code, body)

	parsed = describeASGInstances(t, body)
	group := parsed.Result.AutoScalingGroups.Members[0]
	require.Len(t, group.Instances.Members, 1,
		"instance behind an active terminating hook must remain present during scale-in, not be removed instantly")
	assert.Equal(t, "Terminating:Wait", group.Instances.Members[0].LifecycleState)
	assert.Equal(t, instanceID, group.Instances.Members[0].InstanceID)
	assert.Equal(t, int32(0), group.DesiredCapacity,
		"DesiredCapacity reflects the new target immediately, even while the instance removal itself is deferred")

	code, body = doAS(t, h, "CompleteLifecycleAction", url.Values{
		"AutoScalingGroupName":  {asgName},
		"LifecycleHookName":     {"scale-in-terminate-hook"},
		"InstanceId":            {instanceID},
		"LifecycleActionResult": {"CONTINUE"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {asgName},
	})
	require.Equal(t, 200, code, body)

	parsed = describeASGInstances(t, body)
	group = parsed.Result.AutoScalingGroups.Members[0]
	assert.Empty(t, group.Instances.Members, "instance must be removed once the terminating hook resolves")
	assert.Equal(t, int32(0), group.DesiredCapacity, "DesiredCapacity must not be double-decremented on resolution")
}

// TestAutoscalingHandler_RecordLifecycleActionHeartbeatByInstanceID verifies that
// RecordLifecycleActionHeartbeat can locate a pending action by (group, hook,
// instance) when no LifecycleActionToken is supplied -- AWS accepts either.
func TestAutoscalingHandler_RecordLifecycleActionHeartbeatByInstanceID(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "heartbeat-asg"

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":       {asgName},
		"MinSize":                    {"0"},
		"MaxSize":                    {"5"},
		"AvailabilityZones.member.1": {"us-east-1a"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "PutLifecycleHook", url.Values{
		"AutoScalingGroupName": {asgName},
		"LifecycleHookName":    {"launch-hook"},
		"LifecycleTransition":  {"autoscaling:EC2_INSTANCE_LAUNCHING"},
		"HeartbeatTimeout":     {"60"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "SetDesiredCapacity", url.Values{
		"AutoScalingGroupName": {asgName},
		"DesiredCapacity":      {"1"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {asgName},
	})
	require.Equal(t, 200, code, body)

	parsed := describeASGInstances(t, body)
	instanceID := parsed.Result.AutoScalingGroups.Members[0].Instances.Members[0].InstanceID

	code, body = doAS(t, h, "RecordLifecycleActionHeartbeat", url.Values{
		"AutoScalingGroupName": {asgName},
		"LifecycleHookName":    {"launch-hook"},
		"InstanceId":           {instanceID},
	})
	require.Equal(t, 200, code, "heartbeat by instance ID (no token) must find the pending action; body: %s", body)

	code, body = doAS(t, h, "RecordLifecycleActionHeartbeat", url.Values{
		"AutoScalingGroupName": {asgName},
		"LifecycleHookName":    {"no-such-hook"},
		"InstanceId":           {instanceID},
	})
	assert.Equal(t, 400, code, "heartbeat for an unknown hook with no pending action must fail; body: %s", body)
}

// TestAutoscalingHandler_CreateASGWithInitialLifecycleHooks verifies that CreateAutoScalingGroup
// registers LifecycleHookSpecificationList atomically with group creation, and that
// the group's own initial instances are gated by a matching launch hook -- this is
// the wire shape Terraform's aws_autoscaling_group initial_lifecycle_hook block uses,
// and it was previously accepted and silently discarded.
func TestAutoscalingHandler_CreateASGWithInitialLifecycleHooks(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "initial-hook-asg"

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":       {asgName},
		"MinSize":                    {"1"},
		"MaxSize":                    {"5"},
		"DesiredCapacity":            {"1"},
		"AvailabilityZones.member.1": {"us-east-1a"},
		"LifecycleHookSpecificationList.member.1.LifecycleHookName":    {"init-hook"},
		"LifecycleHookSpecificationList.member.1.LifecycleTransition":  {"autoscaling:EC2_INSTANCE_LAUNCHING"},
		"LifecycleHookSpecificationList.member.1.HeartbeatTimeout":     {"120"},
		"LifecycleHookSpecificationList.member.1.DefaultResult":        {"CONTINUE"},
		"LifecycleHookSpecificationList.member.1.NotificationMetadata": {"boot-meta"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribeLifecycleHooks", url.Values{
		"AutoScalingGroupName": {asgName},
	})
	require.Equal(t, 200, code, body)
	assert.Contains(t, body, "<LifecycleHookName>init-hook</LifecycleHookName>")
	assert.Contains(t, body, "<NotificationMetadata>boot-meta</NotificationMetadata>",
		"NotificationMetadata must round-trip; got: %s", body)
	assert.Contains(t, body, "<GlobalTimeout>120</GlobalTimeout>")

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {asgName},
	})
	require.Equal(t, 200, code, body)

	parsed := describeASGInstances(t, body)
	require.Len(t, parsed.Result.AutoScalingGroups.Members[0].Instances.Members, 1)
	assert.Equal(t, "Pending:Wait", parsed.Result.AutoScalingGroups.Members[0].Instances.Members[0].LifecycleState,
		"the group's own initial instance must be gated by a hook registered at creation time")
}

func TestAutoscalingHandler_CompleteLifecycleAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "complete_lifecycle_action_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=lc-action-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=CompleteLifecycleAction&Version=2011-01-01" +
				"&AutoScalingGroupName=lc-action-asg" +
				"&LifecycleHookName=my-hook" +
				"&LifecycleActionToken=abc123" +
				"&LifecycleActionResult=CONTINUE",
			wantStatus: http.StatusOK,
		},
		{
			name: "complete_lifecycle_group_not_found",
			body: "Action=CompleteLifecycleAction&Version=2011-01-01" +
				"&AutoScalingGroupName=no-such&LifecycleHookName=h&LifecycleActionResult=CONTINUE",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "complete_lifecycle_missing_hook_name",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=miss-hook-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=CompleteLifecycleAction&Version=2011-01-01" +
				"&AutoScalingGroupName=miss-hook-asg&LifecycleActionResult=CONTINUE",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "complete_lifecycle_missing_result",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=miss-result-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=CompleteLifecycleAction&Version=2011-01-01" +
				"&AutoScalingGroupName=miss-result-asg&LifecycleHookName=my-hook",
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

func TestAutoscalingHandler_DeleteLifecycleHook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler, b *autoscaling.InMemoryBackend)
		body       string
		wantStatus int
	}{
		{
			name: "delete_existing_hook",
			setup: func(t *testing.T, _ *autoscaling.Handler, b *autoscaling.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "hook-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				require.NoError(t, err)

				err = b.AddLifecycleHook(autoscaling.LifecycleHook{
					LifecycleHookName:    "my-hook",
					AutoScalingGroupName: "hook-asg",
					LifecycleTransition:  "autoscaling:EC2_INSTANCE_LAUNCHING",
					DefaultResult:        "CONTINUE",
				})
				require.NoError(t, err)
			},
			body:       "Action=DeleteLifecycleHook&Version=2011-01-01&AutoScalingGroupName=hook-asg&LifecycleHookName=my-hook",
			wantStatus: http.StatusOK,
		},
		{
			name: "delete_nonexistent_hook",
			setup: func(t *testing.T, h *autoscaling.Handler, _ *autoscaling.InMemoryBackend) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=no-hook-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=DeleteLifecycleHook&Version=2011-01-01" +
				"&AutoScalingGroupName=no-hook-asg&LifecycleHookName=ghost-hook",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete_hook_group_not_found",
			body:       "Action=DeleteLifecycleHook&Version=2011-01-01&AutoScalingGroupName=no-such&LifecycleHookName=my-hook",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			h := autoscaling.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, h, b)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_PutAndDescribeLifecycleHooks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *autoscaling.Handler)
		checkBody  func(t *testing.T, body string)
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "put_lifecycle_hook_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=hook-asg&MinSize=0&MaxSize=5",
				)
			},
			body: "Action=PutLifecycleHook&Version=2011-01-01&AutoScalingGroupName=hook-asg" +
				"&LifecycleHookName=my-hook&LifecycleTransition=autoscaling:EC2_INSTANCE_LAUNCHING",
			wantStatus: http.StatusOK,
		},
		{
			name:       "put_lifecycle_hook_group_not_found",
			body:       "Action=PutLifecycleHook&Version=2011-01-01&AutoScalingGroupName=no-such&LifecycleHookName=h",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "describe_lifecycle_hooks_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=dlh-asg&MinSize=0&MaxSize=5",
				)
				postAutoscalingForm(
					t, h,
					"Action=PutLifecycleHook&Version=2011-01-01&AutoScalingGroupName=dlh-asg&LifecycleHookName=h1",
				)
			},
			body:       "Action=DescribeLifecycleHooks&Version=2011-01-01&AutoScalingGroupName=dlh-asg",
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "h1")
			},
		},
		{
			name:       "describe_lifecycle_hooks_group_not_found",
			body:       "Action=DescribeLifecycleHooks&Version=2011-01-01&AutoScalingGroupName=no-such",
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

			if tt.checkBody != nil {
				tt.checkBody(t, rec.Body.String())
			}
		})
	}
}
