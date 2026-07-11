package autoscaling_test

import (
	"encoding/xml"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// Test_LifecycleHookGatesLaunch verifies that an EC2_INSTANCE_LAUNCHING lifecycle
// hook pauses newly-launched instances in Pending:Wait, and that
// CompleteLifecycleAction resolves the wait (CONTINUE -> InService, ABANDON ->
// instance removed). Before this fix, PutLifecycleHook/CompleteLifecycleAction never
// touched real instance state -- hooks were pure bookkeeping with no effect.
func Test_LifecycleHookGatesLaunch(t *testing.T) {
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

// Test_LifecycleHookGatesTermination verifies that an EC2_INSTANCE_TERMINATING
// lifecycle hook defers instance removal: TerminateInstanceInAutoScalingGroup pauses
// the instance in Terminating:Wait (with an InProgress activity) instead of removing
// it immediately, and CompleteLifecycleAction finishes the termination and applies
// the originally-requested desired-capacity decrement.
func Test_LifecycleHookGatesTermination(t *testing.T) {
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

// Test_RecordLifecycleActionHeartbeatByInstanceID verifies that
// RecordLifecycleActionHeartbeat can locate a pending action by (group, hook,
// instance) when no LifecycleActionToken is supplied -- AWS accepts either.
func Test_RecordLifecycleActionHeartbeatByInstanceID(t *testing.T) {
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

// Test_MixedInstancesPolicyRoundTrip verifies that CreateAutoScalingGroup parses
// MixedInstancesPolicy (launch template, overrides, instances distribution) and that
// DescribeAutoScalingGroups returns it. Previously this field was silently dropped:
// accepted on the wire, never stored, never returned.
func Test_MixedInstancesPolicyRoundTrip(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "mip-asg"

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":       {asgName},
		"MinSize":                    {"0"},
		"MaxSize":                    {"5"},
		"AvailabilityZones.member.1": {"us-east-1a"},
		"MixedInstancesPolicy.LaunchTemplate.LaunchTemplateSpecification.LaunchTemplateId": {"lt-0123456789"},
		"MixedInstancesPolicy.LaunchTemplate.LaunchTemplateSpecification.Version":          {"$Latest"},
		"MixedInstancesPolicy.LaunchTemplate.Overrides.member.1.InstanceType":              {"t3.micro"},
		"MixedInstancesPolicy.LaunchTemplate.Overrides.member.1.WeightedCapacity":          {"1"},
		"MixedInstancesPolicy.LaunchTemplate.Overrides.member.2.InstanceType":              {"t3.small"},
		"MixedInstancesPolicy.LaunchTemplate.Overrides.member.2.WeightedCapacity":          {"2"},
		"MixedInstancesPolicy.InstancesDistribution.OnDemandBaseCapacity":                  {"1"},
		"MixedInstancesPolicy.InstancesDistribution.SpotAllocationStrategy":                {"capacity-optimized"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {asgName},
	})
	require.Equal(t, 200, code, body)

	parsed := describeASGInstances(t, body)
	mip := parsed.Result.AutoScalingGroups.Members[0].MixedInstancesPolicy

	assert.Equal(t, "lt-0123456789", mip.LaunchTemplate.LaunchTemplateSpecification.LaunchTemplateID)
	require.Len(t, mip.LaunchTemplate.Overrides.Members, 2)
	assert.Equal(t, "t3.micro", mip.LaunchTemplate.Overrides.Members[0].InstanceType)
	assert.Equal(t, "t3.small", mip.LaunchTemplate.Overrides.Members[1].InstanceType)
	assert.Equal(t, int32(1), mip.InstancesDistribution.OnDemandBaseCapacity)
	assert.Equal(t, "capacity-optimized", mip.InstancesDistribution.SpotAllocationStrategy)
}

// xmlLaunchInstancesResponse parses the LaunchInstances response, which returns
// InstanceCollection entries (grouped by AZ/InstanceType with a list of instance
// IDs) rather than a flat per-instance list.
type xmlLaunchInstancesResponse struct {
	XMLName xml.Name `xml:"LaunchInstancesResponse"`
	Result  struct {
		AutoScalingGroupName string `xml:"AutoScalingGroupName"`
		ClientToken          string `xml:"ClientToken"`
		Instances            struct {
			Members []struct {
				AvailabilityZone string `xml:"AvailabilityZone"`
				InstanceIDs      struct {
					Members []string `xml:"member"`
				} `xml:"InstanceIds"`
			} `xml:"member"`
		} `xml:"Instances"`
	} `xml:"LaunchInstancesResult"`
}

// Test_LaunchInstancesWireShapeAndIndex verifies that LaunchInstances (a) reads the
// real RequestedCapacity field (not the DesiredCapacity typo it used to read), (b)
// returns the AWS InstanceCollection shape instead of a flat per-instance list, and
// (c) that launched instances are indexed so a subsequent
// TerminateInstanceInAutoScalingGroup can find them by ID (previously LaunchInstances
// never updated the instance index at all).
func Test_LaunchInstancesWireShapeAndIndex(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "launch-instances-asg"

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":       {asgName},
		"MinSize":                    {"0"},
		"MaxSize":                    {"10"},
		"AvailabilityZones.member.1": {"us-east-1a"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "LaunchInstances", url.Values{
		"AutoScalingGroupName": {asgName},
		"ClientToken":          {"tok-1"},
		"RequestedCapacity":    {"2"},
	})
	require.Equal(t, 200, code, body)

	var parsed xmlLaunchInstancesResponse
	require.NoError(t, xml.Unmarshal([]byte(body), &parsed))
	assert.Equal(t, asgName, parsed.Result.AutoScalingGroupName)
	assert.Equal(t, "tok-1", parsed.Result.ClientToken)

	var instanceIDs []string
	for _, m := range parsed.Result.Instances.Members {
		instanceIDs = append(instanceIDs, m.InstanceIDs.Members...)
	}

	require.Len(t, instanceIDs, 2, "RequestedCapacity=2 must launch exactly 2 instances; got body: %s", body)

	code, body = doAS(t, h, "TerminateInstanceInAutoScalingGroup", url.Values{
		"InstanceId":                     {instanceIDs[0]},
		"ShouldDecrementDesiredCapacity": {"true"},
	})
	assert.Equal(t, 200, code,
		"an instance launched via LaunchInstances must be indexed and terminable by ID; got: %s", body)
}

// Test_ExecutePolicyStepScaling verifies that ExecutePolicy selects the matching
// StepAdjustment via (MetricValue-BreachThreshold) for a StepScaling policy, and
// rejects execution when those required fields are missing.
func Test_ExecutePolicyStepScaling(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name            string
		metricValue     string
		breachThreshold string
		wantCode        int
		wantDesired     int32
	}{
		{name: "low step matches", metricValue: "15", breachThreshold: "10", wantCode: 200, wantDesired: 5},
		{
			name:            "high step matches unbounded upper",
			metricValue:     "25",
			breachThreshold: "10",
			wantCode:        200,
			wantDesired:     8,
		},
		{name: "missing MetricValue is rejected", metricValue: "", breachThreshold: "10", wantCode: 400},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			asgName := "step-exec-asg-" + tc.name

			code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
				"AutoScalingGroupName":       {asgName},
				"MinSize":                    {"0"},
				"MaxSize":                    {"20"},
				"DesiredCapacity":            {"2"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			})
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "PutScalingPolicy", url.Values{
				"AutoScalingGroupName": {asgName},
				"PolicyName":           {"step1"},
				"PolicyType":           {"StepScaling"},
				"AdjustmentType":       {"ChangeInCapacity"},
				"StepAdjustments.member.1.ScalingAdjustment":        {"3"},
				"StepAdjustments.member.1.MetricIntervalLowerBound": {"0"},
				"StepAdjustments.member.1.MetricIntervalUpperBound": {"10"},
				"StepAdjustments.member.2.ScalingAdjustment":        {"6"},
				"StepAdjustments.member.2.MetricIntervalLowerBound": {"10"},
			})
			require.Equal(t, 200, code, body)

			executeVals := url.Values{
				"AutoScalingGroupName": {asgName},
				"PolicyName":           {"step1"},
				"BreachThreshold":      {tc.breachThreshold},
			}
			if tc.metricValue != "" {
				executeVals.Set("MetricValue", tc.metricValue)
			}

			code, body = doAS(t, h, "ExecutePolicy", executeVals)
			require.Equal(t, tc.wantCode, code, body)

			if tc.wantCode != 200 {
				return
			}

			code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
				"AutoScalingGroupNames.member.1": {asgName},
			})
			require.Equal(t, 200, code, body)

			parsed := describeASGInstances(t, body)
			assert.Equal(t, tc.wantDesired, parsed.Result.AutoScalingGroups.Members[0].DesiredCapacity)
		})
	}
}

// Test_ScheduledActionStartEndTimeRoundTrip verifies that PutScheduledUpdateGroupAction
// and BatchPutScheduledUpdateGroupAction both persist StartTime/EndTime -- previously
// both silently dropped these fields, so DescribeScheduledActions never reflected the
// schedule the caller actually requested.
func Test_ScheduledActionStartEndTimeRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		useBatch bool
	}{
		{name: "single PutScheduledUpdateGroupAction", useBatch: false},
		{name: "BatchPutScheduledUpdateGroupAction", useBatch: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			asgName := "sched-time-asg-" + tc.name

			code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
				"AutoScalingGroupName":       {asgName},
				"MinSize":                    {"0"},
				"MaxSize":                    {"5"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			})
			require.Equal(t, 200, code, body)

			if tc.useBatch {
				code, body = doAS(t, h, "BatchPutScheduledUpdateGroupAction", url.Values{
					"AutoScalingGroupName": {asgName},
					"ScheduledUpdateGroupActions.member.1.ScheduledActionName": {"batch-action"},
					"ScheduledUpdateGroupActions.member.1.StartTime":           {"2030-01-01T00:00:00Z"},
					"ScheduledUpdateGroupActions.member.1.EndTime":             {"2030-01-02T00:00:00Z"},
					"ScheduledUpdateGroupActions.member.1.DesiredCapacity":     {"4"},
				})
			} else {
				code, body = doAS(t, h, "PutScheduledUpdateGroupAction", url.Values{
					"AutoScalingGroupName": {asgName},
					"ScheduledActionName":  {"single-action"},
					"StartTime":            {"2030-01-01T00:00:00Z"},
					"EndTime":              {"2030-01-02T00:00:00Z"},
					"DesiredCapacity":      {"4"},
				})
			}
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "DescribeScheduledActions", url.Values{
				"AutoScalingGroupName": {asgName},
			})
			require.Equal(t, 200, code, body)

			assert.Contains(t, body, "<StartTime>2030-01-01T00:00:00Z</StartTime>",
				"StartTime must round-trip; got: %s", body)
			assert.Contains(t, body, "<EndTime>2030-01-02T00:00:00Z</EndTime>",
				"EndTime must round-trip; got: %s", body)
		})
	}
}

// Test_CreateASGWithInitialLifecycleHooks verifies that CreateAutoScalingGroup
// registers LifecycleHookSpecificationList atomically with group creation, and that
// the group's own initial instances are gated by a matching launch hook -- this is
// the wire shape Terraform's aws_autoscaling_group initial_lifecycle_hook block uses,
// and it was previously accepted and silently discarded.
func Test_CreateASGWithInitialLifecycleHooks(t *testing.T) {
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

// Test_CreateASGWithTrafficSources verifies TrafficSources specified at
// CreateAutoScalingGroup time are stored and returned (previously only
// Attach/DetachTrafficSources touched this field; Create silently dropped it).
func Test_CreateASGWithTrafficSources(t *testing.T) {
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

// Test_PutScalingPolicyMetricAggregationType verifies MetricAggregationType is
// parsed on PutScalingPolicy and returned by DescribePolicies (previously silently
// dropped on both the request and response side).
func Test_PutScalingPolicyMetricAggregationType(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "mat-asg"

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":       {asgName},
		"MinSize":                    {"0"},
		"MaxSize":                    {"5"},
		"AvailabilityZones.member.1": {"us-east-1a"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "PutScalingPolicy", url.Values{
		"AutoScalingGroupName":  {asgName},
		"PolicyName":            {"agg-policy"},
		"PolicyType":            {"StepScaling"},
		"AdjustmentType":        {"ChangeInCapacity"},
		"MetricAggregationType": {"Average"},
		"StepAdjustments.member.1.ScalingAdjustment": {"1"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribePolicies", url.Values{
		"AutoScalingGroupName": {asgName},
	})
	require.Equal(t, 200, code, body)
	assert.Contains(t, body, "<MetricAggregationType>Average</MetricAggregationType>",
		"MetricAggregationType must round-trip; got: %s", body)
}

// Test_GetPredictiveScalingForecastNonEmpty verifies the response includes the
// required UpdateTime/CapacityForecast/LoadForecast fields with real, non-empty data
// derived from the group's current DesiredCapacity, instead of an entirely empty
// (required-field-violating) response.
func Test_GetPredictiveScalingForecastNonEmpty(t *testing.T) {
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
