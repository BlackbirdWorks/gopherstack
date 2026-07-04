package autoscaling_test

import (
	"encoding/xml"
	"fmt"
	"maps"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// doAS posts an autoscaling form action on the given handler.
func doAS(t *testing.T, h *autoscaling.Handler, action string, extra url.Values) (int, string) {
	t.Helper()

	vals := url.Values{"Action": {action}, "Version": {"2011-01-01"}}
	maps.Copy(vals, extra)

	rec := postAutoscalingForm(t, h, vals.Encode())

	return rec.Code, rec.Body.String()
}

// TestParity_LBStateIsInService verifies that AttachLoadBalancers results in
// "InService" state rather than "Added" (the old incorrect value).
func TestParity_LBStateIsInService(t *testing.T) {
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

// TestParity_ASGStatusEmptyForOperationalGroup verifies that DescribeAutoScalingGroups
// returns empty Status for a normal group (AWS only populates Status during deletion).
func TestParity_ASGStatusEmptyForOperationalGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		asgName string
	}{
		{"basic group", "status-test-asg"},
		{"second group", "status-test-asg-2"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()

			code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
				"AutoScalingGroupName":       {tc.asgName},
				"MinSize":                    {"0"},
				"MaxSize":                    {"3"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			})
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
				"AutoScalingGroupNames.member.1": {tc.asgName},
			})
			require.Equal(t, 200, code)

			assert.NotContains(t, body, "<Status>Active</Status>",
				"operational group must not have Status='Active'")
		})
	}
}

// TestParity_ASGTagsHaveResourceIdAndType verifies that DescribeAutoScalingGroups
// returns tags with ResourceId and ResourceType populated (matching real AWS).
func TestParity_ASGTagsHaveResourceIdAndType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		asgName string
		tagKey  string
		tagVal  string
	}{
		{"env tag", "tagged-asg", "Environment", "prod"},
		{"name tag", "tagged-asg-2", "Name", "my-asg"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()

			code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
				"AutoScalingGroupName":            {tc.asgName},
				"MinSize":                         {"0"},
				"MaxSize":                         {"3"},
				"AvailabilityZones.member.1":      {"us-east-1a"},
				"Tags.member.1.Key":               {tc.tagKey},
				"Tags.member.1.Value":             {tc.tagVal},
				"Tags.member.1.PropagateAtLaunch": {"true"},
			})
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
				"AutoScalingGroupNames.member.1": {tc.asgName},
			})
			require.Equal(t, 200, code)

			assert.Contains(t, body, fmt.Sprintf("<ResourceId>%s</ResourceId>", tc.asgName),
				"tag must have ResourceId=asgName")
			assert.Contains(t, body, "<ResourceType>auto-scaling-group</ResourceType>",
				"tag must have ResourceType=auto-scaling-group")
		})
	}
}

// TestParity_InstanceTypeInDescribeASG verifies that instances in DescribeAutoScalingGroups
// include InstanceType when the launch configuration specifies one.
func TestParity_InstanceTypeInDescribeASG(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	lcName := "it-lc"
	asgName := "it-asg"

	code, body := doAS(t, h, "CreateLaunchConfiguration", url.Values{
		"LaunchConfigurationName": {lcName},
		"ImageId":                 {"ami-12345678"},
		"InstanceType":            {"m5.large"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":       {asgName},
		"LaunchConfigurationName":    {lcName},
		"MinSize":                    {"1"},
		"MaxSize":                    {"3"},
		"DesiredCapacity":            {"1"},
		"AvailabilityZones.member.1": {"us-east-1a"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {asgName},
	})
	require.Equal(t, 200, code)

	assert.Contains(t, body, "<InstanceType>m5.large</InstanceType>",
		"instance in ASG must include InstanceType; got: %s", body)
}

// TestParity_LaunchConfigUserDataRoundTrip verifies that UserData, KernelId, RamdiskId
// are stored and returned by DescribeLaunchConfigurations.
func TestParity_LaunchConfigUserDataRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		lcName    string
		userData  string
		kernelID  string
		ramdiskID string
	}{
		{
			name:      "with all three fields",
			lcName:    "lc-userdata",
			userData:  "IyEvYmluL2Jhc2gKZWNobyBoZWxsbw==",
			kernelID:  "aki-12345678",
			ramdiskID: "ari-12345678",
		},
		{
			name:     "userData only",
			lcName:   "lc-userdata-only",
			userData: "dXNlcmRhdGE=",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()

			createVals := url.Values{
				"LaunchConfigurationName": {tc.lcName},
				"ImageId":                 {"ami-12345678"},
				"InstanceType":            {"t3.micro"},
				"UserData":                {tc.userData},
			}
			if tc.kernelID != "" {
				createVals.Set("KernelId", tc.kernelID)
			}
			if tc.ramdiskID != "" {
				createVals.Set("RamdiskId", tc.ramdiskID)
			}

			code, body := doAS(t, h, "CreateLaunchConfiguration", createVals)
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "DescribeLaunchConfigurations", url.Values{
				"LaunchConfigurationNames.member.1": {tc.lcName},
			})
			require.Equal(t, 200, code)

			if tc.userData != "" {
				assert.Contains(t, body, fmt.Sprintf("<UserData>%s</UserData>", tc.userData))
			}
			if tc.kernelID != "" {
				assert.Contains(t, body, fmt.Sprintf("<KernelId>%s</KernelId>", tc.kernelID))
			}
			if tc.ramdiskID != "" {
				assert.Contains(t, body, fmt.Sprintf("<RamdiskId>%s</RamdiskId>", tc.ramdiskID))
			}
		})
	}
}

// TestParity_StepScalingPolicyRoundTrip verifies StepAdjustments are stored and returned
// by DescribePolicies, and MinAdjustmentMagnitude is preserved.
func TestParity_StepScalingPolicyRoundTrip(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "step-asg"

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":       {asgName},
		"MinSize":                    {"1"},
		"MaxSize":                    {"10"},
		"AvailabilityZones.member.1": {"us-east-1a"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "PutScalingPolicy", url.Values{
		"AutoScalingGroupName":   {asgName},
		"PolicyName":             {"step-policy"},
		"PolicyType":             {"StepScaling"},
		"AdjustmentType":         {"ChangeInCapacity"},
		"MinAdjustmentMagnitude": {"2"},
		"StepAdjustments.member.1.ScalingAdjustment":        {"2"},
		"StepAdjustments.member.1.MetricIntervalLowerBound": {"0"},
		"StepAdjustments.member.1.MetricIntervalUpperBound": {"10"},
		"StepAdjustments.member.2.ScalingAdjustment":        {"4"},
		"StepAdjustments.member.2.MetricIntervalLowerBound": {"10"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribePolicies", url.Values{
		"AutoScalingGroupName": {asgName},
		"PolicyNames.member.1": {"step-policy"},
	})
	require.Equal(t, 200, code)

	assert.Contains(t, body, "<MinAdjustmentMagnitude>2</MinAdjustmentMagnitude>",
		"MinAdjustmentMagnitude must be returned; got: %s", body)
	assert.Contains(t, body, "<ScalingAdjustment>2</ScalingAdjustment>",
		"first step ScalingAdjustment must be present")
	assert.Contains(t, body, "<ScalingAdjustment>4</ScalingAdjustment>",
		"second step ScalingAdjustment must be present")
	assert.Contains(t, body, "<MetricIntervalLowerBound>0</MetricIntervalLowerBound>",
		"first step MetricIntervalLowerBound must be present")
}

// TestParity_ServiceLinkedRoleARNPresent verifies that DescribeAutoScalingGroups includes
// ServiceLinkedRoleARN in the response.
func TestParity_ServiceLinkedRoleARNPresent(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "slr-asg"

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":       {asgName},
		"MinSize":                    {"0"},
		"MaxSize":                    {"5"},
		"AvailabilityZones.member.1": {"us-east-1a"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {asgName},
	})
	require.Equal(t, 200, code)

	assert.Contains(t, body, "AWSServiceRoleForAutoScaling",
		"DescribeAutoScalingGroups must include ServiceLinkedRoleARN; got: %s", body)
	assert.Contains(t, body, "autoscaling.amazonaws.com",
		"ServiceLinkedRoleARN must reference autoscaling.amazonaws.com service principal")
}

// TestParity_ScheduledActionARNPresent verifies that PutScheduledUpdateGroupAction
// results in a ScheduledActionARN being set on DescribeScheduledActions.
func TestParity_ScheduledActionARNPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		asgName    string
		actionName string
	}{
		{"first action", "sched-asg-1", "daily-scale-up"},
		{"second action", "sched-asg-2", "nightly-scale-down"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()

			code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
				"AutoScalingGroupName":       {tc.asgName},
				"MinSize":                    {"1"},
				"MaxSize":                    {"10"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			})
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "PutScheduledUpdateGroupAction", url.Values{
				"AutoScalingGroupName": {tc.asgName},
				"ScheduledActionName":  {tc.actionName},
				"Recurrence":           {"0 8 * * *"},
				"DesiredCapacity":      {"5"},
			})
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "DescribeScheduledActions", url.Values{
				"AutoScalingGroupName": {tc.asgName},
			})
			require.Equal(t, 200, code)

			assert.Contains(t, body, "<ScheduledActionARN>",
				"DescribeScheduledActions must include ScheduledActionARN; got: %s", body)
			assert.Contains(t, body, "scheduledUpdateGroupAction",
				"ScheduledActionARN must use correct ARN format")
		})
	}
}

// TestParity_LifecycleHookGlobalTimeout verifies that DescribeLifecycleHooks returns
// GlobalTimeout equal to HeartbeatTimeout (numberOfRetries=1 is the default).
func TestParity_LifecycleHookGlobalTimeout(t *testing.T) {
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

// TestParity_MetricCollectionTypesGranularities verifies that
// DescribeMetricCollectionTypes returns a Granularities element with "1Minute".
func TestParity_MetricCollectionTypesGranularities(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()

	code, body := doAS(t, h, "DescribeMetricCollectionTypes", url.Values{})
	require.Equal(t, 200, code)

	assert.Contains(t, body, "<Granularities>",
		"response must include Granularities element; got: %s", body)
	assert.Contains(t, body, "<Granularity>1Minute</Granularity>",
		"Granularities must include 1Minute")
}

// xmlDescribeASGParityResponse is a minimal struct for parsing DescribeAutoScalingGroups.
type xmlDescribeASGParityResponse struct {
	XMLName xml.Name `xml:"DescribeAutoScalingGroupsResponse"`
	Result  struct {
		AutoScalingGroups struct {
			Members []struct {
				AutoScalingGroupName string `xml:"AutoScalingGroupName"`
				Status               string `xml:"Status"`
				ServiceLinkedRoleARN string `xml:"ServiceLinkedRoleARN"`
				Tags                 struct {
					Members []struct {
						Key          string `xml:"Key"`
						Value        string `xml:"Value"`
						ResourceID   string `xml:"ResourceId"`
						ResourceType string `xml:"ResourceType"`
					} `xml:"member"`
				} `xml:"Tags"`
				Instances struct {
					Members []struct {
						InstanceID   string `xml:"InstanceId"`
						InstanceType string `xml:"InstanceType"`
					} `xml:"member"`
				} `xml:"Instances"`
			} `xml:"member"`
		} `xml:"AutoScalingGroups"`
	} `xml:"DescribeAutoScalingGroupsResult"`
}

// TestParity_ASGXMLStructure parses DescribeAutoScalingGroups XML and verifies field values.
func TestParity_ASGXMLStructure(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "xml-struct-asg"
	lcName := "xml-struct-lc"

	code, body := doAS(t, h, "CreateLaunchConfiguration", url.Values{
		"LaunchConfigurationName": {lcName},
		"ImageId":                 {"ami-00000001"},
		"InstanceType":            {"t3.small"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":            {asgName},
		"LaunchConfigurationName":         {lcName},
		"MinSize":                         {"1"},
		"MaxSize":                         {"3"},
		"DesiredCapacity":                 {"1"},
		"AvailabilityZones.member.1":      {"us-east-1a"},
		"Tags.member.1.Key":               {"Project"},
		"Tags.member.1.Value":             {"gopherstack"},
		"Tags.member.1.PropagateAtLaunch": {"true"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {asgName},
	})
	require.Equal(t, 200, code, body)

	var parsed xmlDescribeASGParityResponse
	require.NoError(t, xml.Unmarshal([]byte(body), &parsed))
	require.Len(t, parsed.Result.AutoScalingGroups.Members, 1)

	asg := parsed.Result.AutoScalingGroups.Members[0]

	assert.Equal(t, asgName, asg.AutoScalingGroupName)
	assert.Empty(t, asg.Status, "operational ASG must have empty Status")
	assert.Contains(t, asg.ServiceLinkedRoleARN, "AWSServiceRoleForAutoScaling",
		"ServiceLinkedRoleARN must be set")

	require.Len(t, asg.Tags.Members, 1)
	tag := asg.Tags.Members[0]
	assert.Equal(t, asgName, tag.ResourceID, "tag ResourceId must equal ASG name")
	assert.Equal(t, "auto-scaling-group", tag.ResourceType)

	require.NotEmpty(t, asg.Instances.Members, "ASG with DesiredCapacity=1 must have an instance")
	inst := asg.Instances.Members[0]
	assert.Equal(t, "t3.small", inst.InstanceType, "instance must have InstanceType")
}
