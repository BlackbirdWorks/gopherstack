package autoscaling_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	assdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

// TestRealClient_SetDesiredCapacityHonorCooldown proves SetDesiredCapacity's HonorCooldown
// (SetDesiredCapacityInput doc comment: by default, manual scaling does not honor the
// cooldown period) against the group's DefaultCooldown/LastScalingActivity.
func TestRealClient_SetDesiredCapacityHonorCooldown(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name            string
		defaultCooldown int32
		firstHonor      bool
		secondHonor     bool
		wantReject      bool
	}{
		{
			name:            "honors cooldown and rejects the second call",
			defaultCooldown: 300, firstHonor: false, secondHonor: true, wantReject: true,
		},
		{
			name:            "default ignores cooldown",
			defaultCooldown: 300, firstHonor: false, secondHonor: false, wantReject: false,
		},
		{
			name:            "honors cooldown but nothing in progress yet",
			defaultCooldown: 300, firstHonor: false, secondHonor: true, wantReject: false,
		},
		{
			name:            "honors cooldown with no cooldown configured",
			defaultCooldown: 0, firstHonor: false, secondHonor: true, wantReject: false,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			groupName := "sdc-cooldown-" + tt.name
			_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String(groupName),
				MinSize:              aws.Int32(0),
				MaxSize:              aws.Int32(10),
				DesiredCapacity:      aws.Int32(1),
				DefaultCooldown:      aws.Int32(tt.defaultCooldown),
				AvailabilityZones:    []string{"us-east-1a"},
			})
			require.NoError(t, err)

			// "nothing in progress yet" case: skip the first scale so
			// LastScalingActivity stays zero and the gate cannot fire.
			if tt.name != "honors cooldown but nothing in progress yet" {
				_, err = client.SetDesiredCapacity(ctx, &assdk.SetDesiredCapacityInput{
					AutoScalingGroupName: aws.String(groupName),
					DesiredCapacity:      aws.Int32(2),
					HonorCooldown:        aws.Bool(tt.firstHonor),
				})
				require.NoError(t, err)
			}

			_, err = client.SetDesiredCapacity(ctx, &assdk.SetDesiredCapacityInput{
				AutoScalingGroupName: aws.String(groupName),
				DesiredCapacity:      aws.Int32(3),
				HonorCooldown:        aws.Bool(tt.secondHonor),
			})

			if !tt.wantReject {
				require.NoError(t, err)

				descOut, descErr := client.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
					AutoScalingGroupNames: []string{groupName},
				})
				require.NoError(t, descErr)
				require.Len(t, descOut.AutoScalingGroups, 1)
				assert.Equal(t, int32(3), aws.ToInt32(descOut.AutoScalingGroups[0].DesiredCapacity))

				return
			}

			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "ScalingActivityInProgress", apiErr.ErrorCode())
		})
	}
}

// TestRealClient_CreateAutoScalingGroupServiceLinkedRoleARN proves ServiceLinkedRoleARN is
// echoed back verbatim when the caller supplies one, and defaulted to
// AWSServiceRoleForAutoScaling when omitted.
func TestRealClient_CreateAutoScalingGroupServiceLinkedRoleARN(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		roleARN string
		wantARN string
	}{
		{
			name:    "explicit role echoed",
			roleARN: "arn:aws:iam::123456789012:role/custom-slr",
			wantARN: "arn:aws:iam::123456789012:role/custom-slr",
		},
		{
			name:    "default role when omitted",
			roleARN: "",
			wantARN: fmt.Sprintf(
				"arn:aws:iam::%s:role/aws-service-role/autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling",
				config.DefaultAccountID,
			),
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			groupName := "slr-group-" + tt.name

			input := &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String(groupName),
				MinSize:              aws.Int32(0),
				MaxSize:              aws.Int32(3),
				AvailabilityZones:    []string{"us-east-1a"},
			}
			if tt.roleARN != "" {
				input.ServiceLinkedRoleARN = aws.String(tt.roleARN)
			}

			_, err := client.CreateAutoScalingGroup(ctx, input)
			require.NoError(t, err)

			descOut, err := client.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
				AutoScalingGroupNames: []string{groupName},
			})
			require.NoError(t, err)
			require.Len(t, descOut.AutoScalingGroups, 1)
			assert.Equal(t, tt.wantARN, aws.ToString(descOut.AutoScalingGroups[0].ServiceLinkedRoleARN))
		})
	}
}

// TestRealClient_DescribeAutoScalingGroupsIncludeInstances proves IncludeInstances=false
// suppresses the Instances list, and it is included by default.
func TestRealClient_DescribeAutoScalingGroupsIncludeInstances(t *testing.T) {
	t.Parallel()

	cases := []struct {
		includeInstances *bool
		name             string
		wantInstancesLen int
	}{
		{name: "includes instances by default", includeInstances: nil, wantInstancesLen: 2},
		{name: "includes instances when true", includeInstances: aws.Bool(true), wantInstancesLen: 2},
		{name: "excludes instances when false", includeInstances: aws.Bool(false), wantInstancesLen: 0},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			groupName := "include-instances-" + tt.name
			_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String(groupName),
				MinSize:              aws.Int32(0),
				MaxSize:              aws.Int32(5),
				AvailabilityZones:    []string{"us-east-1a"},
			})
			require.NoError(t, err)

			_, err = client.AttachInstances(ctx, &assdk.AttachInstancesInput{
				AutoScalingGroupName: aws.String(groupName),
				InstanceIds:          []string{"i-ii-1", "i-ii-2"},
			})
			require.NoError(t, err)

			descOut, err := client.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
				AutoScalingGroupNames: []string{groupName},
				IncludeInstances:      tt.includeInstances,
			})
			require.NoError(t, err)
			require.Len(t, descOut.AutoScalingGroups, 1)
			assert.Len(t, descOut.AutoScalingGroups[0].Instances, tt.wantInstancesLen)
		})
	}
}

// TestRealClient_CancelInstanceRefreshWaitForTransitioningInstances proves the immediate
// post-cancel status: true (default) leaves the refresh mid-transition (Cancelling), false
// finishes the cancel synchronously (Cancelled).
func TestRealClient_CancelInstanceRefreshWaitForTransitioningInstances(t *testing.T) {
	t.Parallel()

	cases := []struct {
		wait       *bool
		name       string
		wantStatus types.InstanceRefreshStatus
	}{
		{name: "true default waits", wait: nil, wantStatus: types.InstanceRefreshStatusCancelling},
		{name: "explicit true waits", wait: aws.Bool(true), wantStatus: types.InstanceRefreshStatusCancelling},
		{name: "false finishes immediately", wait: aws.Bool(false), wantStatus: types.InstanceRefreshStatusCancelled},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			groupName := "wait-transition-" + tt.name
			_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String(groupName),
				MinSize:              aws.Int32(0),
				MaxSize:              aws.Int32(5),
				DesiredCapacity:      aws.Int32(3),
				AvailabilityZones:    []string{"us-east-1a"},
			})
			require.NoError(t, err)

			_, err = client.StartInstanceRefresh(ctx, &assdk.StartInstanceRefreshInput{
				AutoScalingGroupName: aws.String(groupName),
			})
			require.NoError(t, err)

			_, err = client.CancelInstanceRefresh(ctx, &assdk.CancelInstanceRefreshInput{
				AutoScalingGroupName:          aws.String(groupName),
				WaitForTransitioningInstances: tt.wait,
			})
			require.NoError(t, err)

			descOut, err := client.DescribeInstanceRefreshes(ctx, &assdk.DescribeInstanceRefreshesInput{
				AutoScalingGroupName: aws.String(groupName),
			})
			require.NoError(t, err)
			require.Len(t, descOut.InstanceRefreshes, 1)
			assert.Equal(t, tt.wantStatus, descOut.InstanceRefreshes[0].Status)
		})
	}
}

// TestRealClient_LaunchInstancesRetryStrategy proves both documented enum values are
// accepted and any other value is rejected as a ValidationError.
func TestRealClient_LaunchInstancesRetryStrategy(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		retryStrategy types.RetryStrategy
		wantErr       bool
	}{
		{name: "none accepted", retryStrategy: types.RetryStrategyNone, wantErr: false},
		{
			name:          "retry-with-group-configuration accepted",
			retryStrategy: types.RetryStrategyRetryWithGroupConfiguration,
			wantErr:       false,
		},
		{name: "invalid rejected", retryStrategy: types.RetryStrategy("bogus-strategy"), wantErr: true},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			groupName := "retry-strategy-" + tt.name
			_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String(groupName),
				MinSize:              aws.Int32(0),
				MaxSize:              aws.Int32(5),
				AvailabilityZones:    []string{"us-east-1a"},
			})
			require.NoError(t, err)

			_, err = client.LaunchInstances(ctx, &assdk.LaunchInstancesInput{
				AutoScalingGroupName: aws.String(groupName),
				ClientToken:          aws.String("rs-token-" + tt.name),
				RequestedCapacity:    aws.Int32(1),
				RetryStrategy:        tt.retryStrategy,
			})

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "ValidationError", apiErr.ErrorCode())
		})
	}
}

// TestRealClient_PutScalingPolicyEstimatedWarmupAndEnabled proves EstimatedInstanceWarmup
// and Enabled are echoed back on DescribePolicies, with Enabled defaulting to true when
// the caller omits it (PutScalingPolicyInput.Enabled doc comment: "The default is
// enabled").
func TestRealClient_PutScalingPolicyEstimatedWarmupAndEnabled(t *testing.T) {
	t.Parallel()

	cases := []struct {
		enabled     *bool
		name        string
		warmup      int32
		wantEnabled bool
	}{
		{name: "explicit warmup and disabled", enabled: aws.Bool(false), warmup: 120, wantEnabled: false},
		{name: "explicit warmup and enabled", enabled: aws.Bool(true), warmup: 90, wantEnabled: true},
		{name: "enabled defaults to true when omitted", enabled: nil, warmup: 60, wantEnabled: true},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			groupName := "warmup-enabled-" + tt.name
			_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String(groupName),
				MinSize:              aws.Int32(0),
				MaxSize:              aws.Int32(5),
				AvailabilityZones:    []string{"us-east-1a"},
			})
			require.NoError(t, err)

			_, err = client.PutScalingPolicy(ctx, &assdk.PutScalingPolicyInput{
				AutoScalingGroupName:    aws.String(groupName),
				PolicyName:              aws.String("warmup-policy"),
				PolicyType:              aws.String("SimpleScaling"),
				AdjustmentType:          aws.String("ChangeInCapacity"),
				ScalingAdjustment:       aws.Int32(1),
				EstimatedInstanceWarmup: aws.Int32(tt.warmup),
				Enabled:                 tt.enabled,
			})
			require.NoError(t, err)

			descOut, err := client.DescribePolicies(ctx, &assdk.DescribePoliciesInput{
				AutoScalingGroupName: aws.String(groupName),
				PolicyNames:          []string{"warmup-policy"},
			})
			require.NoError(t, err)
			require.Len(t, descOut.ScalingPolicies, 1)
			assert.Equal(t, tt.warmup, aws.ToInt32(descOut.ScalingPolicies[0].EstimatedInstanceWarmup))
			assert.Equal(t, tt.wantEnabled, aws.ToBool(descOut.ScalingPolicies[0].Enabled))
		})
	}
}

// TestRealClient_PutWarmPoolInstanceReusePolicy proves InstanceReusePolicy.ReuseOnScaleIn
// is read from the wire form (not just settable via a direct backend call) and echoed back
// on DescribeWarmPool.
func TestRealClient_PutWarmPoolInstanceReusePolicy(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		reuseOnScaleIn bool
	}{
		{name: "reuse enabled", reuseOnScaleIn: true},
		{name: "reuse disabled", reuseOnScaleIn: false},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			groupName := "warm-pool-reuse-" + tt.name
			_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String(groupName),
				MinSize:              aws.Int32(0),
				MaxSize:              aws.Int32(5),
				AvailabilityZones:    []string{"us-east-1a"},
			})
			require.NoError(t, err)

			_, err = client.PutWarmPool(ctx, &assdk.PutWarmPoolInput{
				AutoScalingGroupName: aws.String(groupName),
				MinSize:              aws.Int32(1),
				InstanceReusePolicy: &types.InstanceReusePolicy{
					ReuseOnScaleIn: aws.Bool(tt.reuseOnScaleIn),
				},
			})
			require.NoError(t, err)

			descOut, err := client.DescribeWarmPool(ctx, &assdk.DescribeWarmPoolInput{
				AutoScalingGroupName: aws.String(groupName),
			})
			require.NoError(t, err)
			require.NotNil(t, descOut.WarmPoolConfiguration)

			// The wire only emits InstanceReusePolicy when ReuseOnScaleIn is true
			// (xmlWarmPoolConfiguration.InstanceReusePolicy,omitempty); absent means false.
			got := false
			if p := descOut.WarmPoolConfiguration.InstanceReusePolicy; p != nil {
				got = aws.ToBool(p.ReuseOnScaleIn)
			}
			assert.Equal(t, tt.reuseOnScaleIn, got)
		})
	}
}
