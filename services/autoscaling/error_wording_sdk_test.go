package autoscaling_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	assdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func seedGroup(t *testing.T, c *assdk.Client, name string, minSize, maxSize int32) {
	t.Helper()

	_, err := c.CreateLaunchConfiguration(t.Context(), &assdk.CreateLaunchConfigurationInput{
		LaunchConfigurationName: aws.String("lc-" + name), ImageId: aws.String("ami-12345678"),
		InstanceType: aws.String("t3.micro"),
	})
	require.NoError(t, err)

	_, err = c.CreateAutoScalingGroup(t.Context(), &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String(name), LaunchConfigurationName: aws.String("lc-" + name),
		MinSize: aws.Int32(minSize), MaxSize: aws.Int32(maxSize), AvailabilityZones: []string{"us-east-1a"},
	})
	require.NoError(t, err)
}

func TestSDK_ErrorWording(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call        func(*assdk.Client) error
		name        string
		wantCode    string
		wantMessage string
	}{
		{
			name: "update_unknown_group", wantCode: "ValidationError",
			wantMessage: "AutoScalingGroup name not found - null",
			call: func(c *assdk.Client) error {
				_, err := c.UpdateAutoScalingGroup(t.Context(), &assdk.UpdateAutoScalingGroupInput{
					AutoScalingGroupName: aws.String("nope"), MaxSize: aws.Int32(2),
				})

				return err
			},
		},
		{
			name: "create_duplicate_group", wantCode: "AlreadyExists",
			wantMessage: "AutoScalingGroup by this name already exists - A group with the name g1 already exists",
			call: func(c *assdk.Client) error {
				_, err := c.CreateAutoScalingGroup(t.Context(), &assdk.CreateAutoScalingGroupInput{
					AutoScalingGroupName: aws.String("g1"), LaunchConfigurationName: aws.String("lc-g1"),
					MinSize: aws.Int32(0), MaxSize: aws.Int32(1), AvailabilityZones: []string{"us-east-1a"},
				})

				return err
			},
		},
		{
			name:        "create_unknown_launch_configuration",
			wantCode:    "ValidationError",
			wantMessage: "Launch configuration name not found - A launch configuration with the name: ghost does not exist",
			call: func(c *assdk.Client) error {
				_, err := c.CreateAutoScalingGroup(t.Context(), &assdk.CreateAutoScalingGroupInput{
					AutoScalingGroupName: aws.String("g2"), LaunchConfigurationName: aws.String("ghost"),
					MinSize: aws.Int32(0), MaxSize: aws.Int32(1), AvailabilityZones: []string{"us-east-1a"},
				})

				return err
			},
		},
		{
			name: "desired_above_max", wantCode: "ValidationError",
			wantMessage: "New SetDesiredCapacity value 9 is above max value 3 for the AutoScalingGroup.",
			call: func(c *assdk.Client) error {
				_, err := c.SetDesiredCapacity(t.Context(), &assdk.SetDesiredCapacityInput{
					AutoScalingGroupName: aws.String("g1"), DesiredCapacity: aws.Int32(9),
				})

				return err
			},
		},
		{
			name: "max_zero_with_min", wantCode: "ValidationError",
			wantMessage: "Desired capacity:1 must be between the specified min size:1 and max size:0",
			call: func(c *assdk.Client) error {
				_, err := c.UpdateAutoScalingGroup(t.Context(), &assdk.UpdateAutoScalingGroupInput{
					AutoScalingGroupName: aws.String("g1"), MaxSize: aws.Int32(0),
				})

				return err
			},
		},
		{
			name: "bad_next_token", wantCode: "InvalidNextToken",
			wantMessage: "The NextToken value is invalid.",
			call: func(c *assdk.Client) error {
				_, err := c.DescribeAutoScalingGroups(t.Context(), &assdk.DescribeAutoScalingGroupsInput{
					NextToken: aws.String("***"),
				})

				return err
			},
		},
		{
			name: "bad_next_token_activities", wantCode: "InvalidNextToken",
			wantMessage: "The NextToken value is invalid.",
			call: func(c *assdk.Client) error {
				_, err := c.DescribeScalingActivities(t.Context(), &assdk.DescribeScalingActivitiesInput{
					NextToken: aws.String("***"),
				})

				return err
			},
		},
		{
			name:        "bad_policy_type",
			wantCode:    "ValidationError",
			wantMessage: "PolicyType \"Bogus\" must be SimpleScaling, StepScaling, TargetTrackingScaling or PredictiveScaling",
			call: func(c *assdk.Client) error {
				_, err := c.PutScalingPolicy(t.Context(), &assdk.PutScalingPolicyInput{
					AutoScalingGroupName: aws.String(
						"g1",
					),
					PolicyName: aws.String("p"),
					PolicyType: aws.String("Bogus"),
				})

				return err
			},
		},
		{
			name: "delete_group_with_instances", wantCode: "ResourceInUse",
			wantMessage: "You cannot delete an AutoScalingGroup while there are instances or pending Spot " +
				"instance request(s) still in the group.",
			call: func(c *assdk.Client) error {
				_, err := c.DeleteAutoScalingGroup(t.Context(), &assdk.DeleteAutoScalingGroupInput{
					AutoScalingGroupName: aws.String("g1"),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestHandlerAndClient(t)
			seedGroup(t, c, "g1", 1, 3)

			err := tt.call(c)
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr, "got %T", err)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.Equal(t, tt.wantMessage, apiErr.ErrorMessage())
			assert.False(t, strings.HasPrefix(apiErr.ErrorMessage(), apiErr.ErrorCode()+":"))
		})
	}
}

func TestSDK_CapacityChangeActivities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		change      func(*assdk.Client) error
		name        string
		wantDesc    string
		wantCause   string
		wantDesired int32
	}{
		{
			name: "set_desired_scale_out", wantDesired: 3, wantDesc: "Launching a new EC2 instance: ",
			wantCause: "a user request explicitly set group desired capacity changing the desired capacity from 1 to 3",
			change: func(c *assdk.Client) error {
				_, err := c.SetDesiredCapacity(t.Context(), &assdk.SetDesiredCapacityInput{
					AutoScalingGroupName: aws.String("g1"), DesiredCapacity: aws.Int32(3),
				})

				return err
			},
		},
		{
			name: "update_min_bumps_desired", wantDesired: 2, wantDesc: "Launching a new EC2 instance: ",
			wantCause: "a user request update of AutoScalingGroup constraints to min: 2, max: 3, desired: 2",
			change: func(c *assdk.Client) error {
				_, err := c.UpdateAutoScalingGroup(t.Context(), &assdk.UpdateAutoScalingGroupInput{
					AutoScalingGroupName: aws.String("g1"), MinSize: aws.Int32(2),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestHandlerAndClient(t)
			seedGroup(t, c, "g1", 1, 3)

			before, err := c.DescribeScalingActivities(t.Context(), &assdk.DescribeScalingActivitiesInput{})
			require.NoError(t, err)

			require.NoError(t, tt.change(c))

			after, err := c.DescribeScalingActivities(t.Context(), &assdk.DescribeScalingActivitiesInput{})
			require.NoError(t, err)
			require.Greater(t, len(after.Activities), len(before.Activities))

			found := false
			for _, a := range after.Activities {
				if strings.HasPrefix(aws.ToString(a.Description), tt.wantDesc) &&
					strings.Contains(aws.ToString(a.Cause), tt.wantCause) {
					found = true
				}
			}
			assert.True(t, found, "no activity matched %q / %q", tt.wantDesc, tt.wantCause)

			out, err := c.DescribeAutoScalingGroups(t.Context(), &assdk.DescribeAutoScalingGroupsInput{})
			require.NoError(t, err)
			assert.Equal(t, tt.wantDesired, aws.ToInt32(out.AutoScalingGroups[0].DesiredCapacity))
		})
	}
}
