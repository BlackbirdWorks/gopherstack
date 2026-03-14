package integration_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	autoscalingtypes "github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_AutoScaling_LaunchConfigurationLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createAutoScalingClient(t)
	ctx := t.Context()

	lcName := fmt.Sprintf("test-lc-%s", uuid.NewString()[:8])

	// CreateLaunchConfiguration
	_, err := client.CreateLaunchConfiguration(ctx, &autoscaling.CreateLaunchConfigurationInput{
		LaunchConfigurationName: aws.String(lcName),
		ImageId:                 aws.String("ami-12345678"),
		InstanceType:            aws.String("t2.micro"),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteLaunchConfiguration(ctx, &autoscaling.DeleteLaunchConfigurationInput{
			LaunchConfigurationName: aws.String(lcName),
		})
	})

	// DescribeLaunchConfigurations
	descOut, err := client.DescribeLaunchConfigurations(ctx, &autoscaling.DescribeLaunchConfigurationsInput{
		LaunchConfigurationNames: []string{lcName},
	})
	require.NoError(t, err)
	require.Len(t, descOut.LaunchConfigurations, 1)
	assert.Equal(t, lcName, aws.ToString(descOut.LaunchConfigurations[0].LaunchConfigurationName))
	assert.Equal(t, "ami-12345678", aws.ToString(descOut.LaunchConfigurations[0].ImageId))

	// DeleteLaunchConfiguration
	_, err = client.DeleteLaunchConfiguration(ctx, &autoscaling.DeleteLaunchConfigurationInput{
		LaunchConfigurationName: aws.String(lcName),
	})
	require.NoError(t, err)

	// Verify gone
	descOut2, err := client.DescribeLaunchConfigurations(ctx, &autoscaling.DescribeLaunchConfigurationsInput{
		LaunchConfigurationNames: []string{lcName},
	})
	require.NoError(t, err)
	assert.Empty(t, descOut2.LaunchConfigurations)
}

func TestIntegration_AutoScaling_AutoScalingGroupLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createAutoScalingClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	lcName := fmt.Sprintf("asg-lc-%s", suffix)
	asgName := fmt.Sprintf("test-asg-%s", suffix)

	// Create a launch configuration first
	_, err := client.CreateLaunchConfiguration(ctx, &autoscaling.CreateLaunchConfigurationInput{
		LaunchConfigurationName: aws.String(lcName),
		ImageId:                 aws.String("ami-12345678"),
		InstanceType:            aws.String("t2.micro"),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteAutoScalingGroup(ctx, &autoscaling.DeleteAutoScalingGroupInput{
			AutoScalingGroupName: aws.String(asgName),
			ForceDelete:          aws.Bool(true),
		})
		_, _ = client.DeleteLaunchConfiguration(ctx, &autoscaling.DeleteLaunchConfigurationInput{
			LaunchConfigurationName: aws.String(lcName),
		})
	})

	// CreateAutoScalingGroup
	_, err = client.CreateAutoScalingGroup(ctx, &autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName:    aws.String(asgName),
		LaunchConfigurationName: aws.String(lcName),
		MinSize:                 aws.Int32(1),
		MaxSize:                 aws.Int32(3),
		DesiredCapacity:         aws.Int32(2),
		AvailabilityZones:       []string{"us-east-1a"},
	})
	require.NoError(t, err)

	// DescribeAutoScalingGroups
	descOut, err := client.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{asgName},
	})
	require.NoError(t, err)
	require.Len(t, descOut.AutoScalingGroups, 1)
	assert.Equal(t, asgName, aws.ToString(descOut.AutoScalingGroups[0].AutoScalingGroupName))
	assert.Equal(t, int32(1), aws.ToInt32(descOut.AutoScalingGroups[0].MinSize))
	assert.Equal(t, int32(3), aws.ToInt32(descOut.AutoScalingGroups[0].MaxSize))

	// UpdateAutoScalingGroup
	_, err = client.UpdateAutoScalingGroup(ctx, &autoscaling.UpdateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String(asgName),
		MaxSize:              aws.Int32(5),
	})
	require.NoError(t, err)

	// Verify update
	descOut2, err := client.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{asgName},
	})
	require.NoError(t, err)
	require.Len(t, descOut2.AutoScalingGroups, 1)
	assert.Equal(t, int32(5), aws.ToInt32(descOut2.AutoScalingGroups[0].MaxSize))

	// DeleteAutoScalingGroup
	_, err = client.DeleteAutoScalingGroup(ctx, &autoscaling.DeleteAutoScalingGroupInput{
		AutoScalingGroupName: aws.String(asgName),
		ForceDelete:          aws.Bool(true),
	})
	require.NoError(t, err)

	// Verify deleted
	descOut3, err := client.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{asgName},
	})
	require.NoError(t, err)
	assert.Empty(t, descOut3.AutoScalingGroups)
}

func TestIntegration_AutoScaling_DescribeScalingActivities(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createAutoScalingClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	lcName := fmt.Sprintf("act-lc-%s", suffix)
	asgName := fmt.Sprintf("act-asg-%s", suffix)

	_, err := client.CreateLaunchConfiguration(ctx, &autoscaling.CreateLaunchConfigurationInput{
		LaunchConfigurationName: aws.String(lcName),
		ImageId:                 aws.String("ami-12345678"),
		InstanceType:            aws.String("t2.micro"),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteAutoScalingGroup(ctx, &autoscaling.DeleteAutoScalingGroupInput{
			AutoScalingGroupName: aws.String(asgName),
			ForceDelete:          aws.Bool(true),
		})
		_, _ = client.DeleteLaunchConfiguration(ctx, &autoscaling.DeleteLaunchConfigurationInput{
			LaunchConfigurationName: aws.String(lcName),
		})
	})

	_, err = client.CreateAutoScalingGroup(ctx, &autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName:    aws.String(asgName),
		LaunchConfigurationName: aws.String(lcName),
		MinSize:                 aws.Int32(0),
		MaxSize:                 aws.Int32(2),
		AvailabilityZones:       []string{"us-east-1a"},
	})
	require.NoError(t, err)

	// DescribeScalingActivities
	actOut, err := client.DescribeScalingActivities(ctx, &autoscaling.DescribeScalingActivitiesInput{
		AutoScalingGroupName: aws.String(asgName),
	})
	require.NoError(t, err)
	assert.NotNil(t, actOut)

	// DescribeAutoScalingGroups list all
	listOut, err := client.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{})
	require.NoError(t, err)

	found := false

	for _, g := range listOut.AutoScalingGroups {
		if aws.ToString(g.AutoScalingGroupName) == asgName {
			found = true

			break
		}
	}

	assert.True(t, found, "created ASG should appear in list")
}

func TestIntegration_AutoScaling_DescribeAutoScalingGroups_WithFilter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createAutoScalingClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	lcName := fmt.Sprintf("filt-lc-%s", suffix)
	asgName := fmt.Sprintf("filt-asg-%s", suffix)

	_, err := client.CreateLaunchConfiguration(ctx, &autoscaling.CreateLaunchConfigurationInput{
		LaunchConfigurationName: aws.String(lcName),
		ImageId:                 aws.String("ami-12345678"),
		InstanceType:            aws.String("t2.micro"),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteAutoScalingGroup(ctx, &autoscaling.DeleteAutoScalingGroupInput{
			AutoScalingGroupName: aws.String(asgName),
			ForceDelete:          aws.Bool(true),
		})
		_, _ = client.DeleteLaunchConfiguration(ctx, &autoscaling.DeleteLaunchConfigurationInput{
			LaunchConfigurationName: aws.String(lcName),
		})
	})

	_, err = client.CreateAutoScalingGroup(ctx, &autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName:    aws.String(asgName),
		LaunchConfigurationName: aws.String(lcName),
		MinSize:                 aws.Int32(1),
		MaxSize:                 aws.Int32(2),
		AvailabilityZones:       []string{"us-east-1a"},
		Tags: []autoscalingtypes.Tag{
			{Key: aws.String("env"), Value: aws.String("test"), PropagateAtLaunch: aws.Bool(false)},
		},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{asgName},
	})
	require.NoError(t, err)
	require.Len(t, descOut.AutoScalingGroups, 1)

	found := false

	for _, tag := range descOut.AutoScalingGroups[0].Tags {
		if aws.ToString(tag.Key) == "env" && aws.ToString(tag.Value) == "test" {
			found = true

			break
		}
	}

	assert.True(t, found, "tag should be present on the ASG")
}
