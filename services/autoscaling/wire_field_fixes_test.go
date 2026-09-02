package autoscaling_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	assdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/stretchr/testify/require"
)

// TestUpdateAutoScalingGroup_PlacementGroupCanBeCleared drives
// CreateAutoScalingGroup/UpdateAutoScalingGroup/DescribeAutoScalingGroups
// through the real SDK client. UpdateAutoScalingGroupInput.PlacementGroup was
// a plain string guarded by != "" (not *string like the real SDK's
// UpdateAutoScalingGroupInput, api_op_UpdateAutoScalingGroup.go), whose doc
// comment says "To remove the placement group setting, pass an empty string
// for placement-group" -- so a real client's documented way to clear it was
// silently dropped, leaving the old placement group in place.
func TestUpdateAutoScalingGroup_PlacementGroupCanBeCleared(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String("asg-pg-clear"),
		MinSize:              aws.Int32(0),
		MaxSize:              aws.Int32(1),
		AvailabilityZones:    []string{"us-east-1a"},
		PlacementGroup:       aws.String("my-placement-group"),
	})
	require.NoError(t, err)

	before, err := client.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{"asg-pg-clear"},
	})
	require.NoError(t, err)
	require.Len(t, before.AutoScalingGroups, 1)
	require.Equal(t, "my-placement-group", aws.ToString(before.AutoScalingGroups[0].PlacementGroup))

	_, err = client.UpdateAutoScalingGroup(ctx, &assdk.UpdateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String("asg-pg-clear"),
		PlacementGroup:       aws.String(""),
	})
	require.NoError(t, err)

	after, err := client.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{"asg-pg-clear"},
	})
	require.NoError(t, err)
	require.Len(t, after.AutoScalingGroups, 1)
	require.Empty(t, aws.ToString(after.AutoScalingGroups[0].PlacementGroup),
		"explicit empty PlacementGroup on Update must clear the setting, not be silently ignored")
}
