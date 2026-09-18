package autoscaling_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	assdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpdateAutoScalingGroup_PreservesOmittedMembers proves the zeroguard
// fix (cmd/zeroguard): UpdateAutoScalingGroupInput.LaunchConfigurationName,
// .VPCZoneIdentifier, .Context, .DesiredCapacityType and .HealthCheckType
// are all plain strings in the real SDK's request type
// (api_op_UpdateAutoScalingGroup.go). Autoscaling is awsquery/form-encoded,
// so "omitted" means the form key is absent, not that vals.Get returns "" --
// before the fix each field read via vals.Get alone, so an omitted field and
// one explicitly sent empty were indistinguishable. AutoScalingGroupName is
// left a plain string: it is a required lookup identifier, never written
// back to state.
func TestUpdateAutoScalingGroup_PreservesOmittedMembers(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName:    aws.String("uds-asg"),
		MinSize:                 aws.Int32(0),
		MaxSize:                 aws.Int32(3),
		AvailabilityZones:       []string{"us-east-1a"},
		LaunchConfigurationName: aws.String("lc-1"),
	})
	require.NoError(t, err)

	_, err = client.UpdateAutoScalingGroup(ctx, &assdk.UpdateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String("uds-asg"),
		VPCZoneIdentifier:    aws.String("subnet-1"),
		Context:              aws.String("ctx-1"),
		DesiredCapacityType:  aws.String("units"),
		HealthCheckType:      aws.String("ELB"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{"uds-asg"},
	})
	require.NoError(t, err)
	require.Len(t, desc.AutoScalingGroups, 1)
	g := desc.AutoScalingGroups[0]
	assert.Equal(t, "subnet-1", aws.ToString(g.VPCZoneIdentifier))
	assert.Equal(t, "ctx-1", aws.ToString(g.Context))
	assert.Equal(t, "units", aws.ToString(g.DesiredCapacityType))
	assert.Equal(t, "ELB", aws.ToString(g.HealthCheckType))
	assert.Equal(t, "lc-1", aws.ToString(g.LaunchConfigurationName))

	// Omitted fields must preserve every prior value.
	_, err = client.UpdateAutoScalingGroup(ctx, &assdk.UpdateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String("uds-asg"),
	})
	require.NoError(t, err)

	desc, err = client.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{"uds-asg"},
	})
	require.NoError(t, err)
	g = desc.AutoScalingGroups[0]
	assert.Equal(t, "subnet-1", aws.ToString(g.VPCZoneIdentifier), "omitted VPCZoneIdentifier must survive")
	assert.Equal(t, "ctx-1", aws.ToString(g.Context), "omitted Context must survive")
	assert.Equal(t, "units", aws.ToString(g.DesiredCapacityType), "omitted DesiredCapacityType must survive")
	assert.Equal(t, "ELB", aws.ToString(g.HealthCheckType), "omitted HealthCheckType must survive")
	assert.Equal(t, "lc-1", aws.ToString(g.LaunchConfigurationName), "omitted LaunchConfigurationName must survive")

	// Explicit empty string clears VPCZoneIdentifier/Context (both accept an
	// empty value); unrelated LaunchConfigurationName untouched.
	_, err = client.UpdateAutoScalingGroup(ctx, &assdk.UpdateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String("uds-asg"),
		VPCZoneIdentifier:    aws.String(""),
		Context:              aws.String(""),
	})
	require.NoError(t, err)

	desc, err = client.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{"uds-asg"},
	})
	require.NoError(t, err)
	g = desc.AutoScalingGroups[0]
	assert.Empty(t, aws.ToString(g.VPCZoneIdentifier))
	assert.Empty(t, aws.ToString(g.Context))
	assert.Equal(t, "lc-1", aws.ToString(g.LaunchConfigurationName), "unrelated field untouched")
}
