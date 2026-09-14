package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	assdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	astypes "github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWireAutoScalingEC2_InstanceRequirementsResolvesViaEC2Catalog proves
// gopherstack-jgrn6: a MixedInstancesPolicy override carrying
// InstanceRequirements (attribute-based selection, no explicit InstanceType)
// resolves against ec2's real instance-type catalog engine
// (wireAutoScalingEC2's ec2AutoScalingInstanceTypeResolverAdapter calling
// EC2's exported MatchInstanceTypes/GetInstanceTypesFromInstanceRequirements)
// instead of being ignored. VCpuCount {2,2} + MemoryMiB {8192,8192} +
// AllowedInstanceTypes ["m5.*"] matches exactly m5.large in the catalog
// (services/ec2/instance_type_catalog.go) among the m5 family. Uses the same
// composition-root harness as
// TestWireAutoScalingEC2_MixedInstancesPolicyRoundRobin
// (cli_asg_ec2_launch_template_wiring_test.go).
func TestWireAutoScalingEC2_InstanceRequirementsResolvesViaEC2Catalog(t *testing.T) {
	t.Parallel()

	ec2Client, asgClient, ec2H := newASGEC2LaunchTemplateWiringClients(t)
	ctx := t.Context()

	ltOut, err := ec2Client.CreateLaunchTemplate(ctx, &ec2sdk.CreateLaunchTemplateInput{
		LaunchTemplateName: aws.String("wiring-lt-instance-requirements"),
		LaunchTemplateData: &ec2types.RequestLaunchTemplateData{
			ImageId:      aws.String("ami-ir-base"),
			InstanceType: ec2types.InstanceTypeT3Micro,
		},
	})
	require.NoError(t, err)
	ltID := aws.ToString(ltOut.LaunchTemplate.LaunchTemplateId)

	_, err = asgClient.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String("wiring-asg-instance-requirements"),
		MixedInstancesPolicy: &astypes.MixedInstancesPolicy{
			LaunchTemplate: &astypes.LaunchTemplate{
				LaunchTemplateSpecification: &astypes.LaunchTemplateSpecification{
					LaunchTemplateId: aws.String(ltID),
				},
				Overrides: []astypes.LaunchTemplateOverrides{
					{
						InstanceRequirements: &astypes.InstanceRequirements{
							VCpuCount:            &astypes.VCpuCountRequest{Min: aws.Int32(2), Max: aws.Int32(2)},
							MemoryMiB:            &astypes.MemoryMiBRequest{Min: aws.Int32(8192), Max: aws.Int32(8192)},
							AllowedInstanceTypes: []string{"m5.*"},
						},
					},
				},
			},
		},
		MinSize:           aws.Int32(0),
		MaxSize:           aws.Int32(5),
		DesiredCapacity:   aws.Int32(1),
		AvailabilityZones: []string{"us-east-1a"},
	})
	require.NoError(t, err)

	groupsOut, err := asgClient.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{"wiring-asg-instance-requirements"},
	})
	require.NoError(t, err)
	require.Len(t, groupsOut.AutoScalingGroups, 1)
	require.Len(t, groupsOut.AutoScalingGroups[0].Instances, 1)

	inst := groupsOut.AutoScalingGroups[0].Instances[0]
	assert.Equal(t, "m5.large", aws.ToString(inst.InstanceType),
		"DescribeAutoScalingGroups must report the resolved instance type")

	mip := groupsOut.AutoScalingGroups[0].MixedInstancesPolicy
	require.NotNil(t, mip)
	require.NotNil(t, mip.LaunchTemplate)
	require.Len(t, mip.LaunchTemplate.Overrides, 1)
	gotIR := mip.LaunchTemplate.Overrides[0].InstanceRequirements
	require.NotNil(t, gotIR, "DescribeAutoScalingGroups must keep echoing InstanceRequirements as given")
	require.NotNil(t, gotIR.VCpuCount)
	assert.Equal(t, int32(2), aws.ToInt32(gotIR.VCpuCount.Min))
	assert.Equal(t, int32(2), aws.ToInt32(gotIR.VCpuCount.Max))

	instanceID := aws.ToString(inst.InstanceId)

	ec2Instances := ec2H.Backend.DescribeInstances([]string{instanceID}, "")
	require.Len(t, ec2Instances, 1,
		"the ASG-reported instance ID must be a real EC2 record, not a fabricated one")
	assert.Equal(t, "ami-ir-base", ec2Instances[0].ImageID)
	assert.Equal(t, "m5.large", ec2Instances[0].InstanceType,
		"EC2 DescribeInstances must show the instance type resolved from ec2's real catalog engine")
}
