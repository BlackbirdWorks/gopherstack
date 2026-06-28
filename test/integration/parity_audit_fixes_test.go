package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudformationsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_CFN_StopStackSetOperation_NotFound verifies that stopping an
// operation on a non-existent stack set returns an error instead of a silent
// HTTP 200. (Regression: the handler used to discard the backend error.)
func TestIntegration_CFN_StopStackSetOperation_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	ctx := t.Context()

	_, err := client.StopStackSetOperation(ctx, &cloudformationsdk.StopStackSetOperationInput{
		StackSetName: aws.String("missing-set-" + cfnStackID()),
		OperationId:  aws.String("missing-op"),
	})
	require.Error(t, err, "stopping an operation on a missing stack set must return an error")
}

// TestIntegration_CFN_ListStackSetAutoDeploymentTargets_NotFound verifies that
// listing auto-deployment targets for a non-existent stack set returns an error
// rather than an empty success. (Regression: the handler discarded the backend
// StackSetNotFound error.)
func TestIntegration_CFN_ListStackSetAutoDeploymentTargets_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	ctx := t.Context()

	_, err := client.ListStackSetAutoDeploymentTargets(
		ctx,
		&cloudformationsdk.ListStackSetAutoDeploymentTargetsInput{
			StackSetName: aws.String("missing-set-" + cfnStackID()),
		},
	)
	require.Error(t, err, "listing auto-deployment targets for a missing stack set must error")
}

// TestIntegration_EC2_DescribeFleets_ReturnsCreatedFleet verifies that a fleet
// created via CreateFleet is returned by DescribeFleets. (Regression: a stub
// handler used to shadow the real DescribeFleets implementation and always
// returned an empty fleet set.)
func TestIntegration_EC2_DescribeFleets_ReturnsCreatedFleet(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEC2Client(t)
	ctx := t.Context()

	createOut, err := client.CreateFleet(ctx, &ec2sdk.CreateFleetInput{
		Type: ec2types.FleetTypeInstant,
		TargetCapacitySpecification: &ec2types.TargetCapacitySpecificationRequest{
			TotalTargetCapacity:       aws.Int32(2),
			DefaultTargetCapacityType: ec2types.DefaultTargetCapacityTypeOnDemand,
		},
		LaunchTemplateConfigs: []ec2types.FleetLaunchTemplateConfigRequest{
			{
				LaunchTemplateSpecification: &ec2types.FleetLaunchTemplateSpecificationRequest{
					LaunchTemplateName: aws.String("audit-tmpl"),
					Version:            aws.String("$Latest"),
				},
			},
		},
	})
	require.NoError(t, err, "CreateFleet should succeed")
	fleetID := aws.ToString(createOut.FleetId)
	require.NotEmpty(t, fleetID, "CreateFleet must return a fleet id")

	descOut, err := client.DescribeFleets(ctx, &ec2sdk.DescribeFleetsInput{
		FleetIds: []string{fleetID},
	})
	require.NoError(t, err, "DescribeFleets should succeed")
	require.Len(t, descOut.Fleets, 1, "the created fleet must be returned by DescribeFleets")
	assert.Equal(t, fleetID, aws.ToString(descOut.Fleets[0].FleetId))
}
