package ec2_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestCreateFleet_LaunchesTrackedInstances covers gopherstack-q5k5:
// DescribeFleetInstances and DescribeFleetHistory always returned an empty
// set because CreateFleet never launched or recorded any instance against
// the fleet -- a correct FleetId read still found nothing to return, since
// there was nothing to find. A real client's DescribeFleetInstances call was
// always empty regardless of the fleet's TargetCapacitySpecification.
func TestCreateFleet_LaunchesTrackedInstances(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)
	ctx := t.Context()

	created, err := client.CreateFleet(ctx, &ec2sdk.CreateFleetInput{
		Type: types.FleetTypeMaintain,
		TargetCapacitySpecification: &types.TargetCapacitySpecificationRequest{
			TotalTargetCapacity:       aws.Int32(3),
			DefaultTargetCapacityType: types.DefaultTargetCapacityTypeOnDemand,
		},
		LaunchTemplateConfigs: []types.FleetLaunchTemplateConfigRequest{
			{
				LaunchTemplateSpecification: &types.FleetLaunchTemplateSpecificationRequest{
					LaunchTemplateId: aws.String("lt-trackedinst0001"),
					Version:          aws.String("$Latest"),
				},
			},
		},
	})
	require.NoError(t, err, "CreateFleet should succeed")
	fleetID := aws.ToString(created.FleetId)
	require.NotEmpty(t, fleetID)

	instOut, err := client.DescribeFleetInstances(ctx, &ec2sdk.DescribeFleetInstancesInput{
		FleetId: aws.String(fleetID),
	})
	require.NoError(t, err)
	require.Len(t, instOut.ActiveInstances, 3,
		"DescribeFleetInstances must return the instances CreateFleet actually launched, not a hardcoded empty set")

	seen := make(map[string]bool)

	for _, inst := range instOut.ActiveInstances {
		id := aws.ToString(inst.InstanceId)
		assert.True(t, strings.HasPrefix(id, "i-"), "instance id must be i-prefixed, got %q", id)
		assert.False(t, seen[id], "instance ids must be unique, got duplicate %q", id)

		seen[id] = true
	}

	histOut, err := client.DescribeFleetHistory(ctx, &ec2sdk.DescribeFleetHistoryInput{
		FleetId:   aws.String(fleetID),
		StartTime: aws.Time(time.Now().Add(-time.Hour)),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, histOut.HistoryRecords,
		"DescribeFleetHistory must return the fleet's creation event, not a hardcoded empty set")
}

// TestDescribeFleets_InstantType_ShowsLaunchedInstances covers
// gopherstack-q5k5: DescribeFleets never emitted an Instances field at all
// (FleetData.Instances, ec2@v1.319.1 types/types.go:6672, "valid only when
// Type is set to instant"), so even once CreateFleet started tracking real
// instances, an instant fleet's Fleets[i].Instances stayed empty -- a
// correctly wire-shaped fleet exposing an empty instance set over data the
// handler never wired up, rather than over data the backend never held.
func TestDescribeFleets_InstantType_ShowsLaunchedInstances(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)
	ctx := t.Context()

	created, err := client.CreateFleet(ctx, &ec2sdk.CreateFleetInput{
		Type: types.FleetTypeInstant,
		TargetCapacitySpecification: &types.TargetCapacitySpecificationRequest{
			TotalTargetCapacity: aws.Int32(2),
		},
		LaunchTemplateConfigs: []types.FleetLaunchTemplateConfigRequest{
			{
				LaunchTemplateSpecification: &types.FleetLaunchTemplateSpecificationRequest{
					LaunchTemplateId: aws.String("lt-instantinst0001"),
				},
			},
		},
	})
	require.NoError(t, err)
	fleetID := aws.ToString(created.FleetId)

	require.Len(t, created.Instances, 1, "instant fleet's CreateFleetOutput must report launched instances")
	assert.Len(t, created.Instances[0].InstanceIds, 2)

	descOut, err := client.DescribeFleets(ctx, &ec2sdk.DescribeFleetsInput{FleetIds: []string{fleetID}})
	require.NoError(t, err)
	require.Len(t, descOut.Fleets, 1)
	require.Len(t, descOut.Fleets[0].Instances, 1,
		"DescribeFleets must report an instant fleet's launched instances, not an empty set")
	assert.Len(t, descOut.Fleets[0].Instances[0].InstanceIds, 2)

	// DescribeFleetInstances documents no support for fleets of type instant
	// (api_op_DescribeFleetInstances.go doc comment) -- verify we return an
	// empty set for it rather than fabricating support the real API lacks.
	instOut, err := client.DescribeFleetInstances(ctx, &ec2sdk.DescribeFleetInstancesInput{
		FleetId: aws.String(fleetID),
	})
	require.NoError(t, err)
	assert.Empty(t, instOut.ActiveInstances)
}
