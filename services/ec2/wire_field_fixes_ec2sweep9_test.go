package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestDescribeSpotFleetRequests_FulfilledCapacity_RealClient covers
// gopherstack-6flj: FulfilledCapacity was emitted flat on the outer
// SpotFleetRequestConfig item ("fulfilledCapacity" directly under <item>), but
// the real deserializer (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeDocumentSpotFleetRequestConfig) has no case for that
// name at all -- FulfilledCapacity only exists one level deeper, nested inside
// SpotFleetRequestConfigData (awsEc2query_deserializeDocumentSpotFleetRequestConfigData's
// "fulfilledCapacity" case). A real client's
// SpotFleetRequestConfigs[i].SpotFleetRequestConfig.FulfilledCapacity was
// always nil regardless of how many instances the fleet actually fulfilled.
func TestDescribeSpotFleetRequests_FulfilledCapacity_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	req, err := client.RequestSpotFleet(t.Context(), &ec2sdk.RequestSpotFleetInput{
		SpotFleetRequestConfig: &types.SpotFleetRequestConfigData{
			IamFleetRole:   aws.String("arn:aws:iam::000000000000:role/fleet-role"),
			TargetCapacity: aws.Int32(1),
			LaunchSpecifications: []types.SpotFleetLaunchSpecification{{
				ImageId:      aws.String("ami-fulfilled0001"),
				InstanceType: types.InstanceTypeM5Large,
			}},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeSpotFleetRequests(t.Context(), &ec2sdk.DescribeSpotFleetRequestsInput{
		SpotFleetRequestIds: []string{aws.ToString(req.SpotFleetRequestId)},
	})
	require.NoError(t, err)
	require.Len(t, out.SpotFleetRequestConfigs, 1)

	cfg := out.SpotFleetRequestConfigs[0].SpotFleetRequestConfig
	require.NotNil(t, cfg, "SpotFleetRequestConfig nil")
	require.NotNil(t, cfg.FulfilledCapacity,
		"FulfilledCapacity nil - pre-fix it was emitted one level too shallow")
	assert.InDelta(t, 1.0, aws.ToFloat64(cfg.FulfilledCapacity), 0.001)
}

// TestDescribeFleets_Type_RealClient covers gopherstack-6flj: the FleetData
// item emitted the fleet's request type under "fleetType", a key that exists
// nowhere in the real schema (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeDocumentFleetData's EqualFold list has "type", never
// "fleetType"). A real client's Fleets[i].Type was always empty regardless of
// what CreateFleet was asked to create.
func TestDescribeFleets_Type_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	created, err := client.CreateFleet(t.Context(), &ec2sdk.CreateFleetInput{
		Type: types.FleetTypeRequest,
		TargetCapacitySpecification: &types.TargetCapacitySpecificationRequest{
			TotalTargetCapacity: aws.Int32(2),
		},
		LaunchTemplateConfigs: []types.FleetLaunchTemplateConfigRequest{{
			LaunchTemplateSpecification: &types.FleetLaunchTemplateSpecificationRequest{
				LaunchTemplateId: aws.String("lt-fleettype0001"),
			},
		}},
	})
	require.NoError(t, err)
	fleetID := aws.ToString(created.FleetId)

	out, err := client.DescribeFleets(t.Context(), &ec2sdk.DescribeFleetsInput{
		FleetIds: []string{fleetID},
	})
	require.NoError(t, err)
	require.Len(t, out.Fleets, 1)
	assert.Equal(t, types.FleetTypeRequest, out.Fleets[0].Type,
		"Type empty - pre-fix wire key \"fleetType\" doesn't exist in the real schema")
}

// TestGetSubnetCidrReservations_WrapperKey_RealClient covers gopherstack-6flj:
// the handler wrapped IPv4 reservations under "subnetIpv4CidrReservations", a
// key that doesn't exist in the real schema at all -- the real deserializer
// (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentGetSubnetCidrReservationsOutput) reads
// "subnetIpv4CidrReservationSet" (plus a separate "subnetIpv6CidrReservationSet"
// gopherstack never emitted at all). A real client's
// SubnetIpv4CidrReservations was always nil regardless of what
// CreateSubnetCidrReservation had created. Also covers a g8k9-flavor gap in
// the same op: Description and OwnerID are tracked by the backend (proven by
// CreateSubnetCidrReservation's own response, which does emit them) but the
// old Get item type (subnetCidrReservationItem2) dropped both.
func TestGetSubnetCidrReservations_WrapperKey_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.70.0.0/16")})
	require.NoError(t, err)
	subnet, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId: vpc.Vpc.VpcId, CidrBlock: aws.String("10.70.1.0/24"),
	})
	require.NoError(t, err)
	subnetID := subnet.Subnet.SubnetId

	_, err = client.CreateSubnetCidrReservation(t.Context(), &ec2sdk.CreateSubnetCidrReservationInput{
		SubnetId:        subnetID,
		Cidr:            aws.String("10.70.1.128/28"),
		ReservationType: types.SubnetCidrReservationTypePrefix,
		Description:     aws.String("wire-field-fixes-reservation"),
	})
	require.NoError(t, err)

	out, err := client.GetSubnetCidrReservations(t.Context(), &ec2sdk.GetSubnetCidrReservationsInput{
		SubnetId: subnetID,
	})
	require.NoError(t, err)
	require.Len(
		t,
		out.SubnetIpv4CidrReservations,
		1,
		"SubnetIpv4CidrReservations empty - pre-fix wire key \"subnetIpv4CidrReservations\" doesn't exist in the real schema",
	)
	assert.Empty(t, out.SubnetIpv6CidrReservations)

	got := out.SubnetIpv4CidrReservations[0]
	assert.Equal(t, "10.70.1.128/28", aws.ToString(got.Cidr))
	assert.Equal(t, "wire-field-fixes-reservation", aws.ToString(got.Description),
		"Description empty - the old Get item type dropped it despite the backend tracking it")
	assert.NotEmpty(t, aws.ToString(got.OwnerId),
		"OwnerId empty - the old Get item type dropped it despite the backend tracking it")
}

// TestDeleteFleets_StateShape_RealClient covers gopherstack-6flj:
// DeleteFleets reused the Describe item type (fleetItem, mapped to
// types.FleetData) for its SuccessfulFleetDeletions entries, emitting a flat
// "fleetState" -- but the real per-op item is types.DeleteFleetSuccessItem,
// which has no plain fleetState member at all, only
// currentFleetState/previousFleetState (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeDocumentDeleteFleetSuccessItem). A real client's
// CurrentFleetState and PreviousFleetState were always empty regardless of
// what state the fleet was actually in.
func TestDeleteFleets_StateShape_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	created, err := client.CreateFleet(t.Context(), &ec2sdk.CreateFleetInput{
		TargetCapacitySpecification: &types.TargetCapacitySpecificationRequest{
			TotalTargetCapacity: aws.Int32(1),
		},
		LaunchTemplateConfigs: []types.FleetLaunchTemplateConfigRequest{{
			LaunchTemplateSpecification: &types.FleetLaunchTemplateSpecificationRequest{
				LaunchTemplateId: aws.String("lt-deletefleet0001"),
			},
		}},
	})
	require.NoError(t, err)
	fleetID := aws.ToString(created.FleetId)

	out, err := client.DeleteFleets(t.Context(), &ec2sdk.DeleteFleetsInput{
		FleetIds:           []string{fleetID},
		TerminateInstances: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, out.SuccessfulFleetDeletions, 1)

	deletion := out.SuccessfulFleetDeletions[0]
	assert.Equal(t, fleetID, aws.ToString(deletion.FleetId))
	assert.Equal(t, types.FleetStateCodeDeleted, deletion.CurrentFleetState,
		"CurrentFleetState empty - pre-fix the item had no currentFleetState member at all")
	assert.Equal(t, types.FleetStateCodeActive, deletion.PreviousFleetState,
		"PreviousFleetState empty - pre-fix the item had no previousFleetState member at all")
}
