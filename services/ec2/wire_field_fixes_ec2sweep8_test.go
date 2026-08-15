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

// TestGetNetworkInsightsAccessScopeContent_WrapperKey_RealClient covers
// gopherstack-6flj: the handler wrapped its response under "networkInsightsAccessScope",
// but the real GetNetworkInsightsAccessScopeContentOutput deserializer (ec2@v1.319.1
// deserializers.go: awsEc2query_deserializeOpDocumentGetNetworkInsightsAccessScopeContentOutput)
// reads "networkInsightsAccessScopeContent" -- a key that doesn't exist in the handler's
// old response at all. A real client's NetworkInsightsAccessScopeContent was always nil
// regardless of what CreateNetworkInsightsAccessScope had set up.
func TestGetNetworkInsightsAccessScopeContent_WrapperKey_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	createOut, err := client.CreateNetworkInsightsAccessScope(
		ctx, &ec2sdk.CreateNetworkInsightsAccessScopeInput{},
	)
	require.NoError(t, err)
	require.NotNil(t, createOut.NetworkInsightsAccessScope)
	scopeID := aws.ToString(createOut.NetworkInsightsAccessScope.NetworkInsightsAccessScopeId)
	require.NotEmpty(t, scopeID)

	out, err := client.GetNetworkInsightsAccessScopeContent(
		ctx, &ec2sdk.GetNetworkInsightsAccessScopeContentInput{
			NetworkInsightsAccessScopeId: aws.String(scopeID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.NetworkInsightsAccessScopeContent,
		"NetworkInsightsAccessScopeContent must round-trip; pre-fix the wrapper key didn't "+
			"match the real deserializer's, so this was always nil")
	assert.Equal(t, scopeID, aws.ToString(out.NetworkInsightsAccessScopeContent.NetworkInsightsAccessScopeId))
}

// TestGetNetworkInsightsAccessScopeAnalysisFindings_WrapperKeys_RealClient covers
// gopherstack-6flj: the handler emitted the analysis ID under "analysisId" and the
// findings list under "accessScopeAnalysisFindingSet", but the real
// GetNetworkInsightsAccessScopeAnalysisFindingsOutput deserializer (ec2@v1.319.1
// deserializers.go) reads "networkInsightsAccessScopeAnalysisId" and
// "analysisFindingSet" -- neither of the handler's old keys exist in the real shape.
func TestGetNetworkInsightsAccessScopeAnalysisFindings_WrapperKeys_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	scopeOut, err := client.CreateNetworkInsightsAccessScope(
		ctx, &ec2sdk.CreateNetworkInsightsAccessScopeInput{},
	)
	require.NoError(t, err)
	scopeID := aws.ToString(scopeOut.NetworkInsightsAccessScope.NetworkInsightsAccessScopeId)

	analysisOut, err := client.StartNetworkInsightsAccessScopeAnalysis(
		ctx, &ec2sdk.StartNetworkInsightsAccessScopeAnalysisInput{
			NetworkInsightsAccessScopeId: aws.String(scopeID),
		},
	)
	require.NoError(t, err)
	analysisID := aws.ToString(analysisOut.NetworkInsightsAccessScopeAnalysis.NetworkInsightsAccessScopeAnalysisId)
	require.NotEmpty(t, analysisID)

	out, err := client.GetNetworkInsightsAccessScopeAnalysisFindings(
		ctx, &ec2sdk.GetNetworkInsightsAccessScopeAnalysisFindingsInput{
			NetworkInsightsAccessScopeAnalysisId: aws.String(analysisID),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, analysisID, aws.ToString(out.NetworkInsightsAccessScopeAnalysisId),
		"NetworkInsightsAccessScopeAnalysisId decoded empty - the old \"analysisId\" wire key "+
			"doesn't exist in the real output shape")
	assert.Equal(t, "succeeded", string(out.AnalysisStatus))
}

// TestDescribeCapacityReservations_OwnerId_RealClient covers gopherstack-6flj:
// capacityReservationItem emitted the account under "ownedBy", a key that does not
// exist anywhere in the real CapacityReservation schema (ec2@v1.319.1 deserializers.go:
// awsEc2query_deserializeDocumentCapacityReservation reads "ownerId"). The
// neighbouring hostItem type in the same file already used the correct "ownerId"
// name, making this a sibling trap. A real client's OwnerId was always empty
// regardless of who created the reservation.
func TestDescribeCapacityReservations_OwnerId_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("111122223333", "us-east-1")))
	ctx := t.Context()

	createOut, err := client.CreateCapacityReservation(ctx, &ec2sdk.CreateCapacityReservationInput{
		InstanceType:     aws.String("m5.large"),
		InstancePlatform: types.CapacityReservationInstancePlatformLinuxUnix,
		AvailabilityZone: aws.String("us-east-1a"),
		InstanceCount:    aws.Int32(2),
	})
	require.NoError(t, err)
	crID := aws.ToString(createOut.CapacityReservation.CapacityReservationId)
	require.NotEmpty(t, crID)
	assert.Equal(t, "111122223333", aws.ToString(createOut.CapacityReservation.OwnerId),
		"OwnerId decoded empty on CreateCapacityReservation's response too - "+
			"same shared item type, same wrong key")

	out, err := client.DescribeCapacityReservations(ctx, &ec2sdk.DescribeCapacityReservationsInput{
		CapacityReservationIds: []string{crID},
	})
	require.NoError(t, err)
	require.Len(t, out.CapacityReservations, 1)
	assert.Equal(t, "111122223333", aws.ToString(out.CapacityReservations[0].OwnerId),
		"OwnerId decoded empty - pre-fix the wire key was \"ownedBy\", which doesn't exist "+
			"in the real schema at all")
}

// TestAcceptCapacityReservationBillingOwnership_Return_RealClient covers
// gopherstack-6flj: the handler's AcceptCapacityReservationBillingOwnershipResponse
// wrapped an invented full CapacityReservation object under a "capacityReservation"
// key that doesn't exist in the real output at all, while never emitting the one
// member the real shape does have. The real
// AcceptCapacityReservationBillingOwnershipOutput has only Return, no
// CapacityReservation member (ec2@v1.319.1 deserializers.go:
// awsEc2query_deserializeOpDocumentAcceptCapacityReservationBillingOwnershipOutput).
// A real client's Return was always nil/false regardless of whether the call
// succeeded.
func TestAcceptCapacityReservationBillingOwnership_Return_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("111122223333", "us-east-1")
	b.AddCapacityReservationInternal(&ec2.CapacityReservation{
		CapacityReservationID: "cr-billing-1",
		State:                 "pending",
		OwnedBy:               "999988887777",
	})
	client := newTestEC2Client(t, ec2.NewHandler(b))

	out, err := client.AcceptCapacityReservationBillingOwnership(
		t.Context(), &ec2sdk.AcceptCapacityReservationBillingOwnershipInput{
			CapacityReservationId: aws.String("cr-billing-1"),
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(out.Return),
		"Return decoded false - pre-fix the response had no \"return\" member at all")
}

// TestDescribeCapacityBlockOfferings_UpfrontFee_RealClient covers gopherstack-6flj:
// capacityBlockOfferingItem emitted the price under "upfrontPrice", but the real
// CapacityBlockOffering deserializer (ec2@v1.319.1 deserializers.go:
// awsEc2query_deserializeDocumentCapacityBlockOffering) reads "upfrontFee". The
// unrelated Host Reservation family legitimately uses "upfrontPrice" for its own,
// differently-named real field, which is what made this sibling trap invisible.
func TestDescribeCapacityBlockOfferings_UpfrontFee_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	out, err := client.DescribeCapacityBlockOfferings(ctx, &ec2sdk.DescribeCapacityBlockOfferingsInput{
		InstanceType:          aws.String("p4d.24xlarge"),
		CapacityDurationHours: aws.Int32(24),
		InstanceCount:         aws.Int32(1),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.CapacityBlockOfferings)
	assert.NotEmpty(t, aws.ToString(out.CapacityBlockOfferings[0].UpfrontFee),
		"UpfrontFee decoded empty - pre-fix the wire key was \"upfrontPrice\", which doesn't "+
			"exist on this shape")
}

// TestCreateCapacityReservationFleet_ReservationList_RealClient covers
// gopherstack-6flj: CreateCapacityReservationFleet shared capacityReservationFleetItem's
// "instanceTypeSpecificationSet" tag for its constituent-reservation list, but the real
// CreateCapacityReservationFleetOutput deserializer (ec2@v1.319.1 deserializers.go:
// awsEc2query_deserializeOpDocumentCreateCapacityReservationFleetOutput) reads
// "fleetCapacityReservationSet" for this op specifically -- a different name than the
// sibling CapacityReservationFleet type used by DescribeCapacityReservationFleets,
// which genuinely does use "instanceTypeSpecificationSet"
// (awsEc2query_deserializeDocumentCapacityReservationFleet). A real client's
// FleetCapacityReservations was always empty on the Create response even though the
// backend had just created one CapacityReservation per spec.
func TestCreateCapacityReservationFleet_ReservationList_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	out, err := client.CreateCapacityReservationFleet(ctx, &ec2sdk.CreateCapacityReservationFleetInput{
		TotalTargetCapacity: aws.Int32(4),
		InstanceTypeSpecifications: []types.ReservationFleetInstanceSpecification{
			{
				InstanceType:     types.InstanceTypeM5Large,
				InstancePlatform: types.CapacityReservationInstancePlatformLinuxUnix,
				AvailabilityZone: aws.String("us-east-1a"),
				Weight:           aws.Float64(1),
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.FleetCapacityReservations,
		"FleetCapacityReservations decoded empty - pre-fix the wire key was "+
			"\"instanceTypeSpecificationSet\", which this op's real output doesn't use")
	assert.NotEmpty(t, aws.ToString(out.FleetCapacityReservations[0].CapacityReservationId))
	assert.Equal(t, types.InstanceTypeM5Large, out.FleetCapacityReservations[0].InstanceType)

	// The sibling Describe op genuinely uses "instanceTypeSpecificationSet" and must
	// keep doing so.
	describeOut, err := client.DescribeCapacityReservationFleets(
		ctx, &ec2sdk.DescribeCapacityReservationFleetsInput{
			CapacityReservationFleetIds: []string{aws.ToString(out.CapacityReservationFleetId)},
		},
	)
	require.NoError(t, err)
	require.Len(t, describeOut.CapacityReservationFleets, 1)
	require.NotEmpty(t, describeOut.CapacityReservationFleets[0].InstanceTypeSpecifications)
	assert.NotEmpty(t,
		aws.ToString(describeOut.CapacityReservationFleets[0].InstanceTypeSpecifications[0].CapacityReservationId),
	)
}
