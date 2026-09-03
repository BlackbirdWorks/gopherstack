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

// TestDescribeReservedInstancesListings_ListingIdFilter_RealClient covers
// handleDescribeReservedInstancesListings. ReservedInstancesListingId is a
// scalar field serialized as the bare key "ReservedInstancesListingId"
// (ec2@v1.319.1 serializers.go:80264-80266,
// awsEc2query_serializeOpDocumentDescribeReservedInstancesListingsInput), not
// an indexed list. The handler read it via parseMemberList, which looks for
// "ReservedInstancesListingId.1", a key a real client's single-listing
// lookup never sends -- so the filter was always silently ignored and every
// call returned every listing.
func TestDescribeReservedInstancesListings_ListingIdFilter_RealClient(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	backend.SeedReservedInstancesOffering(
		"rio-sweep38-001", "t3.medium", "us-east-1a", "Linux/UNIX", "All Upfront", 94608000, 500.0, 0.0,
	)

	client := newTestEC2Client(t, ec2.NewHandler(backend))
	ctx := t.Context()

	riA, err := client.PurchaseReservedInstancesOffering(ctx, &ec2sdk.PurchaseReservedInstancesOfferingInput{
		ReservedInstancesOfferingId: aws.String("rio-sweep38-001"),
		InstanceCount:               aws.Int32(1),
	})
	require.NoError(t, err)

	riB, err := client.PurchaseReservedInstancesOffering(ctx, &ec2sdk.PurchaseReservedInstancesOfferingInput{
		ReservedInstancesOfferingId: aws.String("rio-sweep38-001"),
		InstanceCount:               aws.Int32(1),
	})
	require.NoError(t, err)

	priceSchedules := []types.PriceScheduleSpecification{{Term: aws.Int64(1), Price: aws.Float64(100.0)}}

	listingA, err := client.CreateReservedInstancesListing(ctx, &ec2sdk.CreateReservedInstancesListingInput{
		ClientToken:         aws.String("sweep38-a"),
		InstanceCount:       aws.Int32(1),
		ReservedInstancesId: riA.ReservedInstancesId,
		PriceSchedules:      priceSchedules,
	})
	require.NoError(t, err)
	require.Len(t, listingA.ReservedInstancesListings, 1)

	_, err = client.CreateReservedInstancesListing(ctx, &ec2sdk.CreateReservedInstancesListingInput{
		ClientToken:         aws.String("sweep38-b"),
		InstanceCount:       aws.Int32(1),
		ReservedInstancesId: riB.ReservedInstancesId,
		PriceSchedules:      priceSchedules,
	})
	require.NoError(t, err)

	targetID := listingA.ReservedInstancesListings[0].ReservedInstancesListingId

	out, err := client.DescribeReservedInstancesListings(ctx, &ec2sdk.DescribeReservedInstancesListingsInput{
		ReservedInstancesListingId: targetID,
	})
	require.NoError(t, err)
	require.Len(t, out.ReservedInstancesListings, 1,
		"ReservedInstancesListingId filter ignored - returned every listing")
	assert.Equal(t, aws.ToString(targetID), aws.ToString(out.ReservedInstancesListings[0].ReservedInstancesListingId))
}
