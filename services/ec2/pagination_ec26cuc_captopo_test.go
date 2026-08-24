package ec2_test

// DescribeCapacityReservationTopology defines MaxResults/NextToken on its
// real SDK input (api_op_DescribeCapacityReservationTopology.go) but the
// handler ignored both, always returning every entry in one page with no
// NextToken. Found during the gopherstack-6cuc pass while auditing
// handler_capacity_reservations.go.

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/require"
)

func TestDescribeCapacityReservationTopology_Pagination(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	for range 3 {
		_, err := client.CreateCapacityReservation(
			t.Context(), &ec2sdk.CreateCapacityReservationInput{
				InstanceType:     aws.String("m5.large"),
				InstancePlatform: types.CapacityReservationInstancePlatformLinuxUnix,
				AvailabilityZone: aws.String("us-east-1a"),
				InstanceCount:    aws.Int32(1),
			},
		)
		require.NoError(t, err)
	}

	seen := make(map[string]bool, 3)

	page1, err := client.DescribeCapacityReservationTopology(
		t.Context(), &ec2sdk.DescribeCapacityReservationTopologyInput{MaxResults: aws.Int32(1)},
	)
	require.NoError(t, err)
	require.Len(t, page1.CapacityReservations, 1, "MaxResults=1 ignored - all entries returned on page one")
	require.NotEmpty(t, page1.NextToken, "NextToken empty despite more entries remaining")

	for _, e := range page1.CapacityReservations {
		seen[*e.CapacityReservationId] = true
	}

	page2, err := client.DescribeCapacityReservationTopology(
		t.Context(), &ec2sdk.DescribeCapacityReservationTopologyInput{
			MaxResults: aws.Int32(1),
			NextToken:  page1.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.CapacityReservations, 1)

	for _, e := range page2.CapacityReservations {
		require.Falsef(
			t, seen[*e.CapacityReservationId],
			"entry %q returned on more than one page", *e.CapacityReservationId,
		)
		seen[*e.CapacityReservationId] = true
	}

	page3, err := client.DescribeCapacityReservationTopology(
		t.Context(), &ec2sdk.DescribeCapacityReservationTopologyInput{
			MaxResults: aws.Int32(1),
			NextToken:  page2.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, page3.CapacityReservations, 1)
	require.Empty(t, page3.NextToken)

	for _, e := range page3.CapacityReservations {
		require.Falsef(
			t, seen[*e.CapacityReservationId],
			"entry %q returned on more than one page", *e.CapacityReservationId,
		)
		seen[*e.CapacityReservationId] = true
	}

	require.Len(t, seen, 3)
}
