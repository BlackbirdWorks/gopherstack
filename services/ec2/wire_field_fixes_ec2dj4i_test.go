package ec2_test

// Fixes for gopherstack-dj4i: the pinned SDK (ec2@v1.319.1 serializers.go)
// has exactly five ops that serialize tags as the plural TagSpecifications.N
// rather than the near-universal singular TagSpecification.N:
// CreateTransitGatewayMeteringPolicy (fixed separately), CreateCapacityReservation,
// CreateTransitGatewayPolicyTable, CreateTransitGatewayRouteTable, and
// CreateTransitGatewayVpcAttachment. None of the latter four parsed tags at
// all (not even with the singular helper), so a tag sent at creation was
// accepted and silently dropped.

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateCapacityReservation_Tags_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	created, err := client.CreateCapacityReservation(
		t.Context(), &ec2sdk.CreateCapacityReservationInput{
			InstanceType:     aws.String("m5.large"),
			InstancePlatform: types.CapacityReservationInstancePlatformLinuxUnix,
			AvailabilityZone: aws.String("us-east-1a"),
			InstanceCount:    aws.Int32(1),
			TagSpecifications: []types.TagSpecification{{
				ResourceType: types.ResourceTypeCapacityReservation,
				Tags:         []types.Tag{{Key: aws.String("Team"), Value: aws.String("dj4i")}},
			}},
		},
	)
	require.NoError(t, err)
	require.NotEmpty(
		t, created.CapacityReservation.Tags,
		"Tags empty on create response - TagSpecifications accepted but never applied",
	)

	crID := created.CapacityReservation.CapacityReservationId

	out, err := client.DescribeCapacityReservations(
		t.Context(), &ec2sdk.DescribeCapacityReservationsInput{
			CapacityReservationIds: []string{aws.ToString(crID)},
		},
	)
	require.NoError(t, err)
	require.Len(t, out.CapacityReservations, 1)
	require.NotEmpty(
		t, out.CapacityReservations[0].Tags,
		"Tags empty on describe - TagSpecifications accepted at create but dropped from Describe",
	)
	assert.Equal(t, "Team", aws.ToString(out.CapacityReservations[0].Tags[0].Key))
	assert.Equal(t, "dj4i", aws.ToString(out.CapacityReservations[0].Tags[0].Value))
}

func TestCreateTransitGatewayPolicyTable_Tags_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)
	tgwID := createTestTGW(t, client)

	created, err := client.CreateTransitGatewayPolicyTable(
		t.Context(), &ec2sdk.CreateTransitGatewayPolicyTableInput{
			TransitGatewayId: aws.String(tgwID),
			TagSpecifications: []types.TagSpecification{{
				ResourceType: types.ResourceTypeTransitGatewayPolicyTable,
				Tags:         []types.Tag{{Key: aws.String("Team"), Value: aws.String("dj4i")}},
			}},
		},
	)
	require.NoError(t, err)
	require.NotEmpty(
		t, created.TransitGatewayPolicyTable.Tags,
		"Tags empty on create response - TagSpecifications accepted but never applied",
	)

	ptID := created.TransitGatewayPolicyTable.TransitGatewayPolicyTableId

	out, err := client.DescribeTransitGatewayPolicyTables(
		t.Context(), &ec2sdk.DescribeTransitGatewayPolicyTablesInput{
			TransitGatewayPolicyTableIds: []string{aws.ToString(ptID)},
		},
	)
	require.NoError(t, err)
	require.Len(t, out.TransitGatewayPolicyTables, 1)
	require.NotEmpty(
		t, out.TransitGatewayPolicyTables[0].Tags,
		"Tags empty on describe - TagSpecifications accepted at create but dropped from Describe",
	)
	assert.Equal(t, "Team", aws.ToString(out.TransitGatewayPolicyTables[0].Tags[0].Key))
	assert.Equal(t, "dj4i", aws.ToString(out.TransitGatewayPolicyTables[0].Tags[0].Value))
}

func TestCreateTransitGatewayRouteTable_Tags_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)
	tgwID := createTestTGW(t, client)

	created, err := client.CreateTransitGatewayRouteTable(
		t.Context(), &ec2sdk.CreateTransitGatewayRouteTableInput{
			TransitGatewayId: aws.String(tgwID),
			TagSpecifications: []types.TagSpecification{{
				ResourceType: types.ResourceTypeTransitGatewayRouteTable,
				Tags:         []types.Tag{{Key: aws.String("Team"), Value: aws.String("dj4i")}},
			}},
		},
	)
	require.NoError(t, err)
	require.NotEmpty(
		t, created.TransitGatewayRouteTable.Tags,
		"Tags empty on create response - TagSpecifications accepted but never applied",
	)

	rtID := created.TransitGatewayRouteTable.TransitGatewayRouteTableId

	out, err := client.DescribeTransitGatewayRouteTables(
		t.Context(), &ec2sdk.DescribeTransitGatewayRouteTablesInput{
			TransitGatewayRouteTableIds: []string{aws.ToString(rtID)},
		},
	)
	require.NoError(t, err)
	require.Len(t, out.TransitGatewayRouteTables, 1)
	require.NotEmpty(
		t, out.TransitGatewayRouteTables[0].Tags,
		"Tags empty on describe - TagSpecifications accepted at create but dropped from Describe",
	)
	assert.Equal(t, "Team", aws.ToString(out.TransitGatewayRouteTables[0].Tags[0].Key))
	assert.Equal(t, "dj4i", aws.ToString(out.TransitGatewayRouteTables[0].Tags[0].Value))
}

func TestCreateTransitGatewayVpcAttachment_Tags_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)
	tgwID := createTestTGW(t, client)

	created, err := client.CreateTransitGatewayVpcAttachment(
		t.Context(), &ec2sdk.CreateTransitGatewayVpcAttachmentInput{
			TransitGatewayId: aws.String(tgwID),
			VpcId:            aws.String("vpc-dj4i"),
			SubnetIds:        []string{"subnet-dj4i"},
			TagSpecifications: []types.TagSpecification{{
				ResourceType: types.ResourceTypeTransitGatewayAttachment,
				Tags:         []types.Tag{{Key: aws.String("Team"), Value: aws.String("dj4i")}},
			}},
		},
	)
	require.NoError(t, err)
	require.NotEmpty(
		t, created.TransitGatewayVpcAttachment.Tags,
		"Tags empty on create response - TagSpecifications accepted but never applied",
	)

	attID := created.TransitGatewayVpcAttachment.TransitGatewayAttachmentId

	out, err := client.DescribeTransitGatewayVpcAttachments(
		t.Context(), &ec2sdk.DescribeTransitGatewayVpcAttachmentsInput{
			TransitGatewayAttachmentIds: []string{aws.ToString(attID)},
		},
	)
	require.NoError(t, err)
	require.Len(t, out.TransitGatewayVpcAttachments, 1)
	require.NotEmpty(
		t, out.TransitGatewayVpcAttachments[0].Tags,
		"Tags empty on describe - TagSpecifications accepted at create but dropped from Describe",
	)
	assert.Equal(t, "Team", aws.ToString(out.TransitGatewayVpcAttachments[0].Tags[0].Key))
	assert.Equal(t, "dj4i", aws.ToString(out.TransitGatewayVpcAttachments[0].Tags[0].Value))
}
