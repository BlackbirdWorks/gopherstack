package ec2_test

// Fix for gopherstack-6cuc: CreateTransitGatewayVpcAttachment's backend
// method already took a subnetIDs []string parameter and discarded it (read
// as `_ []string`), so SubnetIds sent on create never reached stored state.

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateTransitGatewayVpcAttachment_SubnetIds_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)
	tgwID := createTestTGW(t, client)

	created, err := client.CreateTransitGatewayVpcAttachment(
		t.Context(), &ec2sdk.CreateTransitGatewayVpcAttachmentInput{
			TransitGatewayId: aws.String(tgwID),
			VpcId:            aws.String("vpc-6cuc"),
			SubnetIds:        []string{"subnet-6cuc-a", "subnet-6cuc-b"},
		},
	)
	require.NoError(t, err)
	assert.ElementsMatch(
		t, []string{"subnet-6cuc-a", "subnet-6cuc-b"}, created.TransitGatewayVpcAttachment.SubnetIds,
		"SubnetIds empty on create response - accepted but never applied",
	)

	attID := created.TransitGatewayVpcAttachment.TransitGatewayAttachmentId

	out, err := client.DescribeTransitGatewayVpcAttachments(
		t.Context(), &ec2sdk.DescribeTransitGatewayVpcAttachmentsInput{
			TransitGatewayAttachmentIds: []string{aws.ToString(attID)},
		},
	)
	require.NoError(t, err)
	require.Len(t, out.TransitGatewayVpcAttachments, 1)
	assert.ElementsMatch(
		t, []string{"subnet-6cuc-a", "subnet-6cuc-b"}, out.TransitGatewayVpcAttachments[0].SubnetIds,
		"SubnetIds empty on describe - SubnetIds accepted at create but dropped from stored state",
	)
}
