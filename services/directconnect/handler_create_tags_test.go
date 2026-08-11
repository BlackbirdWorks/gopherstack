package directconnect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	directconnectsdk "github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/aws/aws-sdk-go-v2/service/directconnect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateOps_TagsRoundTrip drives every Direct Connect Create op that
// accepts Tags in the real SDK (directconnect@v1.44.1: CreateConnection,
// CreateDirectConnectGateway, CreateInterconnect, CreateLag,
// CreatePrivateVirtualInterface, CreatePublicVirtualInterface,
// CreateTransitVirtualInterface) through a real SDK client and asserts
// DescribeTags sees what was supplied at creation (gopherstack-2mwl).
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tag := types.Tag{Key: aws.String("env"), Value: aws.String("prod")}

	requireTags := func(t *testing.T, client *directconnectsdk.Client, resourceARN string) {
		t.Helper()

		out, err := client.DescribeTags(t.Context(), &directconnectsdk.DescribeTagsInput{
			ResourceArns: []string{resourceARN},
		})
		require.NoError(t, err)
		require.Len(t, out.ResourceTags, 1)
		require.Len(t, out.ResourceTags[0].Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.ResourceTags[0].Tags[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.ResourceTags[0].Tags[0].Value))
	}

	t.Run("createconnection", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClient(t)

		out, err := client.CreateConnection(t.Context(), &directconnectsdk.CreateConnectionInput{
			Bandwidth:      aws.String("1Gbps"),
			ConnectionName: aws.String("tagged-conn"),
			Location:       aws.String("EqDC2"),
			Tags:           []types.Tag{tag},
		})
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)

		arn := "arn:aws:directconnect:us-east-1:000000000000:dxcon/" + aws.ToString(
			out.ConnectionId,
		)
		requireTags(t, client, arn)
	})

	t.Run("createdirectconnectgateway", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClient(t)

		out, err := client.CreateDirectConnectGateway(
			t.Context(),
			&directconnectsdk.CreateDirectConnectGatewayInput{
				DirectConnectGatewayName: aws.String("tagged-gw"),
				Tags:                     []types.Tag{tag},
			},
		)
		require.NoError(t, err)

		arn := "arn:aws:directconnect:us-east-1:000000000000:dx-gateway/" +
			aws.ToString(out.DirectConnectGateway.DirectConnectGatewayId)
		requireTags(t, client, arn)
	})

	t.Run("createinterconnect", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClient(t)

		out, err := client.CreateInterconnect(
			t.Context(),
			&directconnectsdk.CreateInterconnectInput{
				Bandwidth:        aws.String("1Gbps"),
				InterconnectName: aws.String("tagged-interconnect"),
				Location:         aws.String("EqDC2"),
				Tags:             []types.Tag{tag},
			},
		)
		require.NoError(t, err)

		arn := "arn:aws:directconnect:us-east-1:000000000000:dxcon/" + aws.ToString(
			out.InterconnectId,
		)
		requireTags(t, client, arn)
	})

	t.Run("createlag", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClient(t)

		out, err := client.CreateLag(t.Context(), &directconnectsdk.CreateLagInput{
			ConnectionsBandwidth: aws.String("1Gbps"),
			LagName:              aws.String("tagged-lag"),
			Location:             aws.String("EqDC2"),
			NumberOfConnections:  1,
			Tags:                 []types.Tag{tag},
		})
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)

		arn := "arn:aws:directconnect:us-east-1:000000000000:dxlag/" + aws.ToString(out.LagId)
		requireTags(t, client, arn)
	})

	t.Run("createprivatevirtualinterface", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClient(t)

		conn := createTestConnection(t, client)

		out, err := client.CreatePrivateVirtualInterface(
			t.Context(),
			&directconnectsdk.CreatePrivateVirtualInterfaceInput{
				ConnectionId: conn.ConnectionId,
				NewPrivateVirtualInterface: &types.NewPrivateVirtualInterface{
					VirtualInterfaceName: aws.String("tagged-private-vif"),
					Vlan:                 101,
					VirtualGatewayId:     aws.String("vgw-tagged"),
					Tags:                 []types.Tag{tag},
				},
			},
		)
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)

		arn := "arn:aws:directconnect:us-east-1:000000000000:dxvif/" + aws.ToString(
			out.VirtualInterfaceId,
		)
		requireTags(t, client, arn)
	})

	t.Run("createpublicvirtualinterface", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClient(t)

		conn := createTestConnection(t, client)

		out, err := client.CreatePublicVirtualInterface(
			t.Context(),
			&directconnectsdk.CreatePublicVirtualInterfaceInput{
				ConnectionId: conn.ConnectionId,
				NewPublicVirtualInterface: &types.NewPublicVirtualInterface{
					VirtualInterfaceName: aws.String("tagged-public-vif"),
					Vlan:                 102,
					Tags:                 []types.Tag{tag},
				},
			},
		)
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)

		arn := "arn:aws:directconnect:us-east-1:000000000000:dxvif/" + aws.ToString(
			out.VirtualInterfaceId,
		)
		requireTags(t, client, arn)
	})

	t.Run("createtransitvirtualinterface", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClient(t)

		conn := createTestConnection(t, client)

		gw, err := client.CreateDirectConnectGateway(
			t.Context(),
			&directconnectsdk.CreateDirectConnectGatewayInput{
				DirectConnectGatewayName: aws.String("transit-vif-gw"),
			},
		)
		require.NoError(t, err)

		out, err := client.CreateTransitVirtualInterface(
			t.Context(),
			&directconnectsdk.CreateTransitVirtualInterfaceInput{
				ConnectionId: conn.ConnectionId,
				NewTransitVirtualInterface: &types.NewTransitVirtualInterface{
					VirtualInterfaceName:   aws.String("tagged-transit-vif"),
					Vlan:                   103,
					DirectConnectGatewayId: gw.DirectConnectGateway.DirectConnectGatewayId,
					Tags:                   []types.Tag{tag},
				},
			},
		)
		require.NoError(t, err)
		require.Len(t, out.VirtualInterface.Tags, 1)

		arn := "arn:aws:directconnect:us-east-1:000000000000:dxvif/" +
			aws.ToString(out.VirtualInterface.VirtualInterfaceId)
		requireTags(t, client, arn)
	})
}
