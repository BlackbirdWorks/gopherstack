package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_EC2RouteServerTypes(t *testing.T) {
	t.Parallel()
	t.Run("server_endpoint_peer", testEC2RouteServerChain)
}

func testEC2RouteServerChain(t *testing.T) {
	t.Parallel()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "VPC": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}},
  "Subnet": {"Type": "AWS::EC2::Subnet",
    "Properties": {"VpcId": {"Ref": "VPC"}, "CidrBlock": "10.0.1.0/24"}},
  "RS": {
    "Type": "AWS::EC2::RouteServer",
    "Properties": {"AmazonSideAsn": 65000, "PersistRoutesState": "enabled"}
  },
  "Endpoint": {
    "Type": "AWS::EC2::RouteServerEndpoint",
    "Properties": {"RouteServerId": {"Ref": "RS"}, "SubnetId": {"Ref": "Subnet"}}
  },
  "Peer": {
    "Type": "AWS::EC2::RouteServerPeer",
    "Properties": {
      "RouteServerEndpointId": {"Ref": "Endpoint"},
      "PeerAddress": "10.0.1.100",
      "BgpOptions": {"PeerAsn": 65001}
    }
  }
},
"Outputs": {
  "RSRef": {"Value": {"Ref": "RS"}},
  "RSArn": {"Value": {"Fn::GetAtt": ["RS", "Arn"]}},
  "EndpointRef": {"Value": {"Ref": "Endpoint"}},
  "EndpointVpcId": {"Value": {"Fn::GetAtt": ["Endpoint", "VpcId"]}},
  "PeerRef": {"Value": {"Ref": "Peer"}},
  "PeerRouteServerId": {"Value": {"Fn::GetAtt": ["Peer", "RouteServerId"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-rs-stack", tmpl)
	assert.Contains(t, outputs["RSRef"], "rs-")
	assert.Contains(t, outputs["RSArn"], "route-server/"+outputs["RSRef"])
	assert.NotEmpty(t, outputs["EndpointVpcId"])
	assert.Equal(t, outputs["RSRef"], outputs["PeerRouteServerId"])

	require.Len(t, backends.EC2.Backend.DescribeRouteServers([]string{outputs["RSRef"]}), 1)
	require.Len(t, backends.EC2.Backend.DescribeRouteServerEndpoints([]string{outputs["EndpointRef"]}), 1)
	require.Len(t, backends.EC2.Backend.DescribeRouteServerPeers([]string{outputs["PeerRef"]}), 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-rs-stack")})
	require.NoError(t, err)

	assert.Empty(t, backends.EC2.Backend.DescribeRouteServerPeers([]string{outputs["PeerRef"]}))
	assert.Empty(t, backends.EC2.Backend.DescribeRouteServerEndpoints([]string{outputs["EndpointRef"]}))
	assert.Empty(t, backends.EC2.Backend.DescribeRouteServers([]string{outputs["RSRef"]}))
}
