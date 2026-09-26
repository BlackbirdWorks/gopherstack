package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_EC2VPNTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testEC2VPNGatewayAndConnection, "vpn_gateway_and_connection"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testEC2VPNGatewayAndConnection(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "VGW": {
    "Type": "AWS::EC2::VPNGateway",
    "Properties": {"Type": "ipsec.1"}
  },
  "CGW": {
    "Type": "AWS::EC2::CustomerGateway",
    "Properties": {"Type": "ipsec.1", "IpAddress": "203.0.113.1", "BgpAsn": "65000"}
  },
  "Conn": {
    "Type": "AWS::EC2::VPNConnection",
    "Properties": {
      "Type": "ipsec.1",
      "CustomerGatewayId": {"Ref": "CGW"},
      "VpnGatewayId": {"Ref": "VGW"}
    }
  }
},
"Outputs": {
  "VGWRef": {"Value": {"Ref": "VGW"}},
  "VGWId": {"Value": {"Fn::GetAtt": ["VGW", "VPNGatewayId"]}},
  "ConnRef": {"Value": {"Ref": "Conn"}},
  "ConnId": {"Value": {"Fn::GetAtt": ["Conn", "VpnConnectionId"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-vpn-stack", tmpl)
	assert.Equal(t, outputs["VGWRef"], outputs["VGWId"])
	assert.Contains(t, outputs["VGWRef"], "vgw-")
	assert.Equal(t, outputs["ConnRef"], outputs["ConnId"])
	assert.Contains(t, outputs["ConnRef"], "vpn-")

	require.Len(t, backends.EC2.Backend.DescribeVpnGateways([]string{outputs["VGWRef"]}), 1)
	require.Len(t, backends.EC2.Backend.DescribeVpnConnections([]string{outputs["ConnRef"]}), 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-vpn-stack")})
	require.NoError(t, err)

	assert.Empty(t, backends.EC2.Backend.DescribeVpnGateways([]string{outputs["VGWRef"]}))
	// DeleteVpnConnection keeps a tombstone reachable by explicit ID; an
	// unfiltered Describe never surfaces it (see DescribeVpnConnections).
	found := false

	for _, c := range backends.EC2.Backend.DescribeVpnConnections(nil) {
		if c.VpnConnectionID == outputs["ConnRef"] {
			found = true
		}
	}

	assert.False(t, found, "deleted VPN connection must not appear in an unfiltered Describe")
}
