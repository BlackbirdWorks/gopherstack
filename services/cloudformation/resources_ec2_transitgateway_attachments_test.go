package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_EC2TransitGatewayMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testEC2TGWVpcAttachment, "vpc_attachment"},
		{testEC2TGWPeeringAttachment, "peering_attachment"},
		{testEC2TGWMulticastDomain, "multicast_domain"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testEC2TGWVpcAttachment(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "VPC": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}},
  "Subnet": {"Type": "AWS::EC2::Subnet",
    "Properties": {"VpcId": {"Ref": "VPC"}, "CidrBlock": "10.0.1.0/24"}},
  "TGW": {"Type": "AWS::EC2::TransitGateway", "Properties": {"Description": "test tgw"}},
  "Att": {
    "Type": "AWS::EC2::TransitGatewayVpcAttachment",
    "Properties": {"TransitGatewayId": {"Ref": "TGW"}, "VpcId": {"Ref": "VPC"}, "SubnetIds": [{"Ref": "Subnet"}]}
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Att"}},
  "Id": {"Value": {"Fn::GetAtt": ["Att", "Id"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-tgwvpcatt-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Id"])
	require.Len(t, backends.EC2.Backend.DescribeTransitGatewayVpcAttachments([]string{outputs["Ref"]}), 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-tgwvpcatt-stack")})
	require.NoError(t, err)

	found := false

	for _, a := range backends.EC2.Backend.DescribeTransitGatewayVpcAttachments(nil) {
		if a.TransitGatewayAttachmentID == outputs["Ref"] {
			found = true
		}
	}

	assert.False(t, found, "deleted TGW VPC attachment must not appear in an unfiltered Describe")
}

func testEC2TGWPeeringAttachment(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "TGW": {"Type": "AWS::EC2::TransitGateway", "Properties": {"Description": "test tgw"}},
  "Peer": {
    "Type": "AWS::EC2::TransitGatewayPeeringAttachment",
    "Properties": {
      "TransitGatewayId": {"Ref": "TGW"},
      "PeerTransitGatewayId": "tgw-peer12345",
      "PeerAccountId": "222222222222",
      "PeerRegion": "us-west-2"
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Peer"}},
  "Id": {"Value": {"Fn::GetAtt": ["Peer", "TransitGatewayAttachmentId"]}},
  "State": {"Value": {"Fn::GetAtt": ["Peer", "State"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-tgwpeer-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Id"])
	assert.Equal(t, "pendingAcceptance", outputs["State"])
	require.Len(t, backends.EC2.Backend.DescribeTransitGatewayPeeringAttachments([]string{outputs["Ref"]}), 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-tgwpeer-stack")})
	require.NoError(t, err)

	found := false

	for _, a := range backends.EC2.Backend.DescribeTransitGatewayPeeringAttachments(nil) {
		if a.TransitGatewayAttachmentID == outputs["Ref"] {
			found = true
		}
	}

	assert.False(t, found, "deleted TGW peering attachment must not appear in an unfiltered Describe")
}

func testEC2TGWMulticastDomain(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "TGW": {"Type": "AWS::EC2::TransitGateway", "Properties": {"Description": "test tgw"}},
  "Domain": {
    "Type": "AWS::EC2::TransitGatewayMulticastDomain",
    "Properties": {"TransitGatewayId": {"Ref": "TGW"}, "Options": {"Igmpv2Support": "enable"}}
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Domain"}},
  "Id": {"Value": {"Fn::GetAtt": ["Domain", "TransitGatewayMulticastDomainId"]}},
  "Arn": {"Value": {"Fn::GetAtt": ["Domain", "TransitGatewayMulticastDomainArn"]}},
  "State": {"Value": {"Fn::GetAtt": ["Domain", "State"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-tgwmcast-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Id"])
	assert.Contains(t, outputs["Arn"], "transit-gateway-multicast-domain/"+outputs["Ref"])
	assert.NotEmpty(t, outputs["State"])
	require.Len(t, backends.EC2.Backend.DescribeTransitGatewayMulticastDomains([]string{outputs["Ref"]}), 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-tgwmcast-stack")})
	require.NoError(t, err)
	assert.Empty(t, backends.EC2.Backend.DescribeTransitGatewayMulticastDomains([]string{outputs["Ref"]}))
}
