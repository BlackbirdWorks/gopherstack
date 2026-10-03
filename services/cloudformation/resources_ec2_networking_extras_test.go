package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_EC2NetworkingExtrasTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testEC2PlacementGroup, "placement_group"},
		{testEC2PrefixList, "prefix_list"},
		{testEC2VPCEndpointService, "vpc_endpoint_service"},
		{testEC2VerifiedAccessInstance, "verified_access_instance"},
		{testEC2NetworkInterfacePermission, "network_interface_permission"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testEC2PlacementGroup(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "PG": {"Type": "AWS::EC2::PlacementGroup", "Properties": {"GroupName": "unit-pg", "Strategy": "spread"}}
},
"Outputs": {
  "Ref": {"Value": {"Ref": "PG"}},
  "GroupName": {"Value": {"Fn::GetAtt": ["PG", "GroupName"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-pg-stack", tmpl)
	assert.Equal(t, "unit-pg", outputs["Ref"])
	assert.Equal(t, "unit-pg", outputs["GroupName"])
	require.Len(t, backends.EC2.Backend.DescribePlacementGroups([]string{"unit-pg"}), 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-pg-stack")})
	require.NoError(t, err)
	assert.Empty(t, backends.EC2.Backend.DescribePlacementGroups([]string{"unit-pg"}))
}

func testEC2PrefixList(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "PL": {
    "Type": "AWS::EC2::PrefixList",
    "Properties": {
      "PrefixListName": "unit-pl",
      "AddressFamily": "IPv4",
      "MaxEntries": 5,
      "Entries": [{"Cidr": "10.0.0.0/24", "Description": "office"}]
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "PL"}},
  "Arn": {"Value": {"Fn::GetAtt": ["PL", "Arn"]}},
  "OwnerId": {"Value": {"Fn::GetAtt": ["PL", "OwnerId"]}},
  "Version": {"Value": {"Fn::GetAtt": ["PL", "Version"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-pl-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "pl-")
	assert.Contains(t, outputs["Arn"], "prefix-list/"+outputs["Ref"])
	assert.Equal(t, "000000000000", outputs["OwnerId"])
	assert.Equal(t, "1", outputs["Version"])
	require.Len(t, backends.EC2.Backend.DescribeManagedPrefixLists([]string{outputs["Ref"]}), 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-pl-stack")})
	require.NoError(t, err)
	assert.Empty(t, backends.EC2.Backend.DescribeManagedPrefixLists([]string{outputs["Ref"]}))
}

func testEC2VPCEndpointService(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "Svc": {
    "Type": "AWS::EC2::VPCEndpointService",
    "Properties": {
      "AcceptanceRequired": false,
      "NetworkLoadBalancerArns": ["arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/net/nlb/abc"]
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Svc"}},
  "ServiceId": {"Value": {"Fn::GetAtt": ["Svc", "ServiceId"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-vpces-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["ServiceId"])
	assert.Contains(t, outputs["Ref"], "vpce-svc-")
	require.Len(t, backends.EC2.Backend.DescribeVpcEndpointServiceConfigurations([]string{outputs["Ref"]}), 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-vpces-stack")})
	require.NoError(t, err)
	assert.Empty(t, backends.EC2.Backend.DescribeVpcEndpointServiceConfigurations([]string{outputs["Ref"]}))
}

func testEC2VerifiedAccessInstance(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "VAI": {"Type": "AWS::EC2::VerifiedAccessInstance", "Properties": {"Description": "unit test instance"}}
},
"Outputs": {
  "Ref": {"Value": {"Ref": "VAI"}},
  "Id": {"Value": {"Fn::GetAtt": ["VAI", "VerifiedAccessInstanceId"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-vai-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Id"])
	assert.Contains(t, outputs["Ref"], "vai-")
	require.Len(t, backends.EC2.Backend.DescribeVerifiedAccessInstances([]string{outputs["Ref"]}), 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-vai-stack")})
	require.NoError(t, err)
	assert.Empty(t, backends.EC2.Backend.DescribeVerifiedAccessInstances([]string{outputs["Ref"]}))
}

func testEC2NetworkInterfacePermission(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "VPC": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}},
  "Subnet": {"Type": "AWS::EC2::Subnet",
    "Properties": {"VpcId": {"Ref": "VPC"}, "CidrBlock": "10.0.1.0/24"}},
  "ENI": {"Type": "AWS::EC2::NetworkInterface", "Properties": {"SubnetId": {"Ref": "Subnet"}}},
  "Perm": {
    "Type": "AWS::EC2::NetworkInterfacePermission",
    "Properties": {
      "NetworkInterfaceId": {"Ref": "ENI"},
      "AwsAccountId": "111111111111",
      "AwsService": "ec2.amazonaws.com",
      "Permission": "INSTANCE-ATTACH"
    }
  }
},
"Outputs": {
  "ENIRef": {"Value": {"Ref": "ENI"}},
  "PermRef": {"Value": {"Ref": "Perm"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-enip-stack", tmpl)
	assert.Contains(t, outputs["PermRef"], "eni-perm-")
	require.Len(t, backends.EC2.Backend.DescribeNetworkInterfacePermissions([]string{outputs["ENIRef"]}), 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-enip-stack")})
	require.NoError(t, err)
	assert.Empty(t, backends.EC2.Backend.DescribeNetworkInterfacePermissions([]string{outputs["ENIRef"]}))
}
