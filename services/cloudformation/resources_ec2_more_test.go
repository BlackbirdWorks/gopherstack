package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_EC2DHCPOptionsAndVPCAssociation(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "VPC": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}},
  "Opts": {
    "Type": "AWS::EC2::DHCPOptions",
    "Properties": {"DomainNameServers": ["AmazonProvidedDNS"],
      "DhcpConfigurations": [{"Key": "domain-name-servers", "Values": ["AmazonProvidedDNS"]}]}
  },
  "Assoc": {
    "Type": "AWS::EC2::VPCDHCPOptionsAssociation",
    "Properties": {"VpcId": {"Ref": "VPC"}, "DhcpOptionsId": {"Ref": "Opts"}}
  }
},
"Outputs": {"OptsId": {"Value": {"Ref": "Opts"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-dhcp-stack", tmpl)
	optsID := outputs["OptsId"]
	require.NotEmpty(t, optsID)

	list, err := backends.EC2.Backend.DescribeDhcpOptions([]string{optsID})
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "domain-name-servers", list[0].Configurations[0].Key)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-dhcp-stack")})
	require.NoError(t, err)

	_, err = backends.EC2.Backend.DescribeDhcpOptions([]string{optsID})
	require.Error(t, err)
}

func TestCreateStack_EC2EgressOnlyInternetGateway(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "VPC": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}},
  "EOIGW": {"Type": "AWS::EC2::EgressOnlyInternetGateway", "Properties": {"VpcId": {"Ref": "VPC"}}}
},
"Outputs": {"Id": {"Value": {"Ref": "EOIGW"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-eoigw-stack", tmpl)
	id := outputs["Id"]
	require.NotEmpty(t, id)

	got := backends.EC2.Backend.DescribeEgressOnlyInternetGateways([]string{id})
	require.Len(t, got, 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-eoigw-stack")})
	require.NoError(t, err)

	assert.Empty(t, backends.EC2.Backend.DescribeEgressOnlyInternetGateways([]string{id}))
}

func TestCreateStack_EC2CustomerGateway(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "CGW": {"Type": "AWS::EC2::CustomerGateway",
    "Properties": {"Type": "ipsec.1", "BgpAsn": 65000, "IpAddress": "203.0.113.1",
      "Tags": [{"Key": "Name", "Value": "my-cgw"}]}}
},
"Outputs": {"Id": {"Value": {"Ref": "CGW"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-cgw-stack", tmpl)
	id := outputs["Id"]
	require.NotEmpty(t, id)

	list, err := backends.EC2.Backend.DescribeCustomerGateways([]string{id})
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "203.0.113.1", list[0].IPAddress)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-cgw-stack")})
	require.NoError(t, err)

	_, err = backends.EC2.Backend.DescribeCustomerGateways([]string{id})
	require.Error(t, err)
}

func TestCreateStack_EC2CarrierGateway(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "VPC": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}},
  "CAGW": {"Type": "AWS::EC2::CarrierGateway", "Properties": {"VpcId": {"Ref": "VPC"}}}
},
"Outputs": {"Id": {"Value": {"Ref": "CAGW"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-cagw-stack", tmpl)
	id := outputs["Id"]
	require.NotEmpty(t, id)

	require.Len(t, backends.EC2.Backend.DescribeCarrierGateways([]string{id}), 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-cagw-stack")})
	require.NoError(t, err)

	assert.Empty(t, backends.EC2.Backend.DescribeCarrierGateways([]string{id}))
}

func TestCreateStack_EC2EIPAssociation(t *testing.T) {
	t.Parallel()

	_, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Inst": {"Type": "AWS::EC2::Instance",
    "Properties": {"ImageId": "ami-12345678", "InstanceType": "t3.micro"}},
  "EIP": {"Type": "AWS::EC2::EIP", "Properties": {"Domain": "vpc"}},
  "Assoc": {"Type": "AWS::EC2::EIPAssociation",
    "Properties": {"AllocationId": {"Ref": "EIP"}, "InstanceId": {"Ref": "Inst"}}}
},
"Outputs": {"Id": {"Value": {"Ref": "Assoc"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-eipassoc-stack", tmpl)
	require.NotEmpty(t, outputs["Id"])

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-eipassoc-stack")})
	require.NoError(t, err)
}

func TestCreateStack_EC2InstanceConnectEndpoint(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "VPC": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}},
  "Subnet": {"Type": "AWS::EC2::Subnet",
    "Properties": {"VpcId": {"Ref": "VPC"}, "CidrBlock": "10.0.1.0/24"}},
  "ICE": {"Type": "AWS::EC2::InstanceConnectEndpoint",
    "Properties": {"SubnetId": {"Ref": "Subnet"}, "PreserveClientIp": true}}
},
"Outputs": {"Id": {"Value": {"Ref": "ICE"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-ice-stack", tmpl)
	id := outputs["Id"]
	require.NotEmpty(t, id)

	got := backends.EC2.Backend.DescribeInstanceConnectEndpoints([]string{id})
	require.Len(t, got, 1)
	assert.True(t, got[0].PreserveClientIP)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-ice-stack")})
	require.NoError(t, err)

	assert.Empty(t, backends.EC2.Backend.DescribeInstanceConnectEndpoints([]string{id}))
}

func TestCreateStack_EC2ClientVpnFamily(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "VPC": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}},
  "Subnet": {"Type": "AWS::EC2::Subnet",
    "Properties": {"VpcId": {"Ref": "VPC"}, "CidrBlock": "10.0.1.0/24"}},
  "Endpoint": {"Type": "AWS::EC2::ClientVpnEndpoint",
    "Properties": {"ClientCidrBlock": "192.168.0.0/22", "Description": "test vpn",
      "TransportProtocol": "udp", "ServerCertificateArn": "arn:aws:acm:us-east-1:000000000000:certificate/abc",
      "AuthenticationOptions": [{"Type": "certificate-authentication"}],
      "ConnectionLogOptions": {"Enabled": false}}},
  "Assoc": {"Type": "AWS::EC2::ClientVpnTargetNetworkAssociation",
    "Properties": {"ClientVpnEndpointId": {"Ref": "Endpoint"}, "SubnetId": {"Ref": "Subnet"}}},
  "AuthRule": {"Type": "AWS::EC2::ClientVpnAuthorizationRule",
    "Properties": {"ClientVpnEndpointId": {"Ref": "Endpoint"}, "TargetNetworkCidr": "10.0.0.0/16",
      "AuthorizeAllGroups": true}},
  "Route": {"Type": "AWS::EC2::ClientVpnRoute",
    "Properties": {"ClientVpnEndpointId": {"Ref": "Endpoint"}, "DestinationCidrBlock": "0.0.0.0/0",
      "TargetVpcSubnetId": {"Ref": "Subnet"}}}
},
"Outputs": {"EndpointId": {"Value": {"Ref": "Endpoint"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-cvpn-stack", tmpl)
	endpointID := outputs["EndpointId"]
	require.NotEmpty(t, endpointID)

	eps := backends.EC2.Backend.DescribeClientVpnEndpoints([]string{endpointID})
	require.Len(t, eps, 1)
	assert.Len(t, eps[0].TargetNetworks, 1)
	assert.Len(t, eps[0].AuthRules, 1)
	assert.Len(t, eps[0].Routes, 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-cvpn-stack")})
	require.NoError(t, err)

	assert.Empty(t, backends.EC2.Backend.DescribeClientVpnEndpoints([]string{endpointID}))
}

func TestCreateStack_EC2IpamFamily(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Ipam": {"Type": "AWS::EC2::IPAM", "Properties": {"Description": "test ipam"}},
  "Scope": {"Type": "AWS::EC2::IPAMScope",
    "Properties": {"IpamId": {"Ref": "Ipam"}, "Description": "extra scope"}},
  "Pool": {"Type": "AWS::EC2::IPAMPool",
    "Properties": {"IpamScopeId": {"Ref": "Scope"}, "AddressFamily": "ipv4",
      "Locale": "us-east-1"}},
  "PoolCidr": {"Type": "AWS::EC2::IPAMPoolCidr",
    "Properties": {"IpamPoolId": {"Ref": "Pool"}, "Cidr": "10.1.0.0/16"}}
},
"Outputs": {
  "IpamId": {"Value": {"Ref": "Ipam"}},
  "ScopeId": {"Value": {"Ref": "Scope"}},
  "PoolId": {"Value": {"Ref": "Pool"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-ipam-stack", tmpl)
	ipamID := outputs["IpamId"]
	poolID := outputs["PoolId"]
	require.NotEmpty(t, ipamID)
	require.NotEmpty(t, outputs["ScopeId"])
	require.NotEmpty(t, poolID)

	pools := backends.EC2.Backend.DescribeIpamPools([]string{poolID})
	require.Len(t, pools, 1)
	assert.Equal(t, "ipv4", pools[0].AddressFamily)
	assert.Equal(t, ipamID, pools[0].IpamID)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-ipam-stack")})
	require.NoError(t, err)

	assert.Empty(t, backends.EC2.Backend.DescribeIpams([]string{ipamID}))
}

func TestCreateStack_EC2CapacityReservationAndHost(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "CR": {"Type": "AWS::EC2::CapacityReservation",
    "Properties": {"InstanceType": "t3.micro", "AvailabilityZone": "us-east-1a",
      "InstanceCount": 2, "InstancePlatform": "Linux/UNIX"}},
  "Host": {"Type": "AWS::EC2::Host",
    "Properties": {"AvailabilityZone": "us-east-1a", "InstanceType": "m5.large"}}
},
"Outputs": {
  "CRId": {"Value": {"Ref": "CR"}},
  "HostId": {"Value": {"Ref": "Host"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ec2-cr-host-stack", tmpl)
	crID := outputs["CRId"]
	hostID := outputs["HostId"]
	require.NotEmpty(t, crID)
	require.NotEmpty(t, hostID)

	crs := backends.EC2.Backend.DescribeCapacityReservations([]string{crID})
	require.Len(t, crs, 1)
	assert.Equal(t, 2, crs[0].TotalInstanceCount)

	hosts := backends.EC2.Backend.DescribeHosts([]string{hostID})
	require.Len(t, hosts, 1)
	assert.Equal(t, "m5.large", hosts[0].InstanceType)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec2-cr-host-stack")})
	require.NoError(t, err)
}
