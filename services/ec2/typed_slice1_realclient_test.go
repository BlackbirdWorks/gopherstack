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

// TestKeyPairs_RealClient covers CreateKeyPair, ImportKeyPair and
// DescribeKeyPairs -- no typed-client coverage anywhere in the repo
// (gopherstack-n3zi slice 1).
func TestKeyPairs_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	created, err := client.CreateKeyPair(t.Context(), &ec2sdk.CreateKeyPairInput{
		KeyName: aws.String("created-key"),
	})
	require.NoError(t, err)
	assert.Equal(t, "created-key", aws.ToString(created.KeyName))
	assert.NotEmpty(t, aws.ToString(created.KeyMaterial))
	assert.NotEmpty(t, aws.ToString(created.KeyFingerprint))

	pubKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCtRrefcU79SFvkuGBm0j5SHj4DTH8cPoLSShYJopMJ2tg" +
		"6EYASjsQ1AnLYqABe49OF+NDj3eWzCBaJHPg1w99OWa10N8F7Kstd+H4KBBSTakQ8XnOsVrQgi1AmZ/nzR4hLd5Z9pF6A6TREpu" +
		"v0+sCUl1Y5OQtA6lLT+rzXe1epxDYeMtzjlpUK1inNlz0XAMS2+/j7k/GHGf8qtETclk5+rkwZ9YAxbj0Oba49s/tgZxV00wZWZ" +
		"75npx/k1F/A5HHHmuZKN/HtXgOnzltX8FnYTvBiwlNZwAUfVuI7M3gW/o8P7mD8peI/RV4hqwfaJ4NAIwtiBcOwWEIDjr2AkwAz"

	imported, err := client.ImportKeyPair(t.Context(), &ec2sdk.ImportKeyPairInput{
		KeyName:           aws.String("imported-key"),
		PublicKeyMaterial: []byte(pubKey),
	})
	require.NoError(t, err)
	assert.Equal(t, "imported-key", aws.ToString(imported.KeyName))
	assert.NotEmpty(t, aws.ToString(imported.KeyFingerprint))

	listed, err := client.DescribeKeyPairs(t.Context(), &ec2sdk.DescribeKeyPairsInput{})
	require.NoError(t, err)
	names := make([]string, 0, len(listed.KeyPairs))
	for _, kp := range listed.KeyPairs {
		names = append(names, aws.ToString(kp.KeyName))
	}
	assert.Contains(t, names, "created-key")
	assert.Contains(t, names, "imported-key")
}

// TestDefaultVpcAndSubnet_RealClient covers CreateDefaultVpc and
// CreateDefaultSubnet. A fresh backend already seeds a default VPC
// (store.go's initDefaults, matching a real, never-torn-down AWS account),
// so CreateDefaultVpc's real "cannot have more than one default VPC per
// Region" rejection is exercised directly rather than assuming a from-empty
// happy path CreateDefaultVpc could never actually reach in this emulator.
func TestDefaultVpcAndSubnet_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	existing, err := client.DescribeVpcs(t.Context(), &ec2sdk.DescribeVpcsInput{
		Filters: []types.Filter{{Name: aws.String("isDefault"), Values: []string{"true"}}},
	})
	require.NoError(t, err)
	require.Len(t, existing.Vpcs, 1)
	defaultVpcID := aws.ToString(existing.Vpcs[0].VpcId)

	_, err = client.CreateDefaultVpc(t.Context(), &ec2sdk.CreateDefaultVpcInput{})
	require.Error(t, err, "a second default VPC in the same region must be rejected")

	subnetOut, err := client.CreateDefaultSubnet(t.Context(), &ec2sdk.CreateDefaultSubnetInput{
		AvailabilityZone: aws.String("us-east-1b"),
	})
	require.NoError(t, err)
	require.NotNil(t, subnetOut.Subnet)
	assert.True(t, aws.ToBool(subnetOut.Subnet.DefaultForAz))
	assert.Equal(t, defaultVpcID, aws.ToString(subnetOut.Subnet.VpcId))
	assert.Equal(t, "us-east-1b", aws.ToString(subnetOut.Subnet.AvailabilityZone))
}

// TestInternetGatewayLifecycle_RealClient covers AttachInternetGateway,
// DetachInternetGateway and DeleteInternetGateway.
func TestInternetGatewayLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{
		CidrBlock: aws.String("10.0.0.0/16"),
	})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	igwOut, err := client.CreateInternetGateway(t.Context(), &ec2sdk.CreateInternetGatewayInput{})
	require.NoError(t, err)
	igwID := aws.ToString(igwOut.InternetGateway.InternetGatewayId)

	_, err = client.AttachInternetGateway(t.Context(), &ec2sdk.AttachInternetGatewayInput{
		InternetGatewayId: aws.String(igwID), VpcId: aws.String(vpcID),
	})
	require.NoError(t, err)

	described, err := client.DescribeInternetGateways(t.Context(), &ec2sdk.DescribeInternetGatewaysInput{
		InternetGatewayIds: []string{igwID},
	})
	require.NoError(t, err)
	require.Len(t, described.InternetGateways, 1)
	require.Len(t, described.InternetGateways[0].Attachments, 1)
	assert.Equal(t, vpcID, aws.ToString(described.InternetGateways[0].Attachments[0].VpcId))

	_, err = client.DetachInternetGateway(t.Context(), &ec2sdk.DetachInternetGatewayInput{
		InternetGatewayId: aws.String(igwID), VpcId: aws.String(vpcID),
	})
	require.NoError(t, err)

	afterDetach, err := client.DescribeInternetGateways(t.Context(), &ec2sdk.DescribeInternetGatewaysInput{
		InternetGatewayIds: []string{igwID},
	})
	require.NoError(t, err)
	assert.Empty(t, afterDetach.InternetGateways[0].Attachments)

	_, err = client.DeleteInternetGateway(t.Context(), &ec2sdk.DeleteInternetGatewayInput{
		InternetGatewayId: aws.String(igwID),
	})
	require.NoError(t, err)

	_, err = client.DescribeInternetGateways(t.Context(), &ec2sdk.DescribeInternetGatewaysInput{
		InternetGatewayIds: []string{igwID},
	})
	require.Error(t, err)
}

// TestDhcpOptions_RealClient covers CreateDhcpOptions, AssociateDhcpOptions
// and DeleteDhcpOptions.
func TestDhcpOptions_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{
		CidrBlock: aws.String("10.0.0.0/16"),
	})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	dhcpOut, err := client.CreateDhcpOptions(t.Context(), &ec2sdk.CreateDhcpOptionsInput{
		DhcpConfigurations: []types.NewDhcpConfiguration{
			{Key: aws.String("domain-name-servers"), Values: []string{"10.2.5.1", "10.2.5.2"}},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, dhcpOut.DhcpOptions)
	dhcpID := aws.ToString(dhcpOut.DhcpOptions.DhcpOptionsId)
	require.Len(t, dhcpOut.DhcpOptions.DhcpConfigurations, 1)
	assert.Equal(t, "domain-name-servers", aws.ToString(dhcpOut.DhcpOptions.DhcpConfigurations[0].Key))

	_, err = client.AssociateDhcpOptions(t.Context(), &ec2sdk.AssociateDhcpOptionsInput{
		DhcpOptionsId: aws.String(dhcpID), VpcId: aws.String(vpcID),
	})
	require.NoError(t, err)

	describedVpc, err := client.DescribeVpcs(t.Context(), &ec2sdk.DescribeVpcsInput{VpcIds: []string{vpcID}})
	require.NoError(t, err)
	require.Len(t, describedVpc.Vpcs, 1)
	assert.Equal(t, dhcpID, aws.ToString(describedVpc.Vpcs[0].DhcpOptionsId))

	_, err = client.AssociateDhcpOptions(t.Context(), &ec2sdk.AssociateDhcpOptionsInput{
		DhcpOptionsId: aws.String("default"), VpcId: aws.String(vpcID),
	})
	require.NoError(t, err)

	_, err = client.DeleteDhcpOptions(t.Context(), &ec2sdk.DeleteDhcpOptionsInput{
		DhcpOptionsId: aws.String(dhcpID),
	})
	require.NoError(t, err)

	_, err = client.DescribeDhcpOptions(t.Context(), &ec2sdk.DescribeDhcpOptionsInput{
		DhcpOptionsIds: []string{dhcpID},
	})
	require.Error(t, err)
}

// TestRouteTableRoutes_RealClient covers CreateRoute, DeleteRoute,
// AssociateRouteTable and DeleteRouteTable.
func TestRouteTableRoutes_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	subnetOut, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId: aws.String(vpcID), CidrBlock: aws.String("10.0.1.0/24"),
	})
	require.NoError(t, err)
	subnetID := aws.ToString(subnetOut.Subnet.SubnetId)

	igwOut, err := client.CreateInternetGateway(t.Context(), &ec2sdk.CreateInternetGatewayInput{})
	require.NoError(t, err)
	igwID := aws.ToString(igwOut.InternetGateway.InternetGatewayId)

	_, err = client.AttachInternetGateway(t.Context(), &ec2sdk.AttachInternetGatewayInput{
		InternetGatewayId: aws.String(igwID), VpcId: aws.String(vpcID),
	})
	require.NoError(t, err)

	rtOut, err := client.CreateRouteTable(t.Context(), &ec2sdk.CreateRouteTableInput{VpcId: aws.String(vpcID)})
	require.NoError(t, err)
	rtID := aws.ToString(rtOut.RouteTable.RouteTableId)

	_, err = client.CreateRoute(t.Context(), &ec2sdk.CreateRouteInput{
		RouteTableId:         aws.String(rtID),
		DestinationCidrBlock: aws.String("0.0.0.0/0"),
		GatewayId:            aws.String(igwID),
	})
	require.NoError(t, err)

	described, err := client.DescribeRouteTables(t.Context(), &ec2sdk.DescribeRouteTablesInput{
		RouteTableIds: []string{rtID},
	})
	require.NoError(t, err)
	require.Len(t, described.RouteTables, 1)
	var found bool
	for _, r := range described.RouteTables[0].Routes {
		if aws.ToString(r.DestinationCidrBlock) == "0.0.0.0/0" && aws.ToString(r.GatewayId) == igwID {
			found = true
		}
	}
	assert.True(t, found, "created route must appear in DescribeRouteTables")

	assocOut, err := client.AssociateRouteTable(t.Context(), &ec2sdk.AssociateRouteTableInput{
		RouteTableId: aws.String(rtID), SubnetId: aws.String(subnetID),
	})
	require.NoError(t, err)
	assocID := aws.ToString(assocOut.AssociationId)
	assert.NotEmpty(t, assocID)

	_, err = client.DeleteRoute(t.Context(), &ec2sdk.DeleteRouteInput{
		RouteTableId: aws.String(rtID), DestinationCidrBlock: aws.String("0.0.0.0/0"),
	})
	require.NoError(t, err)

	_, err = client.DisassociateRouteTable(t.Context(), &ec2sdk.DisassociateRouteTableInput{
		AssociationId: aws.String(assocID),
	})
	require.NoError(t, err)

	_, err = client.DeleteRouteTable(t.Context(), &ec2sdk.DeleteRouteTableInput{RouteTableId: aws.String(rtID)})
	require.NoError(t, err)

	_, err = client.DescribeRouteTables(t.Context(), &ec2sdk.DescribeRouteTablesInput{RouteTableIds: []string{rtID}})
	require.Error(t, err)
}

// TestVolumeLifecycle_RealClient covers AttachVolume, DetachVolume,
// DescribeVolumes and DeleteSnapshot.
func TestVolumeLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	runOut, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId: aws.String("ami-parity-test"), InstanceType: types.InstanceTypeT3Micro,
		MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Instances, 1)
	instanceID := aws.ToString(runOut.Instances[0].InstanceId)
	az := aws.ToString(runOut.Instances[0].Placement.AvailabilityZone)

	volOut, err := client.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
		AvailabilityZone: aws.String(az), Size: aws.Int32(8),
	})
	require.NoError(t, err)
	volumeID := aws.ToString(volOut.VolumeId)

	_, err = client.AttachVolume(t.Context(), &ec2sdk.AttachVolumeInput{
		VolumeId: aws.String(volumeID), InstanceId: aws.String(instanceID), Device: aws.String("/dev/sdf"),
	})
	require.NoError(t, err)

	described, err := client.DescribeVolumes(t.Context(), &ec2sdk.DescribeVolumesInput{VolumeIds: []string{volumeID}})
	require.NoError(t, err)
	require.Len(t, described.Volumes, 1)
	require.Len(t, described.Volumes[0].Attachments, 1)
	assert.Equal(t, instanceID, aws.ToString(described.Volumes[0].Attachments[0].InstanceId))

	_, err = client.DetachVolume(t.Context(), &ec2sdk.DetachVolumeInput{
		VolumeId: aws.String(volumeID), InstanceId: aws.String(instanceID),
	})
	require.NoError(t, err)

	afterDetach, err := client.DescribeVolumes(t.Context(), &ec2sdk.DescribeVolumesInput{
		VolumeIds: []string{volumeID},
	})
	require.NoError(t, err)
	assert.Empty(t, afterDetach.Volumes[0].Attachments)

	snapOut, err := client.CreateSnapshot(t.Context(), &ec2sdk.CreateSnapshotInput{VolumeId: aws.String(volumeID)})
	require.NoError(t, err)
	snapshotID := aws.ToString(snapOut.SnapshotId)

	_, err = client.DeleteSnapshot(t.Context(), &ec2sdk.DeleteSnapshotInput{SnapshotId: aws.String(snapshotID)})
	require.NoError(t, err)

	_, err = client.DescribeSnapshots(t.Context(), &ec2sdk.DescribeSnapshotsInput{SnapshotIds: []string{snapshotID}})
	require.Error(t, err)
}

// TestElasticIPAssociation_RealClient covers AssociateAddress and
// DisassociateAddress.
func TestElasticIPAssociation_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	runOut, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId: aws.String("ami-parity-test"), InstanceType: types.InstanceTypeT3Micro,
		MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})
	require.NoError(t, err)
	instanceID := aws.ToString(runOut.Instances[0].InstanceId)

	addrOut, err := client.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{})
	require.NoError(t, err)
	allocationID := aws.ToString(addrOut.AllocationId)

	assocOut, err := client.AssociateAddress(t.Context(), &ec2sdk.AssociateAddressInput{
		AllocationId: aws.String(allocationID), InstanceId: aws.String(instanceID),
	})
	require.NoError(t, err)
	assocID := aws.ToString(assocOut.AssociationId)
	assert.NotEmpty(t, assocID)

	described, err := client.DescribeAddresses(t.Context(), &ec2sdk.DescribeAddressesInput{
		AllocationIds: []string{allocationID},
	})
	require.NoError(t, err)
	require.Len(t, described.Addresses, 1)
	assert.Equal(t, instanceID, aws.ToString(described.Addresses[0].InstanceId))

	_, err = client.DisassociateAddress(t.Context(), &ec2sdk.DisassociateAddressInput{
		AssociationId: aws.String(assocID),
	})
	require.NoError(t, err)

	afterDisassoc, err := client.DescribeAddresses(t.Context(), &ec2sdk.DescribeAddressesInput{
		AllocationIds: []string{allocationID},
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(afterDisassoc.Addresses[0].InstanceId))
}

// TestNetworkAclLifecycle_RealClient covers DeleteNetworkAcl and
// DeleteNetworkAclEntry.
func TestNetworkAclLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	naclOut, err := client.CreateNetworkAcl(t.Context(), &ec2sdk.CreateNetworkAclInput{VpcId: aws.String(vpcID)})
	require.NoError(t, err)
	naclID := aws.ToString(naclOut.NetworkAcl.NetworkAclId)

	_, err = client.CreateNetworkAclEntry(t.Context(), &ec2sdk.CreateNetworkAclEntryInput{
		NetworkAclId: aws.String(naclID), RuleNumber: aws.Int32(100), Protocol: aws.String("-1"),
		RuleAction: types.RuleActionAllow, Egress: aws.Bool(false),
		CidrBlock: aws.String("0.0.0.0/0"),
	})
	require.NoError(t, err)

	_, err = client.DeleteNetworkAclEntry(t.Context(), &ec2sdk.DeleteNetworkAclEntryInput{
		NetworkAclId: aws.String(naclID), RuleNumber: aws.Int32(100), Egress: aws.Bool(false),
	})
	require.NoError(t, err)

	described, err := client.DescribeNetworkAcls(t.Context(), &ec2sdk.DescribeNetworkAclsInput{
		NetworkAclIds: []string{naclID},
	})
	require.NoError(t, err)
	require.Len(t, described.NetworkAcls, 1)
	for _, e := range described.NetworkAcls[0].Entries {
		assert.NotEqual(t, int32(100), aws.ToInt32(e.RuleNumber), "deleted entry must not remain")
	}

	_, err = client.DeleteNetworkAcl(t.Context(), &ec2sdk.DeleteNetworkAclInput{NetworkAclId: aws.String(naclID)})
	require.NoError(t, err)

	_, err = client.DescribeNetworkAcls(t.Context(), &ec2sdk.DescribeNetworkAclsInput{
		NetworkAclIds: []string{naclID},
	})
	require.Error(t, err)
}

// TestVpnGatewayLifecycle_RealClient covers AttachVpnGateway,
// DetachVpnGateway and DeleteCustomerGateway.
func TestVpnGatewayLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	vgwOut, err := client.CreateVpnGateway(t.Context(), &ec2sdk.CreateVpnGatewayInput{
		Type: types.GatewayTypeIpsec1,
	})
	require.NoError(t, err)
	vgwID := aws.ToString(vgwOut.VpnGateway.VpnGatewayId)

	_, err = client.AttachVpnGateway(t.Context(), &ec2sdk.AttachVpnGatewayInput{
		VpnGatewayId: aws.String(vgwID), VpcId: aws.String(vpcID),
	})
	require.NoError(t, err)

	described, err := client.DescribeVpnGateways(t.Context(), &ec2sdk.DescribeVpnGatewaysInput{
		VpnGatewayIds: []string{vgwID},
	})
	require.NoError(t, err)
	require.Len(t, described.VpnGateways, 1)
	require.Len(t, described.VpnGateways[0].VpcAttachments, 1)
	assert.Equal(t, vpcID, aws.ToString(described.VpnGateways[0].VpcAttachments[0].VpcId))

	_, err = client.DetachVpnGateway(t.Context(), &ec2sdk.DetachVpnGatewayInput{
		VpnGatewayId: aws.String(vgwID), VpcId: aws.String(vpcID),
	})
	require.NoError(t, err)

	cgwOut, err := client.CreateCustomerGateway(t.Context(), &ec2sdk.CreateCustomerGatewayInput{
		Type: types.GatewayTypeIpsec1, BgpAsn: aws.Int32(65000), PublicIp: aws.String("203.0.113.1"),
	})
	require.NoError(t, err)
	cgwID := aws.ToString(cgwOut.CustomerGateway.CustomerGatewayId)

	_, err = client.DeleteCustomerGateway(t.Context(), &ec2sdk.DeleteCustomerGatewayInput{
		CustomerGatewayId: aws.String(cgwID),
	})
	require.NoError(t, err)

	_, err = client.DescribeCustomerGateways(t.Context(), &ec2sdk.DescribeCustomerGatewaysInput{
		CustomerGatewayIds: []string{cgwID},
	})
	require.Error(t, err, "deleted customer gateway must NotFound, not silently vanish from the result")
}

// TestDescribeBasics_RealClient covers DescribeAvailabilityZones,
// DescribeRegions, DescribeVpcAttribute and DescribeInstanceAttribute.
func TestDescribeBasics_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	h.Region = "us-east-1"
	client := newTestEC2Client(t, h)

	azs, err := client.DescribeAvailabilityZones(t.Context(), &ec2sdk.DescribeAvailabilityZonesInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, azs.AvailabilityZones)
	for _, az := range azs.AvailabilityZones {
		assert.NotEmpty(t, aws.ToString(az.ZoneName))
		assert.NotEmpty(t, aws.ToString(az.RegionName))
	}

	regions, err := client.DescribeRegions(t.Context(), &ec2sdk.DescribeRegionsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, regions.Regions)

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	vpcAttr, err := client.DescribeVpcAttribute(t.Context(), &ec2sdk.DescribeVpcAttributeInput{
		VpcId: aws.String(vpcID), Attribute: types.VpcAttributeNameEnableDnsSupport,
	})
	require.NoError(t, err)
	require.NotNil(t, vpcAttr.EnableDnsSupport)
	assert.True(t, aws.ToBool(vpcAttr.EnableDnsSupport.Value))

	runOut, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId: aws.String("ami-parity-test"), InstanceType: types.InstanceTypeT3Micro,
		MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})
	require.NoError(t, err)
	instanceID := aws.ToString(runOut.Instances[0].InstanceId)

	instAttr, err := client.DescribeInstanceAttribute(t.Context(), &ec2sdk.DescribeInstanceAttributeInput{
		InstanceId: aws.String(instanceID), Attribute: types.InstanceAttributeNameInstanceType,
	})
	require.NoError(t, err)
	require.NotNil(t, instAttr.InstanceType)
	assert.Equal(t, "t3.micro", aws.ToString(instAttr.InstanceType.Value))
}

// TestVpcEndpointLifecycle_RealClient covers CreateVpcEndpoint and
// DeleteVpcEndpoints.
func TestVpcEndpointLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	epOut, err := client.CreateVpcEndpoint(t.Context(), &ec2sdk.CreateVpcEndpointInput{
		VpcId: aws.String(vpcID), ServiceName: aws.String("com.amazonaws.us-east-1.s3"),
		VpcEndpointType: types.VpcEndpointTypeGateway,
	})
	require.NoError(t, err)
	require.NotNil(t, epOut.VpcEndpoint)
	endpointID := aws.ToString(epOut.VpcEndpoint.VpcEndpointId)
	assert.Equal(t, "com.amazonaws.us-east-1.s3", aws.ToString(epOut.VpcEndpoint.ServiceName))

	described, err := client.DescribeVpcEndpoints(t.Context(), &ec2sdk.DescribeVpcEndpointsInput{
		VpcEndpointIds: []string{endpointID},
	})
	require.NoError(t, err)
	require.Len(t, described.VpcEndpoints, 1)

	delOut, err := client.DeleteVpcEndpoints(t.Context(), &ec2sdk.DeleteVpcEndpointsInput{
		VpcEndpointIds: []string{endpointID},
	})
	require.NoError(t, err)
	assert.Empty(t, delOut.Unsuccessful)

	afterDelete, err := client.DescribeVpcEndpoints(t.Context(), &ec2sdk.DescribeVpcEndpointsInput{
		VpcEndpointIds: []string{endpointID},
	})
	require.NoError(t, err)
	assert.Empty(t, afterDelete.VpcEndpoints)
}

// TestVpcAndSubnetCidrAssociation_RealClient covers AssociateVpcCidrBlock,
// AssociateSubnetCidrBlock and DisassociateSubnetCidrBlock.
func TestVpcAndSubnetCidrAssociation_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	assocOut, err := client.AssociateVpcCidrBlock(t.Context(), &ec2sdk.AssociateVpcCidrBlockInput{
		VpcId: aws.String(vpcID), CidrBlock: aws.String("10.1.0.0/16"),
	})
	require.NoError(t, err)
	require.NotNil(t, assocOut.CidrBlockAssociation)
	assert.Equal(t, "10.1.0.0/16", aws.ToString(assocOut.CidrBlockAssociation.CidrBlock))

	describedVpc, err := client.DescribeVpcs(t.Context(), &ec2sdk.DescribeVpcsInput{VpcIds: []string{vpcID}})
	require.NoError(t, err)
	require.Len(t, describedVpc.Vpcs, 1)
	assert.Len(t, describedVpc.Vpcs[0].CidrBlockAssociationSet, 2)

	subnetOut, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId: aws.String(vpcID), CidrBlock: aws.String("10.0.1.0/24"),
	})
	require.NoError(t, err)
	subnetID := aws.ToString(subnetOut.Subnet.SubnetId)

	subnetAssocOut, err := client.AssociateSubnetCidrBlock(t.Context(), &ec2sdk.AssociateSubnetCidrBlockInput{
		SubnetId: aws.String(subnetID), Ipv6CidrBlock: aws.String("2001:db8:1234:1a00::/64"),
	})
	require.NoError(t, err)
	require.NotNil(t, subnetAssocOut.Ipv6CidrBlockAssociation)
	assocID := aws.ToString(subnetAssocOut.Ipv6CidrBlockAssociation.AssociationId)
	assert.NotEmpty(t, assocID)

	_, err = client.DisassociateSubnetCidrBlock(t.Context(), &ec2sdk.DisassociateSubnetCidrBlockInput{
		AssociationId: aws.String(assocID),
	})
	require.NoError(t, err)

	describedSubnet, err := client.DescribeSubnets(t.Context(), &ec2sdk.DescribeSubnetsInput{
		SubnetIds: []string{subnetID},
	})
	require.NoError(t, err)
	require.Len(t, describedSubnet.Subnets, 1)
	for _, a := range describedSubnet.Subnets[0].Ipv6CidrBlockAssociationSet {
		assert.NotEqual(t, assocID, aws.ToString(a.AssociationId))
	}
}
