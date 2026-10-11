package ec2_test

import (
	"net"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

type deepEnv struct {
	client *ec2sdk.Client
	vpcID  string
	subnet string
}

func newDeepEnv(t *testing.T) deepEnv {
	t.Helper()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"
	c := newTestEC2Client(t, h)

	vpc, err := c.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)

	sn, err := c.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId: vpc.Vpc.VpcId, CidrBlock: aws.String("10.0.1.0/24"), AvailabilityZone: aws.String("us-east-1a"),
	})
	require.NoError(t, err)

	return deepEnv{client: c, vpcID: aws.ToString(vpc.Vpc.VpcId), subnet: aws.ToString(sn.Subnet.SubnetId)}
}

func (e deepEnv) attachedIGW(t *testing.T) string {
	t.Helper()

	igw, err := e.client.CreateInternetGateway(t.Context(), &ec2sdk.CreateInternetGatewayInput{})
	require.NoError(t, err)

	_, err = e.client.AttachInternetGateway(t.Context(), &ec2sdk.AttachInternetGatewayInput{
		InternetGatewayId: igw.InternetGateway.InternetGatewayId, VpcId: aws.String(e.vpcID),
	})
	require.NoError(t, err)

	return aws.ToString(igw.InternetGateway.InternetGatewayId)
}

func (e deepEnv) routeTable(t *testing.T) string {
	t.Helper()

	rt, err := e.client.CreateRouteTable(t.Context(), &ec2sdk.CreateRouteTableInput{VpcId: aws.String(e.vpcID)})
	require.NoError(t, err)

	return aws.ToString(rt.RouteTable.RouteTableId)
}

func TestDeepFlows_RouteLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    func(e deepEnv, igw string) *ec2sdk.CreateRouteInput
		wantAny  func(r types.Route) bool
		name     string
		wantCode string
	}{
		{
			name: "igw_default",
			input: func(_ deepEnv, igw string) *ec2sdk.CreateRouteInput {
				return &ec2sdk.CreateRouteInput{
					DestinationCidrBlock: aws.String("0.0.0.0/0"),
					GatewayId:            aws.String(igw),
				}
			},
			wantAny: func(r types.Route) bool { return aws.ToString(r.DestinationCidrBlock) == "0.0.0.0/0" },
		},
		{
			name: "local_cidr_exists",
			input: func(_ deepEnv, igw string) *ec2sdk.CreateRouteInput {
				return &ec2sdk.CreateRouteInput{
					DestinationCidrBlock: aws.String("10.0.0.0/16"),
					GatewayId:            aws.String(igw),
				}
			},
			wantCode: "RouteAlreadyExists",
		},
		{
			name: "unknown_igw",
			input: func(_ deepEnv, _ string) *ec2sdk.CreateRouteInput {
				return &ec2sdk.CreateRouteInput{
					DestinationCidrBlock: aws.String("1.1.1.0/24"),
					GatewayId:            aws.String("igw-nope"),
				}
			},
			wantCode: "InvalidGatewayID.NotFound",
		},
		{
			name: "ipv6_in_v4_field",
			input: func(_ deepEnv, igw string) *ec2sdk.CreateRouteInput {
				return &ec2sdk.CreateRouteInput{DestinationCidrBlock: aws.String("::/0"), GatewayId: aws.String(igw)}
			},
			wantCode: "InvalidParameterValue",
		},
		{
			name: "ipv6_default",
			input: func(_ deepEnv, igw string) *ec2sdk.CreateRouteInput {
				return &ec2sdk.CreateRouteInput{
					DestinationIpv6CidrBlock: aws.String("::/0"),
					GatewayId:                aws.String(igw),
				}
			},
			wantAny: func(r types.Route) bool { return aws.ToString(r.DestinationIpv6CidrBlock) == "::/0" },
		},
		{
			name: "transit_gateway_target",
			input: func(_ deepEnv, _ string) *ec2sdk.CreateRouteInput {
				return &ec2sdk.CreateRouteInput{
					DestinationCidrBlock: aws.String(
						"192.168.0.0/16",
					),
					TransitGatewayId: aws.String("tgw-0123456789abcdef0"),
				}
			},
			wantAny: func(r types.Route) bool {
				return aws.ToString(r.TransitGatewayId) == "tgw-0123456789abcdef0" &&
					r.Origin == types.RouteOriginCreateRoute
			},
		},
		{
			name: "prefix_list_destination",
			input: func(_ deepEnv, igw string) *ec2sdk.CreateRouteInput {
				return &ec2sdk.CreateRouteInput{
					DestinationPrefixListId: aws.String("pl-0123"),
					GatewayId:               aws.String(igw),
				}
			},
			wantAny: func(r types.Route) bool { return aws.ToString(r.DestinationPrefixListId) == "pl-0123" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newDeepEnv(t)
			igw := e.attachedIGW(t)
			rtID := e.routeTable(t)

			in := tt.input(e, igw)
			in.RouteTableId = aws.String(rtID)

			_, err := e.client.CreateRoute(t.Context(), in)
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(t, err))

				return
			}

			require.NoError(t, err)

			_, err = e.client.CreateRoute(t.Context(), in)
			assert.Equal(t, "RouteAlreadyExists", apiCode(t, err))

			out, err := e.client.DescribeRouteTables(t.Context(), &ec2sdk.DescribeRouteTablesInput{
				RouteTableIds: []string{rtID},
			})
			require.NoError(t, err)
			assert.True(t, anyRoute(out.RouteTables[0].Routes, tt.wantAny))
		})
	}
}

func anyRoute(routes []types.Route, f func(types.Route) bool) bool {
	return slices.ContainsFunc(routes, f)
}

func TestDeepFlows_RouteTableLocalRoute(t *testing.T) {
	t.Parallel()

	e := newDeepEnv(t)
	rtID := e.routeTable(t)

	out, err := e.client.DescribeRouteTables(
		t.Context(),
		&ec2sdk.DescribeRouteTablesInput{RouteTableIds: []string{rtID}},
	)
	require.NoError(t, err)
	require.Len(t, out.RouteTables[0].Routes, 1)
	assert.Equal(t, "local", aws.ToString(out.RouteTables[0].Routes[0].GatewayId))

	_, err = e.client.DeleteRoute(t.Context(), &ec2sdk.DeleteRouteInput{
		RouteTableId: aws.String(rtID), DestinationCidrBlock: aws.String("10.0.0.0/16"),
	})
	assert.Equal(t, "InvalidParameterValue", apiCode(t, err))
}

func TestDeepFlows_SecurityGroupRules(t *testing.T) {
	t.Parallel()

	e := newDeepEnv(t)
	c := e.client

	sg, err := c.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName: aws.String("g"), Description: aws.String("d"), VpcId: aws.String(e.vpcID),
	})
	require.NoError(t, err)

	gid := sg.GroupId

	tcp := func(port int32) types.IpPermission {
		return types.IpPermission{
			IpProtocol: aws.String("tcp"), FromPort: aws.Int32(port), ToPort: aws.Int32(port),
			IpRanges: []types.IpRange{{CidrIp: aws.String("10.0.0.0/8")}},
		}
	}

	for _, p := range []int32{22, 80, 443} {
		_, err = c.AuthorizeSecurityGroupIngress(t.Context(), &ec2sdk.AuthorizeSecurityGroupIngressInput{
			GroupId: gid, IpPermissions: []types.IpPermission{tcp(p)},
		})
		require.NoError(t, err)
	}

	before := ruleIDsByPort(t, c, gid)
	require.Len(t, before, 3)

	_, err = c.RevokeSecurityGroupIngress(t.Context(), &ec2sdk.RevokeSecurityGroupIngressInput{
		GroupId: gid, IpPermissions: []types.IpPermission{tcp(22)},
	})
	require.NoError(t, err)

	after := ruleIDsByPort(t, c, gid)
	assert.Equal(t, before[80], after[80], "surviving rule keeps its id")
	assert.Equal(t, before[443], after[443], "surviving rule keeps its id")
	assert.NotContains(t, after, int32(22))

	t.Run("ipv6_and_group_ref", func(t *testing.T) {
		t.Parallel()

		_, authErr := c.AuthorizeSecurityGroupIngress(t.Context(), &ec2sdk.AuthorizeSecurityGroupIngressInput{
			GroupId: gid,
			IpPermissions: []types.IpPermission{
				{
					IpProtocol: aws.String("tcp"), FromPort: aws.Int32(8443), ToPort: aws.Int32(8443),
					Ipv6Ranges: []types.Ipv6Range{{CidrIpv6: aws.String("::/0")}},
				},
				{
					IpProtocol: aws.String("tcp"), FromPort: aws.Int32(9000), ToPort: aws.Int32(9000),
					UserIdGroupPairs: []types.UserIdGroupPair{{GroupId: gid}},
				},
			},
		})
		require.NoError(t, authErr)

		rules, descErr := c.DescribeSecurityGroupRules(t.Context(), &ec2sdk.DescribeSecurityGroupRulesInput{
			Filters: []types.Filter{{Name: aws.String("group-id"), Values: []string{aws.ToString(gid)}}},
		})
		require.NoError(t, descErr)

		var gotV6, gotRef bool

		for _, r := range rules.SecurityGroupRules {
			gotV6 = gotV6 || aws.ToString(r.CidrIpv6) == "::/0"
			gotRef = gotRef ||
				(r.ReferencedGroupInfo != nil && aws.ToString(r.ReferencedGroupInfo.GroupId) == aws.ToString(gid))
		}

		assert.True(t, gotV6, "ipv6 rule stored and described")
		assert.True(t, gotRef, "group reference described")
	})

	t.Run("unknown_source_group", func(t *testing.T) {
		t.Parallel()

		_, authErr := c.AuthorizeSecurityGroupIngress(t.Context(), &ec2sdk.AuthorizeSecurityGroupIngressInput{
			GroupId: gid,
			IpPermissions: []types.IpPermission{{
				IpProtocol: aws.String("tcp"), FromPort: aws.Int32(7000), ToPort: aws.Int32(7000),
				UserIdGroupPairs: []types.UserIdGroupPair{{GroupId: aws.String("sg-0000000000000dead")}},
			}},
		})
		assert.Equal(t, "InvalidGroup.NotFound", apiCode(t, authErr))
	})
}

func ruleIDsByPort(t *testing.T, c *ec2sdk.Client, gid *string) map[int32]string {
	t.Helper()

	out, err := c.DescribeSecurityGroupRules(t.Context(), &ec2sdk.DescribeSecurityGroupRulesInput{
		Filters: []types.Filter{{Name: aws.String("group-id"), Values: []string{aws.ToString(gid)}}},
	})
	require.NoError(t, err)

	ids := make(map[int32]string)

	for _, r := range out.SecurityGroupRules {
		if !aws.ToBool(r.IsEgress) && r.CidrIpv4 != nil {
			ids[aws.ToInt32(r.FromPort)] = aws.ToString(r.SecurityGroupRuleId)
		}
	}

	return ids
}

func TestDeepFlows_NatGatewayElasticIP(t *testing.T) {
	t.Parallel()

	e := newDeepEnv(t)
	c := e.client

	eip, err := c.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{Domain: types.DomainTypeVpc})
	require.NoError(t, err)

	_, err = c.CreateNatGateway(t.Context(), &ec2sdk.CreateNatGatewayInput{
		SubnetId: aws.String(e.subnet), AllocationId: eip.AllocationId,
	})
	require.NoError(t, err)

	_, err = c.CreateNatGateway(t.Context(), &ec2sdk.CreateNatGatewayInput{
		SubnetId: aws.String(e.subnet), AllocationId: eip.AllocationId,
	})
	assert.Equal(t, "Resource.AlreadyAssociated", apiCode(t, err))

	_, err = c.ReleaseAddress(t.Context(), &ec2sdk.ReleaseAddressInput{AllocationId: eip.AllocationId})
	assert.Equal(t, "InvalidIPAddress.InUse", apiCode(t, err))

	priv, err := c.CreateNatGateway(t.Context(), &ec2sdk.CreateNatGatewayInput{
		SubnetId: aws.String(e.subnet), ConnectivityType: types.ConnectivityTypePrivate,
	})
	require.NoError(t, err)
	assert.Equal(t, types.ConnectivityTypePrivate, priv.NatGateway.ConnectivityType)
	require.Len(t, priv.NatGateway.NatGatewayAddresses, 1)
	assert.Nil(t, priv.NatGateway.NatGatewayAddresses[0].PublicIp)
	assertInSubnet(t, "10.0.1.0/24", aws.ToString(priv.NatGateway.NatGatewayAddresses[0].PrivateIp))
}

func assertInSubnet(t *testing.T, cidr, ip string) {
	t.Helper()

	_, n, err := net.ParseCIDR(cidr)
	require.NoError(t, err)
	assert.True(t, n.Contains(net.ParseIP(ip)), "%s should be inside %s", ip, cidr)
}

func TestDeepFlows_InstanceNetworking(t *testing.T) {
	t.Parallel()

	e := newDeepEnv(t)
	c := e.client

	sg, err := c.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName: aws.String("app"), Description: aws.String("d"), VpcId: aws.String(e.vpcID),
	})
	require.NoError(t, err)

	run, err := c.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId: aws.String("ami-12345678"), InstanceType: types.InstanceTypeT2Micro,
		SubnetId: aws.String(e.subnet), SecurityGroupIds: []string{aws.ToString(sg.GroupId)},
		MinCount: aws.Int32(2), MaxCount: aws.Int32(2),
	})
	require.NoError(t, err)

	seen := map[string]bool{}

	for _, inst := range run.Instances {
		ip := aws.ToString(inst.PrivateIpAddress)
		assertInSubnet(t, "10.0.1.0/24", ip)
		assert.False(t, seen[ip], "private IPs are unique within a subnet")
		seen[ip] = true
	}

	instID := run.Instances[0].InstanceId

	enis, err := c.DescribeNetworkInterfaces(t.Context(), &ec2sdk.DescribeNetworkInterfacesInput{
		Filters: []types.Filter{{Name: aws.String("attachment.instance-id"), Values: []string{aws.ToString(instID)}}},
	})
	require.NoError(t, err)
	require.Len(t, enis.NetworkInterfaces, 1)
	require.Len(t, enis.NetworkInterfaces[0].Groups, 1)
	assert.Equal(t, aws.ToString(sg.GroupId), aws.ToString(enis.NetworkInterfaces[0].Groups[0].GroupId))

	eip, err := c.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{Domain: types.DomainTypeVpc})
	require.NoError(t, err)

	assoc, err := c.AssociateAddress(t.Context(), &ec2sdk.AssociateAddressInput{
		AllocationId: eip.AllocationId, InstanceId: instID,
	})
	require.NoError(t, err)

	desc, err := c.DescribeInstances(
		t.Context(),
		&ec2sdk.DescribeInstancesInput{InstanceIds: []string{aws.ToString(instID)}},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(eip.PublicIp), aws.ToString(desc.Reservations[0].Instances[0].PublicIpAddress))

	addrs, err := c.DescribeAddresses(
		t.Context(),
		&ec2sdk.DescribeAddressesInput{AllocationIds: []string{aws.ToString(eip.AllocationId)}},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		aws.ToString(enis.NetworkInterfaces[0].NetworkInterfaceId),
		aws.ToString(addrs.Addresses[0].NetworkInterfaceId),
	)
	assert.Equal(
		t,
		aws.ToString(enis.NetworkInterfaces[0].PrivateIpAddress),
		aws.ToString(addrs.Addresses[0].PrivateIpAddress),
	)

	_, err = c.DisassociateAddress(t.Context(), &ec2sdk.DisassociateAddressInput{AssociationId: assoc.AssociationId})
	require.NoError(t, err)

	desc, err = c.DescribeInstances(
		t.Context(),
		&ec2sdk.DescribeInstancesInput{InstanceIds: []string{aws.ToString(instID)}},
	)
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(desc.Reservations[0].Instances[0].PublicIpAddress))
}

func TestDeepFlows_CreateNetworkInterfaceOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ip       string
		wantCode string
		sgs      []string
	}{
		{name: "defaults"},
		{name: "explicit_ip", ip: "10.0.1.50"},
		{name: "ip_outside_subnet", ip: "10.0.9.9", wantCode: "InvalidParameterValue"},
		{name: "first_usable_ip", ip: "10.0.1.4"},
		{name: "unknown_sg", sgs: []string{"sg-0000000000000dead"}, wantCode: "InvalidGroup.NotFound"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newDeepEnv(t)
			in := &ec2sdk.CreateNetworkInterfaceInput{SubnetId: aws.String(e.subnet), Groups: tt.sgs}
			if tt.ip != "" {
				in.PrivateIpAddress = aws.String(tt.ip)
			}

			out, err := e.client.CreateNetworkInterface(t.Context(), in)
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(t, err))

				return
			}

			require.NoError(t, err)
			assertInSubnet(t, "10.0.1.0/24", aws.ToString(out.NetworkInterface.PrivateIpAddress))
			require.Len(t, out.NetworkInterface.Groups, 1, "default security group applies")

			if tt.ip != "" {
				assert.Equal(t, tt.ip, aws.ToString(out.NetworkInterface.PrivateIpAddress))

				_, err = e.client.CreateNetworkInterface(t.Context(), in)
				assert.Equal(t, "InvalidIPAddress.InUse", apiCode(t, err))
			}
		})
	}
}

func TestDeepFlows_LaunchTemplateLifecycle(t *testing.T) {
	t.Parallel()

	e := newDeepEnv(t)
	c := e.client

	sg, err := c.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName: aws.String("lt-sg"), Description: aws.String("d"), VpcId: aws.String(e.vpcID),
	})
	require.NoError(t, err)

	_, err = c.CreateKeyPair(t.Context(), &ec2sdk.CreateKeyPairInput{KeyName: aws.String("k")})
	require.NoError(t, err)

	_, err = c.CreateLaunchTemplate(t.Context(), &ec2sdk.CreateLaunchTemplateInput{
		LaunchTemplateName: aws.String("lt"),
		LaunchTemplateData: &types.RequestLaunchTemplateData{
			ImageId:          aws.String("ami-12345678"),
			InstanceType:     types.InstanceTypeT2Micro,
			SecurityGroupIds: []string{aws.ToString(sg.GroupId)},
			KeyName:          aws.String("k"),
			TagSpecifications: []types.LaunchTemplateTagSpecificationRequest{{
				ResourceType: types.ResourceTypeInstance,
				Tags:         []types.Tag{{Key: aws.String("from"), Value: aws.String("lt")}},
			}},
		},
	})
	require.NoError(t, err)

	v2, err := c.CreateLaunchTemplateVersion(t.Context(), &ec2sdk.CreateLaunchTemplateVersionInput{
		LaunchTemplateName: aws.String("lt"), SourceVersion: aws.String("1"), VersionDescription: aws.String("bigger"),
		LaunchTemplateData: &types.RequestLaunchTemplateData{InstanceType: types.InstanceTypeT3Small},
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		"ami-12345678",
		aws.ToString(v2.LaunchTemplateVersion.LaunchTemplateData.ImageId),
		"inherited from source",
	)
	assert.Equal(t, []string{aws.ToString(sg.GroupId)}, v2.LaunchTemplateVersion.LaunchTemplateData.SecurityGroupIds)
	assert.Equal(t, "bigger", aws.ToString(v2.LaunchTemplateVersion.VersionDescription))

	_, err = c.ModifyLaunchTemplate(t.Context(), &ec2sdk.ModifyLaunchTemplateInput{
		LaunchTemplateName: aws.String("lt"), DefaultVersion: aws.String("2"),
	})
	require.NoError(t, err)

	_, err = c.ModifyLaunchTemplate(t.Context(), &ec2sdk.ModifyLaunchTemplateInput{
		LaunchTemplateName: aws.String("lt"), DefaultVersion: aws.String("9"),
	})
	assert.Equal(t, "InvalidLaunchTemplateId.VersionNotFound", apiCode(t, err))

	run := func(version string) *types.Instance {
		out, runErr := c.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
			LaunchTemplate: &types.LaunchTemplateSpecification{
				LaunchTemplateName: aws.String("lt"),
				Version:            aws.String(version),
			},
			SubnetId: aws.String(e.subnet),
			MinCount: aws.Int32(1),
			MaxCount: aws.Int32(1),
		})
		require.NoError(t, runErr)

		return &out.Instances[0]
	}

	assert.Equal(t, types.InstanceTypeT2Micro, run("1").InstanceType)
	assert.Equal(t, types.InstanceTypeT3Small, run("$Default").InstanceType)

	inst := run("$Latest")
	assert.Equal(t, "k", aws.ToString(inst.KeyName))
	assert.Equal(t, aws.ToString(sg.GroupId), aws.ToString(inst.SecurityGroups[0].GroupId))

	desc, err := c.DescribeInstances(
		t.Context(),
		&ec2sdk.DescribeInstancesInput{InstanceIds: []string{aws.ToString(inst.InstanceId)}},
	)
	require.NoError(t, err)
	require.NotEmpty(t, desc.Reservations[0].Instances[0].Tags)
	assert.Equal(t, "from", aws.ToString(desc.Reservations[0].Instances[0].Tags[0].Key))

	_, err = c.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		LaunchTemplate: &types.LaunchTemplateSpecification{LaunchTemplateName: aws.String("nope")},
		SubnetId:       aws.String(e.subnet), MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})
	require.Error(t, err)

	_, err = c.DeleteLaunchTemplateVersions(t.Context(), &ec2sdk.DeleteLaunchTemplateVersionsInput{
		LaunchTemplateName: aws.String("lt"), Versions: []string{"2"},
	})
	require.Error(t, err, "default version cannot be deleted")

	del, err := c.DeleteLaunchTemplateVersions(t.Context(), &ec2sdk.DeleteLaunchTemplateVersionsInput{
		LaunchTemplateName: aws.String("lt"), Versions: []string{"1", "7"},
	})
	require.NoError(t, err)
	assert.Len(t, del.SuccessfullyDeletedLaunchTemplateVersions, 1)
	assert.Len(t, del.UnsuccessfullyDeletedLaunchTemplateVersions, 1)

	vers, err := c.DescribeLaunchTemplateVersions(t.Context(), &ec2sdk.DescribeLaunchTemplateVersionsInput{
		LaunchTemplateName: aws.String("lt"),
	})
	require.NoError(t, err)
	require.Len(t, vers.LaunchTemplateVersions, 1)
	assert.EqualValues(t, 2, aws.ToInt64(vers.LaunchTemplateVersions[0].VersionNumber))
}

func TestDeepFlows_LaunchBlockDevices(t *testing.T) {
	t.Parallel()

	e := newDeepEnv(t)
	c := e.client

	out, err := c.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId: aws.String("ami-12345678"), InstanceType: types.InstanceTypeT2Micro,
		SubnetId: aws.String(e.subnet), MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
		BlockDeviceMappings: []types.BlockDeviceMapping{{
			DeviceName: aws.String("/dev/xvda"),
			Ebs:        &types.EbsBlockDevice{VolumeSize: aws.Int32(20), VolumeType: types.VolumeTypeGp3},
		}},
		TagSpecifications: []types.TagSpecification{{
			ResourceType: types.ResourceTypeVolume,
			Tags:         []types.Tag{{Key: aws.String("v"), Value: aws.String("1")}},
		}},
	})
	require.NoError(t, err)

	id := aws.ToString(out.Instances[0].InstanceId)

	desc, err := c.DescribeInstances(t.Context(), &ec2sdk.DescribeInstancesInput{InstanceIds: []string{id}})
	require.NoError(t, err)

	inst := desc.Reservations[0].Instances[0]
	assert.Equal(t, "/dev/xvda", aws.ToString(inst.RootDeviceName))
	assert.Equal(t, types.DeviceTypeEbs, inst.RootDeviceType)
	require.Len(t, inst.BlockDeviceMappings, 1)
	assert.True(t, aws.ToBool(inst.BlockDeviceMappings[0].Ebs.DeleteOnTermination))

	vols, err := c.DescribeVolumes(t.Context(), &ec2sdk.DescribeVolumesInput{
		Filters: []types.Filter{{Name: aws.String("tag:v"), Values: []string{"1"}}},
	})
	require.NoError(t, err)
	require.Len(t, vols.Volumes, 1)
	assert.EqualValues(t, 20, aws.ToInt32(vols.Volumes[0].Size))
	assert.Equal(t, types.VolumeTypeGp3, vols.Volumes[0].VolumeType)

	_, err = c.TerminateInstances(t.Context(), &ec2sdk.TerminateInstancesInput{InstanceIds: []string{id}})
	require.NoError(t, err)

	vols, err = c.DescribeVolumes(t.Context(), &ec2sdk.DescribeVolumesInput{})
	require.NoError(t, err)
	assert.Empty(t, vols.Volumes, "root volume is deleted with the instance")
}
