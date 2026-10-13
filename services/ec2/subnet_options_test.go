package ec2_test

import (
	"net/netip"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

type subnetFixture struct {
	client *ec2sdk.Client
	vpcID  string
	block  string
}

func newSubnetFixture(t *testing.T) subnetFixture {
	t.Helper()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestEC2Client(t, ec2.NewHandler(b))

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.20.0.0/16")})
	require.NoError(t, err)

	vpcID := aws.ToString(vpc.Vpc.VpcId)
	assoc, err := b.AssociateVpcIpv6CidrBlock(vpcID, "", "", "")
	require.NoError(t, err)

	return subnetFixture{client: client, vpcID: vpcID, block: assoc.Ipv6CidrBlock}
}

func nthIPv6Subnet(t *testing.T, block string) string {
	t.Helper()

	pfx, err := netip.ParsePrefix(block)
	require.NoError(t, err)

	raw := pfx.Addr().As16()
	raw[7]++

	return netip.PrefixFrom(netip.AddrFrom16(raw), 64).String()
}

func apiCode(t *testing.T, err error) string {
	t.Helper()

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)

	return apiErr.ErrorCode()
}

func describeOneSubnet(t *testing.T, c *ec2sdk.Client, id *string) types.Subnet {
	t.Helper()

	out, err := c.DescribeSubnets(t.Context(), &ec2sdk.DescribeSubnetsInput{SubnetIds: []string{aws.ToString(id)}})
	require.NoError(t, err)
	require.Len(t, out.Subnets, 1)

	return out.Subnets[0]
}

func TestCreateSubnet_AvailabilityZoneID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		az       *string
		azID     *string
		wantAZ   string
		wantCode string
	}{
		{name: "by id", azID: aws.String("use1-az2"), wantAZ: "us-east-1b"},
		{
			name: "id and name", az: aws.String("us-east-1b"), azID: aws.String("use1-az2"),
			wantCode: "InvalidParameterCombination",
		},
		{name: "unknown id", azID: aws.String("use1-az99"), wantCode: "InvalidParameterValue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newSubnetFixture(t)
			out, err := f.client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
				VpcId: aws.String(f.vpcID), CidrBlock: aws.String("10.20.1.0/24"),
				AvailabilityZone: tt.az, AvailabilityZoneId: tt.azID,
			})
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiCode(t, err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantAZ, aws.ToString(out.Subnet.AvailabilityZone))
			got := describeOneSubnet(t, f.client, out.Subnet.SubnetId)
			assert.Equal(t, "use1-az2", aws.ToString(got.AvailabilityZoneId))
		})
	}
}

func TestCreateSubnet_IPv6(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		v6       string
		wantCode string
		v4       bool
		native   bool
	}{
		{name: "dual stack", v6: "inside", v4: true},
		{name: "native", v6: "inside", native: true},
		{name: "native with ipv4", v6: "inside", v4: true, native: true, wantCode: "InvalidParameterCombination"},
		{name: "native without ipv6", v6: "none", native: true, wantCode: "MissingParameter"},
		{name: "outside vpc block", v6: "outside", v4: true, wantCode: "InvalidSubnet.Range"},
		{name: "wrong prefix length", v6: "block", v4: true, wantCode: "InvalidParameterValue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newSubnetFixture(t)

			var v6 *string

			switch tt.v6 {
			case "inside":
				v6 = aws.String(nthIPv6Subnet(t, f.block))
			case "outside":
				v6 = aws.String("2600:9999:1:1::/64")
			case "block":
				v6 = aws.String(f.block)
			}

			in := &ec2sdk.CreateSubnetInput{
				VpcId: aws.String(f.vpcID), Ipv6CidrBlock: v6, Ipv6Native: aws.Bool(tt.native),
			}
			if tt.v4 {
				in.CidrBlock = aws.String("10.20.1.0/24")
			}

			out, err := f.client.CreateSubnet(t.Context(), in)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiCode(t, err))

				return
			}

			require.NoError(t, err)

			got := describeOneSubnet(t, f.client, out.Subnet.SubnetId)
			assert.Equal(t, tt.native, aws.ToBool(got.Ipv6Native))
			assert.Equal(t, tt.native, aws.ToString(got.CidrBlock) == "")
			require.Len(t, got.Ipv6CidrBlockAssociationSet, 1)
			assert.Equal(t, aws.ToString(v6), aws.ToString(got.Ipv6CidrBlockAssociationSet[0].Ipv6CidrBlock))
			assoc := got.Ipv6CidrBlockAssociationSet[0]
			assert.Equal(t, types.SubnetCidrBlockStateCodeAssociated, assoc.Ipv6CidrBlockState.State)
			assert.Equal(t, types.IpSourceAmazon, assoc.IpSource)
			require.NotNil(t, got.PrivateDnsNameOptionsOnLaunch)

			wantHost := types.HostnameTypeIpName
			if tt.native {
				wantHost = types.HostnameTypeResourceName
			}

			assert.Equal(t, wantHost, got.PrivateDnsNameOptionsOnLaunch.HostnameType)
		})
	}
}

func TestCreateSubnet_IPv6Conflict(t *testing.T) {
	t.Parallel()

	f := newSubnetFixture(t)
	v6 := aws.String(nthIPv6Subnet(t, f.block))

	_, err := f.client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId: aws.String(f.vpcID), CidrBlock: aws.String("10.20.1.0/24"), Ipv6CidrBlock: v6,
	})
	require.NoError(t, err)

	_, err = f.client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId: aws.String(f.vpcID), CidrBlock: aws.String("10.20.2.0/24"), Ipv6CidrBlock: v6,
	})
	require.Error(t, err)
	assert.Equal(t, "InvalidSubnet.Conflict", apiCode(t, err))
}

func TestCreateSubnet_IPAMPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantPrefix string
		ipv6       bool
	}{
		{name: "ipv4", wantPrefix: "10.20.0.0/24"},
		{name: "ipv6", ipv6: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newSubnetFixture(t)
			ipam, err := f.client.CreateIpam(t.Context(), &ec2sdk.CreateIpamInput{})
			require.NoError(t, err)

			family := types.AddressFamilyIpv4
			if tt.ipv6 {
				family = types.AddressFamilyIpv6
			}

			pool, err := f.client.CreateIpamPool(t.Context(), &ec2sdk.CreateIpamPoolInput{
				IpamScopeId: ipam.Ipam.PrivateDefaultScopeId, AddressFamily: family,
			})
			require.NoError(t, err)

			in := &ec2sdk.CreateSubnetInput{VpcId: aws.String(f.vpcID)}
			if tt.ipv6 {
				in.Ipv6Native = aws.Bool(true)
				in.Ipv6IpamPoolId = pool.IpamPool.IpamPoolId
				in.Ipv6NetmaskLength = aws.Int32(64)
			} else {
				in.Ipv4IpamPoolId = pool.IpamPool.IpamPoolId
				in.Ipv4NetmaskLength = aws.Int32(24)
			}

			out, err := f.client.CreateSubnet(t.Context(), in)
			require.NoError(t, err)

			got := describeOneSubnet(t, f.client, out.Subnet.SubnetId)
			if tt.ipv6 {
				require.Len(t, got.Ipv6CidrBlockAssociationSet, 1)
				assert.True(t, netip.MustParsePrefix(f.block).Contains(
					netip.MustParsePrefix(aws.ToString(got.Ipv6CidrBlockAssociationSet[0].Ipv6CidrBlock)).Addr()))
			} else {
				assert.Equal(t, tt.wantPrefix, aws.ToString(got.CidrBlock))
			}

			allocs, err := f.client.GetIpamPoolAllocations(t.Context(), &ec2sdk.GetIpamPoolAllocationsInput{
				IpamPoolId: pool.IpamPool.IpamPoolId,
			})
			require.NoError(t, err)
			require.Len(t, allocs.IpamPoolAllocations, 1)
			assert.Equal(t, aws.ToString(out.Subnet.SubnetId), aws.ToString(allocs.IpamPoolAllocations[0].ResourceId))

			_, err = f.client.DeleteSubnet(t.Context(), &ec2sdk.DeleteSubnetInput{SubnetId: out.Subnet.SubnetId})
			require.NoError(t, err)

			allocs, err = f.client.GetIpamPoolAllocations(t.Context(), &ec2sdk.GetIpamPoolAllocationsInput{
				IpamPoolId: pool.IpamPool.IpamPoolId,
			})
			require.NoError(t, err)
			assert.Empty(t, allocs.IpamPoolAllocations)
		})
	}
}

func TestModifySubnetAttribute_Members(t *testing.T) {
	t.Parallel()

	yes := &types.AttributeBooleanValue{Value: aws.Bool(true)}

	tests := []struct {
		check    func(t *testing.T, s types.Subnet)
		name     string
		wantCode string
		in       ec2sdk.ModifySubnetAttributeInput
	}{
		{
			name: "dns64",
			in:   ec2sdk.ModifySubnetAttributeInput{EnableDns64: yes},
			check: func(t *testing.T, s types.Subnet) {
				t.Helper()
				assert.True(t, aws.ToBool(s.EnableDns64))
			},
		},
		{
			name: "assign ipv6",
			in:   ec2sdk.ModifySubnetAttributeInput{AssignIpv6AddressOnCreation: yes},
			check: func(t *testing.T, s types.Subnet) {
				t.Helper()
				assert.True(t, aws.ToBool(s.AssignIpv6AddressOnCreation))
			},
		},
		{
			name: "a record",
			in:   ec2sdk.ModifySubnetAttributeInput{EnableResourceNameDnsARecordOnLaunch: yes},
			check: func(t *testing.T, s types.Subnet) {
				t.Helper()
				assert.True(t, aws.ToBool(s.PrivateDnsNameOptionsOnLaunch.EnableResourceNameDnsARecord))
			},
		},
		{
			name: "aaaa record",
			in:   ec2sdk.ModifySubnetAttributeInput{EnableResourceNameDnsAAAARecordOnLaunch: yes},
			check: func(t *testing.T, s types.Subnet) {
				t.Helper()
				assert.True(t, aws.ToBool(s.PrivateDnsNameOptionsOnLaunch.EnableResourceNameDnsAAAARecord))
			},
		},
		{
			name: "hostname type",
			in:   ec2sdk.ModifySubnetAttributeInput{PrivateDnsHostnameTypeOnLaunch: types.HostnameTypeResourceName},
			check: func(t *testing.T, s types.Subnet) {
				t.Helper()
				assert.Equal(t, types.HostnameTypeResourceName, s.PrivateDnsNameOptionsOnLaunch.HostnameType)
			},
		},
		{
			name: "customer owned pool",
			in: ec2sdk.ModifySubnetAttributeInput{
				MapCustomerOwnedIpOnLaunch: yes, CustomerOwnedIpv4Pool: aws.String("ipv4pool-coip-1"),
			},
			check: func(t *testing.T, s types.Subnet) {
				t.Helper()
				assert.True(t, aws.ToBool(s.MapCustomerOwnedIpOnLaunch))
				assert.Equal(t, "ipv4pool-coip-1", aws.ToString(s.CustomerOwnedIpv4Pool))
			},
		},
		{
			name: "lni",
			in:   ec2sdk.ModifySubnetAttributeInput{EnableLniAtDeviceIndex: aws.Int32(2)},
			check: func(t *testing.T, s types.Subnet) {
				t.Helper()
				assert.Equal(t, int32(2), aws.ToInt32(s.EnableLniAtDeviceIndex))
			},
		},
		{
			name:     "two attributes",
			in:       ec2sdk.ModifySubnetAttributeInput{EnableDns64: yes, MapPublicIpOnLaunch: yes},
			wantCode: "InvalidParameterCombination",
		},
		{name: "no attribute", in: ec2sdk.ModifySubnetAttributeInput{}, wantCode: "MissingParameter"},
		{
			name:     "owned ip without pool",
			in:       ec2sdk.ModifySubnetAttributeInput{MapCustomerOwnedIpOnLaunch: yes},
			wantCode: "MissingParameter",
		},
		{
			name:     "bad hostname",
			in:       ec2sdk.ModifySubnetAttributeInput{PrivateDnsHostnameTypeOnLaunch: "bogus"},
			wantCode: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newSubnetFixture(t)
			sub, err := f.client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
				VpcId: aws.String(f.vpcID), CidrBlock: aws.String("10.20.1.0/24"),
			})
			require.NoError(t, err)

			in := tt.in
			in.SubnetId = sub.Subnet.SubnetId

			_, err = f.client.ModifySubnetAttribute(t.Context(), &in)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiCode(t, err))

				return
			}

			require.NoError(t, err)
			tt.check(t, describeOneSubnet(t, f.client, sub.Subnet.SubnetId))
		})
	}
}
