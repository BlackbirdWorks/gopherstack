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

// TestRealClient_DescribeCoipPoolsFilters covers DescribeCoipPools, which
// previously ignored Filters entirely.
func TestRealClient_DescribeCoipPoolsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	pool1, err := client.CreateCoipPool(t.Context(), &ec2sdk.CreateCoipPoolInput{
		LocalGatewayRouteTableId: aws.String("lgw-rtb-aaa"),
	})
	require.NoError(t, err)
	pool2, err := client.CreateCoipPool(t.Context(), &ec2sdk.CreateCoipPoolInput{
		LocalGatewayRouteTableId: aws.String("lgw-rtb-bbb"),
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name: "coip-pool.local-gateway-route-table-id",
			filters: []types.Filter{
				{Name: aws.String("coip-pool.local-gateway-route-table-id"), Values: []string{"lgw-rtb-aaa"}},
			},
			want: []string{aws.ToString(pool1.CoipPool.PoolId)},
		},
		{
			name: "coip-pool.pool-id",
			filters: []types.Filter{
				{Name: aws.String("coip-pool.pool-id"), Values: []string{aws.ToString(pool2.CoipPool.PoolId)}},
			},
			want: []string{aws.ToString(pool2.CoipPool.PoolId)},
		},
		{
			name: "no match",
			filters: []types.Filter{
				{Name: aws.String("coip-pool.local-gateway-route-table-id"), Values: []string{"missing"}},
			},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeCoipPools(t.Context(), &ec2sdk.DescribeCoipPoolsInput{Filters: tt.filters})
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.CoipPools))
			for _, p := range out.CoipPools {
				got = append(got, aws.ToString(p.PoolId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeLocalGatewaysFilters covers DescribeLocalGateways,
// which previously ignored Filters entirely. LocalGateways have no Create
// API (Outpost-provisioned), so this seeds them directly via the backend,
// matching the established convention for this resource family.
func TestRealClient_DescribeLocalGatewaysFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	lg1, err := backend.SeedLocalGateway(ec2.LocalGateway{
		OutpostArn: "arn:aws:outposts:us-east-1:000000000000:outpost/op-1",
	})
	require.NoError(t, err)
	lg2, err := backend.SeedLocalGateway(ec2.LocalGateway{
		OutpostArn: "arn:aws:outposts:us-east-1:000000000000:outpost/op-2",
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "local-gateway-id",
			filters: []types.Filter{{Name: aws.String("local-gateway-id"), Values: []string{lg1.LocalGatewayID}}},
			want:    []string{lg1.LocalGatewayID},
		},
		{
			name:    "outpost-arn",
			filters: []types.Filter{{Name: aws.String("outpost-arn"), Values: []string{lg2.OutpostArn}}},
			want:    []string{lg2.LocalGatewayID},
		},
		{
			name:    "owner-id",
			filters: []types.Filter{{Name: aws.String("owner-id"), Values: []string{"000000000000"}}},
			want:    []string{lg1.LocalGatewayID, lg2.LocalGatewayID},
		},
		{
			name:    "state",
			filters: []types.Filter{{Name: aws.String("state"), Values: []string{"unknown-state"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeLocalGateways(
				t.Context(), &ec2sdk.DescribeLocalGatewaysInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.LocalGateways))
			for _, lg := range out.LocalGateways {
				got = append(got, aws.ToString(lg.LocalGatewayId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeLocalGatewayVirtualInterfacesFilters covers
// DescribeLocalGatewayVirtualInterfaces, which previously ignored Filters
// entirely.
func TestRealClient_DescribeLocalGatewayVirtualInterfacesFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	lg, err := backend.SeedLocalGateway(ec2.LocalGateway{})
	require.NoError(t, err)

	vif1, err := backend.SeedLocalGatewayVirtualInterface(ec2.LocalGatewayVirtualInterface{
		LocalGatewayID: lg.LocalGatewayID,
		LocalAddress:   "169.254.1.1",
		PeerAddress:    "169.254.1.2",
		LocalBgpAsn:    64512,
		PeerBgpAsn:     65000,
		Vlan:           100,
	})
	require.NoError(t, err)
	vif2, err := backend.SeedLocalGatewayVirtualInterface(ec2.LocalGatewayVirtualInterface{
		LocalGatewayID: lg.LocalGatewayID,
		LocalAddress:   "169.254.2.1",
		PeerAddress:    "169.254.2.2",
		LocalBgpAsn:    64513,
		PeerBgpAsn:     65001,
		Vlan:           200,
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "local-address",
			filters: []types.Filter{{Name: aws.String("local-address"), Values: []string{"169.254.1.1"}}},
			want:    []string{vif1.LocalGatewayVirtualInterfaceID},
		},
		{
			name:    "peer-address",
			filters: []types.Filter{{Name: aws.String("peer-address"), Values: []string{"169.254.2.2"}}},
			want:    []string{vif2.LocalGatewayVirtualInterfaceID},
		},
		{
			name:    "local-bgp-asn",
			filters: []types.Filter{{Name: aws.String("local-bgp-asn"), Values: []string{"64512"}}},
			want:    []string{vif1.LocalGatewayVirtualInterfaceID},
		},
		{
			name:    "peer-bgp-asn",
			filters: []types.Filter{{Name: aws.String("peer-bgp-asn"), Values: []string{"65001"}}},
			want:    []string{vif2.LocalGatewayVirtualInterfaceID},
		},
		{
			name:    "vlan",
			filters: []types.Filter{{Name: aws.String("vlan"), Values: []string{"200"}}},
			want:    []string{vif2.LocalGatewayVirtualInterfaceID},
		},
		{
			name: "local-gateway-id",
			filters: []types.Filter{
				{Name: aws.String("local-gateway-id"), Values: []string{lg.LocalGatewayID}},
			},
			want: []string{vif1.LocalGatewayVirtualInterfaceID, vif2.LocalGatewayVirtualInterfaceID},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("vlan"), Values: []string{"999"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeLocalGatewayVirtualInterfaces(
				t.Context(), &ec2sdk.DescribeLocalGatewayVirtualInterfacesInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.LocalGatewayVirtualInterfaces))
			for _, v := range out.LocalGatewayVirtualInterfaces {
				got = append(got, aws.ToString(v.LocalGatewayVirtualInterfaceId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeLocalGatewayVirtualInterfaceGroupsFilters covers
// DescribeLocalGatewayVirtualInterfaceGroups, which previously ignored
// Filters entirely.
func TestRealClient_DescribeLocalGatewayVirtualInterfaceGroupsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	lg, err := backend.SeedLocalGateway(ec2.LocalGateway{})
	require.NoError(t, err)

	group1, err := backend.SeedLocalGatewayVirtualInterfaceGroup(ec2.LocalGatewayVirtualInterfaceGroup{
		LocalGatewayID: lg.LocalGatewayID,
	})
	require.NoError(t, err)
	group2, err := backend.SeedLocalGatewayVirtualInterfaceGroup(ec2.LocalGatewayVirtualInterfaceGroup{
		LocalGatewayID: lg.LocalGatewayID,
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name: "local-gateway-virtual-interface-group-id",
			filters: []types.Filter{
				{
					Name:   aws.String("local-gateway-virtual-interface-group-id"),
					Values: []string{group1.LocalGatewayVirtualInterfaceGroupID},
				},
			},
			want: []string{group1.LocalGatewayVirtualInterfaceGroupID},
		},
		{
			name:    "owner-id",
			filters: []types.Filter{{Name: aws.String("owner-id"), Values: []string{"000000000000"}}},
			want: []string{
				group1.LocalGatewayVirtualInterfaceGroupID,
				group2.LocalGatewayVirtualInterfaceGroupID,
			},
		},
		{
			name: "no match",
			filters: []types.Filter{
				{Name: aws.String("local-gateway-virtual-interface-group-id"), Values: []string{"missing"}},
			},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeLocalGatewayVirtualInterfaceGroups(
				t.Context(), &ec2sdk.DescribeLocalGatewayVirtualInterfaceGroupsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.LocalGatewayVirtualInterfaceGroups))
			for _, g := range out.LocalGatewayVirtualInterfaceGroups {
				got = append(got, aws.ToString(g.LocalGatewayVirtualInterfaceGroupId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
