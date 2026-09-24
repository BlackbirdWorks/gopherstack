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

// TestRealClient_DescribeClientVpnAuthorizationRulesFilters covers
// DescribeClientVpnAuthorizationRules, which previously ignored Filters
// entirely.
func TestRealClient_DescribeClientVpnAuthorizationRulesFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	ep, err := backend.CreateClientVpnEndpointWithOptions(
		"10.10.0.0/22", "cvpn-authz", nil, ec2.ClientVpnEndpointOptions{},
	)
	require.NoError(t, err)

	require.NoError(t, backend.AuthorizeClientVpnIngress(ep.ClientVpnEndpointID, "10.0.0.0/24", "office"))
	require.NoError(t, backend.AuthorizeClientVpnIngress(ep.ClientVpnEndpointID, "10.1.0.0/24", "vpn-users"))

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "destination-cidr",
			filters: []types.Filter{{Name: aws.String("destination-cidr"), Values: []string{"10.1.0.0/24"}}},
			want:    []string{"10.1.0.0/24"},
		},
		{
			name:    "description",
			filters: []types.Filter{{Name: aws.String("description"), Values: []string{"office"}}},
			want:    []string{"10.0.0.0/24"},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("description"), Values: []string{"nonexistent"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeClientVpnAuthorizationRules(
				t.Context(), &ec2sdk.DescribeClientVpnAuthorizationRulesInput{
					ClientVpnEndpointId: aws.String(ep.ClientVpnEndpointID),
					Filters:             tt.filters,
				},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.AuthorizationRules))
			for _, r := range out.AuthorizationRules {
				got = append(got, aws.ToString(r.DestinationCidr))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeClientVpnRoutesFilters covers DescribeClientVpnRoutes,
// which previously ignored Filters entirely.
func TestRealClient_DescribeClientVpnRoutesFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	ep, err := backend.CreateClientVpnEndpointWithOptions(
		"10.20.0.0/22", "cvpn-routes", nil, ec2.ClientVpnEndpointOptions{},
	)
	require.NoError(t, err)

	require.NoError(t, backend.CreateClientVpnRoute(ep.ClientVpnEndpointID, "10.5.0.0/24", "", "route-1"))
	require.NoError(t, backend.CreateClientVpnRoute(ep.ClientVpnEndpointID, "10.6.0.0/24", "", "route-2"))

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "destination-cidr",
			filters: []types.Filter{{Name: aws.String("destination-cidr"), Values: []string{"10.6.0.0/24"}}},
			want:    []string{"10.6.0.0/24"},
		},
		{
			name:    "origin",
			filters: []types.Filter{{Name: aws.String("origin"), Values: []string{"add-route"}}},
			want:    []string{"10.5.0.0/24", "10.6.0.0/24"},
		},
		{
			name:    "target-subnet no match",
			filters: []types.Filter{{Name: aws.String("target-subnet"), Values: []string{"subnet-nonexistent"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeClientVpnRoutes(
				t.Context(), &ec2sdk.DescribeClientVpnRoutesInput{
					ClientVpnEndpointId: aws.String(ep.ClientVpnEndpointID),
					Filters:             tt.filters,
				},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.Routes))
			for _, r := range out.Routes {
				got = append(got, aws.ToString(r.DestinationCidr))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_ClientVpnRoute_TargetSubnetFilter covers the regression
// fixed by gopherstack MegaBatch45: CreateClientVpnRoute previously dropped
// TargetVpcSubnetId, so a route could never be found by destination-cidr
// plus target-subnet filters together.
func TestRealClient_ClientVpnRoute_TargetSubnetFilter(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	ep, err := backend.CreateClientVpnEndpointWithOptions(
		"10.30.0.0/22", "cvpn-target-subnet", nil, ec2.ClientVpnEndpointOptions{},
	)
	require.NoError(t, err)

	_, err = client.CreateClientVpnRoute(t.Context(), &ec2sdk.CreateClientVpnRouteInput{
		ClientVpnEndpointId:  aws.String(ep.ClientVpnEndpointID),
		DestinationCidrBlock: aws.String("0.0.0.0/0"),
		TargetVpcSubnetId:    aws.String("subnet-target"),
	})
	require.NoError(t, err)

	out, err := client.DescribeClientVpnRoutes(t.Context(), &ec2sdk.DescribeClientVpnRoutesInput{
		ClientVpnEndpointId: aws.String(ep.ClientVpnEndpointID),
		Filters: []types.Filter{
			{Name: aws.String("destination-cidr"), Values: []string{"0.0.0.0/0"}},
			{Name: aws.String("target-subnet"), Values: []string{"subnet-target"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Routes, 1)
	assert.Equal(t, "0.0.0.0/0", aws.ToString(out.Routes[0].DestinationCidr))
	assert.Equal(t, "subnet-target", aws.ToString(out.Routes[0].TargetSubnet))
}

// TestRealClient_DescribeClientVpnTargetNetworksFilters covers
// DescribeClientVpnTargetNetworks, which previously ignored Filters entirely.
func TestRealClient_DescribeClientVpnTargetNetworksFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	ep, err := backend.CreateClientVpnEndpointWithOptions(
		"10.30.0.0/22", "cvpn-targets", nil, ec2.ClientVpnEndpointOptions{},
	)
	require.NoError(t, err)

	vpc1, err := backend.CreateVpc("10.31.0.0/16", "default")
	require.NoError(t, err)
	subnet1, err := backend.CreateSubnet(vpc1.ID, "10.31.1.0/24", "us-east-1a")
	require.NoError(t, err)

	vpc2, err := backend.CreateVpc("10.32.0.0/16", "default")
	require.NoError(t, err)
	subnet2, err := backend.CreateSubnet(vpc2.ID, "10.32.1.0/24", "us-east-1a")
	require.NoError(t, err)

	assoc1, err := backend.AssociateClientVpnTargetNetwork(ep.ClientVpnEndpointID, subnet1.ID)
	require.NoError(t, err)
	_, err = backend.AssociateClientVpnTargetNetwork(ep.ClientVpnEndpointID, subnet2.ID)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "association-id",
			filters: []types.Filter{{Name: aws.String("association-id"), Values: []string{assoc1}}},
			want:    []string{subnet1.ID},
		},
		{
			name:    "target-network-id",
			filters: []types.Filter{{Name: aws.String("target-network-id"), Values: []string{subnet2.ID}}},
			want:    []string{subnet2.ID},
		},
		{
			name:    "vpc-id",
			filters: []types.Filter{{Name: aws.String("vpc-id"), Values: []string{vpc2.ID}}},
			want:    []string{subnet2.ID},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeClientVpnTargetNetworks(
				t.Context(), &ec2sdk.DescribeClientVpnTargetNetworksInput{
					ClientVpnEndpointId: aws.String(ep.ClientVpnEndpointID),
					Filters:             tt.filters,
				},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.ClientVpnTargetNetworks))
			for _, n := range out.ClientVpnTargetNetworks {
				got = append(got, aws.ToString(n.TargetNetworkId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeHostsFilters covers DescribeHosts, which previously
// ignored Filter.N entirely.
func TestRealClient_DescribeHostsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	hosts1, err := backend.AllocateHosts("us-east-1a", "m5.large", 1, "on", "off")
	require.NoError(t, err)
	host1 := hosts1[0]

	hosts2, err := backend.AllocateHosts("us-east-1b", "c5.xlarge", 1, "off", "off")
	require.NoError(t, err)
	host2 := hosts2[0]

	require.NoError(t, backend.CreateTags([]string{host1.HostID}, map[string]string{"Name": "host1"}))

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "availability-zone",
			filters: []types.Filter{{Name: aws.String("availability-zone"), Values: []string{"us-east-1b"}}},
			want:    []string{host2.HostID},
		},
		{
			name:    "instance-type",
			filters: []types.Filter{{Name: aws.String("instance-type"), Values: []string{"m5.large"}}},
			want:    []string{host1.HostID},
		},
		{
			name:    "auto-placement",
			filters: []types.Filter{{Name: aws.String("auto-placement"), Values: []string{"on"}}},
			want:    []string{host1.HostID},
		},
		{
			name:    "state",
			filters: []types.Filter{{Name: aws.String("state"), Values: []string{"available"}}},
			want:    []string{host1.HostID, host2.HostID},
		},
		{
			name:    "tag-key",
			filters: []types.Filter{{Name: aws.String("tag-key"), Values: []string{"Name"}}},
			want:    []string{host1.HostID},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeHosts(
				t.Context(), &ec2sdk.DescribeHostsInput{Filter: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.Hosts))
			for _, hh := range out.Hosts {
				got = append(got, aws.ToString(hh.HostId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
