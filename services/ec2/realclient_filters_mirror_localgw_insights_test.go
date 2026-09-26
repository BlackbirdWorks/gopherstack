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

// TestRealClient_DescribePlacementGroupsFilters covers DescribePlacementGroups,
// which previously ignored Filters entirely.
func TestRealClient_DescribePlacementGroupsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	_, err := backend.CreatePlacementGroup("pg-cluster", "cluster", nil)
	require.NoError(t, err)
	_, err = backend.CreatePlacementGroup("pg-spread", "spread", nil)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "group-name",
			filters: []types.Filter{{Name: aws.String("group-name"), Values: []string{"pg-cluster"}}},
			want:    []string{"pg-cluster"},
		},
		{
			name:    "strategy",
			filters: []types.Filter{{Name: aws.String("strategy"), Values: []string{"spread"}}},
			want:    []string{"pg-spread"},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("strategy"), Values: []string{"partition"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribePlacementGroups(
				t.Context(), &ec2sdk.DescribePlacementGroupsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.PlacementGroups))
			for _, pg := range out.PlacementGroups {
				got = append(got, aws.ToString(pg.GroupName))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeFleetsFilters covers DescribeFleets, which
// previously ignored Filters entirely.
func TestRealClient_DescribeFleetsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	instant, _, err := backend.CreateFleet(ec2.FleetCreateInput{Type: "instant", TotalTargetCapacity: 0})
	require.NoError(t, err)
	maintain, _, err := backend.CreateFleet(ec2.FleetCreateInput{Type: "maintain", TotalTargetCapacity: 0})
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "type",
			filters: []types.Filter{{Name: aws.String("type"), Values: []string{"instant"}}},
			want:    []string{instant.FleetID},
		},
		{
			name:    "fleet-state",
			filters: []types.Filter{{Name: aws.String("fleet-state"), Values: []string{"active"}}},
			want:    []string{instant.FleetID, maintain.FleetID},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("type"), Values: []string{"request"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeFleets(t.Context(), &ec2sdk.DescribeFleetsInput{Filters: tt.filters})
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.Fleets))
			for _, f := range out.Fleets {
				got = append(got, aws.ToString(f.FleetId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeSpotPriceHistoryFilters covers
// DescribeSpotPriceHistory, which previously ignored Filters entirely.
func TestRealClient_DescribeSpotPriceHistoryFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	baseline, err := client.DescribeSpotPriceHistory(t.Context(), &ec2sdk.DescribeSpotPriceHistoryInput{
		InstanceTypes: []types.InstanceType{types.InstanceTypeT3Micro, types.InstanceTypeM5Large},
	})
	require.NoError(t, err)
	require.NotEmpty(t, baseline.SpotPriceHistory)

	var t3Price string
	for _, r := range baseline.SpotPriceHistory {
		if r.InstanceType == types.InstanceTypeT3Micro {
			t3Price = aws.ToString(r.SpotPrice)

			break
		}
	}
	require.NotEmpty(t, t3Price)

	tests := []struct {
		name        string
		wantAllType types.InstanceType
		filters     []types.Filter
		wantEmpty   bool
	}{
		{
			name:        "instance-type",
			filters:     []types.Filter{{Name: aws.String("instance-type"), Values: []string{"t3.micro"}}},
			wantAllType: types.InstanceTypeT3Micro,
		},
		{
			name:      "spot-price",
			filters:   []types.Filter{{Name: aws.String("spot-price"), Values: []string{t3Price}}},
			wantEmpty: false,
		},
		{
			name:      "no match",
			filters:   []types.Filter{{Name: aws.String("instance-type"), Values: []string{"c5.xlarge"}}},
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeSpotPriceHistory(t.Context(), &ec2sdk.DescribeSpotPriceHistoryInput{
				InstanceTypes: []types.InstanceType{types.InstanceTypeT3Micro, types.InstanceTypeM5Large},
				Filters:       tt.filters,
			})
			require.NoError(t, reqErr)

			if tt.wantEmpty {
				assert.Empty(t, out.SpotPriceHistory)

				return
			}

			require.NotEmpty(t, out.SpotPriceHistory)

			if tt.wantAllType != "" {
				for _, r := range out.SpotPriceHistory {
					assert.Equal(t, tt.wantAllType, r.InstanceType)
				}
			}
		})
	}
}

// TestRealClient_DescribeReservedInstancesFilters covers
// DescribeReservedInstances, which previously ignored Filters entirely.
func TestRealClient_DescribeReservedInstancesFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	backend.SeedReservedInstancesOffering(
		"rio-filter-001", "t3.micro", "us-east-1a", "Linux/UNIX", "No Upfront", "standard",
		31536000, 0, 0.05,
	)
	backend.SeedReservedInstancesOffering(
		"rio-filter-002", "m5.large", "us-east-1b", "Windows", "No Upfront", "convertible",
		94608000, 0, 0.2,
	)

	ri1, err := backend.PurchaseReservedInstancesOffering("rio-filter-001", 1)
	require.NoError(t, err)
	ri2, err := backend.PurchaseReservedInstancesOffering("rio-filter-002", 2)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "instance-type",
			filters: []types.Filter{{Name: aws.String("instance-type"), Values: []string{"m5.large"}}},
			want:    []string{ri2.ReservedInstancesID},
		},
		{
			name:    "availability-zone",
			filters: []types.Filter{{Name: aws.String("availability-zone"), Values: []string{"us-east-1a"}}},
			want:    []string{ri1.ReservedInstancesID},
		},
		{
			name: "duration",
			filters: []types.Filter{
				{Name: aws.String("duration"), Values: []string{"31536000"}},
			},
			want: []string{ri1.ReservedInstancesID},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("instance-type"), Values: []string{"c5.xlarge"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeReservedInstances(
				t.Context(), &ec2sdk.DescribeReservedInstancesInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.ReservedInstances))
			for _, ri := range out.ReservedInstances {
				got = append(got, aws.ToString(ri.ReservedInstancesId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeTrafficMirrorFiltersFilters covers
// DescribeTrafficMirrorFilters, which previously ignored Filters entirely.
func TestRealClient_DescribeTrafficMirrorFiltersFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	f1, err := backend.CreateTrafficMirrorFilter("batch3-filter-one", nil)
	require.NoError(t, err)
	f2, err := backend.CreateTrafficMirrorFilter("batch3-filter-two", nil)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "description",
			filters: []types.Filter{{Name: aws.String("description"), Values: []string{"batch3-filter-two"}}},
			want:    []string{f2.TrafficMirrorFilterID},
		},
		{
			name: "traffic-mirror-filter-id",
			filters: []types.Filter{
				{Name: aws.String("traffic-mirror-filter-id"), Values: []string{f1.TrafficMirrorFilterID}},
			},
			want: []string{f1.TrafficMirrorFilterID},
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

			out, reqErr := client.DescribeTrafficMirrorFilters(
				t.Context(), &ec2sdk.DescribeTrafficMirrorFiltersInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.TrafficMirrorFilters))
			for _, f := range out.TrafficMirrorFilters {
				got = append(got, aws.ToString(f.TrafficMirrorFilterId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeTrafficMirrorSessionsFilters covers
// DescribeTrafficMirrorSessions, which previously ignored Filters entirely.
func TestRealClient_DescribeTrafficMirrorSessionsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	filter, err := backend.CreateTrafficMirrorFilter("session-owner-filter", nil)
	require.NoError(t, err)
	target, err := backend.CreateTrafficMirrorTarget("eni-batch3-target", "", "target-desc", nil)
	require.NoError(t, err)

	s1, err := backend.CreateTrafficMirrorSession(
		"eni-batch3-a", target.TrafficMirrorTargetID, filter.TrafficMirrorFilterID, "session-a", 1, nil,
	)
	require.NoError(t, err)
	s2, err := backend.CreateTrafficMirrorSession(
		"eni-batch3-b", target.TrafficMirrorTargetID, filter.TrafficMirrorFilterID, "session-b", 2, nil,
	)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "network-interface-id",
			filters: []types.Filter{{Name: aws.String("network-interface-id"), Values: []string{"eni-batch3-a"}}},
			want:    []string{s1.TrafficMirrorSessionID},
		},
		{
			name:    "session-number",
			filters: []types.Filter{{Name: aws.String("session-number"), Values: []string{"2"}}},
			want:    []string{s2.TrafficMirrorSessionID},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("session-number"), Values: []string{"99"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeTrafficMirrorSessions(
				t.Context(), &ec2sdk.DescribeTrafficMirrorSessionsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.TrafficMirrorSessions))
			for _, s := range out.TrafficMirrorSessions {
				got = append(got, aws.ToString(s.TrafficMirrorSessionId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeTrafficMirrorTargetsFilters covers
// DescribeTrafficMirrorTargets, which previously ignored Filters entirely.
func TestRealClient_DescribeTrafficMirrorTargetsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	t1, err := backend.CreateTrafficMirrorTarget("eni-batch3-target1", "", "target-one", nil)
	require.NoError(t, err)
	t2, err := backend.CreateTrafficMirrorTarget(
		"", "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/net/nlb/abc", "target-two", nil,
	)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "network-interface-id",
			filters: []types.Filter{{Name: aws.String("network-interface-id"), Values: []string{"eni-batch3-target1"}}},
			want:    []string{t1.TrafficMirrorTargetID},
		},
		{
			name: "network-load-balancer-arn",
			filters: []types.Filter{{
				Name:   aws.String("network-load-balancer-arn"),
				Values: []string{"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/net/nlb/abc"},
			}},
			want: []string{t2.TrafficMirrorTargetID},
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

			out, reqErr := client.DescribeTrafficMirrorTargets(
				t.Context(), &ec2sdk.DescribeTrafficMirrorTargetsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.TrafficMirrorTargets))
			for _, tgt := range out.TrafficMirrorTargets {
				got = append(got, aws.ToString(tgt.TrafficMirrorTargetId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeVpcEndpointAssociationsFilters covers
// DescribeVpcEndpointAssociations, which previously ignored Filters
// entirely. This backend models a VPC endpoint association as the endpoint
// itself, so only vpc-endpoint-id (the one documented filter with backing
// data) is exercised.
func TestRealClient_DescribeVpcEndpointAssociationsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	vpc, err := backend.CreateVpc("10.40.0.0/16", "default")
	require.NoError(t, err)

	ep1, err := backend.CreateVpcEndpoint(vpc.ID, "com.amazonaws.us-east-1.s3", "Gateway", nil)
	require.NoError(t, err)
	ep2, err := backend.CreateVpcEndpoint(vpc.ID, "com.amazonaws.us-east-1.dynamodb", "Gateway", nil)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "vpc-endpoint-id",
			filters: []types.Filter{{Name: aws.String("vpc-endpoint-id"), Values: []string{ep1.ID}}},
			want:    []string{ep1.ID},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("vpc-endpoint-id"), Values: []string{"vpce-nonexistent"}}},
			want:    []string{},
		},
	}

	_ = ep2

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeVpcEndpointAssociations(
				t.Context(), &ec2sdk.DescribeVpcEndpointAssociationsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.VpcEndpointAssociations))
			for _, a := range out.VpcEndpointAssociations {
				got = append(got, aws.ToString(a.VpcEndpointId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeLocalGatewayRouteTablesFilters covers
// DescribeLocalGatewayRouteTables, which previously ignored Filters
// entirely.
func TestRealClient_DescribeLocalGatewayRouteTablesFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	lg, err := backend.SeedLocalGateway(ec2.LocalGateway{})
	require.NoError(t, err)

	rt1, err := backend.CreateLocalGatewayRouteTable(lg.LocalGatewayID, "coip")
	require.NoError(t, err)
	rt2, err := backend.CreateLocalGatewayRouteTable(lg.LocalGatewayID, "direct-vpc-routing")
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name: "local-gateway-route-table-id",
			filters: []types.Filter{
				{Name: aws.String("local-gateway-route-table-id"), Values: []string{rt2.LocalGatewayRouteTableID}},
			},
			want: []string{rt2.LocalGatewayRouteTableID},
		},
		{
			name:    "local-gateway-id",
			filters: []types.Filter{{Name: aws.String("local-gateway-id"), Values: []string{lg.LocalGatewayID}}},
			want:    []string{rt1.LocalGatewayRouteTableID, rt2.LocalGatewayRouteTableID},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("local-gateway-id"), Values: []string{"lgw-nonexistent"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeLocalGatewayRouteTables(
				t.Context(), &ec2sdk.DescribeLocalGatewayRouteTablesInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.LocalGatewayRouteTables))
			for _, rt := range out.LocalGatewayRouteTables {
				got = append(got, aws.ToString(rt.LocalGatewayRouteTableId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeLocalGatewayRouteTableVpcAssociationsFilters covers
// DescribeLocalGatewayRouteTableVpcAssociations, which previously ignored
// Filters entirely.
func TestRealClient_DescribeLocalGatewayRouteTableVpcAssociationsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	lg, err := backend.SeedLocalGateway(ec2.LocalGateway{})
	require.NoError(t, err)
	rt, err := backend.CreateLocalGatewayRouteTable(lg.LocalGatewayID, "coip")
	require.NoError(t, err)

	vpc1, err := backend.CreateVpc("10.41.0.0/16", "default")
	require.NoError(t, err)
	vpc2, err := backend.CreateVpc("10.42.0.0/16", "default")
	require.NoError(t, err)

	a1, err := backend.CreateLocalGatewayRouteTableVpcAssociation(rt.LocalGatewayRouteTableID, vpc1.ID)
	require.NoError(t, err)
	a2, err := backend.CreateLocalGatewayRouteTableVpcAssociation(rt.LocalGatewayRouteTableID, vpc2.ID)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "vpc-id",
			filters: []types.Filter{{Name: aws.String("vpc-id"), Values: []string{vpc2.ID}}},
			want:    []string{a2.LocalGatewayRouteTableVpcAssociationID},
		},
		{
			name: "local-gateway-route-table-vpc-association-id",
			filters: []types.Filter{{
				Name:   aws.String("local-gateway-route-table-vpc-association-id"),
				Values: []string{a1.LocalGatewayRouteTableVpcAssociationID},
			}},
			want: []string{a1.LocalGatewayRouteTableVpcAssociationID},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("vpc-id"), Values: []string{"vpc-nonexistent"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeLocalGatewayRouteTableVpcAssociations(
				t.Context(), &ec2sdk.DescribeLocalGatewayRouteTableVpcAssociationsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.LocalGatewayRouteTableVpcAssociations))
			for _, a := range out.LocalGatewayRouteTableVpcAssociations {
				got = append(got, aws.ToString(a.LocalGatewayRouteTableVpcAssociationId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociationsFilters
// covers DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations,
// which previously ignored Filters entirely.
func TestRealClient_DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociationsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	lg, err := backend.SeedLocalGateway(ec2.LocalGateway{})
	require.NoError(t, err)
	rt, err := backend.CreateLocalGatewayRouteTable(lg.LocalGatewayID, "coip")
	require.NoError(t, err)

	vifGroup1, err := backend.SeedLocalGatewayVirtualInterfaceGroup(ec2.LocalGatewayVirtualInterfaceGroup{
		LocalGatewayID: lg.LocalGatewayID,
	})
	require.NoError(t, err)
	vifGroup2, err := backend.SeedLocalGatewayVirtualInterfaceGroup(ec2.LocalGatewayVirtualInterfaceGroup{
		LocalGatewayID: lg.LocalGatewayID,
	})
	require.NoError(t, err)

	a1, err := backend.CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation(
		rt.LocalGatewayRouteTableID, vifGroup1.LocalGatewayVirtualInterfaceGroupID,
	)
	require.NoError(t, err)
	a2, err := backend.CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation(
		rt.LocalGatewayRouteTableID, vifGroup2.LocalGatewayVirtualInterfaceGroupID,
	)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name: "local-gateway-route-table-virtual-interface-group-id",
			filters: []types.Filter{{
				Name:   aws.String("local-gateway-route-table-virtual-interface-group-id"),
				Values: []string{vifGroup2.LocalGatewayVirtualInterfaceGroupID},
			}},
			want: []string{a2.LocalGatewayRouteTableVirtualInterfaceGroupAssociationID},
		},
		{
			name: "local-gateway-route-table-virtual-interface-group-association-id",
			filters: []types.Filter{{
				Name:   aws.String("local-gateway-route-table-virtual-interface-group-association-id"),
				Values: []string{a1.LocalGatewayRouteTableVirtualInterfaceGroupAssociationID},
			}},
			want: []string{a1.LocalGatewayRouteTableVirtualInterfaceGroupAssociationID},
		},
		{
			name: "no match",
			filters: []types.Filter{{
				Name:   aws.String("local-gateway-route-table-virtual-interface-group-id"),
				Values: []string{"lgw-vif-grp-nonexistent"},
			}},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations(
				t.Context(),
				&ec2sdk.DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociationsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.LocalGatewayRouteTableVirtualInterfaceGroupAssociations))
			for _, a := range out.LocalGatewayRouteTableVirtualInterfaceGroupAssociations {
				got = append(
					got, aws.ToString(a.LocalGatewayRouteTableVirtualInterfaceGroupAssociationId),
				)
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeNetworkInsightsPathsFilters covers
// DescribeNetworkInsightsPaths, which previously ignored Filters entirely.
func TestRealClient_DescribeNetworkInsightsPathsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	p1, err := backend.CreateNetworkInsightsPath("eni-batch3-src1", "eni-batch3-dst1", "tcp", 443)
	require.NoError(t, err)
	p2, err := backend.CreateNetworkInsightsPath("eni-batch3-src2", "eni-batch3-dst2", "udp", 53)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "source",
			filters: []types.Filter{{Name: aws.String("source"), Values: []string{"eni-batch3-src1"}}},
			want:    []string{p1.NetworkInsightsPathID},
		},
		{
			name:    "protocol",
			filters: []types.Filter{{Name: aws.String("protocol"), Values: []string{"udp"}}},
			want:    []string{p2.NetworkInsightsPathID},
		},
		{
			name:    "destination",
			filters: []types.Filter{{Name: aws.String("destination"), Values: []string{"eni-batch3-dst1"}}},
			want:    []string{p1.NetworkInsightsPathID},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("protocol"), Values: []string{"icmp"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeNetworkInsightsPaths(
				t.Context(), &ec2sdk.DescribeNetworkInsightsPathsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.NetworkInsightsPaths))
			for _, p := range out.NetworkInsightsPaths {
				got = append(got, aws.ToString(p.NetworkInsightsPathId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeNetworkInsightsAnalysesFilters covers
// DescribeNetworkInsightsAnalyses, which previously ignored Filters
// entirely. StartNetworkInsightsAnalysis always produces
// status=succeeded/path-found=true, so this exercises match-all and
// match-none behaviour rather than divergent per-analysis values.
func TestRealClient_DescribeNetworkInsightsAnalysesFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	p1, err := backend.CreateNetworkInsightsPath("eni-batch3-nia1", "eni-batch3-nia2", "tcp", 443)
	require.NoError(t, err)
	a1, err := backend.StartNetworkInsightsAnalysis(p1.NetworkInsightsPathID)
	require.NoError(t, err)

	p2, err := backend.CreateNetworkInsightsPath("eni-batch3-nia3", "eni-batch3-nia4", "tcp", 443)
	require.NoError(t, err)
	a2, err := backend.StartNetworkInsightsAnalysis(p2.NetworkInsightsPathID)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "status matches both",
			filters: []types.Filter{{Name: aws.String("status"), Values: []string{"succeeded"}}},
			want: []string{
				a1.NetworkInsightsAnalysisID, a2.NetworkInsightsAnalysisID,
			},
		},
		{
			name:    "path-found matches both",
			filters: []types.Filter{{Name: aws.String("path-found"), Values: []string{"true"}}},
			want: []string{
				a1.NetworkInsightsAnalysisID, a2.NetworkInsightsAnalysisID,
			},
		},
		{
			name:    "status no match",
			filters: []types.Filter{{Name: aws.String("status"), Values: []string{"running"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeNetworkInsightsAnalyses(
				t.Context(), &ec2sdk.DescribeNetworkInsightsAnalysesInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.NetworkInsightsAnalyses))
			for _, a := range out.NetworkInsightsAnalyses {
				got = append(got, aws.ToString(a.NetworkInsightsAnalysisId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeVerifiedAccessEndpointsScalarParams covers
// DescribeVerifiedAccessEndpoints, which previously ignored the
// VerifiedAccessGroupId and VerifiedAccessInstanceId scalar request
// parameters entirely.
func TestRealClient_DescribeVerifiedAccessEndpointsScalarParams(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	inst1, err := backend.CreateVerifiedAccessInstance("instance-one")
	require.NoError(t, err)
	inst2, err := backend.CreateVerifiedAccessInstance("instance-two")
	require.NoError(t, err)

	grp1, err := backend.CreateVerifiedAccessGroup(inst1.VerifiedAccessInstanceID, "group-one")
	require.NoError(t, err)
	grp2, err := backend.CreateVerifiedAccessGroup(inst2.VerifiedAccessInstanceID, "group-two")
	require.NoError(t, err)

	ep1, err := backend.CreateVerifiedAccessEndpoint(grp1.VerifiedAccessGroupID, "network-interface", "ep-one")
	require.NoError(t, err)
	ep2, err := backend.CreateVerifiedAccessEndpoint(grp2.VerifiedAccessGroupID, "network-interface", "ep-two")
	require.NoError(t, err)

	tests := []struct {
		name       string
		groupID    string
		instanceID string
		want       []string
	}{
		{name: "by group", groupID: grp1.VerifiedAccessGroupID, want: []string{ep1.VerifiedAccessEndpointID}},
		{
			name: "by instance", instanceID: inst2.VerifiedAccessInstanceID,
			want: []string{ep2.VerifiedAccessEndpointID},
		},
		{name: "unknown instance", instanceID: "vai-nonexistent", want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := &ec2sdk.DescribeVerifiedAccessEndpointsInput{}
			if tt.groupID != "" {
				in.VerifiedAccessGroupId = aws.String(tt.groupID)
			}

			if tt.instanceID != "" {
				in.VerifiedAccessInstanceId = aws.String(tt.instanceID)
			}

			out, reqErr := client.DescribeVerifiedAccessEndpoints(t.Context(), in)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.VerifiedAccessEndpoints))
			for _, ep := range out.VerifiedAccessEndpoints {
				got = append(got, aws.ToString(ep.VerifiedAccessEndpointId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeVerifiedAccessGroupsScalarParam covers
// DescribeVerifiedAccessGroups, which previously ignored the
// VerifiedAccessInstanceId scalar request parameter entirely.
func TestRealClient_DescribeVerifiedAccessGroupsScalarParam(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	inst1, err := backend.CreateVerifiedAccessInstance("instance-one")
	require.NoError(t, err)
	inst2, err := backend.CreateVerifiedAccessInstance("instance-two")
	require.NoError(t, err)

	grp1, err := backend.CreateVerifiedAccessGroup(inst1.VerifiedAccessInstanceID, "group-one")
	require.NoError(t, err)
	grp2, err := backend.CreateVerifiedAccessGroup(inst2.VerifiedAccessInstanceID, "group-two")
	require.NoError(t, err)

	tests := []struct {
		name       string
		instanceID string
		want       []string
	}{
		{
			name: "by instance one", instanceID: inst1.VerifiedAccessInstanceID,
			want: []string{grp1.VerifiedAccessGroupID},
		},
		{
			name: "by instance two", instanceID: inst2.VerifiedAccessInstanceID,
			want: []string{grp2.VerifiedAccessGroupID},
		},
		{name: "unknown instance", instanceID: "vai-nonexistent", want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeVerifiedAccessGroups(
				t.Context(), &ec2sdk.DescribeVerifiedAccessGroupsInput{
					VerifiedAccessInstanceId: aws.String(tt.instanceID),
				},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.VerifiedAccessGroups))
			for _, g := range out.VerifiedAccessGroups {
				got = append(got, aws.ToString(g.VerifiedAccessGroupId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
