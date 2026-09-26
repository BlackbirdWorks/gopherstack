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

// TestRealClient_DescribeTransitGatewayConnectsFilters covers
// DescribeTransitGatewayConnects, which previously ignored Filters entirely.
func TestRealClient_DescribeTransitGatewayConnectsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	tgw, att1, att2 := setupTGWWithTwoVPCAttachments(t, backend)
	conn1, err := backend.CreateTransitGatewayConnect(att1.TransitGatewayAttachmentID, tgw.ID)
	require.NoError(t, err)
	conn2, err := backend.CreateTransitGatewayConnect(att2.TransitGatewayAttachmentID, tgw.ID)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name: "transport-transit-gateway-attachment-id",
			filters: []types.Filter{
				{
					Name:   aws.String("transport-transit-gateway-attachment-id"),
					Values: []string{att2.TransitGatewayAttachmentID},
				},
			},
			want: []string{conn2.TransitGatewayAttachmentID},
		},
		{
			name: "transit-gateway-attachment-id",
			filters: []types.Filter{
				{Name: aws.String("transit-gateway-attachment-id"), Values: []string{conn1.TransitGatewayAttachmentID}},
			},
			want: []string{conn1.TransitGatewayAttachmentID},
		},
		{
			name:    "transit-gateway-id",
			filters: []types.Filter{{Name: aws.String("transit-gateway-id"), Values: []string{tgw.ID}}},
			want:    []string{conn1.TransitGatewayAttachmentID, conn2.TransitGatewayAttachmentID},
		},
		{
			name:    "state",
			filters: []types.Filter{{Name: aws.String("state"), Values: []string{"available"}}},
			want:    []string{conn1.TransitGatewayAttachmentID, conn2.TransitGatewayAttachmentID},
		},
		{
			name:    "options.protocol",
			filters: []types.Filter{{Name: aws.String("options.protocol"), Values: []string{"gre"}}},
			want:    []string{conn1.TransitGatewayAttachmentID, conn2.TransitGatewayAttachmentID},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("state"), Values: []string{"deleted"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeTransitGatewayConnects(
				t.Context(), &ec2sdk.DescribeTransitGatewayConnectsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.TransitGatewayConnects))
			for _, c := range out.TransitGatewayConnects {
				got = append(got, aws.ToString(c.TransitGatewayAttachmentId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeTransitGatewayConnectPeersFilters covers
// DescribeTransitGatewayConnectPeers, which previously ignored Filters entirely.
func TestRealClient_DescribeTransitGatewayConnectPeersFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	tgw, att1, _ := setupTGWWithTwoVPCAttachments(t, backend)
	conn, err := backend.CreateTransitGatewayConnect(att1.TransitGatewayAttachmentID, tgw.ID)
	require.NoError(t, err)

	peer1, err := backend.CreateTransitGatewayConnectPeer(conn.TransitGatewayAttachmentID, "169.254.6.1", "", nil, 0)
	require.NoError(t, err)
	peer2, err := backend.CreateTransitGatewayConnectPeer(conn.TransitGatewayAttachmentID, "169.254.6.2", "", nil, 0)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name: "transit-gateway-connect-peer-id",
			filters: []types.Filter{
				{
					Name:   aws.String("transit-gateway-connect-peer-id"),
					Values: []string{peer2.TransitGatewayConnectPeerID},
				},
			},
			want: []string{peer2.TransitGatewayConnectPeerID},
		},
		{
			name: "transit-gateway-attachment-id",
			filters: []types.Filter{
				{Name: aws.String("transit-gateway-attachment-id"), Values: []string{conn.TransitGatewayAttachmentID}},
			},
			want: []string{peer1.TransitGatewayConnectPeerID, peer2.TransitGatewayConnectPeerID},
		},
		{
			name:    "state",
			filters: []types.Filter{{Name: aws.String("state"), Values: []string{"available"}}},
			want:    []string{peer1.TransitGatewayConnectPeerID, peer2.TransitGatewayConnectPeerID},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("state"), Values: []string{"deleted"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeTransitGatewayConnectPeers(
				t.Context(), &ec2sdk.DescribeTransitGatewayConnectPeersInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.TransitGatewayConnectPeers))
			for _, p := range out.TransitGatewayConnectPeers {
				got = append(got, aws.ToString(p.TransitGatewayConnectPeerId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeTransitGatewayMulticastDomainsFilters covers
// DescribeTransitGatewayMulticastDomains, which previously ignored Filters
// entirely.
func TestRealClient_DescribeTransitGatewayMulticastDomainsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	tgw1, err := backend.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "mcast-tgw-1"})
	require.NoError(t, err)
	tgw2, err := backend.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "mcast-tgw-2"})
	require.NoError(t, err)

	domain1, err := backend.CreateTransitGatewayMulticastDomain(tgw1.ID, "", "", "", nil)
	require.NoError(t, err)
	domain2, err := backend.CreateTransitGatewayMulticastDomain(tgw2.ID, "", "", "", nil)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "transit-gateway-id",
			filters: []types.Filter{{Name: aws.String("transit-gateway-id"), Values: []string{tgw2.ID}}},
			want:    []string{domain2.ID},
		},
		{
			name: "transit-gateway-multicast-domain-id",
			filters: []types.Filter{
				{Name: aws.String("transit-gateway-multicast-domain-id"), Values: []string{domain1.ID}},
			},
			want: []string{domain1.ID},
		},
		{
			name:    "state",
			filters: []types.Filter{{Name: aws.String("state"), Values: []string{"available"}}},
			want:    []string{domain1.ID, domain2.ID},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("transit-gateway-id"), Values: []string{"tgw-nonexistent"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeTransitGatewayMulticastDomains(
				t.Context(), &ec2sdk.DescribeTransitGatewayMulticastDomainsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.TransitGatewayMulticastDomains))
			for _, d := range out.TransitGatewayMulticastDomains {
				got = append(got, aws.ToString(d.TransitGatewayMulticastDomainId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeTransitGatewayPeeringAttachmentsFilters covers
// DescribeTransitGatewayPeeringAttachments, which previously ignored Filters
// entirely.
func TestRealClient_DescribeTransitGatewayPeeringAttachmentsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	tgw1, err := backend.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "peer-tgw-1"})
	require.NoError(t, err)
	tgw2, err := backend.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "peer-tgw-2"})
	require.NoError(t, err)

	att1, err := backend.CreateTransitGatewayPeeringAttachment(tgw1.ID, "tgw-remote-1", "111111111111", "us-west-2")
	require.NoError(t, err)
	att2, err := backend.CreateTransitGatewayPeeringAttachment(tgw2.ID, "tgw-remote-2", "222222222222", "us-west-2")
	require.NoError(t, err)
	require.NoError(t, backend.CreateTags(
		[]string{att1.TransitGatewayAttachmentID}, map[string]string{"Name": "att1"},
	))

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "transit-gateway-id",
			filters: []types.Filter{{Name: aws.String("transit-gateway-id"), Values: []string{tgw1.ID}}},
			want:    []string{att1.TransitGatewayAttachmentID},
		},
		{
			name:    "remote-owner-id",
			filters: []types.Filter{{Name: aws.String("remote-owner-id"), Values: []string{"222222222222"}}},
			want:    []string{att2.TransitGatewayAttachmentID},
		},
		{
			name:    "local-owner-id",
			filters: []types.Filter{{Name: aws.String("local-owner-id"), Values: []string{backend.AccountID}}},
			want:    []string{att1.TransitGatewayAttachmentID, att2.TransitGatewayAttachmentID},
		},
		{
			name: "transit-gateway-attachment-id",
			filters: []types.Filter{
				{Name: aws.String("transit-gateway-attachment-id"), Values: []string{att2.TransitGatewayAttachmentID}},
			},
			want: []string{att2.TransitGatewayAttachmentID},
		},
		{
			name:    "state",
			filters: []types.Filter{{Name: aws.String("state"), Values: []string{"pendingAcceptance"}}},
			want:    []string{att1.TransitGatewayAttachmentID, att2.TransitGatewayAttachmentID},
		},
		{
			name:    "tag",
			filters: []types.Filter{{Name: aws.String("tag:Name"), Values: []string{"att1"}}},
			want:    []string{att1.TransitGatewayAttachmentID},
		},
		{
			name:    "tag-key",
			filters: []types.Filter{{Name: aws.String("tag-key"), Values: []string{"Name"}}},
			want:    []string{att1.TransitGatewayAttachmentID},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeTransitGatewayPeeringAttachments(
				t.Context(), &ec2sdk.DescribeTransitGatewayPeeringAttachmentsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.TransitGatewayPeeringAttachments))
			for _, a := range out.TransitGatewayPeeringAttachments {
				got = append(got, aws.ToString(a.TransitGatewayAttachmentId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_GetTransitGatewayAttachmentPropagationsFilters covers
// GetTransitGatewayAttachmentPropagations, which previously ignored Filters
// entirely.
func TestRealClient_GetTransitGatewayAttachmentPropagationsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	tgw, att1, _ := setupTGWWithTwoVPCAttachments(t, backend)

	rt1, err := backend.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)
	rt2, err := backend.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)

	_, err = backend.EnableTransitGatewayRouteTablePropagation(rt1.RouteTableID, att1.TransitGatewayAttachmentID)
	require.NoError(t, err)
	_, err = backend.EnableTransitGatewayRouteTablePropagation(rt2.RouteTableID, att1.TransitGatewayAttachmentID)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name: "transit-gateway-route-table-id",
			filters: []types.Filter{
				{Name: aws.String("transit-gateway-route-table-id"), Values: []string{rt2.RouteTableID}},
			},
			want: []string{rt2.RouteTableID},
		},
		{
			name: "no match",
			filters: []types.Filter{
				{Name: aws.String("transit-gateway-route-table-id"), Values: []string{"tgw-rtb-nonexistent"}},
			},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.GetTransitGatewayAttachmentPropagations(
				t.Context(), &ec2sdk.GetTransitGatewayAttachmentPropagationsInput{
					TransitGatewayAttachmentId: aws.String(att1.TransitGatewayAttachmentID),
					Filters:                    tt.filters,
				},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.TransitGatewayAttachmentPropagations))
			for _, p := range out.TransitGatewayAttachmentPropagations {
				got = append(got, aws.ToString(p.TransitGatewayRouteTableId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_GetTransitGatewayMulticastDomainAssociationsFilters covers
// GetTransitGatewayMulticastDomainAssociations, which previously ignored
// Filters entirely.
func TestRealClient_GetTransitGatewayMulticastDomainAssociationsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	tgw, att1, att2 := setupTGWWithTwoVPCAttachments(t, backend)

	domain, err := backend.CreateTransitGatewayMulticastDomain(tgw.ID, "", "", "", nil)
	require.NoError(t, err)

	subnetA, err := backend.CreateSubnet(att1.VpcID, "10.0.2.0/24", "us-east-1a")
	require.NoError(t, err)
	subnetB, err := backend.CreateSubnet(att2.VpcID, "10.1.2.0/24", "us-east-1a")
	require.NoError(t, err)

	_, err = backend.AssociateTransitGatewayMulticastDomain(
		domain.ID, att1.TransitGatewayAttachmentID, []string{subnetA.ID},
	)
	require.NoError(t, err)
	_, err = backend.AssociateTransitGatewayMulticastDomain(
		domain.ID, att2.TransitGatewayAttachmentID, []string{subnetB.ID},
	)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "resource-id",
			filters: []types.Filter{{Name: aws.String("resource-id"), Values: []string{att1.VpcID}}},
			want:    []string{subnetA.ID},
		},
		{
			name:    "resource-type",
			filters: []types.Filter{{Name: aws.String("resource-type"), Values: []string{"vpc"}}},
			want:    []string{subnetA.ID, subnetB.ID},
		},
		{
			name:    "subnet-id",
			filters: []types.Filter{{Name: aws.String("subnet-id"), Values: []string{subnetB.ID}}},
			want:    []string{subnetB.ID},
		},
		{
			name: "transit-gateway-attachment-id",
			filters: []types.Filter{
				{Name: aws.String("transit-gateway-attachment-id"), Values: []string{att2.TransitGatewayAttachmentID}},
			},
			want: []string{subnetB.ID},
		},
		{
			name:    "state",
			filters: []types.Filter{{Name: aws.String("state"), Values: []string{"associated"}}},
			want:    []string{subnetA.ID, subnetB.ID},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.GetTransitGatewayMulticastDomainAssociations(
				t.Context(), &ec2sdk.GetTransitGatewayMulticastDomainAssociationsInput{
					TransitGatewayMulticastDomainId: aws.String(domain.ID),
					Filters:                         tt.filters,
				},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.MulticastDomainAssociations))
			for _, a := range out.MulticastDomainAssociations {
				require.NotNil(t, a.Subnet)
				got = append(got, aws.ToString(a.Subnet.SubnetId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_GetTransitGatewayPrefixListReferencesFilters covers
// GetTransitGatewayPrefixListReferences, which previously ignored Filters
// entirely.
func TestRealClient_GetTransitGatewayPrefixListReferencesFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	tgw, att1, att2 := setupTGWWithTwoVPCAttachments(t, backend)

	rt, err := backend.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)

	pl1, err := backend.CreateManagedPrefixList("pl1", "IPv4", 5, nil)
	require.NoError(t, err)
	pl2, err := backend.CreateManagedPrefixList("pl2", "IPv4", 5, nil)
	require.NoError(t, err)

	_, err = backend.CreateTransitGatewayPrefixListReference(
		rt.RouteTableID, pl1.PrefixListID, att1.TransitGatewayAttachmentID, false,
	)
	require.NoError(t, err)
	_, err = backend.CreateTransitGatewayPrefixListReference(
		rt.RouteTableID, pl2.PrefixListID, att2.TransitGatewayAttachmentID, true,
	)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "prefix-list-id",
			filters: []types.Filter{{Name: aws.String("prefix-list-id"), Values: []string{pl2.PrefixListID}}},
			want:    []string{pl2.PrefixListID},
		},
		{
			name: "attachment.transit-gateway-attachment-id",
			filters: []types.Filter{
				{
					Name:   aws.String("attachment.transit-gateway-attachment-id"),
					Values: []string{att1.TransitGatewayAttachmentID},
				},
			},
			want: []string{pl1.PrefixListID},
		},
		{
			name:    "is-blackhole",
			filters: []types.Filter{{Name: aws.String("is-blackhole"), Values: []string{"true"}}},
			want:    []string{pl2.PrefixListID},
		},
		{
			name:    "state",
			filters: []types.Filter{{Name: aws.String("state"), Values: []string{"available"}}},
			want:    []string{pl1.PrefixListID, pl2.PrefixListID},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.GetTransitGatewayPrefixListReferences(
				t.Context(), &ec2sdk.GetTransitGatewayPrefixListReferencesInput{
					TransitGatewayRouteTableId: aws.String(rt.RouteTableID),
					Filters:                    tt.filters,
				},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.TransitGatewayPrefixListReferences))
			for _, r := range out.TransitGatewayPrefixListReferences {
				got = append(got, aws.ToString(r.PrefixListId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_GetTransitGatewayRouteTableAssociationsFilters covers
// GetTransitGatewayRouteTableAssociations, which previously ignored Filters
// entirely.
func TestRealClient_GetTransitGatewayRouteTableAssociationsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	tgw, att1, att2 := setupTGWWithTwoVPCAttachments(t, backend)

	rt, err := backend.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)

	_, err = backend.AssociateTransitGatewayRouteTable(rt.RouteTableID, att1.TransitGatewayAttachmentID)
	require.NoError(t, err)

	rt2, err := backend.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)

	_, err = backend.AssociateTransitGatewayRouteTable(rt2.RouteTableID, att2.TransitGatewayAttachmentID)
	require.NoError(t, err)

	tests := []struct {
		name       string
		routeTable string
		filters    []types.Filter
		want       []string
	}{
		{
			name:       "resource-type",
			routeTable: rt.RouteTableID,
			filters:    []types.Filter{{Name: aws.String("resource-type"), Values: []string{"vpc"}}},
			want:       []string{att1.TransitGatewayAttachmentID},
		},
		{
			name:       "transit-gateway-attachment-id match",
			routeTable: rt.RouteTableID,
			filters: []types.Filter{
				{Name: aws.String("transit-gateway-attachment-id"), Values: []string{att1.TransitGatewayAttachmentID}},
			},
			want: []string{att1.TransitGatewayAttachmentID},
		},
		{
			name:       "transit-gateway-attachment-id no match",
			routeTable: rt.RouteTableID,
			filters: []types.Filter{
				{Name: aws.String("transit-gateway-attachment-id"), Values: []string{att2.TransitGatewayAttachmentID}},
			},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.GetTransitGatewayRouteTableAssociations(
				t.Context(), &ec2sdk.GetTransitGatewayRouteTableAssociationsInput{
					TransitGatewayRouteTableId: aws.String(tt.routeTable),
					Filters:                    tt.filters,
				},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.Associations))
			for _, a := range out.Associations {
				got = append(got, aws.ToString(a.TransitGatewayAttachmentId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_GetTransitGatewayRouteTablePropagationsFilters covers
// GetTransitGatewayRouteTablePropagations, which previously ignored Filters
// entirely.
func TestRealClient_GetTransitGatewayRouteTablePropagationsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	tgw, att1, att2 := setupTGWWithTwoVPCAttachments(t, backend)

	rt, err := backend.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)

	_, err = backend.EnableTransitGatewayRouteTablePropagation(rt.RouteTableID, att1.TransitGatewayAttachmentID)
	require.NoError(t, err)
	_, err = backend.EnableTransitGatewayRouteTablePropagation(rt.RouteTableID, att2.TransitGatewayAttachmentID)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "resource-id",
			filters: []types.Filter{{Name: aws.String("resource-id"), Values: []string{att2.VpcID}}},
			want:    []string{att2.TransitGatewayAttachmentID},
		},
		{
			name:    "resource-type",
			filters: []types.Filter{{Name: aws.String("resource-type"), Values: []string{"vpc"}}},
			want:    []string{att1.TransitGatewayAttachmentID, att2.TransitGatewayAttachmentID},
		},
		{
			name: "transit-gateway-attachment-id",
			filters: []types.Filter{
				{Name: aws.String("transit-gateway-attachment-id"), Values: []string{att1.TransitGatewayAttachmentID}},
			},
			want: []string{att1.TransitGatewayAttachmentID},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.GetTransitGatewayRouteTablePropagations(
				t.Context(), &ec2sdk.GetTransitGatewayRouteTablePropagationsInput{
					TransitGatewayRouteTableId: aws.String(rt.RouteTableID),
					Filters:                    tt.filters,
				},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.TransitGatewayRouteTablePropagations))
			for _, p := range out.TransitGatewayRouteTablePropagations {
				got = append(got, aws.ToString(p.TransitGatewayAttachmentId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
