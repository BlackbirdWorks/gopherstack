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

// TestRealClient_DescribeTransitGatewayVpcAttachmentsFilters covers
// gopherstack-rwwvt: DescribeTransitGatewayVpcAttachments previously ignored
// every Filter.N and only honoured TransitGatewayAttachmentIds.
func TestRealClient_DescribeTransitGatewayVpcAttachmentsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	tgw, att1, att2 := setupTGWWithTwoVPCAttachments(t, backend)
	require.NoError(t, backend.CreateTags(
		[]string{att1.TransitGatewayAttachmentID}, map[string]string{"Name": "att1"},
	))

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "vpc-id",
			filters: []types.Filter{{Name: aws.String("vpc-id"), Values: []string{att1.VpcID}}},
			want:    []string{att1.TransitGatewayAttachmentID},
		},
		{
			name: "transit-gateway-attachment-id",
			filters: []types.Filter{
				{Name: aws.String("transit-gateway-attachment-id"), Values: []string{att2.TransitGatewayAttachmentID}},
			},
			want: []string{att2.TransitGatewayAttachmentID},
		},
		{
			name:    "transit-gateway-id",
			filters: []types.Filter{{Name: aws.String("transit-gateway-id"), Values: []string{tgw.ID}}},
			want:    []string{att1.TransitGatewayAttachmentID, att2.TransitGatewayAttachmentID},
		},
		{
			name:    "state",
			filters: []types.Filter{{Name: aws.String("state"), Values: []string{"available"}}},
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
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("vpc-id"), Values: []string{"vpc-nonexistent"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.DescribeTransitGatewayVpcAttachments(
				t.Context(), &ec2sdk.DescribeTransitGatewayVpcAttachmentsInput{Filters: tt.filters},
			)
			require.NoError(t, err)

			got := make([]string, 0, len(out.TransitGatewayVpcAttachments))
			for _, a := range out.TransitGatewayVpcAttachments {
				got = append(got, aws.ToString(a.TransitGatewayAttachmentId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeTransitGatewayAttachmentsFilters covers the sibling
// op DescribeTransitGatewayAttachments, which shared the same
// filters-ignored bug.
func TestRealClient_DescribeTransitGatewayAttachmentsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	_, att1, att2 := setupTGWWithTwoVPCAttachments(t, backend)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "resource-id",
			filters: []types.Filter{{Name: aws.String("resource-id"), Values: []string{att1.VpcID}}},
			want:    []string{att1.TransitGatewayAttachmentID},
		},
		{
			name:    "resource-type",
			filters: []types.Filter{{Name: aws.String("resource-type"), Values: []string{"vpc"}}},
			want:    []string{att1.TransitGatewayAttachmentID, att2.TransitGatewayAttachmentID},
		},
		{
			name: "transit-gateway-attachment-id",
			filters: []types.Filter{
				{Name: aws.String("transit-gateway-attachment-id"), Values: []string{att2.TransitGatewayAttachmentID}},
			},
			want: []string{att2.TransitGatewayAttachmentID},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.DescribeTransitGatewayAttachments(
				t.Context(), &ec2sdk.DescribeTransitGatewayAttachmentsInput{Filters: tt.filters},
			)
			require.NoError(t, err)

			got := make([]string, 0, len(out.TransitGatewayAttachments))
			for _, a := range out.TransitGatewayAttachments {
				got = append(got, aws.ToString(a.TransitGatewayAttachmentId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeClientVpnEndpointsFilters covers
// DescribeClientVpnEndpoints, which previously ignored Filters entirely.
func TestRealClient_DescribeClientVpnEndpointsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	ep1, err := backend.CreateClientVpnEndpointWithOptions(
		"10.10.0.0/22", "tcp-endpoint", nil, ec2.ClientVpnEndpointOptions{TransportProtocol: "tcp"},
	)
	require.NoError(t, err)
	ep2, err := backend.CreateClientVpnEndpointWithOptions(
		"10.20.0.0/22", "udp-endpoint", nil, ec2.ClientVpnEndpointOptions{TransportProtocol: "udp"},
	)
	require.NoError(t, err)
	require.NoError(t, backend.CreateTags(
		[]string{ep1.ClientVpnEndpointID}, map[string]string{"Name": "ep1"},
	))

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "endpoint-id",
			filters: []types.Filter{{Name: aws.String("endpoint-id"), Values: []string{ep2.ClientVpnEndpointID}}},
			want:    []string{ep2.ClientVpnEndpointID},
		},
		{
			name:    "transport-protocol",
			filters: []types.Filter{{Name: aws.String("transport-protocol"), Values: []string{"tcp"}}},
			want:    []string{ep1.ClientVpnEndpointID},
		},
		{
			name:    "tag",
			filters: []types.Filter{{Name: aws.String("tag:Name"), Values: []string{"ep1"}}},
			want:    []string{ep1.ClientVpnEndpointID},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeClientVpnEndpoints(
				t.Context(), &ec2sdk.DescribeClientVpnEndpointsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.ClientVpnEndpoints))
			for _, e := range out.ClientVpnEndpoints {
				got = append(got, aws.ToString(e.ClientVpnEndpointId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeVpnConnectionsFilters covers DescribeVpnConnections,
// which previously ignored Filters entirely.
func TestRealClient_DescribeVpnConnectionsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	vgw, err := backend.CreateVpnGateway("ipsec.1", 0)
	require.NoError(t, err)

	cgw1, err := backend.CreateCustomerGateway("ipsec.1", "203.0.113.1", "65000")
	require.NoError(t, err)
	cgw2, err := backend.CreateCustomerGateway("ipsec.1", "203.0.113.2", "65001")
	require.NoError(t, err)

	conn1, err := backend.CreateVpnConnection("ipsec.1", cgw1.CustomerGatewayID, vgw.VpnGatewayID)
	require.NoError(t, err)
	conn2, err := backend.CreateVpnConnection("ipsec.1", cgw2.CustomerGatewayID, vgw.VpnGatewayID)
	require.NoError(t, err)
	require.NoError(t, backend.CreateTags(
		[]string{conn1.VpnConnectionID}, map[string]string{"Name": "conn1"},
	))

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name: "customer-gateway-id",
			filters: []types.Filter{
				{Name: aws.String("customer-gateway-id"), Values: []string{cgw2.CustomerGatewayID}},
			},
			want: []string{conn2.VpnConnectionID},
		},
		{
			name:    "vpn-connection-id",
			filters: []types.Filter{{Name: aws.String("vpn-connection-id"), Values: []string{conn1.VpnConnectionID}}},
			want:    []string{conn1.VpnConnectionID},
		},
		{
			name:    "state",
			filters: []types.Filter{{Name: aws.String("state"), Values: []string{"available"}}},
			want:    []string{conn1.VpnConnectionID, conn2.VpnConnectionID},
		},
		{
			name:    "tag",
			filters: []types.Filter{{Name: aws.String("tag:Name"), Values: []string{"conn1"}}},
			want:    []string{conn1.VpnConnectionID},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeVpnConnections(
				t.Context(), &ec2sdk.DescribeVpnConnectionsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.VpnConnections))
			for _, c := range out.VpnConnections {
				got = append(got, aws.ToString(c.VpnConnectionId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeVpcEndpointServicesFilters covers the "service-type"
// Filter on DescribeVpcEndpointServices' static service catalogue.
func TestRealClient_DescribeVpcEndpointServicesFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	tests := []struct {
		name        string
		serviceType string
		want        []string
	}{
		{
			name:        "gateway",
			serviceType: "Gateway",
			want: []string{
				"com.amazonaws.us-east-1.s3",
				"com.amazonaws.us-east-1.dynamodb",
			},
		},
		{
			name:        "interface",
			serviceType: "Interface",
			want: []string{
				"com.amazonaws.us-east-1.ec2",
				"com.amazonaws.us-east-1.ec2messages",
				"com.amazonaws.us-east-1.ssm",
				"com.amazonaws.us-east-1.ssmmessages",
				"com.amazonaws.us-east-1.kms",
				"com.amazonaws.us-east-1.secretsmanager",
				"com.amazonaws.us-east-1.sts",
				"com.amazonaws.us-east-1.logs",
				"com.amazonaws.us-east-1.monitoring",
				"com.amazonaws.us-east-1.elasticloadbalancing",
				"com.amazonaws.us-east-1.lambda",
				"com.amazonaws.us-east-1.sqs",
				"com.amazonaws.us-east-1.sns",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.DescribeVpcEndpointServices(
				t.Context(), &ec2sdk.DescribeVpcEndpointServicesInput{
					Filters: []types.Filter{{Name: aws.String("service-type"), Values: []string{tt.serviceType}}},
				},
			)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.want, out.ServiceNames)
		})
	}
}
