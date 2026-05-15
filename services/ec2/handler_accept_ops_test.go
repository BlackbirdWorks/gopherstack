package ec2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestHandler_AcceptAddressTransfer verifies AcceptAddressTransfer routing and response.
func TestHandler_AcceptAddressTransfer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *ec2.InMemoryBackend)
		name     string
		body     string
		wantBody string
		wantCode int
	}{
		{
			name:     "missing_address",
			body:     "Action=AcceptAddressTransfer&Version=2016-11-15",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidParameterValue",
		},
		{
			name:     "not_found",
			body:     "Action=AcceptAddressTransfer&Version=2016-11-15&Address=1.2.3.4",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidAddressTransfer.NotFound",
		},
		{
			name: "accept_pending_transfer",
			body: "Action=AcceptAddressTransfer&Version=2016-11-15&Address=54.10.20.30",
			setup: func(b *ec2.InMemoryBackend) {
				b.AddAddressTransferInternal(&ec2.AddressTransfer{
					AllocationID:        "eipalloc-abc123",
					PublicIP:            "54.10.20.30",
					TransferAccountID:   "111122223333",
					TransferOfferStatus: "pending",
				})
			},
			wantCode: http.StatusOK,
			wantBody: "AcceptAddressTransferResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(bk)
			}

			h := ec2.NewHandler(bk)
			h.AccountID = "000000000000"
			h.Region = "us-east-1"

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// TestHandler_AcceptCapacityReservationBillingOwnership verifies ACRBO routing.
func TestHandler_AcceptCapacityReservationBillingOwnership(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *ec2.InMemoryBackend)
		name     string
		body     string
		wantBody string
		wantCode int
	}{
		{
			name:     "missing_id",
			body:     "Action=AcceptCapacityReservationBillingOwnership&Version=2016-11-15",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidParameterValue",
		},
		{
			name:     "not_found",
			body:     "Action=AcceptCapacityReservationBillingOwnership&Version=2016-11-15&CapacityReservationId=cr-notexist",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidCapacityReservationId.NotFound",
		},
		{
			name: "accept_transfers_state_to_active",
			body: "Action=AcceptCapacityReservationBillingOwnership&Version=2016-11-15&CapacityReservationId=cr-abc123",
			setup: func(b *ec2.InMemoryBackend) {
				b.AddCapacityReservationInternal(&ec2.CapacityReservation{
					CapacityReservationID:  "cr-abc123",
					InstanceType:           "t3.micro",
					AvailabilityZone:       "us-east-1a",
					AvailableInstanceCount: 2,
					TotalInstanceCount:     2,
					State:                  "pending",
				})
			},
			wantCode: http.StatusOK,
			wantBody: "AcceptCapacityReservationBillingOwnershipResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(bk)
			}

			h := ec2.NewHandler(bk)
			h.AccountID = "000000000000"
			h.Region = "us-east-1"

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// TestHandler_AcceptReservedInstancesExchangeQuote verifies exchange quote acceptance.
func TestHandler_AcceptReservedInstancesExchangeQuote(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantBody string
		wantCode int
	}{
		{
			name:     "missing_reserved_instance_ids",
			body:     "Action=AcceptReservedInstancesExchangeQuote&Version=2016-11-15",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidParameterValue",
		},
		{
			name:     "accept_with_one_id",
			body:     "Action=AcceptReservedInstancesExchangeQuote&Version=2016-11-15&ReservedInstanceId.1=ri-abc123",
			wantCode: http.StatusOK,
			wantBody: "AcceptReservedInstancesExchangeQuoteResponse",
		},
		{
			name: "accept_with_multiple_ids",
			body: "Action=AcceptReservedInstancesExchangeQuote&Version=2016-11-15" +
				"&ReservedInstanceId.1=ri-abc123&ReservedInstanceId.2=ri-def456",
			wantCode: http.StatusOK,
			wantBody: "exchangeId",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// TestHandler_AcceptTransitGatewayMulticastDomainAssociations tests multicast domain acceptance.
func TestHandler_AcceptTransitGatewayMulticastDomainAssociations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantBody string
		wantCode int
	}{
		{
			name: "missing_domain_id",
			body: "Action=AcceptTransitGatewayMulticastDomainAssociations&Version=2016-11-15" +
				"&TransitGatewayAttachmentId=tgw-attach-123",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidParameterValue",
		},
		{
			name: "missing_attachment_id",
			body: "Action=AcceptTransitGatewayMulticastDomainAssociations&Version=2016-11-15" +
				"&TransitGatewayMulticastDomainId=tgw-mcast-123",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidParameterValue",
		},
		{
			name: "accept_with_subnets",
			body: "Action=AcceptTransitGatewayMulticastDomainAssociations&Version=2016-11-15" +
				"&TransitGatewayMulticastDomainId=tgw-mcast-123" +
				"&TransitGatewayAttachmentId=tgw-attach-456" +
				"&SubnetIds.1=subnet-abc" +
				"&SubnetIds.2=subnet-def",
			wantCode: http.StatusOK,
			wantBody: "AcceptTransitGatewayMulticastDomainAssociationsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// TestHandler_AcceptTransitGatewayPeeringAttachment tests TGW peering attachment acceptance.
func TestHandler_AcceptTransitGatewayPeeringAttachment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *ec2.InMemoryBackend)
		name     string
		body     string
		wantBody string
		wantCode int
	}{
		{
			name:     "missing_attachment_id",
			body:     "Action=AcceptTransitGatewayPeeringAttachment&Version=2016-11-15",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidParameterValue",
		},
		{
			name: "not_found",
			body: "Action=AcceptTransitGatewayPeeringAttachment&Version=2016-11-15" +
				"&TransitGatewayAttachmentId=tgw-attach-notexist",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidTransitGatewayAttachmentID.NotFound",
		},
		{
			name: "accept_peering_attachment",
			body: "Action=AcceptTransitGatewayPeeringAttachment&Version=2016-11-15&TransitGatewayAttachmentId=tgw-attach-abc",
			setup: func(b *ec2.InMemoryBackend) {
				b.AddTGWPeeringAttachmentInternal(&ec2.TransitGatewayPeeringAttachment{
					TransitGatewayAttachmentID: "tgw-attach-abc",
					RequesterTransitGatewayID:  "tgw-req-111",
					AccepterTransitGatewayID:   "tgw-acc-222",
					State:                      "pendingAcceptance",
				})
			},
			wantCode: http.StatusOK,
			wantBody: "AcceptTransitGatewayPeeringAttachmentResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(bk)
			}

			h := ec2.NewHandler(bk)
			h.AccountID = "000000000000"
			h.Region = "us-east-1"

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// TestHandler_AcceptTransitGatewayVpcAttachment tests TGW VPC attachment acceptance.
func TestHandler_AcceptTransitGatewayVpcAttachment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *ec2.InMemoryBackend)
		name     string
		body     string
		wantBody string
		wantCode int
	}{
		{
			name:     "missing_attachment_id",
			body:     "Action=AcceptTransitGatewayVpcAttachment&Version=2016-11-15",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidParameterValue",
		},
		{
			name: "not_found",
			body: "Action=AcceptTransitGatewayVpcAttachment&Version=2016-11-15" +
				"&TransitGatewayAttachmentId=tgw-attach-notexist",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidTransitGatewayAttachmentID.NotFound",
		},
		{
			name: "accept_vpc_attachment",
			body: "Action=AcceptTransitGatewayVpcAttachment&Version=2016-11-15&TransitGatewayAttachmentId=tgw-attach-vpc1",
			setup: func(b *ec2.InMemoryBackend) {
				b.AddTGWVpcAttachmentInternal(&ec2.TransitGatewayVpcAttachment{
					TransitGatewayAttachmentID: "tgw-attach-vpc1",
					TransitGatewayID:           "tgw-111",
					VpcID:                      "vpc-abc",
					State:                      "pendingAcceptance",
				})
			},
			wantCode: http.StatusOK,
			wantBody: "AcceptTransitGatewayVpcAttachmentResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(bk)
			}

			h := ec2.NewHandler(bk)
			h.AccountID = "000000000000"
			h.Region = "us-east-1"

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// TestHandler_AcceptVpcEndpointConnections tests VPC endpoint connection acceptance.
func TestHandler_AcceptVpcEndpointConnections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantBody string
		wantCode int
	}{
		{
			name:     "missing_service_id",
			body:     "Action=AcceptVpcEndpointConnections&Version=2016-11-15&VpcEndpointId.1=vpce-abc",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidParameterValue",
		},
		{
			name:     "missing_endpoint_ids",
			body:     "Action=AcceptVpcEndpointConnections&Version=2016-11-15&ServiceId=vpce-svc-abc",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidParameterValue",
		},
		{
			name: "accept_endpoint_connections",
			body: "Action=AcceptVpcEndpointConnections&Version=2016-11-15" +
				"&ServiceId=vpce-svc-abc" +
				"&VpcEndpointId.1=vpce-111" +
				"&VpcEndpointId.2=vpce-222",
			wantCode: http.StatusOK,
			wantBody: "AcceptVpcEndpointConnectionsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// TestHandler_AcceptVpcPeeringConnection tests VPC peering connection acceptance.
func TestHandler_AcceptVpcPeeringConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *ec2.InMemoryBackend)
		name     string
		body     string
		wantBody string
		wantCode int
	}{
		{
			name:     "missing_connection_id",
			body:     "Action=AcceptVpcPeeringConnection&Version=2016-11-15",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidParameterValue",
		},
		{
			name:     "not_found",
			body:     "Action=AcceptVpcPeeringConnection&Version=2016-11-15&VpcPeeringConnectionId=pcx-notexist",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidVpcPeeringConnectionID.NotFound",
		},
		{
			name: "accept_peering_connection",
			body: "Action=AcceptVpcPeeringConnection&Version=2016-11-15&VpcPeeringConnectionId=pcx-abc123",
			setup: func(b *ec2.InMemoryBackend) {
				b.AddVpcPeeringConnectionInternal(&ec2.VpcPeeringConnection{
					VpcPeeringConnectionID: "pcx-abc123",
					RequesterVpcID:         "vpc-req",
					AccepterVpcID:          "vpc-acc",
					State:                  "pending-acceptance",
				})
			},
			wantCode: http.StatusOK,
			wantBody: "AcceptVpcPeeringConnectionResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(bk)
			}

			h := ec2.NewHandler(bk)
			h.AccountID = "000000000000"
			h.Region = "us-east-1"

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// TestHandler_AdvertiseByoipCidr tests BYOIP CIDR advertisement.
func TestHandler_AdvertiseByoipCidr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *ec2.Handler)
		name     string
		body     string
		wantBody string
		wantCode int
	}{
		{
			name:     "missing_cidr",
			body:     "Action=AdvertiseByoipCidr&Version=2016-11-15",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidParameterValue",
		},
		{
			name:     "advertise_cidr",
			body:     "Action=AdvertiseByoipCidr&Version=2016-11-15&Cidr=203.0.113.0/24",
			wantCode: http.StatusOK,
			wantBody: "AdvertiseByoipCidrResponse",
		},
		{
			name: "advertise_cidr_idempotent",
			setup: func(h *ec2.Handler) {
				// Seed an existing advertised CIDR so the handler processes it again.
				rec := postForm(
					t,
					h,
					"Action=AdvertiseByoipCidr&Version=2016-11-15&Cidr=203.0.113.0/24",
				)
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     "Action=AdvertiseByoipCidr&Version=2016-11-15&Cidr=203.0.113.0/24",
			wantCode: http.StatusOK,
			wantBody: "advertised",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// TestHandler_AllocateHosts tests Dedicated Host allocation.
func TestHandler_AllocateHosts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantBody string
		wantCode int
	}{
		{
			name:     "missing_availability_zone",
			body:     "Action=AllocateHosts&Version=2016-11-15&InstanceType=t3.micro&Quantity=1",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidParameterValue",
		},
		{
			name:     "missing_instance_type",
			body:     "Action=AllocateHosts&Version=2016-11-15&AvailabilityZone=us-east-1a&Quantity=1",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidParameterValue",
		},
		{
			name:     "allocate_single_host",
			body:     "Action=AllocateHosts&Version=2016-11-15&AvailabilityZone=us-east-1a&InstanceType=t3.micro&Quantity=1",
			wantCode: http.StatusOK,
			wantBody: "AllocateHostsResponse",
		},
		{
			name:     "allocate_multiple_hosts",
			body:     "Action=AllocateHosts&Version=2016-11-15&AvailabilityZone=us-east-1b&InstanceType=m5.large&Quantity=3",
			wantCode: http.StatusOK,
			wantBody: "h-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// TestHandler_NewAcceptOps_GetSupportedOperations verifies new operations are listed.
func TestHandler_NewAcceptOps_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newHandler()
	ops := h.GetSupportedOperations()

	expectedOps := []string{
		"AcceptAddressTransfer",
		"AcceptCapacityReservationBillingOwnership",
		"AcceptReservedInstancesExchangeQuote",
		"AcceptTransitGatewayMulticastDomainAssociations",
		"AcceptTransitGatewayPeeringAttachment",
		"AcceptTransitGatewayVpcAttachment",
		"AcceptVpcEndpointConnections",
		"AcceptVpcPeeringConnection",
		"AdvertiseByoipCidr",
		"AllocateHosts",
	}

	opsSet := make(map[string]bool, len(ops))
	for _, op := range ops {
		opsSet[op] = true
	}

	for _, expected := range expectedOps {
		assert.True(
			t,
			opsSet[expected],
			"expected operation %q in GetSupportedOperations()",
			expected,
		)
	}
}

// TestHandler_AcceptVpcPeeringConnection_StateTransition verifies state is set to active.
func TestHandler_AcceptVpcPeeringConnection_StateTransition(t *testing.T) {
	t.Parallel()

	bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	bk.AddVpcPeeringConnectionInternal(&ec2.VpcPeeringConnection{
		VpcPeeringConnectionID: "pcx-transition",
		RequesterVpcID:         "vpc-req",
		AccepterVpcID:          "vpc-acc",
		State:                  "pending-acceptance",
	})

	h := ec2.NewHandler(bk)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	rec := postForm(
		t,
		h,
		"Action=AcceptVpcPeeringConnection&Version=2016-11-15&VpcPeeringConnectionId=pcx-transition",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "active")
}

// TestHandler_AllocateHosts_PersistenceRoundTrip verifies hosts survive snapshot/restore.
func TestHandler_AllocateHosts_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(bk)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	// Allocate some hosts.
	rec := postForm(
		t,
		h,
		"Action=AllocateHosts&Version=2016-11-15&AvailabilityZone=us-east-1a&InstanceType=t3.micro&Quantity=2",
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Snapshot and restore.
	snap := bk.Snapshot()
	require.NotNil(t, snap)

	bk2 := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, bk2.Restore(snap))

	// Allocate again in restored backend - should work fine.
	h2 := ec2.NewHandler(bk2)
	h2.AccountID = "000000000000"
	h2.Region = "us-east-1"

	rec2 := postForm(
		t,
		h2,
		"Action=AllocateHosts&Version=2016-11-15&AvailabilityZone=us-east-1a&InstanceType=m5.large&Quantity=1",
	)
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "h-")
}
