package ec2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ec2 "github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestRefinement1_Reset verifies that Backend.Reset() clears all resource maps
// and repopulates the default VPC, subnet, and security group.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	// Add resources.
	b.AddAddressTransferInternal(&ec2.AddressTransfer{PublicIP: "1.2.3.4"})
	b.AddCapacityReservationInternal(&ec2.CapacityReservation{CapacityReservationID: "cr-1"})
	b.AddVpcPeeringConnectionInternal(&ec2.VpcPeeringConnection{VpcPeeringConnectionID: "pcx-1"})
	b.AddByoipCidrInternal(&ec2.ByoipCidr{Cidr: "203.0.113.0/24", State: "advertised"})

	require.Equal(t, 1, b.AddressTransferCount())
	require.Equal(t, 1, b.CapacityReservationCount())

	// Reset.
	b.Reset()

	assert.Equal(t, 0, b.AddressTransferCount())
	assert.Equal(t, 0, b.CapacityReservationCount())
	assert.Equal(t, 0, b.VpcPeeringConnectionCount())
	assert.Equal(t, 0, b.ByoipCidrCount())
	assert.Equal(t, 0, b.DedicatedHostCount())

	// Defaults re-populated.
	vpcs := b.DescribeVpcs(nil)
	assert.Len(t, vpcs, 1)
	assert.Equal(t, "vpc-default", vpcs[0].ID)
}

// TestRefinement1_MultipleResetCycle ensures repeated resets are idempotent.
func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	for range 3 {
		b.AddByoipCidrInternal(&ec2.ByoipCidr{Cidr: "10.0.0.0/8"})
		b.Reset()
		assert.Equal(t, 0, b.ByoipCidrCount())
	}
}

// TestRefinement1_HandlerReset verifies Handler.Reset() delegates to Backend.Reset().
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	b.AddByoipCidrInternal(&ec2.ByoipCidr{Cidr: "10.0.0.0/8"})
	require.Equal(t, 1, b.ByoipCidrCount())

	h.Reset()

	assert.Equal(t, 0, b.ByoipCidrCount())
}

// TestRefinement1_ProviderInit_NilCtx verifies that Provider.Init returns ErrNilAppContext
// when called with a nil context.
func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &ec2.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, ec2.ErrNilAppContext)
}

// TestRefinement1_HandlerOpsPreBuilt verifies the dispatch table is cached at construction.
func TestRefinement1_HandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	// ops map should be non-empty (contains all registered ops).
	assert.Positive(t, h.HandlerOpsLen())
}

// TestRefinement1_SeedHelpers verifies all seed helper functions work correctly.
func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddAddressTransferInternal(
		&ec2.AddressTransfer{PublicIP: "1.2.3.4", AllocationID: "eipalloc-1"},
	)
	b.AddCapacityReservationInternal(
		&ec2.CapacityReservation{CapacityReservationID: "cr-1", InstanceType: "t3.micro"},
	)
	b.AddTGWPeeringAttachmentInternal(&ec2.TransitGatewayPeeringAttachment{
		TransitGatewayAttachmentID: "tgw-attach-1",
		State:                      "pending-acceptance",
	})
	b.AddTGWVpcAttachmentInternal(&ec2.TransitGatewayVpcAttachment{
		TransitGatewayAttachmentID: "tgw-attach-2",
		State:                      "pending-acceptance",
	})
	b.AddVpcPeeringConnectionInternal(&ec2.VpcPeeringConnection{
		VpcPeeringConnectionID: "pcx-1",
		RequesterVpcID:         "vpc-a",
		AccepterVpcID:          "vpc-b",
		State:                  "pending-acceptance",
	})
	b.AddByoipCidrInternal(&ec2.ByoipCidr{Cidr: "203.0.113.0/24", State: "provisioned"})
	b.AddVpcEndpointConnectionInternal(&ec2.VpcEndpointConnection{
		ServiceID:     "vpce-svc-1",
		VpcEndpointID: "vpce-1",
		State:         "pending-acceptance",
	})
	b.AddTGWMulticastDomainAssociationInternal(&ec2.TransitGatewayMulticastDomainAssociation{
		TransitGatewayMulticastDomainID: "tgw-mcast-1",
		SubnetID:                        "subnet-1",
		State:                           "associated",
	})

	assert.Equal(t, 1, b.AddressTransferCount())
	assert.Equal(t, 1, b.CapacityReservationCount())
	assert.Equal(t, 1, b.TGWPeeringAttachmentCount())
	assert.Equal(t, 1, b.TGWVpcAttachmentCount())
	assert.Equal(t, 1, b.VpcPeeringConnectionCount())
	assert.Equal(t, 1, b.ByoipCidrCount())
	assert.Equal(t, 1, b.VpcEndpointConnectionCount())
	assert.Equal(t, 1, b.TGWMulticastDomainAssociationCount())
}

// TestRefinement1_ExportCountHelpers verifies count helpers return 0 on fresh backend.
func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	assert.Equal(t, 0, b.AddressTransferCount())
	assert.Equal(t, 0, b.CapacityReservationCount())
	assert.Equal(t, 0, b.ReservedInstancesExchangeCount())
	assert.Equal(t, 0, b.TGWMulticastDomainAssociationCount())
	assert.Equal(t, 0, b.TGWPeeringAttachmentCount())
	assert.Equal(t, 0, b.TGWVpcAttachmentCount())
	assert.Equal(t, 0, b.VpcEndpointConnectionCount())
	assert.Equal(t, 0, b.VpcPeeringConnectionCount())
	assert.Equal(t, 0, b.ByoipCidrCount())
	assert.Equal(t, 0, b.DedicatedHostCount())
}

// TestRefinement1_SeedHelpers_DeepCopy verifies that seed helpers copy the value so
// mutations to the original struct do not affect the stored entry.
func TestRefinement1_SeedHelpers_DeepCopy(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	original := &ec2.VpcPeeringConnection{
		VpcPeeringConnectionID: "pcx-1",
		State:                  "pending-acceptance",
	}
	b.AddVpcPeeringConnectionInternal(original)

	// Mutate the original; the stored value should not change.
	original.State = "mutated"

	conns := b.DescribeVpcPeeringConnections([]string{"pcx-1"})
	require.Len(t, conns, 1)
	assert.Equal(t, "pending-acceptance", conns[0].State, "seed helper should deep-copy")
}

// TestRefinement1_CapacityReservationOwnedBy verifies OwnedBy is populated.
func TestRefinement1_CapacityReservationOwnedBy(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddCapacityReservationInternal(&ec2.CapacityReservation{CapacityReservationID: "cr-1"})

	reservations := b.DescribeCapacityReservations(nil)
	require.Len(t, reservations, 1)
	assert.Equal(t, "123456789012", reservations[0].OwnedBy, "OwnedBy should be set from AccountID")
}

// TestRefinement1_VpcPeeringConnectionExpirationTime verifies ExpirationTime is set.
func TestRefinement1_VpcPeeringConnectionExpirationTime(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddVpcPeeringConnectionInternal(&ec2.VpcPeeringConnection{
		VpcPeeringConnectionID: "pcx-1",
		State:                  "pending-acceptance",
	})

	conns := b.DescribeVpcPeeringConnections(nil)
	require.Len(t, conns, 1)
	assert.False(t, conns[0].ExpirationTime.IsZero(), "ExpirationTime should be set automatically")
}

// TestRefinement1_TGWPeeringAttachmentCreationTime verifies CreationTime is set.
func TestRefinement1_TGWPeeringAttachmentCreationTime(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddTGWPeeringAttachmentInternal(&ec2.TransitGatewayPeeringAttachment{
		TransitGatewayAttachmentID: "tgw-attach-1",
	})

	res, err := b.AcceptTransitGatewayPeeringAttachment("tgw-attach-1")
	require.NoError(t, err)
	assert.False(t, res.CreationTime.IsZero())
}

// TestRefinement1_NonNilSlices_TGWMulticast verifies non-nil result when no subnets.
func TestRefinement1_NonNilSlices_TGWMulticast(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	assocs, err := b.AcceptTransitGatewayMulticastDomainAssociations(
		"tgw-mcast-1",
		"tgw-attach-1",
		nil,
	)
	require.NoError(t, err)
	assert.NotNil(t, assocs, "result should be non-nil even when no subnets provided")
	assert.Empty(t, assocs)
}

// TestRefinement1_SortedDescribeCapacityReservations verifies sorted output.
func TestRefinement1_SortedDescribeCapacityReservations(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddCapacityReservationInternal(&ec2.CapacityReservation{CapacityReservationID: "cr-z"})
	b.AddCapacityReservationInternal(&ec2.CapacityReservation{CapacityReservationID: "cr-a"})
	b.AddCapacityReservationInternal(&ec2.CapacityReservation{CapacityReservationID: "cr-m"})

	result := b.DescribeCapacityReservations(nil)
	require.Len(t, result, 3)
	assert.Equal(t, "cr-a", result[0].CapacityReservationID)
	assert.Equal(t, "cr-m", result[1].CapacityReservationID)
	assert.Equal(t, "cr-z", result[2].CapacityReservationID)
}

// TestRefinement1_SortedDescribeVpcPeeringConnections verifies sorted output.
func TestRefinement1_SortedDescribeVpcPeeringConnections(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddVpcPeeringConnectionInternal(&ec2.VpcPeeringConnection{VpcPeeringConnectionID: "pcx-z"})
	b.AddVpcPeeringConnectionInternal(&ec2.VpcPeeringConnection{VpcPeeringConnectionID: "pcx-a"})

	result := b.DescribeVpcPeeringConnections(nil)
	require.Len(t, result, 2)
	assert.Equal(t, "pcx-a", result[0].VpcPeeringConnectionID)
	assert.Equal(t, "pcx-z", result[1].VpcPeeringConnectionID)
}

// TestRefinement1_SortedDescribeByoipCidrs verifies sorted output.
func TestRefinement1_SortedDescribeByoipCidrs(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddByoipCidrInternal(&ec2.ByoipCidr{Cidr: "203.0.113.0/24"})
	b.AddByoipCidrInternal(&ec2.ByoipCidr{Cidr: "10.0.0.0/8"})

	result := b.DescribeByoipCidrs("")
	require.Len(t, result, 2)
	assert.Equal(t, "10.0.0.0/8", result[0].Cidr)
}

// TestRefinement1_SortedDescribeHosts verifies sorted output.
func TestRefinement1_SortedDescribeHosts(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	hosts, err := b.AllocateHosts("us-east-1a", "t3.micro", 2)
	require.NoError(t, err)
	require.Len(t, hosts, 2)

	result := b.DescribeHosts(nil)
	require.Len(t, result, 2)
	// Sorted: first <= second.
	assert.LessOrEqual(t, result[0].HostID, result[1].HostID)
}

// TestRefinement1_DescribeCapacityReservations_Filter verifies ID filter works.
func TestRefinement1_DescribeCapacityReservations_Filter(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddCapacityReservationInternal(&ec2.CapacityReservation{CapacityReservationID: "cr-1"})
	b.AddCapacityReservationInternal(&ec2.CapacityReservation{CapacityReservationID: "cr-2"})

	result := b.DescribeCapacityReservations([]string{"cr-1"})
	require.Len(t, result, 1)
	assert.Equal(t, "cr-1", result[0].CapacityReservationID)
}

// TestRefinement1_DescribeByoipCidrs_StateFilter verifies state filter works.
func TestRefinement1_DescribeByoipCidrs_StateFilter(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddByoipCidrInternal(&ec2.ByoipCidr{Cidr: "10.0.0.0/8", State: "advertised"})
	b.AddByoipCidrInternal(&ec2.ByoipCidr{Cidr: "203.0.113.0/24", State: "provisioned"})

	advertised := b.DescribeByoipCidrs("advertised")
	require.Len(t, advertised, 1)
	assert.Equal(t, "10.0.0.0/8", advertised[0].Cidr)
}

// TestRefinement1_DescribeHosts_Filter verifies ID filter works.
func TestRefinement1_DescribeHosts_Filter(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	hosts, err := b.AllocateHosts("us-east-1a", "t3.micro", 2)
	require.NoError(t, err)
	require.Len(t, hosts, 2)

	result := b.DescribeHosts([]string{hosts[0].HostID})
	require.Len(t, result, 1)
	assert.Equal(t, hosts[0].HostID, result[0].HostID)
}

// TestRefinement1_PersistenceRoundTrip verifies new resource maps survive Snapshot/Restore.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddAddressTransferInternal(
		&ec2.AddressTransfer{PublicIP: "1.2.3.4", AllocationID: "eipalloc-1"},
	)
	b.AddCapacityReservationInternal(&ec2.CapacityReservation{CapacityReservationID: "cr-1"})
	b.AddByoipCidrInternal(&ec2.ByoipCidr{Cidr: "10.0.0.0/8", State: "advertised"})
	b.AddVpcPeeringConnectionInternal(&ec2.VpcPeeringConnection{VpcPeeringConnectionID: "pcx-1"})

	_, err := b.AllocateHosts("us-east-1a", "t3.micro", 1)
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	assert.Equal(t, 1, b2.AddressTransferCount())
	assert.Equal(t, 1, b2.CapacityReservationCount())
	assert.Equal(t, 1, b2.ByoipCidrCount())
	assert.Equal(t, 1, b2.VpcPeeringConnectionCount())
	assert.Equal(t, 1, b2.DedicatedHostCount())
}

// TestRefinement1_GetSupportedOperations_NewDescribeOps verifies the 4 new Describe
// operations appear in GetSupportedOperations().
func TestRefinement1_GetSupportedOperations_NewDescribeOps(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	ops := h.GetSupportedOperations()
	opSet := make(map[string]bool, len(ops))
	for _, op := range ops {
		opSet[op] = true
	}

	assert.True(t, opSet["DescribeCapacityReservations"])
	assert.True(t, opSet["DescribeByoipCidrs"])
	assert.True(t, opSet["DescribeHosts"])
	assert.True(t, opSet["DescribeVpcPeeringConnections"])
}

// TestRefinement1_HTTPDescribeCapacityReservations verifies the HTTP handler.
func TestRefinement1_HTTPDescribeCapacityReservations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *ec2.InMemoryBackend)
		name     string
		body     string
		wantBody string
		wantCode int
	}{
		{
			name:     "empty_list",
			body:     "Action=DescribeCapacityReservations&Version=2016-11-15",
			wantCode: http.StatusOK,
			wantBody: "DescribeCapacityReservationsResponse",
		},
		{
			name: "with_reservations",
			setup: func(b *ec2.InMemoryBackend) {
				b.AddCapacityReservationInternal(&ec2.CapacityReservation{
					CapacityReservationID: "cr-abc123",
					InstanceType:          "t3.micro",
					State:                 "active",
				})
			},
			body:     "Action=DescribeCapacityReservations&Version=2016-11-15",
			wantCode: http.StatusOK,
			wantBody: "cr-abc123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.setup != nil {
				b, ok := h.Backend.(*ec2.InMemoryBackend)
				require.True(t, ok)
				tt.setup(b)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// TestRefinement1_HTTPDescribeHosts verifies the HTTP handler for DescribeHosts.
func TestRefinement1_HTTPDescribeHosts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantBody string
		wantCode int
	}{
		{
			name:     "empty_list",
			body:     "Action=DescribeHosts&Version=2016-11-15",
			wantCode: http.StatusOK,
			wantBody: "DescribeHostsResponse",
		},
		{
			name:     "allocate_then_describe",
			body:     "Action=DescribeHosts&Version=2016-11-15",
			wantCode: http.StatusOK,
			wantBody: "DescribeHostsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.name == "allocate_then_describe" {
				rec := postForm(t, h,
					"Action=AllocateHosts&Version=2016-11-15"+
						"&AvailabilityZone=us-east-1a&InstanceType=t3.micro&Quantity=1")
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// TestRefinement1_HTTPDescribeByoipCidrs verifies the HTTP handler for DescribeByoipCidrs.
func TestRefinement1_HTTPDescribeByoipCidrs(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Seed by advertising.
	rec := postForm(t, h, "Action=AdvertiseByoipCidr&Version=2016-11-15&Cidr=203.0.113.0/24")
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postForm(t, h, "Action=DescribeByoipCidrs&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "203.0.113.0/24")
}

// TestRefinement1_HTTPDescribeVpcPeeringConnections verifies the HTTP handler.
func TestRefinement1_HTTPDescribeVpcPeeringConnections(t *testing.T) {
	t.Parallel()

	h := newHandler()
	b, ok := h.Backend.(*ec2.InMemoryBackend)
	require.True(t, ok)

	b.AddVpcPeeringConnectionInternal(&ec2.VpcPeeringConnection{
		VpcPeeringConnectionID: "pcx-test1",
		State:                  "pending-acceptance",
	})

	rec := postForm(t, h, "Action=DescribeVpcPeeringConnections&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "pcx-test1")
}

// TestRefinement1_AcceptCapacityReservation_SetsOwnedBy verifies OwnedBy is updated.
func TestRefinement1_AcceptCapacityReservation_SetsOwnedBy(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("111222333444", "us-east-1")
	b.AddCapacityReservationInternal(&ec2.CapacityReservation{
		CapacityReservationID: "cr-1",
		State:                 "pending",
		OwnedBy:               "999888777666",
	})

	cr, err := b.AcceptCapacityReservationBillingOwnership("cr-1")
	require.NoError(t, err)
	assert.Equal(t, "active", cr.State)
	assert.Equal(t, "111222333444", cr.OwnedBy, "OwnedBy should be updated to accepter AccountID")
}

// TestRefinement1_ErrValidationMapping verifies ErrInvalidParameter maps to HTTP 400.
func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=AcceptAddressTransfer&Version=2016-11-15")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}
