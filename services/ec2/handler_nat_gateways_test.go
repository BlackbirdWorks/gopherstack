package ec2_test

import (
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- NAT gateway address ops ---- //nolint:godot // existing issue.
func TestNatGatewayAddressOps(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	addr, _ := b.AllocateAddress()
	nat, setupErr := b.CreateNatGateway("subnet-default", addr.AllocationID)
	require.NoError(t, setupErr)

	t.Run("disassociate address", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DisassociateNatGatewayAddress(nat.ID))
	})

	t.Run("associate address", func(t *testing.T) { //nolint:paralleltest // existing issue.
		addr2, _ := b.AllocateAddress()
		require.NoError(t, b.AssociateNatGatewayAddress(nat.ID, addr2.AllocationID))
	})

	t.Run("assign private address", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.AssignPrivateNatGatewayAddress(nat.ID))
	})

	t.Run("unknown NAT GW returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.DisassociateNatGatewayAddress("nat-missing"))
	})
}

// ---- Image lifecycle ---- //nolint:godot // existing issue.

// TestNatGateway_AssociateAddress tests AssociateNatGatewayAddress.
func TestNatGateway_AssociateAddress(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	addr, err := b.AllocateAddress()
	require.NoError(t, err)
	ngw, err := b.CreateNatGateway("subnet-default", addr.AllocationID)
	require.NoError(t, err)
	addr2, err := b.AllocateAddress()
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":         {"AssociateNatGatewayAddress"},
		"NatGatewayId":   {ngw.ID},
		"AllocationId.1": {addr2.AllocationID},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, resp)
}

// TestNatGateway_DisassociateAddress tests DisassociateNatGatewayAddress.

// TestNatGateway_DisassociateAddress tests DisassociateNatGatewayAddress.
func TestNatGateway_DisassociateAddress(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	addr, err := b.AllocateAddress()
	require.NoError(t, err)
	ngw, err := b.CreateNatGateway("subnet-default", addr.AllocationID)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"DisassociateNatGatewayAddress"},
		"NatGatewayId":    {ngw.ID},
		"AssociationId.1": {"eipassoc-test"},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, resp)
}

// ============================================================================
// Route Table accuracy
// ============================================================================

// TestRouteTable_CreateReturnsRTBID verifies CreateRouteTable returns
// an rtb- prefixed ID.

func TestUnassignPrivateNatGatewayAddressHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	addr, err := h.Backend.AllocateAddress()
	require.NoError(t, err)
	nat, err := h.Backend.CreateNatGateway("subnet-default", addr.AllocationID)
	require.NoError(t, err)
	require.NoError(t, h.Backend.AssignPrivateNatGatewayAddress(nat.ID))

	described := h.Backend.DescribeNatGateways([]string{nat.ID})
	require.Len(t, described, 1)
	require.Len(t, described[0].SecondaryPrivateIPs, 1)
	secondary := described[0].SecondaryPrivateIPs[0]

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":             {"UnassignPrivateNatGatewayAddress"},
		"NatGatewayId":       {nat.ID},
		"PrivateIpAddress.1": {secondary},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<UnassignPrivateNatGatewayAddressResponse")
	assert.NotContains(t, resp, "<privateIp>"+secondary+"</privateIp>")
}

// TestNatGateway_CreateReturnsNatGwID verifies NAT gateway creation
// returns a nat- prefixed ID.
func TestNatGateway_CreateReturnsNatGwID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		subnetID     string
		allocationID string
	}{
		{name: "public_ngw", subnetID: "subnet-default", allocationID: "eipalloc-abc123"},
		{name: "another_public", subnetID: "subnet-default", allocationID: "eipalloc-def456"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			h := ec2.NewHandler(b)
			h.AccountID = "123456789012"
			h.Region = "us-east-1"
			addr, err := b.AllocateAddress()
			require.NoError(t, err)
			resp, err := ec2.ExportDispatch(h, url.Values{
				"Action":       {"CreateNatGateway"},
				"SubnetId":     {tt.subnetID},
				"AllocationId": {addr.AllocationID},
			})
			require.NoError(t, err)
			assert.Contains(t, resp, "<natGatewayId>nat-", "must return nat- prefixed ID")
			assert.Contains(t, resp, "<state>available</state>")
		})
	}
}

// TestNatGateway_DescribeReturnsAllFields verifies DescribeNatGateways
// returns all required fields.

// TestNatGateway_DescribeReturnsAllFields verifies DescribeNatGateways
// returns all required fields.
func TestNatGateway_DescribeReturnsAllFields(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	addr, err := b.AllocateAddress()
	require.NoError(t, err)
	ngw, err := b.CreateNatGateway("subnet-default", addr.AllocationID)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":         {"DescribeNatGateways"},
		"NatGatewayId.1": {ngw.ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<natGatewayId>"+ngw.ID+"</natGatewayId>")
	assert.Contains(t, resp, "<subnetId>subnet-default</subnetId>")
	assert.Contains(t, resp, "<state>available</state>")
}

// TestNatGateway_DeleteSetsDeletedState verifies that
// DeleteNatGateway transitions the state to "deleted".

// TestNatGateway_DeleteSetsDeletedState verifies that
// DeleteNatGateway transitions the state to "deleted".
func TestNatGateway_DeleteSetsDeletedState(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	addr, err := b.AllocateAddress()
	require.NoError(t, err)
	ngw, err := b.CreateNatGateway("subnet-default", addr.AllocationID)
	require.NoError(t, err)

	_, err = ec2.ExportDispatch(h, url.Values{
		"Action":       {"DeleteNatGateway"},
		"NatGatewayId": {ngw.ID},
	})
	require.NoError(t, err)

	// deleted NGW is removed from describe results
	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action": {"DescribeNatGateways"},
	})
	require.NoError(t, err)
	assert.NotContains(t, resp, ngw.ID, "deleted NGW must not appear in DescribeNatGateways")
}

// TestNatGateway_DescribeAll verifies listing all NAT gateways.

// TestNatGateway_DescribeAll verifies listing all NAT gateways.
func TestNatGateway_DescribeAll(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	addr1, err := b.AllocateAddress()
	require.NoError(t, err)
	addr2, err := b.AllocateAddress()
	require.NoError(t, err)
	ngw1, err := b.CreateNatGateway("subnet-default", addr1.AllocationID)
	require.NoError(t, err)
	ngw2, err := b.CreateNatGateway("subnet-default", addr2.AllocationID)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action": {"DescribeNatGateways"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, ngw1.ID)
	assert.Contains(t, resp, ngw2.ID)
}

// TestNatGateway_DeleteNonExistentReturnsError verifies that
// deleting a non-existent NAT gateway returns an error.

// TestNatGateway_DeleteNonExistentReturnsError verifies that
// deleting a non-existent NAT gateway returns an error.
func TestNatGateway_DeleteNonExistentReturnsError(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":       {"DeleteNatGateway"},
		"NatGatewayId": {"nat-nonexistent"},
	})
	require.Error(t, err)
}

// TestNatGateway_AssociateAddress tests AssociateNatGatewayAddress.
