package ec2_test

import (
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplaceRoute(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	rt, _ := b.CreateRouteTable("vpc-default")
	igw, _ := b.CreateInternetGateway()
	_ = b.CreateRoute(rt.ID, "0.0.0.0/0", igw.ID, "")

	t.Run("replaces existing route", func(t *testing.T) {
		igw2, _ := b.CreateInternetGateway()
		require.NoError(t, b.ReplaceRoute(rt.ID, "0.0.0.0/0", igw2.ID, ""))
	})
}

// TestRouteTable_CreateReturnsRTBID verifies CreateRouteTable returns
// an rtb- prefixed ID.
func TestRouteTable_CreateReturnsRTBID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		vpcID string
	}{
		{name: "default_vpc", vpcID: "vpc-default"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp, err := ec2.ExportDispatch(h, url.Values{
				"Action": {"CreateRouteTable"},
				"VpcId":  {tt.vpcID},
			})
			require.NoError(t, err)
			assert.Contains(t, resp, "<routeTableId>rtb-", "must return rtb- prefixed ID")
			assert.Contains(t, resp, "<vpcId>"+tt.vpcID+"</vpcId>")
		})
	}
}

// TestRouteTable_DescribeReturnsRouteSet verifies DescribeRouteTables
// returns the routeSet element.

// TestRouteTable_DescribeReturnsRouteSet verifies DescribeRouteTables
// returns the routeSet element.
func TestRouteTable_DescribeReturnsRouteSet(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	rt, err := b.CreateRouteTable("vpc-default")
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":         {"DescribeRouteTables"},
		"RouteTableId.1": {rt.ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<routeTableId>"+rt.ID+"</routeTableId>")
	assert.Contains(t, resp, "<routeSet>", "must include routeSet element")
}

// TestRouteTable_CreateRoute verifies CreateRoute adds a route.

// TestRouteTable_CreateRoute verifies CreateRoute adds a route.
func TestRouteTable_CreateRoute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		destCIDR     string
		gatewayID    string
		natGatewayID string
	}{
		{name: "igw_route", destCIDR: "0.0.0.0/0", gatewayID: "igw-123"},
		{name: "nat_route", destCIDR: "0.0.0.0/0", natGatewayID: "nat-456"},
		{name: "specific_cidr", destCIDR: "192.168.0.0/16", gatewayID: "igw-789"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			h := ec2.NewHandler(b)
			h.AccountID = "123456789012"
			h.Region = "us-east-1"

			rt, err := b.CreateRouteTable("vpc-default")
			require.NoError(t, err)

			vals := url.Values{
				"Action":               {"CreateRoute"},
				"RouteTableId":         {rt.ID},
				"DestinationCidrBlock": {tt.destCIDR},
			}
			if tt.gatewayID != "" {
				vals.Set("GatewayId", tt.gatewayID)
			}
			if tt.natGatewayID != "" {
				vals.Set("NatGatewayId", tt.natGatewayID)
			}

			resp, err := ec2.ExportDispatch(h, vals)
			require.NoError(t, err)
			assert.Contains(t, resp, "<return>true</return>", "CreateRoute must return true")

			// verify route appears in describe
			descResp, err := ec2.ExportDispatch(h, url.Values{
				"Action":         {"DescribeRouteTables"},
				"RouteTableId.1": {rt.ID},
			})
			require.NoError(t, err)
			assert.Contains(t, descResp, tt.destCIDR)
		})
	}
}

// TestRouteTable_DeleteRoute verifies DeleteRoute removes the route.

// TestRouteTable_DeleteRoute verifies DeleteRoute removes the route.
func TestRouteTable_DeleteRoute(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	rt, err := b.CreateRouteTable("vpc-default")
	require.NoError(t, err)

	require.NoError(t, b.CreateRoute(rt.ID, "10.0.0.0/8", "igw-123", ""))

	_, err = ec2.ExportDispatch(h, url.Values{
		"Action":               {"DeleteRoute"},
		"RouteTableId":         {rt.ID},
		"DestinationCidrBlock": {"10.0.0.0/8"},
	})
	require.NoError(t, err)

	// route must be gone
	descResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":         {"DescribeRouteTables"},
		"RouteTableId.1": {rt.ID},
	})
	require.NoError(t, err)
	assert.NotContains(t, descResp, "10.0.0.0/8")
}

// TestRouteTable_AssociateSubnet verifies AssociateRouteTable returns
// an association ID.

// TestRouteTable_AssociateSubnet verifies AssociateRouteTable returns
// an association ID.
func TestRouteTable_AssociateSubnet(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	rt, err := b.CreateRouteTable("vpc-default")
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":       {"AssociateRouteTable"},
		"RouteTableId": {rt.ID},
		"SubnetId":     {"subnet-default"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<associationId>", "AssociateRouteTable must return associationId")
}

// TestRouteTable_DisassociateSubnet verifies DisassociateRouteTable works.

// TestRouteTable_DisassociateSubnet verifies DisassociateRouteTable works.
func TestRouteTable_DisassociateSubnet(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	rt, err := b.CreateRouteTable("vpc-default")
	require.NoError(t, err)

	assocID, err := b.AssociateRouteTable(rt.ID, "subnet-default")
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":        {"DisassociateRouteTable"},
		"AssociationId": {assocID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<return>true</return>")
}

// TestRouteTable_DescribeAll returns all route tables.

// TestRouteTable_DescribeAll returns all route tables.
func TestRouteTable_DescribeAll(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	rt1, err := b.CreateRouteTable("vpc-default")
	require.NoError(t, err)
	rt2, err := b.CreateRouteTable("vpc-default")
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action": {"DescribeRouteTables"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, rt1.ID)
	assert.Contains(t, resp, rt2.ID)
}

// TestRouteTable_ReplaceRouteTableAssociation verifies that
// ReplaceRouteTableAssociation works correctly.

// TestRouteTable_ReplaceRouteTableAssociation verifies that
// ReplaceRouteTableAssociation works correctly.
func TestRouteTable_ReplaceRouteTableAssociation(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	rt1, err := b.CreateRouteTable("vpc-default")
	require.NoError(t, err)
	rt2, err := b.CreateRouteTable("vpc-default")
	require.NoError(t, err)

	// associate subnet to rt1
	assocID, err := b.AssociateRouteTable(rt1.ID, "subnet-default")
	require.NoError(t, err)

	// replace association to rt2
	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":        {"ReplaceRouteTableAssociation"},
		"AssociationId": {assocID},
		"RouteTableId":  {rt2.ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<newAssociationId>", "must return newAssociationId")
}

// TestRouteTable_DeleteNonExistentReturnsError verifies that
// deleting a non-existent route table returns an error.

// TestRouteTable_DeleteNonExistentReturnsError verifies that
// deleting a non-existent route table returns an error.
func TestRouteTable_DeleteNonExistentReturnsError(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":       {"DeleteRouteTable"},
		"RouteTableId": {"rtb-nonexistent"},
	})
	require.Error(t, err)
}

// TestRouteTable_DeleteRouteMissingParams verifies validation.

// TestRouteTable_DeleteRouteMissingParams verifies validation.
func TestRouteTable_DeleteRouteMissingParams(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":       {"DeleteRoute"},
		"RouteTableId": {"rtb-123"},
		// missing DestinationCidrBlock
	})
	require.Error(t, err)
}

// ============================================================================
// Transit Gateway (ClientVPN) accuracy
// ============================================================================

// TestClientVPN_TargetNetworkHasAssociationID verifies that
// DescribeClientVpnTargetNetworks now returns associationId and status
// (was missing per parity.md §R).
