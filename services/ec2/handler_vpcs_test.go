package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- VPC/Subnet/SG ---- //nolint:godot // existing issue.
func TestCreateDefaultVpc(t *testing.T) { //nolint:paralleltest // existing issue.
	t.Run("creates default vpc", func(t *testing.T) { //nolint:paralleltest // existing issue.
		b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		// Remove existing default VPC first (initDefaults creates one). Real AWS
		// requires dependents removed before the VPC itself, so drop the default
		// subnet first.
		b.DeleteSubnet("subnet-default")
		b.DeleteVpc("vpc-default")
		vpc, err := b.CreateDefaultVpc()
		require.NoError(t, err)
		assert.True(t, vpc.IsDefault)
		assert.Equal(t, "172.31.0.0/16", vpc.CIDRBlock)
	})

	t.Run("error when default vpc exists", func(t *testing.T) { //nolint:paralleltest // existing issue.
		b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		_, err := b.CreateDefaultVpc()
		require.Error(t, err)
	})
}

func TestModifyVpcTenancy(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("sets tenancy", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyVpcTenancy("vpc-default", "dedicated"))
	})

	t.Run("unknown vpc returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.ModifyVpcTenancy("vpc-missing", "default"))
	})
}

func TestModifyVpcPeeringConnectionOptions(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vpc2, _ := b.CreateVpc("10.0.0.0/16", "default")
	pc, _ := b.CreateVpcPeeringConnection("vpc-default", vpc2.ID)

	t.Run("stores options", func(t *testing.T) {
		opts := ec2.PeeringConnectionOptions{AllowDNSResolutionFromRemoteVPC: true}
		require.NoError(t, b.ModifyVpcPeeringConnectionOptions(pc.VpcPeeringConnectionID, opts))
		stored := b.GetVpcPeeringConnectionOptions(pc.VpcPeeringConnectionID)
		require.NotNil(t, stored)
		assert.True(t, stored.AllowDNSResolutionFromRemoteVPC)
	})
}

// ---- EIP attributes ---- //nolint:godot // existing issue.

func TestHTTP_CreateDefaultVpc(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	_ = b.DeleteSubnet("subnet-default")
	_ = b.DeleteVpc("vpc-default")

	_, err := ec2.ExportDispatch(h, url.Values{
		"Action": []string{"CreateDefaultVpc"},
	})
	require.NoError(t, err)
}

// ---- VPC CIDR disassociation ---- //nolint:godot // existing issue.
func TestDisassociateVpcCidrBlock(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vpc, _ := b.CreateVpc("10.0.0.0/16", "default")
	assoc, setupErr := b.AssociateVpcCidrBlock(vpc.ID, "10.1.0.0/16")
	require.NoError(t, setupErr)

	t.Run("disassociates cidr block", func(t *testing.T) {
		vpcID, disassoc, err := b.DisassociateVpcCidrBlock(assoc.AssociationID)
		require.NoError(t, err)
		assert.Equal(t, vpc.ID, vpcID)
		assert.Equal(t, "disassociated", disassoc.State)
	})
}

// ---- NAT gateway address ops ---- //nolint:godot // existing issue.

// TestDisassociateVpcCidrBlock_RemovesAssociation verifies that
// DisassociateVpcCidrBlock removes the association so it cannot be disassociated again.
func TestDisassociateVpcCidrBlock_RemovesAssociation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	vpcResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":    {"CreateVpc"},
		"CidrBlock": {"10.10.0.0/16"},
	})
	require.NoError(t, err)
	vpcID := extractXMLTag(vpcResp, "vpcId")

	assocResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":    {"AssociateVpcCidrBlock"},
		"VpcId":     {vpcID},
		"CidrBlock": {"100.64.0.0/16"},
	})
	require.NoError(t, err)
	assocID := extractXMLTag(assocResp, "associationId")
	require.NotEmpty(t, assocID)

	// First disassociate succeeds.
	_, err = ec2.ExportDispatch(h, url.Values{
		"Action":        {"DisassociateVpcCidrBlock"},
		"AssociationId": {assocID},
	})
	require.NoError(t, err)

	// Second disassociate on same ID must fail.
	_, err = ec2.ExportDispatch(h, url.Values{
		"Action":        {"DisassociateVpcCidrBlock"},
		"AssociationId": {assocID},
	})
	require.Error(t, err, "DisassociateVpcCidrBlock on already-removed association must error")
}

// --- CreateVpcEndpointConnectionNotification ---

// TestCreateVpcEndpointConnectionNotification_ResponseShape verifies that
// CreateVpcEndpointConnectionNotification returns an ID with the "vpce-nfn-" prefix,
// state=Enabled, type=Topic, and echoes the requested events, matching AWS EC2 behaviour.

// TestHandlerVpcPeeringConnectionHandlers covers handleCreateVpcPeeringConnection,
// handleDeleteVpcPeeringConnection, and handleRejectVpcPeeringConnection.
func TestHandlerVpcPeeringConnectionHandlers(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	vpc1, err := b.CreateVpc("10.20.0.0/16", "default")
	require.NoError(t, err)
	vpc2, err := b.CreateVpc("10.21.0.0/16", "default")
	require.NoError(t, err)

	// Create peering connection.
	rec := postForm(t, h, "Action=CreateVpcPeeringConnection&Version=2016-11-15"+
		"&VpcId="+vpc1.ID+
		"&PeerVpcId="+vpc2.ID)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateVpcPeeringConnectionResponse")
	pcID := extractXMLField(rec.Body.String(), "vpcPeeringConnectionId")
	require.NotEmpty(t, pcID)

	// Create another for reject test.
	vpc3, err := b.CreateVpc("10.22.0.0/16", "default")
	require.NoError(t, err)
	pc2, err := b.CreateVpcPeeringConnection(vpc1.ID, vpc3.ID)
	require.NoError(t, err)

	// Reject it.
	rec = postForm(
		t,
		h,
		"Action=RejectVpcPeeringConnection&Version=2016-11-15&VpcPeeringConnectionId="+pc2.VpcPeeringConnectionID,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "RejectVpcPeeringConnectionResponse")

	// Delete the first one.
	rec = postForm(
		t,
		h,
		"Action=DeleteVpcPeeringConnection&Version=2016-11-15&VpcPeeringConnectionId="+pcID,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteVpcPeeringConnectionResponse")
}

// TestHandlerDescribeTransitGatewaysAndDelete covers handleDescribeTransitGateways
// and handleDeleteTransitGateway.

// TestCreateVpc_IDFormat verifies that CreateVpc returns an ID with the
// "vpc-" prefix, matching AWS EC2 behaviour.
func TestCreateVpc_IDFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cidr string
	}{
		{name: "class_c", cidr: "10.0.0.0/24"},
		{name: "class_b", cidr: "172.16.0.0/16"},
		{name: "class_a", cidr: "192.168.0.0/16"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp, err := ec2.ExportDispatch(h, url.Values{
				"Action":    {"CreateVpc"},
				"CidrBlock": {tt.cidr},
			})
			require.NoError(t, err)
			assert.Contains(t, resp, "<vpcId>vpc-",
				"CreateVpc response must contain a vpc- prefixed ID")
			assert.Contains(t, resp, "<cidrBlock>"+tt.cidr+"</cidrBlock>",
				"CreateVpc response must echo the requested CIDR block")
			assert.Contains(t, resp, "<state>available</state>",
				"CreateVpc response must return state=available")
		})
	}
}

// TestDeleteVpc_NotFound verifies that DeleteVpc on a non-existent VPC
// returns an error, matching AWS EC2 behaviour.

// TestDeleteVpc_NotFound verifies that DeleteVpc on a non-existent VPC
// returns an error, matching AWS EC2 behaviour.
func TestDeleteVpc_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action": {"DeleteVpc"},
		"VpcId":  {"vpc-doesnotexist"},
	})
	require.Error(t, err, "DeleteVpc on non-existent VPC must return an error")
}

// TestDescribeVpcs_FilterByID verifies that DescribeVpcs filters correctly
// when a VpcId list is supplied, matching AWS EC2 behaviour.

// TestDescribeVpcs_FilterByID verifies that DescribeVpcs filters correctly
// when a VpcId list is supplied, matching AWS EC2 behaviour.
func TestDescribeVpcs_FilterByID(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp1, err := ec2.ExportDispatch(h, url.Values{
		"Action":    {"CreateVpc"},
		"CidrBlock": {"10.1.0.0/16"},
	})
	require.NoError(t, err)

	resp2, err := ec2.ExportDispatch(h, url.Values{
		"Action":    {"CreateVpc"},
		"CidrBlock": {"10.2.0.0/16"},
	})
	require.NoError(t, err)

	id1 := extractXMLTag(resp1, "vpcId")
	id2 := extractXMLTag(resp2, "vpcId")
	require.NotEmpty(t, id1)
	require.NotEmpty(t, id2)

	// Filter by id1 only.
	filtered, err := ec2.ExportDispatch(h, url.Values{
		"Action":  {"DescribeVpcs"},
		"VpcId.1": {id1},
	})
	require.NoError(t, err)
	assert.Contains(t, filtered, id1, "filtered response must include requested VPC")
	assert.NotContains(t, filtered, id2, "filtered response must exclude other VPCs")
}

// --- AssociateVpcCidrBlock / DisassociateVpcCidrBlock ---

// TestAssociateVpcCidrBlock_ResponseShape verifies that
// AssociateVpcCidrBlock returns an association ID and echoes the CIDR and state,
// matching AWS EC2 behaviour.

// TestAssociateVpcCidrBlock_ResponseShape verifies that
// AssociateVpcCidrBlock returns an association ID and echoes the CIDR and state,
// matching AWS EC2 behaviour.
func TestAssociateVpcCidrBlock_ResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cidr string
	}{
		{name: "ipv4_secondary", cidr: "100.64.0.0/16"},
		{name: "ipv4_rfc1918", cidr: "172.31.0.0/16"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vpcResp, err := ec2.ExportDispatch(h, url.Values{
				"Action":    {"CreateVpc"},
				"CidrBlock": {"10.0.0.0/16"},
			})
			require.NoError(t, err)
			vpcID := extractXMLTag(vpcResp, "vpcId")
			require.NotEmpty(t, vpcID)

			assocResp, err := ec2.ExportDispatch(h, url.Values{
				"Action":    {"AssociateVpcCidrBlock"},
				"VpcId":     {vpcID},
				"CidrBlock": {tt.cidr},
			})
			require.NoError(t, err)
			assert.Contains(t, assocResp, "vpc-cidr-assoc-",
				"AssociateVpcCidrBlock must return a vpc-cidr-assoc- prefixed ID")
			assert.Contains(t, assocResp, tt.cidr,
				"AssociateVpcCidrBlock must echo the requested CIDR")
			assert.Contains(t, assocResp, "available",
				"AssociateVpcCidrBlock state must be available")
		})
	}
}

// TestDisassociateVpcCidrBlock_RemovesAssociation verifies that
// DisassociateVpcCidrBlock removes the association so it cannot be disassociated again.

// TestVpcPeeringConnectionExpirationTime verifies ExpirationTime is set.
func TestVpcPeeringConnectionExpirationTime(t *testing.T) {
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

// TestTGWPeeringAttachmentCreationTime verifies CreationTime is set.

// TestSortedDescribeVpcPeeringConnections verifies sorted output.
func TestSortedDescribeVpcPeeringConnections(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddVpcPeeringConnectionInternal(&ec2.VpcPeeringConnection{VpcPeeringConnectionID: "pcx-z"})
	b.AddVpcPeeringConnectionInternal(&ec2.VpcPeeringConnection{VpcPeeringConnectionID: "pcx-a"})

	result := b.DescribeVpcPeeringConnections(nil)
	require.Len(t, result, 2)
	assert.Equal(t, "pcx-a", result[0].VpcPeeringConnectionID)
	assert.Equal(t, "pcx-z", result[1].VpcPeeringConnectionID)
}

// TestSortedDescribeByoipCidrs verifies sorted output.

// TestHTTPDescribeVpcPeeringConnections verifies the HTTP handler.
func TestHTTPDescribeVpcPeeringConnections(t *testing.T) {
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
