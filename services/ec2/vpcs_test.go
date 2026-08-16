package ec2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestModifyVpcAttribute verifies VPC attribute modification.
func TestModifyVpcAttribute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		attribute string
		wantErr   bool
	}{
		{name: "dns_support", attribute: "enableDnsSupport", wantErr: false},
		{name: "dns_hostnames", attribute: "enableDnsHostnames", wantErr: false},
		{name: "unknown_attribute", attribute: "unknownAttr", wantErr: true},
		{name: "missing_vpc", attribute: "enableDnsSupport", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			vpcID := "vpc-default"
			if tt.name == "missing_vpc" {
				vpcID = "vpc-nonexistent"
			}

			err := b.ModifyVpcAttribute(vpcID, tt.attribute, true)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestModifySubnetAttribute verifies subnet attribute modification.

// TestCreateVpcPeeringConnection tests peering creation.
func TestCreateVpcPeeringConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		requesterVPCID string
		accepterVPCID  string
		createVPC      bool
		wantErr        bool
	}{
		{
			name:          "valid_peering",
			accepterVPCID: "vpc-peer-12345678",
			createVPC:     true,
			wantErr:       false,
		},
		{
			name:           "missing_requester",
			requesterVPCID: "",
			accepterVPCID:  "vpc-peer",
			wantErr:        true,
		},
		{
			name:           "non_existent_requester",
			requesterVPCID: "vpc-nonexistent",
			accepterVPCID:  "vpc-peer",
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			requesterID := tt.requesterVPCID

			if tt.createVPC {
				vpc, err := b.CreateVpc("10.0.0.0/16")
				require.NoError(t, err)

				requesterID = vpc.ID
			}

			pc, err := b.CreateVpcPeeringConnection(requesterID, tt.accepterVPCID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, pc.VpcPeeringConnectionID)
			assert.Equal(t, "pending-acceptance", pc.State)

			// Verify it shows up in Describe.
			pcs := b.DescribeVpcPeeringConnections([]string{pc.VpcPeeringConnectionID})
			require.Len(t, pcs, 1)
			assert.Equal(t, pc.VpcPeeringConnectionID, pcs[0].VpcPeeringConnectionID)
		})
	}
}

// TestDeleteVpcPeeringConnection tests peering deletion.

// TestDeleteVpcPeeringConnection tests peering deletion.
func TestDeleteVpcPeeringConnection(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	pc, err := b.CreateVpcPeeringConnection(vpc.ID, "vpc-remote-12345678")
	require.NoError(t, err)

	// Delete it.
	require.NoError(t, b.DeleteVpcPeeringConnection(pc.VpcPeeringConnectionID))

	// Should no longer appear.
	pcs := b.DescribeVpcPeeringConnections([]string{pc.VpcPeeringConnectionID})
	assert.Empty(t, pcs)

	// Deleting again should fail.
	require.Error(t, b.DeleteVpcPeeringConnection(pc.VpcPeeringConnectionID))
}

// TestTransitGateway tests transit gateway CRUD.

func TestCreateVpc_CIDRConflict(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateVpc("192.168.0.0/16")
	require.NoError(t, err)

	// Exact same CIDR should conflict.
	_, err = b.CreateVpc("192.168.0.0/16")
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrCIDRConflict)

	// Overlapping CIDR should also conflict.
	_, err = b.CreateVpc("192.168.1.0/24")
	require.Error(t, err)
	assert.ErrorIs(t, err, ec2.ErrCIDRConflict)
}

// TestHTTP_ModifyVpcAttribute verifies the HTTP handler for ModifyVpcAttribute.
func TestHTTP_ModifyVpcAttribute(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(
		t,
		h,
		"Action=ModifyVpcAttribute&Version=2016-11-15&VpcId=vpc-default&EnableDnsSupport.Value=true",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "true")
}

// TestHTTP_CreateNetworkAcl verifies the HTTP handler for CreateNetworkAcl.
