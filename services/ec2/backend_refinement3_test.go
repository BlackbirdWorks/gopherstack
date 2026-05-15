package ec2_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ec2 "github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestRefinement3_ReplaceNetworkACLEntry verifies NACL rule replacement.
func TestRefinement3_ReplaceNetworkACLEntry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		aclID      string
		protocol   string
		action     string
		cidr       string
		ruleNumber int
		setupACL   bool
		egress     bool
		wantErr    bool
	}{
		{
			name:       "replace_existing_rule",
			setupACL:   true,
			ruleNumber: 32767,
			protocol:   "-1",
			action:     "deny",
			cidr:       "10.0.0.0/8",
			egress:     false,
			wantErr:    false,
		},
		{
			name:       "missing_acl_id",
			aclID:      "",
			ruleNumber: 100,
			wantErr:    true,
		},
		{
			name:       "non_existent_acl",
			aclID:      "acl-nonexistent",
			ruleNumber: 100,
			wantErr:    true,
		},
		{
			name:       "non_existent_rule",
			setupACL:   true,
			ruleNumber: 999,
			protocol:   "-1",
			action:     "allow",
			cidr:       "0.0.0.0/0",
			egress:     false,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			aclID := tt.aclID

			if tt.setupACL {
				vpc, err := b.CreateVpc("10.0.0.0/16")
				require.NoError(t, err)

				acl, err := b.CreateNetworkACL(vpc.ID)
				require.NoError(t, err)

				aclID = acl.ID
			}

			err := b.ReplaceNetworkACLEntry(
				aclID,
				tt.ruleNumber,
				tt.protocol,
				tt.action,
				tt.cidr,
				tt.egress,
				0,
				0,
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			acls := b.DescribeStoredNetworkAcls([]string{aclID})
			require.Len(t, acls, 1)

			var found bool

			for _, e := range acls[0].Entries {
				if e.RuleNumber == tt.ruleNumber && e.Egress == tt.egress {
					assert.Equal(t, tt.cidr, e.CIDRBlock)
					found = true

					break
				}
			}

			assert.True(t, found, "replaced rule should be present in ACL")
		})
	}
}

// TestRefinement3_ReplaceNetworkACLAssociation tests subnet reassociation.
func TestRefinement3_ReplaceNetworkACLAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *ec2.InMemoryBackend) (aclID, subnetID string)
		name     string
		aclID    string
		subnetID string
		wantErr  bool
	}{
		{
			name: "valid_reassociation",
			setup: func(b *ec2.InMemoryBackend) (string, string) {
				vpc, err := b.CreateVpc("10.0.0.0/16")
				if err != nil {
					return "", ""
				}

				acl, err := b.CreateNetworkACL(vpc.ID)
				if err != nil {
					return "", ""
				}

				subnet, err := b.CreateSubnet(vpc.ID, "10.0.1.0/24", "us-east-1a")
				if err != nil {
					return "", ""
				}

				return acl.ID, subnet.ID
			},
			wantErr: false,
		},
		{
			name:    "missing_acl_id",
			aclID:   "",
			wantErr: true,
		},
		{
			name:     "missing_subnet_id",
			subnetID: "",
			wantErr:  true,
		},
		{
			name:    "non_existent_acl",
			aclID:   "acl-nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			aclID := tt.aclID
			subnetID := tt.subnetID

			if tt.setup != nil {
				aclID, subnetID = tt.setup(b)
			}

			assocID, err := b.ReplaceNetworkACLAssociation(aclID, subnetID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, assocID)
		})
	}
}

// TestRefinement3_DescribeVpcEndpointServices verifies the endpoint services list.
func TestRefinement3_DescribeVpcEndpointServices(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	services := b.DescribeVpcEndpointServices()

	assert.NotEmpty(t, services)

	var foundS3 bool

	if slices.Contains(services, "com.amazonaws.us-east-1.s3") {
		foundS3 = true
	}

	assert.True(t, foundS3, "s3 endpoint service should be present")
}

// TestRefinement3_ExportKeyPair tests key pair export.
func TestRefinement3_ExportKeyPair(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		keyName string
		create  bool
		wantErr bool
	}{
		{
			name:    "export_existing_key",
			keyName: "my-key",
			create:  true,
			wantErr: false,
		},
		{
			name:    "export_missing_key",
			keyName: "nonexistent-key",
			create:  false,
			wantErr: true,
		},
		{
			name:    "empty_key_name",
			keyName: "",
			create:  false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			if tt.create {
				_, err := b.CreateKeyPair(tt.keyName)
				require.NoError(t, err)
			}

			pubKey, err := b.ExportKeyPair(tt.keyName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, pubKey)
			assert.Contains(t, pubKey, "ssh-rsa")
		})
	}
}

// TestRefinement3_DescribeInstanceTypeOfferings verifies the instance type list.
func TestRefinement3_DescribeInstanceTypeOfferings(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	offerings := b.DescribeInstanceTypeOfferings()

	assert.NotEmpty(t, offerings)

	var foundT3Micro bool

	for _, o := range offerings {
		if o.InstanceType == "t3.micro" {
			foundT3Micro = true

			assert.Equal(t, "availability-zone", o.LocationType)

			break
		}
	}

	assert.True(t, foundT3Micro, "t3.micro should be in offerings")
}

// TestRefinement3_CreateVpcPeeringConnection tests peering creation.
func TestRefinement3_CreateVpcPeeringConnection(t *testing.T) {
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

// TestRefinement3_DeleteVpcPeeringConnection tests peering deletion.
func TestRefinement3_DeleteVpcPeeringConnection(t *testing.T) {
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

// TestRefinement3_TransitGateway tests transit gateway CRUD.
func TestRefinement3_TransitGateway(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		description string
		wantErr     bool
	}{
		{name: "create_tgw", description: "my transit gateway", wantErr: false},
		{name: "empty_description", description: "", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			tgw, err := b.CreateTransitGateway(tt.description)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, tgw.ID)
			assert.Equal(t, "available", tgw.State)

			// List returns the new gateway.
			tgws := b.DescribeTransitGateways(nil)
			assert.NotEmpty(t, tgws)

			// Delete.
			require.NoError(t, b.DeleteTransitGateway(tgw.ID))

			tgws = b.DescribeTransitGateways([]string{tgw.ID})
			assert.Empty(t, tgws)
		})
	}
}

// TestRefinement3_HandlerReplaceNetworkACLEntry verifies the HTTP handler.
func TestRefinement3_HandlerReplaceNetworkACLEntry(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	acl, err := b.CreateNetworkACL(vpc.ID)
	require.NoError(t, err)

	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	body := "Action=ReplaceNetworkAclEntry&Version=2016-11-15" +
		"&NetworkAclId=" + acl.ID +
		"&RuleNumber=32767" +
		"&Protocol=-1" +
		"&RuleAction=deny" +
		"&CidrBlock=192.168.0.0%2F16" +
		"&Egress=false"

	rec := postForm(t, h, body)
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "ReplaceNetworkAclEntryResponse")
}

// TestRefinement3_HandlerDescribeVpcEndpointServices verifies endpoint services.
func TestRefinement3_HandlerDescribeVpcEndpointServices(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=DescribeVpcEndpointServices&Version=2016-11-15")
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeVpcEndpointServicesResponse")
	assert.Contains(t, rec.Body.String(), "s3")
}

// TestRefinement3_HandlerExportKeyPair verifies key pair export handler.
func TestRefinement3_HandlerExportKeyPair(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create a key pair first.
	rec := postForm(t, h, "Action=CreateKeyPair&Version=2016-11-15&KeyName=test-export-key")
	require.Equal(t, 200, rec.Code)

	rec = postForm(t, h, "Action=ExportKeyPair&Version=2016-11-15&KeyName=test-export-key")
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "ExportKeyPairResponse")
	assert.Contains(t, rec.Body.String(), "ssh-rsa")
}

// TestRefinement3_HandlerCreateTransitGateway verifies TGW creation handler.
func TestRefinement3_HandlerCreateTransitGateway(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=CreateTransitGateway&Version=2016-11-15&Description=test-tgw")
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateTransitGatewayResponse")
	assert.Contains(t, rec.Body.String(), "available")
}
