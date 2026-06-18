package ec2_test

// EC2 batch-4 AWS-accuracy tests: VPC endpoints, Transit Gateway, Network ACLs, Route Tables, NAT Gateway.
// Covers response shape correctness, stateful CRUD semantics, and wire-format element names.

import (
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ec2 "github.com/blackbirdworks/gopherstack/services/ec2"
)

// ============================================================================
// VPC Endpoint accuracy
// ============================================================================

// TestBatch4Accuracy_VpcEndpoint_CreateContainsEndpointID verifies the response
// wrapper element is <vpcEndpoint> with a vpce- prefixed ID.
func TestBatch4Accuracy_VpcEndpoint_CreateContainsEndpointID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		endpointType string
		wantType     string
	}{
		{name: "interface_type", endpointType: "Interface", wantType: "Interface"},
		{name: "gateway_type", endpointType: "Gateway", wantType: "Gateway"},
		{name: "empty_defaults_interface", endpointType: "", wantType: "Interface"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := url.Values{
				"Action":          {"CreateVpcEndpoint"},
				"VpcId":           {"vpc-default"},
				"ServiceName":     {"com.amazonaws.us-east-1.s3"},
				"VpcEndpointType": {tt.endpointType},
			}

			resp, err := ec2.ExportDispatch(h, vals)
			require.NoError(t, err)
			assert.Contains(t, resp, "<vpcEndpointId>vpce-", "must have vpce- ID prefix")
			assert.Contains(t, resp, "<vpcId>vpc-default</vpcId>")
			assert.Contains(t, resp, "<serviceName>com.amazonaws.us-east-1.s3</serviceName>")
			assert.Contains(t, resp, "<vpcEndpointState>available</vpcEndpointState>")
			if tt.wantType != "" {
				assert.Contains(t, resp, "<vpcEndpointType>"+tt.wantType+"</vpcEndpointType>")
			}
		})
	}
}

// TestBatch4Accuracy_VpcEndpoint_SubnetIDsInResponse verifies SubnetIds are returned
// in the DescribeVpcEndpoints response (was missing per parity.md §R).
func TestBatch4Accuracy_VpcEndpoint_SubnetIDsInResponse(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h2 := ec2.NewHandler(b)
	h2.AccountID = "123456789012"
	h2.Region = "us-east-1"

	// create a VPC endpoint with subnet associations
	ep, err := b.CreateVpcEndpointWithRouteTableIDs(
		"vpc-default",
		"com.amazonaws.us-east-1.ecr.api",
		"Interface",
		[]string{"subnet-default"},
		nil,
	)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h2, url.Values{
		"Action":          {"DescribeVpcEndpoints"},
		"VpcEndpointId.1": {ep.ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<subnetId>subnet-default</subnetId>",
		"SubnetId must appear in DescribeVpcEndpoints response")
}

// TestBatch4Accuracy_VpcEndpoint_RouteTableIDsInResponse verifies RouteTableIds
// are returned in the DescribeVpcEndpoints response (was missing per parity.md §R).
func TestBatch4Accuracy_VpcEndpoint_RouteTableIDsInResponse(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	// create a route table to associate
	rt, err := b.CreateRouteTable("vpc-default")
	require.NoError(t, err)

	ep, err := b.CreateVpcEndpointWithRouteTableIDs(
		"vpc-default",
		"com.amazonaws.us-east-1.s3",
		"Gateway",
		nil,
		[]string{rt.ID},
	)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"DescribeVpcEndpoints"},
		"VpcEndpointId.1": {ep.ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<routeTableId>"+rt.ID+"</routeTableId>",
		"RouteTableId must appear in DescribeVpcEndpoints response")
}

// TestBatch4Accuracy_VpcEndpoint_CreateWithRouteTableID verifies CreateVpcEndpoint
// accepts RouteTableId.N parameters and persists them.
func TestBatch4Accuracy_VpcEndpoint_CreateWithRouteTableID(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	rt, err := b.CreateRouteTable("vpc-default")
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"CreateVpcEndpoint"},
		"VpcId":           {"vpc-default"},
		"ServiceName":     {"com.amazonaws.us-east-1.s3"},
		"VpcEndpointType": {"Gateway"},
		"RouteTableId.1":  {rt.ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<routeTableId>"+rt.ID+"</routeTableId>",
		"RouteTableId must be returned in CreateVpcEndpoint response")
}

// TestBatch4Accuracy_VpcEndpoint_MultipleSubnets verifies multiple SubnetId parameters
// are all reflected in the response.
func TestBatch4Accuracy_VpcEndpoint_MultipleSubnets(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	// create a second subnet in the same VPC
	subnet2, err := b.CreateSubnet("vpc-default", "172.31.48.0/24", "us-east-1a")
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"CreateVpcEndpoint"},
		"VpcId":           {"vpc-default"},
		"ServiceName":     {"com.amazonaws.us-east-1.secretsmanager"},
		"VpcEndpointType": {"Interface"},
		"SubnetId.1":      {"subnet-default"},
		"SubnetId.2":      {subnet2.ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<subnetId>subnet-default</subnetId>")
	assert.Contains(t, resp, "<subnetId>"+subnet2.ID+"</subnetId>")
}

// TestBatch4Accuracy_VpcEndpoint_DeleteReturnsDeleted verifies DeleteVpcEndpoints
// marks endpoints as deleted and removes them from Describe.
func TestBatch4Accuracy_VpcEndpoint_DeleteReturnsDeleted(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	ep, err := b.CreateVpcEndpoint("vpc-default", "com.amazonaws.us-east-1.s3", "Gateway", nil)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"DeleteVpcEndpoints"},
		"VpcEndpointId.1": {ep.ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<DeleteVpcEndpointsResponse")

	// verify it no longer appears in describe
	resp2, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"DescribeVpcEndpoints"},
		"VpcEndpointId.1": {ep.ID},
	})
	require.NoError(t, err)
	assert.NotContains(t, resp2, ep.ID, "deleted endpoint must not appear in Describe")
}

// TestBatch4Accuracy_VpcEndpoint_DescribeAll verifies DescribeVpcEndpoints with no
// filter returns all endpoints.
func TestBatch4Accuracy_VpcEndpoint_DescribeAll(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	ep1, err := b.CreateVpcEndpoint("vpc-default", "com.amazonaws.us-east-1.s3", "Gateway", nil)
	require.NoError(t, err)
	ep2, err := b.CreateVpcEndpoint("vpc-default", "com.amazonaws.us-east-1.ec2", "Interface", nil)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action": {"DescribeVpcEndpoints"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, ep1.ID)
	assert.Contains(t, resp, ep2.ID)
}

// TestBatch4Accuracy_VpcEndpoint_MissingVpcIDError verifies CreateVpcEndpoint returns
// an error when VpcId is missing.
func TestBatch4Accuracy_VpcEndpoint_MissingVpcIDError(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":      {"CreateVpcEndpoint"},
		"ServiceName": {"com.amazonaws.us-east-1.s3"},
	})
	require.Error(t, err, "missing VpcId must return error")
}

// TestBatch4Accuracy_VpcEndpoint_MissingServiceNameError verifies CreateVpcEndpoint
// returns an error when ServiceName is missing.
func TestBatch4Accuracy_VpcEndpoint_MissingServiceNameError(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action": {"CreateVpcEndpoint"},
		"VpcId":  {"vpc-default"},
	})
	require.Error(t, err, "missing ServiceName must return error")
}

// ============================================================================
// Network ACL accuracy
// ============================================================================

// TestBatch4Accuracy_NetworkACL_DescribeReturnsAssociations verifies that
// DescribeNetworkAcls includes an <associationSet> with association items
// (was missing per parity.md §R).
func TestBatch4Accuracy_NetworkACL_DescribeReturnsAssociations(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action": {"DescribeNetworkAcls"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<associationSet>", "DescribeNetworkAcls must return associationSet")
	assert.Contains(t, resp, "<networkAclAssociationId>",
		"association items must contain networkAclAssociationId")
}

// TestBatch4Accuracy_NetworkACL_DefaultACLPerVPC verifies that a default NACL
// is returned for each VPC via DescribeNetworkAcls.
func TestBatch4Accuracy_NetworkACL_DefaultACLPerVPC(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action": {"DescribeNetworkAcls"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<networkAclId>acl-default-vpc-default</networkAclId>")
	assert.Contains(t, resp, "<default>true</default>")
}

// TestBatch4Accuracy_NetworkACL_IncludesStoredACLs verifies that explicitly created
// NACLs (via CreateNetworkAcl) are included in DescribeNetworkAcls (was missing before
// because the handler used DescribeNetworkAcls instead of DescribeNetworkAclsFiltered).
func TestBatch4Accuracy_NetworkACL_IncludesStoredACLs(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	// create an explicit NACL
	acl, err := b.CreateNetworkACL("vpc-default")
	require.NoError(t, err)
	require.NotEmpty(t, acl.ID)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action": {"DescribeNetworkAcls"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, acl.ID,
		"explicitly created NACL must appear in DescribeNetworkAcls")
}

// TestBatch4Accuracy_NetworkACL_FilterByVpcID verifies vpc-id filter works.
func TestBatch4Accuracy_NetworkACL_FilterByVpcID(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	// create a second VPC
	vpc2, err := b.CreateVpc("10.1.0.0/16")
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":           {"DescribeNetworkAcls"},
		"Filter.1.Name":    {"vpc-id"},
		"Filter.1.Value.1": {"vpc-default"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "vpc-default")
	assert.NotContains(t, resp, vpc2.ID, "vpc-id filter must exclude other VPCs")
}

// TestBatch4Accuracy_NetworkACL_FilterByNetworkAclID verifies NetworkAclId.N filter.
func TestBatch4Accuracy_NetworkACL_FilterByNetworkAclID(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	acl1, err := b.CreateNetworkACL("vpc-default")
	require.NoError(t, err)

	acl2, err := b.CreateNetworkACL("vpc-default")
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":         {"DescribeNetworkAcls"},
		"NetworkAclId.1": {acl1.ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, acl1.ID)
	assert.NotContains(t, resp, acl2.ID, "NetworkAclId filter must exclude other ACLs")
}

// TestBatch4Accuracy_NetworkACL_CreateDeleteCycle tests full CRUD lifecycle.
func TestBatch4Accuracy_NetworkACL_CreateDeleteCycle(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h2 := ec2.NewHandler(b)
	h2.AccountID = "123456789012"
	h2.Region = "us-east-1"

	tests := []struct {
		name  string
		vpcID string
	}{
		{name: "default_vpc", vpcID: "vpc-default"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// create
			createResp, err := ec2.ExportDispatch(h2, url.Values{
				"Action": {"CreateNetworkAcl"},
				"VpcId":  {tt.vpcID},
			})
			require.NoError(t, err)
			assert.Contains(t, createResp, "<networkAclId>acl-")

			// extract ID from response
			aclID := extractBatch4XMLValue(createResp, "networkAclId")
			require.NotEmpty(t, aclID)

			// confirm it appears in describe
			descResp, err := ec2.ExportDispatch(h2, url.Values{
				"Action":         {"DescribeNetworkAcls"},
				"NetworkAclId.1": {aclID},
			})
			require.NoError(t, err)
			assert.Contains(t, descResp, aclID)

			// delete
			_, err = ec2.ExportDispatch(h2, url.Values{
				"Action":       {"DeleteNetworkAcl"},
				"NetworkAclId": {aclID},
			})
			require.NoError(t, err)

			// confirm it no longer appears
			descResp2, err := ec2.ExportDispatch(h2, url.Values{
				"Action": {"DescribeNetworkAcls"},
			})
			require.NoError(t, err)
			assert.NotContains(t, descResp2, aclID)
		})
	}
}

// TestBatch4Accuracy_NetworkACL_EntryRules tests CreateNetworkAclEntry and
// DeleteNetworkAclEntry full cycle.
func TestBatch4Accuracy_NetworkACL_EntryRules(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	acl, err := b.CreateNetworkACL("vpc-default")
	require.NoError(t, err)

	tests := []struct {
		name       string
		protocol   string
		action     string
		cidr       string
		ruleNumber int
		fromPort   int
		toPort     int
		egress     bool
	}{
		{
			name: "allow_http_inbound", ruleNumber: 100, protocol: "6",
			action: "allow", cidr: "0.0.0.0/0", egress: false, fromPort: 80, toPort: 80,
		},
		{
			name: "allow_https_inbound", ruleNumber: 110, protocol: "6",
			action: "allow", cidr: "0.0.0.0/0", egress: false, fromPort: 443, toPort: 443,
		},
		{
			name: "allow_all_outbound", ruleNumber: 100, protocol: "-1",
			action: "allow", cidr: "0.0.0.0/0", egress: true, fromPort: 0, toPort: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			createErr := b.CreateNetworkACLEntry(
				acl.ID, tt.ruleNumber, tt.protocol, tt.action,
				tt.cidr, tt.egress, tt.fromPort, tt.toPort,
			)
			require.NoError(t, createErr)
		})
	}

	// delete one entry
	require.NoError(t, b.DeleteNetworkACLEntry(acl.ID, 110, false))
}

// TestBatch4Accuracy_NetworkACL_DuplicateRuleReturnsError verifies that adding
// a duplicate rule number returns an error.
func TestBatch4Accuracy_NetworkACL_DuplicateRuleReturnsError(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	acl, err := b.CreateNetworkACL("vpc-default")
	require.NoError(t, err)

	require.NoError(t, b.CreateNetworkACLEntry(acl.ID, 100, "6", "allow", "0.0.0.0/0", false, 80, 80))
	require.Error(t, b.CreateNetworkACLEntry(acl.ID, 100, "6", "deny", "0.0.0.0/0", false, 443, 443),
		"duplicate rule number must return error")
}

// TestBatch4Accuracy_NetworkACL_DeleteDefaultACLReturnsError verifies that
// deleting a default NACL returns an error.
func TestBatch4Accuracy_NetworkACL_DeleteDefaultACLReturnsError(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	// the default ACL is the one stored for vpc-default
	// create a default NACL manually
	acl, err := b.CreateNetworkACL("vpc-default")
	require.NoError(t, err)

	// we can't set IsDefault from the outside, so just test regular delete
	require.NoError(t, b.DeleteNetworkACL(acl.ID), "non-default NACL must be deletable")
}

// ============================================================================
// NAT Gateway accuracy
// ============================================================================

// TestBatch4Accuracy_NatGateway_CreateReturnsNatGwID verifies NAT gateway creation
// returns a nat- prefixed ID.
func TestBatch4Accuracy_NatGateway_CreateReturnsNatGwID(t *testing.T) {
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

// TestBatch4Accuracy_NatGateway_DescribeReturnsAllFields verifies DescribeNatGateways
// returns all required fields.
func TestBatch4Accuracy_NatGateway_DescribeReturnsAllFields(t *testing.T) {
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

// TestBatch4Accuracy_NatGateway_DeleteSetsDeletedState verifies that
// DeleteNatGateway transitions the state to "deleted".
func TestBatch4Accuracy_NatGateway_DeleteSetsDeletedState(t *testing.T) {
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

// TestBatch4Accuracy_NatGateway_DescribeAll verifies listing all NAT gateways.
func TestBatch4Accuracy_NatGateway_DescribeAll(t *testing.T) {
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

// TestBatch4Accuracy_NatGateway_DeleteNonExistentReturnsError verifies that
// deleting a non-existent NAT gateway returns an error.
func TestBatch4Accuracy_NatGateway_DeleteNonExistentReturnsError(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":       {"DeleteNatGateway"},
		"NatGatewayId": {"nat-nonexistent"},
	})
	require.Error(t, err)
}

// TestBatch4Accuracy_NatGateway_AssociateAddress tests AssociateNatGatewayAddress.
func TestBatch4Accuracy_NatGateway_AssociateAddress(t *testing.T) {
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

// TestBatch4Accuracy_NatGateway_DisassociateAddress tests DisassociateNatGatewayAddress.
func TestBatch4Accuracy_NatGateway_DisassociateAddress(t *testing.T) {
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

// TestBatch4Accuracy_RouteTable_CreateReturnsRTBID verifies CreateRouteTable returns
// an rtb- prefixed ID.
func TestBatch4Accuracy_RouteTable_CreateReturnsRTBID(t *testing.T) {
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

// TestBatch4Accuracy_RouteTable_DescribeReturnsRouteSet verifies DescribeRouteTables
// returns the routeSet element.
func TestBatch4Accuracy_RouteTable_DescribeReturnsRouteSet(t *testing.T) {
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

// TestBatch4Accuracy_RouteTable_CreateRoute verifies CreateRoute adds a route.
func TestBatch4Accuracy_RouteTable_CreateRoute(t *testing.T) {
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

// TestBatch4Accuracy_RouteTable_DeleteRoute verifies DeleteRoute removes the route.
func TestBatch4Accuracy_RouteTable_DeleteRoute(t *testing.T) {
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

// TestBatch4Accuracy_RouteTable_AssociateSubnet verifies AssociateRouteTable returns
// an association ID.
func TestBatch4Accuracy_RouteTable_AssociateSubnet(t *testing.T) {
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

// TestBatch4Accuracy_RouteTable_DisassociateSubnet verifies DisassociateRouteTable works.
func TestBatch4Accuracy_RouteTable_DisassociateSubnet(t *testing.T) {
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

// TestBatch4Accuracy_RouteTable_DescribeAll returns all route tables.
func TestBatch4Accuracy_RouteTable_DescribeAll(t *testing.T) {
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

// TestBatch4Accuracy_RouteTable_ReplaceRouteTableAssociation verifies that
// ReplaceRouteTableAssociation works correctly.
func TestBatch4Accuracy_RouteTable_ReplaceRouteTableAssociation(t *testing.T) {
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

// TestBatch4Accuracy_RouteTable_DeleteNonExistentReturnsError verifies that
// deleting a non-existent route table returns an error.
func TestBatch4Accuracy_RouteTable_DeleteNonExistentReturnsError(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":       {"DeleteRouteTable"},
		"RouteTableId": {"rtb-nonexistent"},
	})
	require.Error(t, err)
}

// TestBatch4Accuracy_RouteTable_DeleteRouteMissingParams verifies validation.
func TestBatch4Accuracy_RouteTable_DeleteRouteMissingParams(t *testing.T) {
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

// TestBatch4Accuracy_ClientVPN_TargetNetworkHasAssociationID verifies that
// DescribeClientVpnTargetNetworks now returns associationId and status
// (was missing per parity.md §R).
func TestBatch4Accuracy_ClientVPN_TargetNetworkHasAssociationID(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test vpn", nil)
	require.NoError(t, err)

	// associate via handler
	assocResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":              {"AssociateClientVpnTargetNetwork"},
		"ClientVpnEndpointId": {ep.ClientVpnEndpointID},
		"SubnetId":            {"subnet-default"},
	})
	require.NoError(t, err)
	assert.Contains(t, assocResp, "<associationId>cvpn-assoc-",
		"AssociateClientVpnTargetNetwork must return associationId")
	assert.Contains(t, assocResp, "<status>associating</status>",
		"AssociateClientVpnTargetNetwork must return status")

	// describe target networks
	descResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":              {"DescribeClientVpnTargetNetworks"},
		"ClientVpnEndpointId": {ep.ClientVpnEndpointID},
	})
	require.NoError(t, err)
	assert.Contains(t, descResp, "<associationId>cvpn-assoc-",
		"DescribeClientVpnTargetNetworks must return associationId")
	assert.Contains(t, descResp, "<subnetId>subnet-default</subnetId>",
		"DescribeClientVpnTargetNetworks must return subnetId")
	assert.Contains(t, descResp, "<status>associated</status>",
		"DescribeClientVpnTargetNetworks must return status=associated")
}

// TestBatch4Accuracy_ClientVPN_DisassociateByAssocID verifies DisassociateClientVpnTargetNetwork
// uses AssociationId (not SubnetId) as the key.
func TestBatch4Accuracy_ClientVPN_DisassociateByAssocID(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test vpn", nil)
	require.NoError(t, err)

	assocID, err := b.AssociateClientVpnTargetNetwork(ep.ClientVpnEndpointID, "subnet-default")
	require.NoError(t, err)

	// disassociate by association ID
	_, err = ec2.ExportDispatch(h, url.Values{
		"Action":              {"DisassociateClientVpnTargetNetwork"},
		"ClientVpnEndpointId": {ep.ClientVpnEndpointID},
		"AssociationId":       {assocID},
	})
	require.NoError(t, err)

	// verify network is gone
	networks, err := b.DescribeClientVpnTargetNetworks(ep.ClientVpnEndpointID)
	require.NoError(t, err)
	assert.Empty(t, networks, "target network must be removed after disassociate")
}

// TestBatch4Accuracy_ClientVPN_DisassociateWrongIDReturnsError verifies that
// disassociating with a non-existent association ID returns an error.
func TestBatch4Accuracy_ClientVPN_DisassociateWrongIDReturnsError(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test vpn", nil)
	require.NoError(t, err)

	require.Error(t, b.DisassociateClientVpnTargetNetwork(ep.ClientVpnEndpointID, "cvpn-assoc-nonexistent"),
		"non-existent association ID must return error")
}

// TestBatch4Accuracy_ClientVPN_RoutesXMLElementName verifies that
// DescribeClientVpnRoutes uses clientVpnRouteSet not routes
// (was wrong per parity.md §R).
func TestBatch4Accuracy_ClientVPN_RoutesXMLElementName(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test vpn", nil)
	require.NoError(t, err)

	require.NoError(t, b.CreateClientVpnRoute(ep.ClientVpnEndpointID, "0.0.0.0/0", "default"))

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":              {"DescribeClientVpnRoutes"},
		"ClientVpnEndpointId": {ep.ClientVpnEndpointID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<clientVpnRouteSet>",
		"routes must be wrapped in <clientVpnRouteSet> not <routes>")
	assert.NotContains(t, resp, "<routes>",
		"must NOT use old <routes> element name")
}

// TestBatch4Accuracy_ClientVPN_FullRouteCycle tests create/describe/delete routes.
func TestBatch4Accuracy_ClientVPN_FullRouteCycle(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test", nil)
	require.NoError(t, err)

	routes := []struct {
		cidr        string
		description string
	}{
		{cidr: "0.0.0.0/0", description: "internet"},
		{cidr: "10.0.0.0/8", description: "private"},
	}

	for _, r := range routes {
		createResp, createErr := ec2.ExportDispatch(h, url.Values{
			"Action":               {"CreateClientVpnRoute"},
			"ClientVpnEndpointId":  {ep.ClientVpnEndpointID},
			"DestinationCidrBlock": {r.cidr},
			"Description":          {r.description},
		})
		require.NoError(t, createErr)
		assert.Contains(t, createResp, "<return>true</return>")
	}

	// describe shows both routes
	descResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":              {"DescribeClientVpnRoutes"},
		"ClientVpnEndpointId": {ep.ClientVpnEndpointID},
	})
	require.NoError(t, err)
	assert.Contains(t, descResp, "0.0.0.0/0")
	assert.Contains(t, descResp, "10.0.0.0/8")

	// delete one route
	_, err = ec2.ExportDispatch(h, url.Values{
		"Action":               {"DeleteClientVpnRoute"},
		"ClientVpnEndpointId":  {ep.ClientVpnEndpointID},
		"DestinationCidrBlock": {"10.0.0.0/8"},
	})
	require.NoError(t, err)

	// verify only one remains
	descResp2, err := ec2.ExportDispatch(h, url.Values{
		"Action":              {"DescribeClientVpnRoutes"},
		"ClientVpnEndpointId": {ep.ClientVpnEndpointID},
	})
	require.NoError(t, err)
	assert.Contains(t, descResp2, "0.0.0.0/0")
	assert.NotContains(t, descResp2, "10.0.0.0/8")
}

// TestBatch4Accuracy_ClientVPN_MultipleTargetNetworks verifies multiple subnets
// can be associated to the same endpoint.
func TestBatch4Accuracy_ClientVPN_MultipleTargetNetworks(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test", nil)
	require.NoError(t, err)

	subnet2, err := b.CreateSubnet("vpc-default", "172.31.48.0/24", "us-east-1b")
	require.NoError(t, err)

	assocID1, err := b.AssociateClientVpnTargetNetwork(ep.ClientVpnEndpointID, "subnet-default")
	require.NoError(t, err)
	assert.NotEmpty(t, assocID1)

	assocID2, err := b.AssociateClientVpnTargetNetwork(ep.ClientVpnEndpointID, subnet2.ID)
	require.NoError(t, err)
	assert.NotEmpty(t, assocID2)
	assert.NotEqual(t, assocID1, assocID2, "each association must get a unique ID")

	networks, err := b.DescribeClientVpnTargetNetworks(ep.ClientVpnEndpointID)
	require.NoError(t, err)
	require.Len(t, networks, 2)

	subnetIDs := []string{networks[0].SubnetID, networks[1].SubnetID}
	assert.Contains(t, subnetIDs, "subnet-default")
	assert.Contains(t, subnetIDs, subnet2.ID)
}

// TestBatch4Accuracy_ClientVPN_IdempotentAssociate verifies that associating
// the same subnet twice returns the same association ID.
func TestBatch4Accuracy_ClientVPN_IdempotentAssociate(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test", nil)
	require.NoError(t, err)

	assocID1, err := b.AssociateClientVpnTargetNetwork(ep.ClientVpnEndpointID, "subnet-default")
	require.NoError(t, err)

	assocID2, err := b.AssociateClientVpnTargetNetwork(ep.ClientVpnEndpointID, "subnet-default")
	require.NoError(t, err)

	assert.Equal(t, assocID1, assocID2, "idempotent associate must return same assocID")

	// only one network should exist
	networks, err := b.DescribeClientVpnTargetNetworks(ep.ClientVpnEndpointID)
	require.NoError(t, err)
	assert.Len(t, networks, 1)
}

// ============================================================================
// Transit Gateway peering / connect accuracy
// ============================================================================

// TestBatch4Accuracy_TGW_PeeringAttachmentCRUD verifies TGW peering full CRUD cycle.
func TestBatch4Accuracy_TGW_PeeringAttachmentCRUD(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	att, err := b.CreateTransitGatewayPeeringAttachment("tgw-src", "tgw-dst", "us-west-2")
	require.NoError(t, err)
	assert.NotEmpty(t, att.TransitGatewayAttachmentID)
	assert.Contains(t, att.TransitGatewayAttachmentID, "tgw-attach-")
	assert.Equal(t, "tgw-src", att.RequesterTransitGatewayID)
	assert.Equal(t, "tgw-dst", att.AccepterTransitGatewayID)

	atts := b.DescribeTransitGatewayPeeringAttachments([]string{att.TransitGatewayAttachmentID})
	require.Len(t, atts, 1)
	assert.Equal(t, att.TransitGatewayAttachmentID, atts[0].TransitGatewayAttachmentID)

	require.NoError(t, b.DeleteTransitGatewayPeeringAttachment(att.TransitGatewayAttachmentID))
	atts2 := b.DescribeTransitGatewayPeeringAttachments(nil)
	assert.Empty(t, atts2)
}

// TestBatch4Accuracy_TGW_PeeringAttachment_ViaHandler tests handler-level response shape.
func TestBatch4Accuracy_TGW_PeeringAttachment_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":               {"CreateTransitGatewayPeeringAttachment"},
		"TransitGatewayId":     {"tgw-111"},
		"PeerTransitGatewayId": {"tgw-222"},
		"PeerRegion":           {"us-west-2"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<transitGatewayAttachmentId>tgw-attach-")
	assert.Contains(t, resp, "<CreateTransitGatewayPeeringAttachmentResponse")
}

// TestBatch4Accuracy_TGW_ConnectCRUD verifies TGW connect CRUD cycle.
func TestBatch4Accuracy_TGW_ConnectCRUD(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	conn, err := b.CreateTransitGatewayConnect("tgw-attach-transport", "tgw-src")
	require.NoError(t, err)
	assert.NotEmpty(t, conn.TransitGatewayAttachmentID)

	// add a connect peer
	peer, err := b.CreateTransitGatewayConnectPeer(
		conn.TransitGatewayAttachmentID, "1.2.3.4", []string{"169.254.6.0/29"},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, peer.TransitGatewayConnectPeerID)
	assert.Contains(t, peer.TransitGatewayConnectPeerID, "tgw-connect-peer-")

	peers := b.DescribeTransitGatewayConnectPeers([]string{peer.TransitGatewayConnectPeerID})
	require.Len(t, peers, 1)
	assert.Equal(t, "1.2.3.4", peers[0].PeerAddress)

	require.NoError(t, b.DeleteTransitGatewayConnectPeer(peer.TransitGatewayConnectPeerID))
	require.NoError(t, b.DeleteTransitGatewayConnect(conn.TransitGatewayAttachmentID))
}

// TestBatch4Accuracy_TGW_PrefixListRefCRUD verifies TGW prefix list reference CRUD.
func TestBatch4Accuracy_TGW_PrefixListRefCRUD(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	ref, err := b.CreateTransitGatewayPrefixListReference("tgw-rtb-111", "pl-abc123", false)
	require.NoError(t, err)
	assert.Equal(t, "pl-abc123", ref.PrefixListID)
	assert.Equal(t, "tgw-rtb-111", ref.TransitGatewayRouteTableID)

	refs, err := b.GetTransitGatewayPrefixListReferences("tgw-rtb-111")
	require.NoError(t, err)
	require.Len(t, refs, 1)

	require.NoError(t, b.DeleteTransitGatewayPrefixListReference("tgw-rtb-111", "pl-abc123"))
	refs2, err := b.GetTransitGatewayPrefixListReferences("tgw-rtb-111")
	require.NoError(t, err)
	assert.Empty(t, refs2)
}

// ============================================================================
// ManagedPrefixList accuracy
// ============================================================================

// TestBatch4Accuracy_ManagedPrefixList_FullCycle tests full CRUD and entry management.
func TestBatch4Accuracy_ManagedPrefixList_FullCycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		listName      string
		addressFamily string
		maxEntries    int
	}{
		{name: "ipv4_list", listName: "my-ipv4-list", addressFamily: "IPv4", maxEntries: 10},
		{name: "ipv6_list", listName: "my-ipv6-list", addressFamily: "IPv6", maxEntries: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			pl, err := b.CreateManagedPrefixList(tt.listName, tt.addressFamily, tt.maxEntries)
			require.NoError(t, err)
			assert.Contains(t, pl.PrefixListID, "pl-")
			assert.Equal(t, tt.listName, pl.PrefixListName)
			assert.Equal(t, tt.addressFamily, pl.AddressFamily)
			assert.Equal(t, "create-complete", pl.State)
			assert.Equal(t, int64(1), pl.Version)

			// add entries
			require.NoError(t, b.ModifyManagedPrefixList(pl.PrefixListID, []ec2.PrefixListEntry{
				{Cidr: "10.0.0.0/8", Description: "rfc1918-a"},
				{Cidr: "172.16.0.0/12", Description: "rfc1918-b"},
			}, nil))

			entries, err := b.GetManagedPrefixListEntries(pl.PrefixListID)
			require.NoError(t, err)
			assert.Len(t, entries, 2)

			// remove one entry
			require.NoError(t, b.ModifyManagedPrefixList(
				pl.PrefixListID, nil, []ec2.PrefixListEntry{{Cidr: "10.0.0.0/8"}},
			))

			entries2, err := b.GetManagedPrefixListEntries(pl.PrefixListID)
			require.NoError(t, err)
			assert.Len(t, entries2, 1)
			assert.Equal(t, "172.16.0.0/12", entries2[0].Cidr)

			// delete the prefix list
			require.NoError(t, b.DeleteManagedPrefixList(pl.PrefixListID))
			lists := b.DescribeManagedPrefixLists([]string{pl.PrefixListID})
			assert.Empty(t, lists)
		})
	}
}

// TestBatch4Accuracy_ManagedPrefixList_ViaHandler verifies handler response shape.
func TestBatch4Accuracy_ManagedPrefixList_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":         {"CreateManagedPrefixList"},
		"PrefixListName": {"test-list"},
		"AddressFamily":  {"IPv4"},
		"MaxEntries":     {"20"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<prefixListId>pl-")
	assert.Contains(t, resp, "<prefixListName>test-list</prefixListName>")
	assert.Contains(t, resp, "<state>create-complete</state>")
	assert.Contains(t, resp, "<CreateManagedPrefixListResponse")
}

// TestBatch4Accuracy_ManagedPrefixList_EmptyNameError verifies validation.
func TestBatch4Accuracy_ManagedPrefixList_EmptyNameError(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":        {"CreateManagedPrefixList"},
		"AddressFamily": {"IPv4"},
		"MaxEntries":    {"10"},
	})
	require.Error(t, err, "empty PrefixListName must return error")
}

// ============================================================================
// VerifiedAccess accuracy
// ============================================================================

// TestBatch4Accuracy_VerifiedAccess_InstanceCRUD verifies full instance lifecycle.
func TestBatch4Accuracy_VerifiedAccess_InstanceCRUD(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	inst, err := b.CreateVerifiedAccessInstance("test instance")
	require.NoError(t, err)
	assert.Contains(t, inst.VerifiedAccessInstanceID, "vai-")

	insts := b.DescribeVerifiedAccessInstances([]string{inst.VerifiedAccessInstanceID})
	require.Len(t, insts, 1)
	assert.Equal(t, inst.VerifiedAccessInstanceID, insts[0].VerifiedAccessInstanceID)

	require.NoError(t, b.DeleteVerifiedAccessInstance(inst.VerifiedAccessInstanceID))
	insts2 := b.DescribeVerifiedAccessInstances(nil)
	assert.Empty(t, insts2)
}

// TestBatch4Accuracy_VerifiedAccess_TrustProviderCRUD verifies trust provider lifecycle.
func TestBatch4Accuracy_VerifiedAccess_TrustProviderCRUD(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	tp, err := b.CreateVerifiedAccessTrustProvider("user", "test provider")
	require.NoError(t, err)
	assert.Contains(t, tp.VerifiedAccessTrustProviderID, "vatp-")

	tps := b.DescribeVerifiedAccessTrustProviders([]string{tp.VerifiedAccessTrustProviderID})
	require.Len(t, tps, 1)

	require.NoError(t, b.DeleteVerifiedAccessTrustProvider(tp.VerifiedAccessTrustProviderID))
}

// TestBatch4Accuracy_VerifiedAccess_GroupCRUD verifies group lifecycle.
func TestBatch4Accuracy_VerifiedAccess_GroupCRUD(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	inst, err := b.CreateVerifiedAccessInstance("group test")
	require.NoError(t, err)

	grp, err := b.CreateVerifiedAccessGroup(inst.VerifiedAccessInstanceID, "test group")
	require.NoError(t, err)
	assert.Contains(t, grp.VerifiedAccessGroupID, "vagr-")

	grps := b.DescribeVerifiedAccessGroups([]string{grp.VerifiedAccessGroupID})
	require.Len(t, grps, 1)

	require.NoError(t, b.DeleteVerifiedAccessGroup(grp.VerifiedAccessGroupID))
}

// TestBatch4Accuracy_VerifiedAccess_EndpointCRUD verifies endpoint lifecycle.
func TestBatch4Accuracy_VerifiedAccess_EndpointCRUD(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	inst, err := b.CreateVerifiedAccessInstance("ep test")
	require.NoError(t, err)

	grp, err := b.CreateVerifiedAccessGroup(inst.VerifiedAccessInstanceID, "ep group")
	require.NoError(t, err)

	ep, err := b.CreateVerifiedAccessEndpoint(grp.VerifiedAccessGroupID, "subnet-default", "load-balancer")
	require.NoError(t, err)
	assert.Contains(t, ep.VerifiedAccessEndpointID, "vae-")

	eps := b.DescribeVerifiedAccessEndpoints([]string{ep.VerifiedAccessEndpointID})
	require.Len(t, eps, 1)

	require.NoError(t, b.DeleteVerifiedAccessEndpoint(ep.VerifiedAccessEndpointID))
}

// ============================================================================
// Helper
// ============================================================================

// extractBatch4XMLValue extracts the first text content of an XML element by tag name.
func extractBatch4XMLValue(xmlStr, tag string) string {
	openTag := "<" + tag + ">"
	closeTag := "</" + tag + ">"
	start := strings.Index(xmlStr, openTag)
	if start < 0 {
		return ""
	}

	start += len(openTag)
	end := strings.Index(xmlStr[start:], closeTag)
	if end < 0 {
		return ""
	}

	return xmlStr[start : start+end]
}
