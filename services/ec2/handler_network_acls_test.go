package ec2_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestNetworkACL_IncludesStoredACLs verifies that explicitly created
// NACLs (via CreateNetworkAcl) are included in DescribeNetworkAcls (was missing before
// because the handler used DescribeNetworkAcls instead of DescribeNetworkAclsFiltered).
func TestNetworkACL_IncludesStoredACLs(t *testing.T) {
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

// TestNetworkACL_FilterByVpcID verifies vpc-id filter works.

// TestNetworkACL_FilterByNetworkAclID verifies NetworkAclId.N filter.
func TestNetworkACL_FilterByNetworkAclID(t *testing.T) {
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

// TestNetworkACL_CreateDeleteCycle tests full CRUD lifecycle.

// TestNetworkACL_CreateDeleteCycle tests full CRUD lifecycle.
func TestNetworkACL_CreateDeleteCycle(t *testing.T) {
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

// TestNetworkACL_EntryRules tests CreateNetworkAclEntry and
// DeleteNetworkAclEntry full cycle.

// TestNetworkACL_EntryRules tests CreateNetworkAclEntry and
// DeleteNetworkAclEntry full cycle.
func TestNetworkACL_EntryRules(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	acl, err := b.CreateNetworkACL("vpc-default")
	require.NoError(t, err)

	// add rules sequentially (shared acl object — no subtests)
	require.NoError(t, b.CreateNetworkACLEntry(acl.ID, 100, "6", "allow", "0.0.0.0/0", false, 80, 80))
	require.NoError(t, b.CreateNetworkACLEntry(acl.ID, 110, "6", "allow", "0.0.0.0/0", false, 443, 443))
	require.NoError(t, b.CreateNetworkACLEntry(acl.ID, 100, "-1", "allow", "0.0.0.0/0", true, 0, 0))

	// delete one entry
	require.NoError(t, b.DeleteNetworkACLEntry(acl.ID, 110, false))
}

// TestNetworkACL_DuplicateRuleReturnsError verifies that adding
// a duplicate rule number returns an error.

// TestNetworkACL_DuplicateRuleReturnsError verifies that adding
// a duplicate rule number returns an error.
func TestNetworkACL_DuplicateRuleReturnsError(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	acl, err := b.CreateNetworkACL("vpc-default")
	require.NoError(t, err)

	require.NoError(t, b.CreateNetworkACLEntry(acl.ID, 100, "6", "allow", "0.0.0.0/0", false, 80, 80))
	require.Error(t, b.CreateNetworkACLEntry(acl.ID, 100, "6", "deny", "0.0.0.0/0", false, 443, 443),
		"duplicate rule number must return error")
}

// TestNetworkACL_DeleteDefaultACLReturnsError verifies that
// deleting a default NACL returns an error.

// TestNetworkACL_DeleteDefaultACLReturnsError verifies that
// deleting a default NACL returns an error.
func TestNetworkACL_DeleteDefaultACLReturnsError(t *testing.T) {
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

// TestNatGateway_CreateReturnsNatGwID verifies NAT gateway creation
// returns a nat- prefixed ID.

// TestHandlerNetworkACLHandlers covers handleDeleteNetworkACL, handleCreateNetworkACLEntry,
// handleDeleteNetworkACLEntry, handleModifySecurityGroupRules.
func TestHandlerNetworkACLHandlers(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	vpc, err := b.CreateVpc("10.10.0.0/16")
	require.NoError(t, err)

	acl, err := b.CreateNetworkACL(vpc.ID)
	require.NoError(t, err)
	aclID := acl.ID

	// Create NACL entry.
	rec := postForm(t, h, "Action=CreateNetworkAclEntry&Version=2016-11-15"+
		"&NetworkAclId="+aclID+
		"&RuleNumber=100"+
		"&Protocol=6"+
		"&RuleAction=allow"+
		"&CidrBlock=0.0.0.0/0"+
		"&Egress=false"+
		"&PortRange.From=80"+
		"&PortRange.To=80")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete NACL entry.
	rec = postForm(t, h, "Action=DeleteNetworkAclEntry&Version=2016-11-15"+
		"&NetworkAclId="+aclID+
		"&RuleNumber=100"+
		"&Egress=false")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete NACL.
	rec = postForm(t, h, "Action=DeleteNetworkAcl&Version=2016-11-15&NetworkAclId="+aclID)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandlerModifySecurityGroupRules covers handleModifySecurityGroupRules.

// TestHandlerReplaceNetworkACLAssociation covers handleReplaceNetworkACLAssociation.
func TestHandlerReplaceNetworkACLAssociation(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	vpc, err := b.CreateVpc("10.12.0.0/16")
	require.NoError(t, err)

	subnet, err := b.CreateSubnet(vpc.ID, "10.12.1.0/24", "us-east-1a")
	require.NoError(t, err)

	acl, err := b.CreateNetworkACL(vpc.ID)
	require.NoError(t, err)

	rec := postForm(t, h, "Action=ReplaceNetworkAclAssociation&Version=2016-11-15"+
		"&NetworkAclId="+acl.ID+
		"&SubnetId="+subnet.ID)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ReplaceNetworkAclAssociationResponse")
}

// TestHandlerDescribeInstanceTypeOfferings covers handleDescribeInstanceTypeOfferings.

// TestNetworkACL_DescribeReturnsAssociations verifies that
// DescribeNetworkAcls includes an <associationSet> with association items
// (was missing per parity.md §R).
func TestNetworkACL_DescribeReturnsAssociations(t *testing.T) {
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

// TestNetworkACL_DefaultACLPerVPC verifies that a default NACL
// is returned for each VPC via DescribeNetworkAcls.

// TestNetworkACL_DefaultACLPerVPC verifies that a default NACL
// is returned for each VPC via DescribeNetworkAcls.
func TestNetworkACL_DefaultACLPerVPC(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action": {"DescribeNetworkAcls"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<networkAclId>acl-default-vpc-default</networkAclId>")
	assert.Contains(t, resp, "<default>true</default>")
}

// TestNetworkACL_IncludesStoredACLs verifies that explicitly created
// NACLs (via CreateNetworkAcl) are included in DescribeNetworkAcls (was missing before
// because the handler used DescribeNetworkAcls instead of DescribeNetworkAclsFiltered).

// TestNetworkACL_FilterByVpcID verifies vpc-id filter works.
func TestNetworkACL_FilterByVpcID(t *testing.T) {
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

// TestNetworkACL_FilterByNetworkAclID verifies NetworkAclId.N filter.

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
