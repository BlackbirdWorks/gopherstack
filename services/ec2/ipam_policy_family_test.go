package ec2_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestIpamPolicy_CRUD exercises the IPAM Policy backend at the resource level: creation
// under an IPAM, describe, allocation rule get/modify, enablement (account-level and
// Organizations-target-level), and deletion.
func TestIpamPolicy_CRUD(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	ipam, err := bk.CreateIpam(ec2.IpamOptions{Description: "test ipam"})
	require.NoError(t, err)

	// ---- Create / Describe ----
	_, err = bk.CreateIpamPolicy("ipam-missing")
	require.Error(t, err)

	policy, err := bk.CreateIpamPolicy(ipam.IpamID)
	require.NoError(t, err)
	assert.NotEmpty(t, policy.IpamPolicyID)
	assert.Contains(t, policy.IpamPolicyARN, policy.IpamPolicyID)
	assert.Equal(t, ipam.IpamID, policy.IpamID)
	assert.Equal(t, "create-complete", policy.State)

	found := bk.DescribeIpamPolicies([]string{policy.IpamPolicyID})
	require.Len(t, found, 1)
	assert.Equal(t, policy.IpamPolicyID, found[0].IpamPolicyID)

	assert.Empty(t, bk.DescribeIpamPolicies([]string{"ipam-policy-missing"}))

	other, err := bk.CreateIpamPolicy(ipam.IpamID)
	require.NoError(t, err)
	assert.Len(t, bk.DescribeIpamPolicies(nil), 2)

	// ---- Allocation rules: get (empty) / modify / get (populated) ----
	rules, err := bk.GetIpamPolicyAllocationRules(policy.IpamPolicyID, "", "")
	require.NoError(t, err)
	assert.Empty(t, rules)

	_, err = bk.GetIpamPolicyAllocationRules("ipam-policy-missing", "", "")
	require.Error(t, err)

	pool, err := bk.CreateIpamPool(ipam.IpamID, "ipv4", "us-east-1", "10.0.0.0/8")
	require.NoError(t, err)

	_, err = bk.ModifyIpamPolicyAllocationRules(policy.IpamPolicyID, "", "eip", nil)
	require.Error(t, err, "Locale is required")

	_, err = bk.ModifyIpamPolicyAllocationRules(policy.IpamPolicyID, "us-east-1", "", nil)
	require.Error(t, err, "ResourceType is required")

	doc, err := bk.ModifyIpamPolicyAllocationRules(
		policy.IpamPolicyID, "us-east-1", "eip",
		[]ec2.IpamPolicyAllocationRule{{SourceIpamPoolID: pool.IpamPoolID}},
	)
	require.NoError(t, err)
	assert.Equal(t, "us-east-1", doc.Locale)
	assert.Equal(t, "eip", doc.ResourceType)
	require.Len(t, doc.AllocationRules, 1)
	assert.Equal(t, pool.IpamPoolID, doc.AllocationRules[0].SourceIpamPoolID)

	docs, err := bk.GetIpamPolicyAllocationRules(policy.IpamPolicyID, "us-east-1", "eip")
	require.NoError(t, err)
	require.Len(t, docs, 1)
	assert.Equal(t, pool.IpamPoolID, docs[0].AllocationRules[0].SourceIpamPoolID)

	// Filtering by a locale/resourceType with no matching document returns empty, not an error.
	noneDocs, err := bk.GetIpamPolicyAllocationRules(policy.IpamPolicyID, "eu-west-1", "eip")
	require.NoError(t, err)
	assert.Empty(t, noneDocs)

	// ---- Enablement: account-level ----
	id, enabled, _ := bk.GetEnabledIpamPolicy()
	assert.False(t, enabled)
	assert.Empty(t, id)

	require.Error(t, bk.EnableIpamPolicy("ipam-policy-missing", ""))
	require.NoError(t, bk.EnableIpamPolicy(policy.IpamPolicyID, ""))

	id, enabled, managedBy := bk.GetEnabledIpamPolicy()
	assert.True(t, enabled)
	assert.Equal(t, policy.IpamPolicyID, id)
	assert.Equal(t, "account", managedBy)

	// Enabling a second policy for the account supersedes the first.
	require.NoError(t, bk.EnableIpamPolicy(other.IpamPolicyID, ""))
	id, enabled, _ = bk.GetEnabledIpamPolicy()
	assert.True(t, enabled)
	assert.Equal(t, other.IpamPolicyID, id)

	require.Error(t, bk.DisableIpamPolicy("ipam-policy-missing", ""))
	require.NoError(t, bk.DisableIpamPolicy(other.IpamPolicyID, ""))

	_, enabled, _ = bk.GetEnabledIpamPolicy()
	assert.False(t, enabled)

	// ---- Enablement: Organizations target ----
	targets, err := bk.GetIpamPolicyOrganizationTargets(policy.IpamPolicyID)
	require.NoError(t, err)
	assert.Empty(t, targets)

	_, err = bk.GetIpamPolicyOrganizationTargets("ipam-policy-missing")
	require.Error(t, err)

	require.NoError(t, bk.EnableIpamPolicy(policy.IpamPolicyID, "ou-example-1"))
	require.NoError(t, bk.EnableIpamPolicy(policy.IpamPolicyID, "ou-example-2"))

	targets, err = bk.GetIpamPolicyOrganizationTargets(policy.IpamPolicyID)
	require.NoError(t, err)
	assert.Equal(t, []string{"ou-example-1", "ou-example-2"}, targets)

	require.NoError(t, bk.DisableIpamPolicy(policy.IpamPolicyID, "ou-example-1"))

	targets, err = bk.GetIpamPolicyOrganizationTargets(policy.IpamPolicyID)
	require.NoError(t, err)
	assert.Equal(t, []string{"ou-example-2"}, targets)

	// The account-level enablement is independent of Organizations-target enablement.
	_, enabled, _ = bk.GetEnabledIpamPolicy()
	assert.False(t, enabled)

	// ---- Delete ----
	_, err = bk.DeleteIpamPolicy("ipam-policy-missing")
	require.Error(t, err)

	deleted, err := bk.DeleteIpamPolicy(policy.IpamPolicyID)
	require.NoError(t, err)
	assert.Equal(t, "delete-complete", deleted.State)
	assert.Empty(t, bk.DescribeIpamPolicies([]string{policy.IpamPolicyID}))

	// Deleting a policy clears any Organizations target enablement referencing it.
	remainingTargets, err := bk.GetIpamPolicyOrganizationTargets(other.IpamPolicyID)
	require.NoError(t, err)
	assert.Empty(t, remainingTargets)
}

// TestIpamOrganizationAdminAccount_EnableDisable exercises the IPAM Organizations delegated
// admin account setting.
func TestIpamOrganizationAdminAccount_EnableDisable(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	require.Error(t, bk.EnableIpamOrganizationAdminAccount(""))
	require.NoError(t, bk.EnableIpamOrganizationAdminAccount("222222222222"))

	// Disabling a non-matching account ID fails.
	require.Error(t, bk.DisableIpamOrganizationAdminAccount("333333333333"))

	require.NoError(t, bk.DisableIpamOrganizationAdminAccount("222222222222"))

	// Disabling again (nothing currently enabled) fails.
	require.Error(t, bk.DisableIpamOrganizationAdminAccount("222222222222"))
}

// TestMoveByoipCidrToIpam_Backend exercises associating an existing BYOIP CIDR with an IPAM
// pool: the CIDR must already exist, the pool must exist, and after the move the pool's
// provisioned CIDRs include it.
func TestMoveByoipCidrToIpam_Backend(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	ipam, err := bk.CreateIpam()
	require.NoError(t, err)

	pool, err := bk.CreateIpamPool(ipam.IpamID, "ipv4", "us-east-1", "")
	require.NoError(t, err)

	// The CIDR must already exist as a BYOIP CIDR.
	_, err = bk.MoveByoipCidrToIpam("198.51.100.0/24", pool.IpamPoolID, "123456789012")
	require.Error(t, err)

	_, err = bk.ProvisionByoipCidr("198.51.100.0/24", "test cidr")
	require.NoError(t, err)

	// IpamPoolOwner is required.
	_, err = bk.MoveByoipCidrToIpam("198.51.100.0/24", pool.IpamPoolID, "")
	require.Error(t, err)

	// The IPAM pool must exist.
	_, err = bk.MoveByoipCidrToIpam("198.51.100.0/24", "ipam-pool-missing", "123456789012")
	require.Error(t, err)

	entry, err := bk.MoveByoipCidrToIpam("198.51.100.0/24", pool.IpamPoolID, "123456789012")
	require.NoError(t, err)
	assert.Equal(t, "198.51.100.0/24", entry.Cidr)
	assert.Equal(t, "provisioned", entry.State)

	cidrs := bk.GetIpamPoolCidrs(pool.IpamPoolID)
	require.Len(t, cidrs, 1)
	assert.Equal(t, "198.51.100.0/24", cidrs[0].Cidr)

	// Moving the same CIDR again does not duplicate the pool's provisioned CIDR entry.
	_, err = bk.MoveByoipCidrToIpam("198.51.100.0/24", pool.IpamPoolID, "123456789012")
	require.NoError(t, err)
	assert.Len(t, bk.GetIpamPoolCidrs(pool.IpamPoolID), 1)
}

// TestIpamPolicy_HTTP exercises the family end-to-end through the EC2 Query-protocol HTTP
// handler, verifying real AWS-shaped XML element names (not the generic stub placeholder).
func TestIpamPolicy_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createIpamRec := postForm(t, h, "Action=CreateIpam&Version=2016-11-15")
	require.Equal(t, http.StatusOK, createIpamRec.Code)

	ipamBody := createIpamRec.Body.String()
	ipamID := ipamBody[indexOf(ipamBody, "<ipamId>")+len("<ipamId>") : indexOf(ipamBody, "</ipamId>")]
	require.NotEmpty(t, ipamID)

	// CreateIpamPolicy requires IpamId; verify it dispatches to the real handler (not a stub
	// placeholder) and errors correctly when the ID is missing.
	missingIDRec := postForm(t, h, "Action=CreateIpamPolicy&Version=2016-11-15")
	assert.NotEqual(t, http.StatusOK, missingIDRec.Code)
	assert.Contains(t, missingIDRec.Body.String(), "InvalidParameterValue")

	createRec := postForm(t, h, fmt.Sprintf("Action=CreateIpamPolicy&Version=2016-11-15&IpamId=%s", ipamID))
	require.Equal(t, http.StatusOK, createRec.Code)

	body := createRec.Body.String()
	assert.Contains(t, body, "<CreateIpamPolicyResponse")
	assert.Contains(t, body, "<state>create-complete</state>")
	assert.Contains(t, body, fmt.Sprintf("<ipamId>%s</ipamId>", ipamID))

	policyID := body[indexOf(body, "<ipamPolicyId>")+len("<ipamPolicyId>") : indexOf(body, "</ipamPolicyId>")]
	require.NotEmpty(t, policyID)

	describeRec := postForm(
		t, h, fmt.Sprintf("Action=DescribeIpamPolicies&Version=2016-11-15&IpamPolicyId.1=%s", policyID),
	)
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Contains(t, describeRec.Body.String(), "<ipamPolicySet>")

	modifyRulesRec := postForm(t, h, fmt.Sprintf(
		"Action=ModifyIpamPolicyAllocationRules&Version=2016-11-15&IpamPolicyId=%s&Locale=us-east-1"+
			"&ResourceType=eip&AllocationRule.1.SourceIpamPoolId=ipam-pool-abc123",
		policyID,
	))
	require.Equal(t, http.StatusOK, modifyRulesRec.Code)

	modifyBody := modifyRulesRec.Body.String()
	assert.Contains(t, modifyBody, "<ModifyIpamPolicyAllocationRulesResponse")
	assert.Contains(t, modifyBody, "<sourceIpamPoolId>ipam-pool-abc123</sourceIpamPoolId>")

	getRulesRec := postForm(
		t, h, fmt.Sprintf("Action=GetIpamPolicyAllocationRules&Version=2016-11-15&IpamPolicyId=%s", policyID),
	)
	require.Equal(t, http.StatusOK, getRulesRec.Code)
	assert.Contains(t, getRulesRec.Body.String(), "<ipamPolicyDocumentSet>")
	assert.Contains(t, getRulesRec.Body.String(), "ipam-pool-abc123")

	enableRec := postForm(
		t, h, fmt.Sprintf("Action=EnableIpamPolicy&Version=2016-11-15&IpamPolicyId=%s", policyID),
	)
	require.Equal(t, http.StatusOK, enableRec.Code)
	assert.Contains(t, enableRec.Body.String(), "<EnableIpamPolicyResponse")
	assert.Contains(t, enableRec.Body.String(), fmt.Sprintf("<ipamPolicyId>%s</ipamPolicyId>", policyID))

	getEnabledRec := postForm(t, h, "Action=GetEnabledIpamPolicy&Version=2016-11-15")
	require.Equal(t, http.StatusOK, getEnabledRec.Code)
	assert.Contains(t, getEnabledRec.Body.String(), "<ipamPolicyEnabled>true</ipamPolicyEnabled>")
	assert.Contains(t, getEnabledRec.Body.String(), fmt.Sprintf("<ipamPolicyId>%s</ipamPolicyId>", policyID))

	orgTargetsRec := postForm(t, h, fmt.Sprintf(
		"Action=EnableIpamPolicy&Version=2016-11-15&IpamPolicyId=%s&OrganizationTargetId=ou-example-1", policyID,
	))
	require.Equal(t, http.StatusOK, orgTargetsRec.Code)

	getOrgTargetsRec := postForm(
		t, h, fmt.Sprintf("Action=GetIpamPolicyOrganizationTargets&Version=2016-11-15&IpamPolicyId=%s", policyID),
	)
	require.Equal(t, http.StatusOK, getOrgTargetsRec.Code)
	assert.Contains(t, getOrgTargetsRec.Body.String(), "<organizationTargetSet>")
	assert.Contains(t, getOrgTargetsRec.Body.String(), "<organizationTargetId>ou-example-1</organizationTargetId>")

	disableRec := postForm(
		t, h, fmt.Sprintf("Action=DisableIpamPolicy&Version=2016-11-15&IpamPolicyId=%s", policyID),
	)
	require.Equal(t, http.StatusOK, disableRec.Code)
	assert.Contains(t, disableRec.Body.String(), "<return>true</return>")

	deleteRec := postForm(
		t, h, fmt.Sprintf("Action=DeleteIpamPolicy&Version=2016-11-15&IpamPolicyId=%s", policyID),
	)
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Contains(t, deleteRec.Body.String(), "<state>delete-complete</state>")

	// ---- Organizations delegated admin account ----
	enableAdminRec := postForm(
		t, h, "Action=EnableIpamOrganizationAdminAccount&Version=2016-11-15&DelegatedAdminAccountId=222222222222",
	)
	require.Equal(t, http.StatusOK, enableAdminRec.Code)
	assert.Contains(t, enableAdminRec.Body.String(), "<success>true</success>")

	disableAdminBadRec := postForm(
		t, h, "Action=DisableIpamOrganizationAdminAccount&Version=2016-11-15&DelegatedAdminAccountId=333333333333",
	)
	assert.NotEqual(t, http.StatusOK, disableAdminBadRec.Code)

	disableAdminRec := postForm(
		t, h, "Action=DisableIpamOrganizationAdminAccount&Version=2016-11-15&DelegatedAdminAccountId=222222222222",
	)
	require.Equal(t, http.StatusOK, disableAdminRec.Code)
	assert.Contains(t, disableAdminRec.Body.String(), "<success>true</success>")

	// ---- MoveByoipCidrToIpam ----
	poolRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateIpamPool&Version=2016-11-15&IpamId=%s&AddressFamily=ipv4", ipamID,
	))
	require.Equal(t, http.StatusOK, poolRec.Code)

	poolBody := poolRec.Body.String()
	poolID := poolBody[indexOf(poolBody, "<ipamPoolId>")+len("<ipamPoolId>") : indexOf(poolBody, "</ipamPoolId>")]
	require.NotEmpty(t, poolID)

	provisionRec := postForm(
		t, h, "Action=ProvisionByoipCidr&Version=2016-11-15&Cidr=203.0.113.0/24",
	)
	require.Equal(t, http.StatusOK, provisionRec.Code)

	moveMissingRec := postForm(t, h, "Action=MoveByoipCidrToIpam&Version=2016-11-15")
	assert.NotEqual(t, http.StatusOK, moveMissingRec.Code)

	moveRec := postForm(t, h, fmt.Sprintf(
		"Action=MoveByoipCidrToIpam&Version=2016-11-15&Cidr=203.0.113.0/24&IpamPoolId=%s&IpamPoolOwner=123456789012",
		poolID,
	))
	require.Equal(t, http.StatusOK, moveRec.Code)

	moveBody := moveRec.Body.String()
	assert.Contains(t, moveBody, "<MoveByoipCidrToIpamResponse")
	assert.Contains(t, moveBody, "<cidr>203.0.113.0/24</cidr>")
	assert.Contains(t, moveBody, "<state>provisioned</state>")
}
