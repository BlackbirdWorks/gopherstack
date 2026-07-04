package ec2_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestIpam_CRUD exercises the full IPAM resource family lifecycle at the backend level:
// Ipam -> IpamScope -> IpamPool -> IpamPoolCidr (provision/deprovision) -> IpamPoolAllocation,
// plus the auto-created default resource discovery and association.
func TestIpam_CRUD(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	// ---- Create IPAM: verify default scopes + resource discovery are created. ----
	ipam, err := bk.CreateIpam(ec2.IpamOptions{Description: "test ipam", Tier: "advanced"})
	require.NoError(t, err)
	assert.NotEmpty(t, ipam.IpamID)
	assert.Contains(t, ipam.IpamARN, ipam.IpamID)
	assert.Equal(t, "test ipam", ipam.Description)
	assert.NotEmpty(t, ipam.PublicDefaultScopeID)
	assert.NotEmpty(t, ipam.PrivateDefaultScopeID)
	assert.EqualValues(t, 2, ipam.ScopeCount)
	assert.NotEmpty(t, ipam.DefaultResourceDiscoveryID)
	assert.NotEmpty(t, ipam.DefaultResourceDiscoveryAssociationID)

	scopes := bk.DescribeIpamScopes([]string{ipam.PublicDefaultScopeID, ipam.PrivateDefaultScopeID})
	require.Len(t, scopes, 2)

	for _, s := range scopes {
		assert.True(t, s.IsDefault)
		assert.Equal(t, ipam.IpamID, s.IpamID)
	}

	discoveries := bk.DescribeIpamResourceDiscoveries([]string{ipam.DefaultResourceDiscoveryID})
	require.Len(t, discoveries, 1)
	assert.True(t, discoveries[0].IsDefault)

	assocs := bk.DescribeIpamResourceDiscoveryAssociations([]string{ipam.DefaultResourceDiscoveryAssociationID})
	require.Len(t, assocs, 1)
	assert.Equal(t, ipam.IpamID, assocs[0].IpamID)

	// ---- Describe / Modify Ipam ----
	found := bk.DescribeIpams([]string{ipam.IpamID})
	require.Len(t, found, 1)
	assert.Equal(t, "test ipam", found[0].Description)

	modified, err := bk.ModifyIpam(ipam.IpamID, ec2.IpamOptions{Description: "updated"})
	require.NoError(t, err)
	assert.Equal(t, "updated", modified.Description)

	// ---- IPAM Scope: create additional (non-default), modify, delete. ----
	scope, err := bk.CreateIpamScope(ipam.IpamID, "extra scope")
	require.NoError(t, err)
	assert.NotEmpty(t, scope.IpamScopeID)
	assert.False(t, scope.IsDefault)
	assert.Equal(t, "private", scope.IpamScopeType)

	scopesAfter := bk.DescribeIpamScopes(nil)
	assert.Len(t, scopesAfter, 3) // 2 default + 1 new

	modScope, err := bk.ModifyIpamScope(scope.IpamScopeID, "renamed scope")
	require.NoError(t, err)
	assert.Equal(t, "renamed scope", modScope.Description)

	require.NoError(t, bk.DeleteIpamScope(scope.IpamScopeID))
	assert.Empty(t, bk.DescribeIpamScopes([]string{scope.IpamScopeID}))

	// Default scopes cannot be deleted.
	err = bk.DeleteIpamScope(ipam.PrivateDefaultScopeID)
	require.Error(t, err)

	// ---- IPAM Pool: create (with initial CIDR), modify, provision/deprovision, allocate. ----
	pool, err := bk.CreateIpamPool(ipam.IpamID, "ipv4", "us-east-1", "10.0.0.0/8", ec2.IpamPoolOptions{
		Description: "test pool",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, pool.IpamPoolID)
	assert.Equal(t, ipam.PrivateDefaultScopeID, pool.IpamScopeID)

	pools := bk.DescribeIpamPools([]string{pool.IpamPoolID})
	require.Len(t, pools, 1)

	modPool, err := bk.ModifyIpamPool(pool.IpamPoolID, ec2.IpamPoolOptions{Description: "updated pool"})
	require.NoError(t, err)
	assert.Equal(t, "updated pool", modPool.Description)

	// The initial CIDR passed at creation is already provisioned.
	cidrs := bk.GetIpamPoolCidrs(pool.IpamPoolID)
	require.Len(t, cidrs, 1)
	assert.Equal(t, "10.0.0.0/8", cidrs[0].Cidr)

	provisioned, err := bk.ProvisionIpamPoolCidr(pool.IpamPoolID, "10.1.0.0/16")
	require.NoError(t, err)
	assert.Equal(t, "10.1.0.0/16", provisioned.Cidr)

	cidrs = bk.GetIpamPoolCidrs(pool.IpamPoolID)
	require.Len(t, cidrs, 2)

	deprovisioned, err := bk.DeprovisionIpamPoolCidr(pool.IpamPoolID, "10.1.0.0/16")
	require.NoError(t, err)
	assert.Equal(t, "10.1.0.0/16", deprovisioned.Cidr)

	cidrs = bk.GetIpamPoolCidrs(pool.IpamPoolID)
	require.Len(t, cidrs, 1)

	// ---- Allocations ----
	alloc, err := bk.AllocateIpamPoolCidr(pool.IpamPoolID, "10.0.1.0/24", 0, ec2.IpamAllocationOptions{
		Description: "my allocation",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, alloc.IpamPoolAllocationID)
	assert.Equal(t, "my allocation", alloc.Description)

	allocs, err := bk.GetIpamPoolAllocations(pool.IpamPoolID, "")
	require.NoError(t, err)
	require.Len(t, allocs, 1)
	assert.Equal(t, alloc.IpamPoolAllocationID, allocs[0].IpamPoolAllocationID)

	allocsFiltered, err := bk.GetIpamPoolAllocations(pool.IpamPoolID, alloc.IpamPoolAllocationID)
	require.NoError(t, err)
	require.Len(t, allocsFiltered, 1)

	allocsMissing, err := bk.GetIpamPoolAllocations(pool.IpamPoolID, "ipam-alloc-doesnotexist")
	require.NoError(t, err)
	assert.Empty(t, allocsMissing)

	require.NoError(t, bk.ReleaseIpamPoolAllocation(pool.IpamPoolID, alloc.IpamPoolAllocationID))

	allocsAfterRelease, err := bk.GetIpamPoolAllocations(pool.IpamPoolID, "")
	require.NoError(t, err)
	assert.Empty(t, allocsAfterRelease)

	// ---- Cleanup ----
	require.NoError(t, bk.DeleteIpamPool(pool.IpamPoolID))
	assert.Empty(t, bk.DescribeIpamPools([]string{pool.IpamPoolID}))

	require.NoError(t, bk.DeleteIpam(ipam.IpamID))
	assert.Empty(t, bk.DescribeIpams([]string{ipam.IpamID}))
	assert.Empty(t, bk.DescribeIpamScopes([]string{ipam.PublicDefaultScopeID, ipam.PrivateDefaultScopeID}))
	assert.Empty(t, bk.DescribeIpamResourceDiscoveries([]string{ipam.DefaultResourceDiscoveryID}))
}

// TestIpam_NotFound verifies error behavior for every mutating IPAM op given an unknown ID.
func TestIpam_NotFound(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	_, err := bk.ModifyIpam("ipam-missing", ec2.IpamOptions{Description: "x"})
	require.Error(t, err)

	err = bk.DeleteIpam("ipam-missing")
	require.Error(t, err)

	_, err = bk.CreateIpamScope("ipam-missing", "desc")
	require.Error(t, err)

	_, err = bk.ModifyIpamScope("ipam-scope-missing", "desc")
	require.Error(t, err)

	err = bk.DeleteIpamScope("ipam-scope-missing")
	require.Error(t, err)

	_, err = bk.CreateIpamPool("ipam-missing", "ipv4", "", "")
	require.Error(t, err)

	_, err = bk.ModifyIpamPool("ipam-pool-missing", ec2.IpamPoolOptions{})
	require.Error(t, err)

	err = bk.DeleteIpamPool("ipam-pool-missing")
	require.Error(t, err)

	_, err = bk.ProvisionIpamPoolCidr("ipam-pool-missing", "10.0.0.0/24")
	require.Error(t, err)

	_, err = bk.DeprovisionIpamPoolCidr("ipam-pool-missing", "10.0.0.0/24")
	require.Error(t, err)

	_, err = bk.AllocateIpamPoolCidr("ipam-pool-missing", "10.0.0.0/24", 0)
	require.Error(t, err)

	_, err = bk.GetIpamPoolAllocations("ipam-pool-missing", "")
	require.Error(t, err)

	err = bk.ReleaseIpamPoolAllocation("ipam-pool-missing", "ipam-alloc-missing")
	require.Error(t, err)

	// A pool that exists, but a CIDR that was never provisioned to it.
	ipam, err := bk.CreateIpam()
	require.NoError(t, err)

	pool, err := bk.CreateIpamPool(ipam.IpamID, "ipv4", "", "")
	require.NoError(t, err)

	_, err = bk.DeprovisionIpamPoolCidr(pool.IpamPoolID, "192.168.0.0/24")
	require.Error(t, err)

	// Releasing an allocation with a mismatched pool ID must fail.
	alloc, err := bk.AllocateIpamPoolCidr(pool.IpamPoolID, "10.5.0.0/24", 0)
	require.NoError(t, err)

	otherPool, err := bk.CreateIpamPool(ipam.IpamID, "ipv4", "", "")
	require.NoError(t, err)

	err = bk.ReleaseIpamPoolAllocation(otherPool.IpamPoolID, alloc.IpamPoolAllocationID)
	require.Error(t, err)
}

// TestIpam_DescribeFiltersByID verifies Describe*/Get* ops filter correctly by ID and that
// an empty ID list returns every resource (list/"pagination" semantics mirrored from
// neighboring VPC/pool Describe ops in this package).
func TestIpam_DescribeFiltersByID(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	ipamIDs := make([]string, 0, 3)

	for range 3 {
		ipam, err := bk.CreateIpam()
		require.NoError(t, err)
		ipamIDs = append(ipamIDs, ipam.IpamID)
	}

	all := bk.DescribeIpams(nil)
	assert.Len(t, all, 3)

	oneOnly := bk.DescribeIpams([]string{ipamIDs[1]})
	require.Len(t, oneOnly, 1)
	assert.Equal(t, ipamIDs[1], oneOnly[0].IpamID)

	twoOfThree := bk.DescribeIpams([]string{ipamIDs[0], ipamIDs[2]})
	assert.Len(t, twoOfThree, 2)

	none := bk.DescribeIpams([]string{"ipam-doesnotexist"})
	assert.Empty(t, none)
}

// TestIpam_HTTP exercises the family end-to-end through the EC2 Query-protocol HTTP handler,
// verifying real AWS-shaped XML element names (not the generic stub placeholder).
func TestIpam_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createRec := postForm(t, h, "Action=CreateIpam&Version=2016-11-15&Description=my-ipam")
	require.Equal(t, http.StatusOK, createRec.Code)

	body := createRec.Body.String()
	assert.Contains(t, body, "<CreateIpamResponse")
	assert.Contains(t, body, "<publicDefaultScopeId>")
	assert.Contains(t, body, "<privateDefaultScopeId>")
	assert.Contains(t, body, "<state>create-complete</state>")

	ipamID := body[indexOf(body, "<ipamId>")+len("<ipamId>") : indexOf(body, "</ipamId>")]
	require.NotEmpty(t, ipamID)

	describeRec := postForm(t, h, fmt.Sprintf("Action=DescribeIpams&Version=2016-11-15&IpamId.1=%s", ipamID))
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Contains(t, describeRec.Body.String(), "<ipamSet>")

	// CreateIpamScope requires IpamId; verify it dispatches to the real handler (not a stub
	// placeholder) and errors correctly when the ID is missing.
	missingIDRec := postForm(t, h, "Action=CreateIpamScope&Version=2016-11-15")
	assert.NotEqual(t, http.StatusOK, missingIDRec.Code)

	scopeRec := postForm(
		t,
		h,
		fmt.Sprintf("Action=CreateIpamScope&Version=2016-11-15&IpamId=%s&Description=extra", ipamID),
	)
	require.Equal(t, http.StatusOK, scopeRec.Code)

	scopeBody := scopeRec.Body.String()
	assert.Contains(t, scopeBody, "<CreateIpamScopeResponse")
	assert.Contains(t, scopeBody, "<ipamScopeType>private</ipamScopeType>")

	poolRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateIpamPool&Version=2016-11-15&IpamId=%s&AddressFamily=ipv4&ProvisionedCidrs.item.1.Cidr=10.0.0.0/8",
		ipamID,
	))
	require.Equal(t, http.StatusOK, poolRec.Code)

	poolBody := poolRec.Body.String()
	poolID := poolBody[indexOf(poolBody, "<ipamPoolId>")+len("<ipamPoolId>") : indexOf(poolBody, "</ipamPoolId>")]
	require.NotEmpty(t, poolID)

	getCidrsRec := postForm(
		t, h, fmt.Sprintf("Action=GetIpamPoolCidrs&Version=2016-11-15&IpamPoolId=%s", poolID),
	)
	require.Equal(t, http.StatusOK, getCidrsRec.Code)
	assert.Contains(t, getCidrsRec.Body.String(), "<ipamPoolCidrSet>")
	assert.Contains(t, getCidrsRec.Body.String(), "10.0.0.0/8")

	// GetIpamPoolAllocations requires IpamPoolId.
	getAllocMissingRec := postForm(t, h, "Action=GetIpamPoolAllocations&Version=2016-11-15")
	assert.NotEqual(t, http.StatusOK, getAllocMissingRec.Code)

	getAllocRec := postForm(
		t, h, fmt.Sprintf("Action=GetIpamPoolAllocations&Version=2016-11-15&IpamPoolId=%s", poolID),
	)
	require.Equal(t, http.StatusOK, getAllocRec.Code)
	assert.Contains(t, getAllocRec.Body.String(), "<ipamPoolAllocationSet>")

	// Correct-empty-shape ops: no state modeled, but must return the right root element.
	historyRec := postForm(t, h, "Action=GetIpamAddressHistory&Version=2016-11-15")
	require.Equal(t, http.StatusOK, historyRec.Code)
	assert.Contains(t, historyRec.Body.String(), "<historyRecordSet")

	discoveredAccountsRec := postForm(t, h, "Action=GetIpamDiscoveredAccounts&Version=2016-11-15")
	require.Equal(t, http.StatusOK, discoveredAccountsRec.Code)
	assert.Contains(t, discoveredAccountsRec.Body.String(), "<ipamDiscoveredAccountSet")

	describeDiscoveriesRec := postForm(t, h, "Action=DescribeIpamResourceDiscoveries&Version=2016-11-15")
	require.Equal(t, http.StatusOK, describeDiscoveriesRec.Code)
	assert.Contains(t, describeDiscoveriesRec.Body.String(), "<ipamResourceDiscoverySet>")
}
