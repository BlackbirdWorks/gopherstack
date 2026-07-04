package ec2_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestIpamResourceDiscovery_CRUD exercises user-created (non-default) IPAM resource
// discoveries: create, associate with an IPAM, modify, disassociate, delete.
func TestIpamResourceDiscovery_CRUD(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	ipam, err := bk.CreateIpam(ec2.IpamOptions{Description: "test ipam"})
	require.NoError(t, err)

	disco, err := bk.CreateIpamResourceDiscovery("standalone discovery", []string{"us-east-1"})
	require.NoError(t, err)
	assert.NotEmpty(t, disco.IpamResourceDiscoveryID)
	assert.False(t, disco.IsDefault)
	assert.Equal(t, []string{"us-east-1"}, disco.OperatingRegions)

	found := bk.DescribeIpamResourceDiscoveries([]string{disco.IpamResourceDiscoveryID})
	require.Len(t, found, 1)

	// A resource discovery not yet associated with any IPAM can be deleted.
	// (Verified via a throwaway discovery so we don't disturb the one under test below.)
	throwaway, err := bk.CreateIpamResourceDiscovery("throwaway", nil)
	require.NoError(t, err)
	_, err = bk.DeleteIpamResourceDiscovery(throwaway.IpamResourceDiscoveryID)
	require.NoError(t, err)

	assoc, err := bk.AssociateIpamResourceDiscovery(ipam.IpamID, disco.IpamResourceDiscoveryID)
	require.NoError(t, err)
	assert.Equal(t, ipam.IpamID, assoc.IpamID)
	assert.Equal(t, disco.IpamResourceDiscoveryID, assoc.IpamResourceDiscoveryID)
	assert.False(t, assoc.IsDefault)
	assert.NotEmpty(t, assoc.IpamResourceDiscoveryAssociationARN)

	assocs := bk.DescribeIpamResourceDiscoveryAssociations([]string{assoc.IpamResourceDiscoveryAssociationID})
	require.Len(t, assocs, 1)

	// Cannot delete a discovery while it is still associated with an IPAM.
	_, err = bk.DeleteIpamResourceDiscovery(disco.IpamResourceDiscoveryID)
	require.Error(t, err)

	modified, err := bk.ModifyIpamResourceDiscovery(
		disco.IpamResourceDiscoveryID, "updated description", []string{"eu-west-1"}, []string{"us-east-1"},
	)
	require.NoError(t, err)
	assert.Equal(t, "updated description", modified.Description)
	assert.Equal(t, []string{"eu-west-1"}, modified.OperatingRegions)

	disassoc, err := bk.DisassociateIpamResourceDiscovery(assoc.IpamResourceDiscoveryAssociationID)
	require.NoError(t, err)
	assert.Equal(t, "disassociate-complete", disassoc.State)
	assert.Empty(t, bk.DescribeIpamResourceDiscoveryAssociations([]string{assoc.IpamResourceDiscoveryAssociationID}))

	deleted, err := bk.DeleteIpamResourceDiscovery(disco.IpamResourceDiscoveryID)
	require.NoError(t, err)
	assert.Equal(t, "delete-complete", deleted.State)
	assert.Empty(t, bk.DescribeIpamResourceDiscoveries([]string{disco.IpamResourceDiscoveryID}))
}

// TestIpamResourceDiscovery_DefaultCannotBeDeletedOrDisassociated verifies that the default
// resource discovery/association auto-created by CreateIpam is protected.
func TestIpamResourceDiscovery_DefaultCannotBeDeletedOrDisassociated(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	ipam, err := bk.CreateIpam()
	require.NoError(t, err)

	_, err = bk.DeleteIpamResourceDiscovery(ipam.DefaultResourceDiscoveryID)
	require.Error(t, err)

	_, err = bk.DisassociateIpamResourceDiscovery(ipam.DefaultResourceDiscoveryAssociationID)
	require.Error(t, err)

	_, err = bk.AssociateIpamResourceDiscovery("ipam-missing", ipam.DefaultResourceDiscoveryID)
	require.Error(t, err)

	_, err = bk.AssociateIpamResourceDiscovery(ipam.IpamID, "ipam-res-disco-missing")
	require.Error(t, err)

	_, err = bk.ModifyIpamResourceDiscovery("ipam-res-disco-missing", "x", nil, nil)
	require.Error(t, err)

	_, err = bk.DeleteIpamResourceDiscovery("ipam-res-disco-missing")
	require.Error(t, err)

	_, err = bk.DisassociateIpamResourceDiscovery("ipam-res-disco-assoc-missing")
	require.Error(t, err)
}

// TestIpamResourceCidr_ViaPoolAllocation verifies that allocating a pool CIDR to a resource
// creates a monitored IpamResourceCidr entry, that it can be moved between scopes and have its
// monitored flag toggled, and that releasing the allocation removes it.
func TestIpamResourceCidr_ViaPoolAllocation(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	ipam, err := bk.CreateIpam()
	require.NoError(t, err)

	pool, err := bk.CreateIpamPool(ipam.IpamID, "ipv4", "", "10.0.0.0/8")
	require.NoError(t, err)

	// GetIpamResourceCidrs requires a valid IpamScopeId.
	_, err = bk.GetIpamResourceCidrs("", "", "", "", "")
	require.Error(t, err)

	_, err = bk.GetIpamResourceCidrs("ipam-scope-missing", "", "", "", "")
	require.Error(t, err)

	empty, err := bk.GetIpamResourceCidrs(ipam.PrivateDefaultScopeID, "", "", "", "")
	require.NoError(t, err)
	assert.Empty(t, empty)

	alloc, err := bk.AllocateIpamPoolCidr(pool.IpamPoolID, "10.0.1.0/24", 0, ec2.IpamAllocationOptions{
		ResourceID:   "vpc-abc123",
		ResourceType: "vpc",
	})
	require.NoError(t, err)

	cidrs, err := bk.GetIpamResourceCidrs(ipam.PrivateDefaultScopeID, "", "", "", "")
	require.NoError(t, err)
	require.Len(t, cidrs, 1)
	assert.Equal(t, "vpc-abc123", cidrs[0].ResourceID)
	assert.Equal(t, "10.0.1.0/24", cidrs[0].ResourceCidr)
	assert.Equal(t, pool.IpamPoolID, cidrs[0].IpamPoolID)

	filteredByPool, err := bk.GetIpamResourceCidrs(ipam.PrivateDefaultScopeID, pool.IpamPoolID, "", "", "")
	require.NoError(t, err)
	require.Len(t, filteredByPool, 1)

	filteredByMissingType, err := bk.GetIpamResourceCidrs(ipam.PrivateDefaultScopeID, "", "", "", "subnet")
	require.NoError(t, err)
	assert.Empty(t, filteredByMissingType)

	// Move the resource CIDR to a second scope and unmonitor it.
	extraScope, err := bk.CreateIpamScope(ipam.IpamID, "extra")
	require.NoError(t, err)

	modified, err := bk.ModifyIpamResourceCidr(
		ipam.PrivateDefaultScopeID, "10.0.1.0/24", "vpc-abc123", "us-east-1", false, extraScope.IpamScopeID,
	)
	require.NoError(t, err)
	assert.Equal(t, extraScope.IpamScopeID, modified.IpamScopeID)
	assert.Equal(t, "unmanaged", modified.ManagementState)

	// It is no longer visible in the original scope.
	afterMove, err := bk.GetIpamResourceCidrs(ipam.PrivateDefaultScopeID, "", "", "", "")
	require.NoError(t, err)
	assert.Empty(t, afterMove)

	// Modifying with a stale CurrentIpamScopeId now fails (the resource has moved).
	_, err = bk.ModifyIpamResourceCidr(ipam.PrivateDefaultScopeID, "10.0.1.0/24", "vpc-abc123", "", true, "")
	require.Error(t, err)

	// Unknown (resourceId, cidr) pairs are rejected.
	_, err = bk.ModifyIpamResourceCidr(extraScope.IpamScopeID, "10.0.9.0/24", "vpc-doesnotexist", "", true, "")
	require.Error(t, err)

	require.NoError(t, bk.ReleaseIpamPoolAllocation(pool.IpamPoolID, alloc.IpamPoolAllocationID))

	afterRelease, err := bk.GetIpamResourceCidrs(extraScope.IpamScopeID, "", "", "", "")
	require.NoError(t, err)
	assert.Empty(t, afterRelease)
}

// TestIpamByoasn_CRUD exercises Bring-Your-Own-ASN provisioning and its association with
// BYOIP CIDRs.
func TestIpamByoasn_CRUD(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	ipam, err := bk.CreateIpam()
	require.NoError(t, err)

	_, err = bk.ProvisionIpamByoasn("ipam-missing", "64512")
	require.Error(t, err)

	byoasn, err := bk.ProvisionIpamByoasn(ipam.IpamID, "64512")
	require.NoError(t, err)
	assert.Equal(t, "64512", byoasn.Asn)
	assert.Equal(t, "provisioned", byoasn.State)

	all := bk.DescribeIpamByoasn()
	require.Len(t, all, 1)
	assert.Equal(t, "64512", all[0].Asn)

	_, err = bk.AssociateIpamByoasn("64513", "203.0.113.0/24")
	require.Error(t, err)

	assoc, err := bk.AssociateIpamByoasn("64512", "203.0.113.0/24")
	require.NoError(t, err)
	assert.Equal(t, "associated", assoc.State)

	_, err = bk.DisassociateIpamByoasn("64512", "198.51.100.0/24")
	require.Error(t, err)

	disassoc, err := bk.DisassociateIpamByoasn("64512", "203.0.113.0/24")
	require.NoError(t, err)
	assert.Equal(t, "disassociated", disassoc.State)

	_, err = bk.DeprovisionIpamByoasn(ipam.IpamID, "64599")
	require.Error(t, err)

	deprovisioned, err := bk.DeprovisionIpamByoasn(ipam.IpamID, "64512")
	require.NoError(t, err)
	assert.Equal(t, "deprovisioned", deprovisioned.State)
	assert.Empty(t, bk.DescribeIpamByoasn())
}

// TestIpamExternalResourceVerificationToken_CRUD exercises token creation, description, and
// deletion.
func TestIpamExternalResourceVerificationToken_CRUD(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	_, err := bk.CreateIpamExternalResourceVerificationToken("ipam-missing")
	require.Error(t, err)

	ipam, err := bk.CreateIpam()
	require.NoError(t, err)

	token, err := bk.CreateIpamExternalResourceVerificationToken(ipam.IpamID)
	require.NoError(t, err)
	assert.NotEmpty(t, token.IpamExternalResourceVerificationTokenID)
	assert.Equal(t, "valid", token.Status)
	assert.NotEmpty(t, token.TokenValue)
	assert.False(t, token.NotAfter.IsZero())

	found := bk.DescribeIpamExternalResourceVerificationTokens([]string{token.IpamExternalResourceVerificationTokenID})
	require.Len(t, found, 1)

	all := bk.DescribeIpamExternalResourceVerificationTokens(nil)
	require.Len(t, all, 1)

	_, err = bk.DeleteIpamExternalResourceVerificationToken("ipam-ext-res-verification-token-missing")
	require.Error(t, err)

	deleted, err := bk.DeleteIpamExternalResourceVerificationToken(token.IpamExternalResourceVerificationTokenID)
	require.NoError(t, err)
	assert.Equal(t, "delete-complete", deleted.State)
	tokenID := token.IpamExternalResourceVerificationTokenID
	remaining := bk.DescribeIpamExternalResourceVerificationTokens([]string{tokenID})
	assert.Empty(t, remaining)
}

// TestIpamPrefixListResolver_CRUD exercises the prefix list resolver lifecycle: creation with
// rules, modification (which creates a new version), rule/version/version-entry retrieval, and
// deletion.
func TestIpamPrefixListResolver_CRUD(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	ipam, err := bk.CreateIpam()
	require.NoError(t, err)

	rules := []ec2.IpamPrefixListResolverRule{
		{
			RuleType:    "ipam-resource-cidr",
			IpamScopeID: ipam.PrivateDefaultScopeID,
			Conditions: []ec2.IpamPrefixListResolverRuleCondition{
				{Operation: "equals", ResourceOwner: "000000000000"},
			},
		},
	}

	_, err = bk.CreateIpamPrefixListResolver("ipam-missing", "ipv4", "x", nil)
	require.Error(t, err)

	resolver, err := bk.CreateIpamPrefixListResolver(ipam.IpamID, "ipv4", "my resolver", rules)
	require.NoError(t, err)
	assert.NotEmpty(t, resolver.IpamPrefixListResolverID)
	assert.Equal(t, "ipv4", resolver.AddressFamily)
	assert.Equal(t, "create-complete", resolver.State)

	found := bk.DescribeIpamPrefixListResolvers([]string{resolver.IpamPrefixListResolverID})
	require.Len(t, found, 1)

	gotRules, err := bk.GetIpamPrefixListResolverRules(resolver.IpamPrefixListResolverID)
	require.NoError(t, err)
	require.Len(t, gotRules, 1)
	assert.Equal(t, "ipam-resource-cidr", gotRules[0].RuleType)
	require.Len(t, gotRules[0].Conditions, 1)

	versions, err := bk.GetIpamPrefixListResolverVersions(resolver.IpamPrefixListResolverID)
	require.NoError(t, err)
	assert.Equal(t, []int64{1}, versions)

	entries, err := bk.GetIpamPrefixListResolverVersionEntries(resolver.IpamPrefixListResolverID, 1)
	require.NoError(t, err)
	assert.Empty(t, entries)

	_, err = bk.GetIpamPrefixListResolverVersionEntries(resolver.IpamPrefixListResolverID, 99)
	require.Error(t, err)

	// Modifying with new rules creates a new version.
	newRules := []ec2.IpamPrefixListResolverRule{{RuleType: "static-cidr", StaticCidr: "10.0.0.0/8"}}
	modified, err := bk.ModifyIpamPrefixListResolver(resolver.IpamPrefixListResolverID, "updated", newRules, true)
	require.NoError(t, err)
	assert.Equal(t, "updated", modified.Description)

	versionsAfter, err := bk.GetIpamPrefixListResolverVersions(resolver.IpamPrefixListResolverID)
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2}, versionsAfter)

	rulesAfter, err := bk.GetIpamPrefixListResolverRules(resolver.IpamPrefixListResolverID)
	require.NoError(t, err)
	require.Len(t, rulesAfter, 1)
	assert.Equal(t, "static-cidr", rulesAfter[0].RuleType)

	// ---- Targets ----
	target, err := bk.CreateIpamPrefixListResolverTarget(resolver.IpamPrefixListResolverID, "pl-abc123", "", true, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, target.IpamPrefixListResolverTargetID)
	assert.True(t, target.TrackLatestVersion)
	require.NotNil(t, target.LastSyncedVersion)
	assert.EqualValues(t, 2, *target.LastSyncedVersion)

	targets := bk.DescribeIpamPrefixListResolverTargets(resolver.IpamPrefixListResolverID, nil)
	require.Len(t, targets, 1)

	desired := int64(1)
	modTarget, err := bk.ModifyIpamPrefixListResolverTarget(target.IpamPrefixListResolverTargetID, &desired, nil)
	require.NoError(t, err)
	require.NotNil(t, modTarget.DesiredVersion)
	assert.EqualValues(t, 1, *modTarget.DesiredVersion)

	deletedTarget, err := bk.DeleteIpamPrefixListResolverTarget(target.IpamPrefixListResolverTargetID)
	require.NoError(t, err)
	assert.Equal(t, "delete-complete", deletedTarget.State)
	assert.Empty(t, bk.DescribeIpamPrefixListResolverTargets(resolver.IpamPrefixListResolverID, nil))

	_, err = bk.CreateIpamPrefixListResolverTarget("ipam-prefix-list-resolver-missing", "pl-x", "", false, nil)
	require.Error(t, err)

	_, err = bk.DeleteIpamPrefixListResolverTarget("ipam-prefix-list-resolver-target-missing")
	require.Error(t, err)

	deletedResolver, err := bk.DeleteIpamPrefixListResolver(resolver.IpamPrefixListResolverID)
	require.NoError(t, err)
	assert.Equal(t, "delete-complete", deletedResolver.State)
	assert.Empty(t, bk.DescribeIpamPrefixListResolvers([]string{resolver.IpamPrefixListResolverID}))
}

// TestIpamDiscoveryFamily_HTTP exercises the new operations through the EC2 Query-protocol HTTP
// handler, verifying real AWS-shaped XML element names (not the generic stub placeholder).
func TestIpamDiscoveryFamily_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createIpamRec := postForm(t, h, "Action=CreateIpam&Version=2016-11-15")
	require.Equal(t, http.StatusOK, createIpamRec.Code)

	ipamBody := createIpamRec.Body.String()
	ipamID := extractXMLValue(t, ipamBody, "ipamId")
	require.NotEmpty(t, ipamID)

	// ---- Resource Discovery ----
	discoRec := postForm(t, h, "Action=CreateIpamResourceDiscovery&Version=2016-11-15&Description=disco")
	require.Equal(t, http.StatusOK, discoRec.Code)

	discoBody := discoRec.Body.String()
	assert.Contains(t, discoBody, "<CreateIpamResourceDiscoveryResponse")
	assert.Contains(t, discoBody, "<ipamResourceDiscoveryId>")

	discoID := extractXMLValue(t, discoBody, "ipamResourceDiscoveryId")
	require.NotEmpty(t, discoID)

	assocRec := postForm(t, h, fmt.Sprintf(
		"Action=AssociateIpamResourceDiscovery&Version=2016-11-15&IpamId=%s&IpamResourceDiscoveryId=%s",
		ipamID, discoID,
	))
	require.Equal(t, http.StatusOK, assocRec.Code)
	assert.Contains(t, assocRec.Body.String(), "<ipamResourceDiscoveryAssociation>")

	assocBody := assocRec.Body.String()
	assocID := extractXMLValue(t, assocBody, "ipamResourceDiscoveryAssociationId")
	require.NotEmpty(t, assocID)

	disassocRec := postForm(t, h, fmt.Sprintf(
		"Action=DisassociateIpamResourceDiscovery&Version=2016-11-15&IpamResourceDiscoveryAssociationId=%s", assocID,
	))
	require.Equal(t, http.StatusOK, disassocRec.Code)
	assert.Contains(t, disassocRec.Body.String(), "disassociate-complete")

	deleteDiscoRec := postForm(t, h, fmt.Sprintf(
		"Action=DeleteIpamResourceDiscovery&Version=2016-11-15&IpamResourceDiscoveryId=%s", discoID,
	))
	require.Equal(t, http.StatusOK, deleteDiscoRec.Code)
	assert.Contains(t, deleteDiscoRec.Body.String(), "<DeleteIpamResourceDiscoveryResponse")

	// ---- BYOASN ----
	provisionRec := postForm(t, h, fmt.Sprintf(
		"Action=ProvisionIpamByoasn&Version=2016-11-15&IpamId=%s&Asn=64512"+
			"&AsnAuthorizationContext.Message=m&AsnAuthorizationContext.Signature=s",
		ipamID,
	))
	require.Equal(t, http.StatusOK, provisionRec.Code)
	assert.Contains(t, provisionRec.Body.String(), "<byoasn>")
	assert.Contains(t, provisionRec.Body.String(), "<state>provisioned</state>")

	describeByoasnRec := postForm(t, h, "Action=DescribeIpamByoasn&Version=2016-11-15")
	require.Equal(t, http.StatusOK, describeByoasnRec.Code)
	assert.Contains(t, describeByoasnRec.Body.String(), "<byoasnSet>")

	// ---- External Resource Verification Token ----
	tokenRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateIpamExternalResourceVerificationToken&Version=2016-11-15&IpamId=%s", ipamID,
	))
	require.Equal(t, http.StatusOK, tokenRec.Code)
	tokenBody := tokenRec.Body.String()
	assert.Contains(t, tokenBody, "<ipamExternalResourceVerificationToken>")
	assert.Contains(t, tokenBody, "<status>valid</status>")

	tokenID := extractXMLValue(t, tokenBody, "ipamExternalResourceVerificationTokenId")
	require.NotEmpty(t, tokenID)

	describeTokensRec := postForm(t, h, "Action=DescribeIpamExternalResourceVerificationTokens&Version=2016-11-15")
	require.Equal(t, http.StatusOK, describeTokensRec.Code)
	assert.Contains(t, describeTokensRec.Body.String(), "<ipamExternalResourceVerificationTokenSet>")

	deleteTokenRec := postForm(t, h, fmt.Sprintf(
		"Action=DeleteIpamExternalResourceVerificationToken&Version=2016-11-15&IpamExternalResourceVerificationTokenId=%s",
		tokenID,
	))
	require.Equal(t, http.StatusOK, deleteTokenRec.Code)
	assert.Contains(t, deleteTokenRec.Body.String(), "<state>delete-complete</state>")

	// ---- Prefix List Resolver ----
	resolverRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateIpamPrefixListResolver&Version=2016-11-15&IpamId=%s&AddressFamily=ipv4"+
			"&Rule.1.RuleType=static-cidr&Rule.1.StaticCidr=10.0.0.0%%2F8",
		ipamID,
	))
	require.Equal(t, http.StatusOK, resolverRec.Code)
	resolverBody := resolverRec.Body.String()
	assert.Contains(t, resolverBody, "<ipamPrefixListResolver>")

	resolverID := extractXMLValue(t, resolverBody, "ipamPrefixListResolverId")
	require.NotEmpty(t, resolverID)

	rulesRec := postForm(t, h, fmt.Sprintf(
		"Action=GetIpamPrefixListResolverRules&Version=2016-11-15&IpamPrefixListResolverId=%s", resolverID,
	))
	require.Equal(t, http.StatusOK, rulesRec.Code)
	assert.Contains(t, rulesRec.Body.String(), "<ruleSet>")
	assert.Contains(t, rulesRec.Body.String(), "<staticCidr>10.0.0.0/8</staticCidr>")

	versionsRec := postForm(t, h, fmt.Sprintf(
		"Action=GetIpamPrefixListResolverVersions&Version=2016-11-15&IpamPrefixListResolverId=%s", resolverID,
	))
	require.Equal(t, http.StatusOK, versionsRec.Code)
	assert.Contains(t, versionsRec.Body.String(), "<ipamPrefixListResolverVersionSet>")

	targetRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateIpamPrefixListResolverTarget&Version=2016-11-15&IpamPrefixListResolverId=%s"+
			"&PrefixListId=pl-abc123&PrefixListRegion=us-east-1&TrackLatestVersion=true",
		resolverID,
	))
	require.Equal(t, http.StatusOK, targetRec.Code)
	assert.Contains(t, targetRec.Body.String(), "<ipamPrefixListResolverTarget>")

	describeResolversRec := postForm(t, h, "Action=DescribeIpamPrefixListResolvers&Version=2016-11-15")
	require.Equal(t, http.StatusOK, describeResolversRec.Code)
	assert.Contains(t, describeResolversRec.Body.String(), "<ipamPrefixListResolverSet>")
}
