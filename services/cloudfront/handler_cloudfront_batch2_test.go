package cloudfront_test

import (
	"net/http"
	"strings"
	"testing"
)

// TestBatch2_DistributionTenantCRUD tests create, get, update, list, delete for DistributionTenant.
func TestBatch2_DistributionTenantCRUD(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	// Create distribution tenant
	createBody := `<CreateDistributionTenantRequest>
		<DistributionId>dist-001</DistributionId>
		<Domain>example.com</Domain>
	</CreateDistributionTenantRequest>`
	resp := cfOK(t, h, http.MethodPost, prefix+"distribution-tenant", createBody)
	if !strings.Contains(resp, "DistributionTenant") {
		t.Fatalf("expected DistributionTenant in response, got: %s", resp)
	}
	if !strings.Contains(resp, "example.com") {
		t.Fatalf("expected domain in response, got: %s", resp)
	}

	// Extract tenant ID
	tenantID := extractXMLID(t, resp)
	if tenantID == "" {
		t.Fatal("expected non-empty tenant ID")
	}

	// Get tenant by ID
	getResp := cfOK(t, h, http.MethodGet, prefix+"distribution-tenant/"+tenantID, "")
	if !strings.Contains(getResp, tenantID) {
		t.Errorf("get response missing tenant ID: %s", getResp)
	}
	if !strings.Contains(getResp, "example.com") {
		t.Errorf("get response missing domain: %s", getResp)
	}

	// List tenants
	listResp := cfOK(t, h, http.MethodGet, prefix+"distribution-tenant", "")
	if !strings.Contains(listResp, "DistributionTenantList") {
		t.Errorf("expected DistributionTenantList, got: %s", listResp)
	}
	if !strings.Contains(listResp, tenantID) {
		t.Errorf("list missing created tenant: %s", listResp)
	}

	// Update tenant
	updateResp := cfOK(t, h, http.MethodPut, prefix+"distribution-tenant/"+tenantID, "")
	if !strings.Contains(updateResp, "DistributionTenant") {
		t.Errorf("update response missing DistributionTenant: %s", updateResp)
	}

	// Delete tenant
	rr := cfRequest(t, h, http.MethodDelete, prefix+"distribution-tenant/"+tenantID, "")
	if rr.Code != http.StatusNoContent {
		t.Errorf("expected 204 on delete, got %d: %s", rr.Code, rr.Body.String())
	}

	// List should be empty after delete
	listAfter := cfOK(t, h, http.MethodGet, prefix+"distribution-tenant", "")
	if strings.Contains(listAfter, tenantID) {
		t.Errorf("deleted tenant still in list: %s", listAfter)
	}
}

// TestBatch2_DistributionTenantByDomain tests GetDistributionTenantByDomain.
func TestBatch2_DistributionTenantByDomain(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	// Create tenant
	createBody := `<CreateDistributionTenantRequest>
		<DistributionId>dist-001</DistributionId>
		<Domain>mysite.com</Domain>
	</CreateDistributionTenantRequest>`
	cfOK(t, h, http.MethodPost, prefix+"distribution-tenant", createBody)

	// Get by domain
	resp := cfOK(t, h, http.MethodGet, prefix+"distribution-tenant-by-domain?domain=mysite.com", "")
	if !strings.Contains(resp, "mysite.com") {
		t.Errorf("expected domain in response, got: %s", resp)
	}
}

// TestBatch2_DistributionTenantInvalidation tests create/list invalidations for a tenant.
func TestBatch2_DistributionTenantInvalidation(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	// Create tenant
	createBody := `<CreateDistributionTenantRequest>
		<DistributionId>dist-001</DistributionId>
		<Domain>inv-test.com</Domain>
	</CreateDistributionTenantRequest>`
	tenantResp := cfOK(t, h, http.MethodPost, prefix+"distribution-tenant", createBody)
	tenantID := extractXMLID(t, tenantResp)
	if tenantID == "" {
		t.Fatal("no tenant ID in response")
	}

	// Create invalidation
	invBody := `<InvalidationBatch><CallerReference>ref1</CallerReference><Paths><Quantity>1</Quantity><Items><Path>/*</Path></Items></Paths></InvalidationBatch>`
	invResp := cfOK(t, h, http.MethodPost, prefix+"distribution-tenant/"+tenantID+"/invalidation", invBody)
	if !strings.Contains(invResp, "Invalidation") {
		t.Fatalf("expected Invalidation in response, got: %s", invResp)
	}
	invID := extractXMLID(t, invResp)
	if invID == "" {
		t.Fatal("no invalidation ID in response")
	}

	// List invalidations
	listResp := cfOK(t, h, http.MethodGet, prefix+"distribution-tenant/"+tenantID+"/invalidation", "")
	if !strings.Contains(listResp, "InvalidationList") {
		t.Errorf("expected InvalidationList, got: %s", listResp)
	}

	// Get specific invalidation
	getResp := cfOK(t, h, http.MethodGet, prefix+"distribution-tenant/"+tenantID+"/invalidation/"+invID, "")
	if !strings.Contains(getResp, invID) {
		t.Errorf("expected invalidation ID in response, got: %s", getResp)
	}
}

// TestBatch2_CreateDistributionWithTags tests creating a distribution with tags.
func TestBatch2_CreateDistributionWithTags(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	body := `<DistributionConfigWithTags>
		<DistributionConfig>
			<CallerReference>cr-with-tags</CallerReference>
			<Comment>tagged dist</Comment>
			<Enabled>true</Enabled>
		</DistributionConfig>
		<Tags>
			<Tag><Key>env</Key><Value>prod</Value></Tag>
		</Tags>
	</DistributionConfigWithTags>`
	resp := cfOK(t, h, http.MethodPost, prefix+"distribution?Resource=WithTags", body)
	if !strings.Contains(resp, "Distribution") {
		t.Fatalf("expected Distribution in response, got: %s", resp)
	}
	distID := extractXMLID(t, resp)
	if distID == "" {
		t.Fatal("expected distribution ID in response")
	}

	// Verify the distribution exists via get
	getResp := cfOK(t, h, http.MethodGet, prefix+"distribution/"+distID, "")
	if !strings.Contains(getResp, distID) {
		t.Errorf("get did not return distribution: %s", getResp)
	}
}

// TestBatch2_ListDistributionsByKeyGroup tests ListDistributionsByKeyGroup.
func TestBatch2_ListDistributionsByKeyGroup(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	// Create distribution that references a key group in its config
	distBody := `<DistributionConfig>
		<CallerReference>cr-kg</CallerReference>
		<Enabled>true</Enabled>
		<KeyGroupId>key-group-abc123</KeyGroupId>
	</DistributionConfig>`
	cfOK(t, h, http.MethodPost, prefix+"distribution", distBody)

	// List by key group - should find the distribution
	resp := cfOK(t, h, http.MethodGet, prefix+"distributions/by-key-group/key-group-abc123", "")
	if !strings.Contains(resp, "DistributionList") {
		t.Errorf("expected DistributionList, got: %s", resp)
	}
	// Should have quantity > 0
	if strings.Contains(resp, "<Quantity>0</Quantity>") {
		t.Errorf("expected non-empty list, got: %s", resp)
	}

	// Different key group should return empty list
	resp2 := cfOK(t, h, http.MethodGet, prefix+"distributions/by-key-group/nonexistent-key-group", "")
	if !strings.Contains(resp2, "DistributionList") {
		t.Errorf("expected DistributionList for empty result, got: %s", resp2)
	}
}

// TestBatch2_UpdateKeyValueStore tests that UpdateKeyValueStore works correctly.
func TestBatch2_UpdateKeyValueStore(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	// Create KVS
	createResp := cfOK(t, h, http.MethodPost, prefix+"key-value-store",
		`<KeyValueStoreRequest><Name>test-kvs</Name><Comment>initial</Comment></KeyValueStoreRequest>`)
	kvsID := extractXMLID(t, createResp)
	if kvsID == "" {
		t.Fatal("no KVS ID in create response")
	}

	// Update KVS
	updateResp := cfOK(t, h, http.MethodPut, prefix+"key-value-store/"+kvsID,
		`<KeyValueStoreRequest><Name>test-kvs</Name><Comment>updated</Comment></KeyValueStoreRequest>`)
	if !strings.Contains(updateResp, "KeyValueStore") {
		t.Errorf("expected KeyValueStore in update response, got: %s", updateResp)
	}
	if !strings.Contains(updateResp, "updated") {
		t.Errorf("expected updated comment in response, got: %s", updateResp)
	}
}

// TestBatch2_DisassociateWebACL tests DisassociateDistributionWebACL.
func TestBatch2_DisassociateWebACL(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	// Create distribution
	distResp := cfOK(t, h, http.MethodPost, prefix+"distribution",
		`<DistributionConfig><CallerReference>cr-wacl</CallerReference><Enabled>true</Enabled></DistributionConfig>`)
	distID := extractXMLID(t, distResp)
	if distID == "" {
		t.Fatal("no dist ID in create response")
	}

	// Associate web ACL
	cfOK(t, h, http.MethodPut, prefix+"distribution/"+distID+"/associate-web-acl",
		`<WebACLAssociation><WebACLId>waf-123</WebACLId></WebACLAssociation>`)

	// Disassociate web ACL
	disResp := cfOK(t, h, http.MethodPut, prefix+"distribution/"+distID+"/disassociate-web-acl", "")
	if !strings.Contains(disResp, "Distribution") {
		t.Errorf("expected Distribution in disassociate response, got: %s", disResp)
	}
}

// TestBatch2_UpdateDistributionWithStagingConfig tests promotion of staging config.
func TestBatch2_UpdateDistributionWithStagingConfig(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	// Create primary distribution
	primaryResp := cfOK(t, h, http.MethodPost, prefix+"distribution",
		`<DistributionConfig><CallerReference>cr-primary</CallerReference><Enabled>true</Enabled></DistributionConfig>`)
	primaryID := extractXMLID(t, primaryResp)

	// Create staging distribution
	stagingResp := cfOK(
		t,
		h,
		http.MethodPost,
		prefix+"distribution",
		`<DistributionConfig><CallerReference>cr-staging</CallerReference><Enabled>false</Enabled></DistributionConfig>`,
	)
	stagingID := extractXMLID(t, stagingResp)

	// Promote staging to primary
	promoteBody := `<UpdateDistributionWithStagingConfigRequest><StagingDistributionId>` + stagingID + `</StagingDistributionId></UpdateDistributionWithStagingConfigRequest>`
	promoteResp := cfOK(t, h, http.MethodPut, prefix+"distribution/"+primaryID+"/staging", promoteBody)
	if !strings.Contains(promoteResp, "Distribution") {
		t.Errorf("expected Distribution in response, got: %s", promoteResp)
	}
}

// TestBatch2_GetManagedCertificateDetails tests the managed cert details endpoint.
func TestBatch2_GetManagedCertificateDetails(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	// Create tenant first
	createBody := `<CreateDistributionTenantRequest>
		<DistributionId>dist-001</DistributionId>
		<Domain>cert-test.com</Domain>
	</CreateDistributionTenantRequest>`
	tenantResp := cfOK(t, h, http.MethodPost, prefix+"distribution-tenant", createBody)
	tenantID := extractXMLID(t, tenantResp)

	// Get managed certificate details
	resp := cfOK(t, h, http.MethodGet, prefix+"distribution-tenant/"+tenantID+"/managed-certificate-details", "")
	if !strings.Contains(resp, "ManagedCertificateDetails") {
		t.Errorf("expected ManagedCertificateDetails, got: %s", resp)
	}
	if !strings.Contains(resp, "SUCCESS") {
		t.Errorf("expected SUCCESS status, got: %s", resp)
	}
}

// TestBatch2_VerifyDNSConfiguration tests the DNS verification endpoint.
func TestBatch2_VerifyDNSConfiguration(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	resp := cfOK(t, h, http.MethodPost, prefix+"verify-dns-configuration", "")
	if !strings.Contains(resp, "VerifyDnsConfigurationResponse") {
		t.Errorf("expected VerifyDnsConfigurationResponse, got: %s", resp)
	}
	if !strings.Contains(resp, "PASSED") {
		t.Errorf("expected PASSED, got: %s", resp)
	}
}
