package cloudfront_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

const tenantDomainPrefix = "/2020-05-31/"

// createTestTenant creates a distribution tenant for the given domain and returns its ID.
func createTestTenant(t *testing.T, h *cloudfront.Handler, distID, domain string) string {
	t.Helper()

	body := `<CreateDistributionTenantRequest>` +
		`<DistributionId>` + distID + `</DistributionId>` +
		`<Domain>` + domain + `</Domain>` +
		`</CreateDistributionTenantRequest>`
	rr := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"distribution-tenant", body)
	if rr.Code != http.StatusCreated {
		t.Fatalf("create tenant failed: %d %s", rr.Code, rr.Body.String())
	}

	return extractXMLID(t, rr.Body.String())
}

// TestCreateDistributionTenant_DomainConflict_WithExistingTenant verifies that creating a tenant
// with a domain already claimed by another tenant returns a real 409 DomainConflictException.
func TestCreateDistributionTenant_DomainConflict_WithExistingTenant(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)
	createTestTenant(t, h, "dist-a", "conflict.example.com")

	body := `<CreateDistributionTenantRequest>` +
		`<DistributionId>dist-b</DistributionId>` +
		`<Domain>conflict.example.com</Domain>` +
		`</CreateDistributionTenantRequest>`
	rr := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"distribution-tenant", body)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rr.Code, rr.Body.String())
	}

	if !strings.Contains(rr.Body.String(), "DomainConflictException") {
		t.Errorf("expected DomainConflictException in body, got: %s", rr.Body.String())
	}
}

// TestCreateDistributionTenant_DomainConflict_WithDistributionAlias verifies that creating a
// tenant with a domain already used as a distribution CNAME alias returns a real conflict.
func TestCreateDistributionTenant_DomainConflict_WithDistributionAlias(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)

	distResp := cfOK(
		t,
		h,
		http.MethodPost,
		tenantDomainPrefix+"distribution",
		`<DistributionConfig><CallerReference>cr-alias-conf</CallerReference><Enabled>true</Enabled></DistributionConfig>`,
	)
	distID := extractXMLID(t, distResp)

	cfOK(
		t,
		h,
		http.MethodPut,
		tenantDomainPrefix+"distribution/"+distID+"/associate-alias?Alias=shared.example.com",
		"",
	)

	body := `<CreateDistributionTenantRequest>` +
		`<DistributionId>dist-x</DistributionId>` +
		`<Domain>shared.example.com</Domain>` +
		`</CreateDistributionTenantRequest>`
	rr := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"distribution-tenant", body)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rr.Code, rr.Body.String())
	}
}

// TestListDomainConflicts_RealConflicts verifies ListDomainConflicts returns actual conflicting
// resources for a claimed domain and an empty list for an unclaimed one.
func TestListDomainConflicts_RealConflicts(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)
	tenantID := createTestTenant(t, h, "dist-conflicts", "claimed.example.com")

	rr := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"domain-conflict",
		`<ListDomainConflictsRequest><Domain>claimed.example.com</Domain></ListDomainConflictsRequest>`)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	resp := rr.Body.String()
	if !strings.Contains(resp, "<Quantity>1</Quantity>") {
		t.Errorf("expected 1 conflict, got: %s", resp)
	}

	if !strings.Contains(resp, tenantID) {
		t.Errorf("expected conflicting tenant ID in response, got: %s", resp)
	}

	rr2 := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"domain-conflict",
		`<ListDomainConflictsRequest><Domain>unclaimed.example.com</Domain></ListDomainConflictsRequest>`)
	if !strings.Contains(rr2.Body.String(), "<Quantity>0</Quantity>") {
		t.Errorf("expected 0 conflicts for unclaimed domain, got: %s", rr2.Body.String())
	}
}

// TestListDistributionTenantsByCustomization_FiltersByWebACL verifies that the customization
// listing filters tenants by their associated WAF web ACL ARN.
func TestListDistributionTenantsByCustomization_FiltersByWebACL(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)
	tenantWithACL := createTestTenant(t, h, "dist-cust-1", "with-acl.example.com")
	tenantWithoutACL := createTestTenant(t, h, "dist-cust-2", "without-acl.example.com")

	cfOK(t, h, http.MethodPut, tenantDomainPrefix+"distribution-tenant/"+tenantWithACL+"/associate-web-acl",
		`<WebACLAssociation><WebACLId>arn:aws:wafv2:us-east-1:123:global/webacl/x/1</WebACLId></WebACLAssociation>`)

	rr := cfRequest(
		t,
		h,
		http.MethodGet,
		tenantDomainPrefix+"distribution-tenants/by-customization?WebACLArn=arn:aws:wafv2:us-east-1:123:global/webacl/x/1",
		"",
	)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	resp := rr.Body.String()
	if !strings.Contains(resp, tenantWithACL) {
		t.Errorf("expected tenant with matching web ACL in results: %s", resp)
	}

	if strings.Contains(resp, tenantWithoutACL) {
		t.Errorf("did not expect tenant without matching web ACL in results: %s", resp)
	}
}

// TestUpdateDomainAssociation_MoveToTenant verifies that a domain not yet claimed by any
// resource can be associated with a distribution tenant, and that moving it to a *different*
// resource that already owns a conflicting domain fails.
func TestUpdateDomainAssociation_MoveToTenant(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)
	tenantID := createTestTenant(t, h, "dist-move", "primary.example.com")

	body := `<UpdateDomainAssociationRequest>` +
		`<Domain>secondary.example.com</Domain>` +
		`<TargetResource><DistributionTenantId>` + tenantID + `</DistributionTenantId></TargetResource>` +
		`</UpdateDomainAssociationRequest>`
	rr := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"domain-association", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	if !strings.Contains(rr.Body.String(), "secondary.example.com") {
		t.Errorf("expected domain in response, got: %s", rr.Body.String())
	}

	// The tenant should now resolve by its newly-associated domain.
	getResp := cfOK(
		t,
		h,
		http.MethodGet,
		tenantDomainPrefix+"distribution-tenant-by-domain?domain=secondary.example.com",
		"",
	)
	if !strings.Contains(getResp, tenantID) {
		t.Errorf("expected tenant to be resolvable by newly associated domain, got: %s", getResp)
	}
}

// TestUpdateDomainAssociation_ConflictAndValidation covers the error paths for
// UpdateDomainAssociation: missing domain, missing/both targets, and an existing conflict.
func TestUpdateDomainAssociation_ConflictAndValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name: "missing_domain",
			body: `<UpdateDomainAssociationRequest>` +
				`<TargetResource><DistributionTenantId>x</DistributionTenantId></TargetResource>` +
				`</UpdateDomainAssociationRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "no_target",
			body:     `<UpdateDomainAssociationRequest><Domain>d.example.com</Domain></UpdateDomainAssociationRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "both_targets",
			body: `<UpdateDomainAssociationRequest><Domain>d.example.com</Domain>` +
				`<TargetResource><DistributionTenantId>a</DistributionTenantId>` +
				`<DistributionId>b</DistributionId></TargetResource>` +
				`</UpdateDomainAssociationRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "unknown_tenant",
			body: `<UpdateDomainAssociationRequest><Domain>d.example.com</Domain>` +
				`<TargetResource><DistributionTenantId>nope</DistributionTenantId></TargetResource>` +
				`</UpdateDomainAssociationRequest>`,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newCFHandler(t)
			rr := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"domain-association", tc.body)
			if rr.Code != tc.wantCode {
				t.Errorf("got %d want %d: %s", rr.Code, tc.wantCode, rr.Body.String())
			}
		})
	}

	// Conflict: try to move a domain already owned by tenant A onto tenant B.
	h := newCFHandler(t)
	createTestTenant(t, h, "dist-c1", "owned.example.com")
	tenantB := createTestTenant(t, h, "dist-c2", "b.example.com")

	body := `<UpdateDomainAssociationRequest>` +
		`<Domain>owned.example.com</Domain>` +
		`<TargetResource><DistributionTenantId>` + tenantB + `</DistributionTenantId></TargetResource>` +
		`</UpdateDomainAssociationRequest>`
	rr := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"domain-association", body)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rr.Code, rr.Body.String())
	}
}

// TestVerifyDNSConfiguration_RealPerTenantStatus verifies that VerifyDNSConfiguration returns a
// real per-domain status list for a known tenant, FAILED for malformed domains, 404 for an
// unknown identifier, and the legacy generic PASSED response when no identifier is given.
func TestVerifyDNSConfiguration_RealPerTenantStatus(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)
	tenantID := createTestTenant(t, h, "dist-dns", "valid.example.com")

	rr := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"verify-dns-configuration",
		`<VerifyDnsConfigurationRequest><Identifier>`+tenantID+`</Identifier></VerifyDnsConfigurationRequest>`)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	resp := rr.Body.String()
	if !strings.Contains(resp, "valid.example.com") || !strings.Contains(resp, "PASSED") {
		t.Errorf("expected valid domain marked PASSED, got: %s", resp)
	}

	// Unknown identifier -> 404.
	rrNotFound := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"verify-dns-configuration",
		`<VerifyDnsConfigurationRequest><Identifier>does-not-exist</Identifier></VerifyDnsConfigurationRequest>`)
	if rrNotFound.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown identifier, got %d: %s", rrNotFound.Code, rrNotFound.Body.String())
	}

	// No identifier -> legacy generic PASSED response (back-compat).
	rrLegacy := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"verify-dns-configuration", "")
	if rrLegacy.Code != http.StatusOK || !strings.Contains(rrLegacy.Body.String(), "PASSED") {
		t.Errorf("expected legacy PASSED response, got %d: %s", rrLegacy.Code, rrLegacy.Body.String())
	}
}

// TestDistributionTenant_TagsRoundTrip verifies that tags applied at creation, and via
// TagResource/ListTagsForResource against the tenant's ARN, are stored and retrievable.
func TestDistributionTenant_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)

	createBody := `<CreateDistributionTenantRequest>` +
		`<DistributionId>dist-tags</DistributionId>` +
		`<Domain>tags.example.com</Domain>` +
		`<Tags><Tag><Key>team</Key><Value>edge</Value></Tag></Tags>` +
		`</CreateDistributionTenantRequest>`
	createRR := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"distribution-tenant", createBody)
	if createRR.Code != http.StatusCreated {
		t.Fatalf("create failed: %d %s", createRR.Code, createRR.Body.String())
	}

	resp := createRR.Body.String()
	arn := extractBetween(resp, "<Arn>", "</Arn>")
	if arn == "" {
		t.Fatalf("expected ARN in create response, got: %s", resp)
	}

	// Add another tag via TagResource.
	tagRR := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"tagging?Resource="+arn,
		`<Tags><Items><Tag><Key>env</Key><Value>prod</Value></Tag></Items></Tags>`)
	if tagRR.Code != http.StatusNoContent {
		t.Fatalf("expected 204 from TagResource, got %d: %s", tagRR.Code, tagRR.Body.String())
	}

	listRR := cfRequest(t, h, http.MethodGet, tenantDomainPrefix+"tagging?Resource="+arn, "")
	if listRR.Code != http.StatusOK {
		t.Fatalf("expected 200 from ListTagsForResource, got %d: %s", listRR.Code, listRR.Body.String())
	}

	listResp := listRR.Body.String()
	if !strings.Contains(listResp, "team") || !strings.Contains(listResp, "edge") {
		t.Errorf("expected original tenant tag in list, got: %s", listResp)
	}

	if !strings.Contains(listResp, "env") || !strings.Contains(listResp, "prod") {
		t.Errorf("expected newly tagged key/value in list, got: %s", listResp)
	}
}

// TestDistributionTenant_PersistenceRoundTrip verifies that tenants, their domains, tags, web
// ACL association, and invalidations all survive a Snapshot/Restore cycle.
func TestDistributionTenant_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)

	createBody := `<CreateDistributionTenantRequest>` +
		`<DistributionId>dist-persist</DistributionId>` +
		`<Domain>persist.example.com</Domain>` +
		`<Tags><Tag><Key>owner</Key><Value>team-a</Value></Tag></Tags>` +
		`</CreateDistributionTenantRequest>`
	createRR := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"distribution-tenant", createBody)
	if createRR.Code != http.StatusCreated {
		t.Fatalf("create failed: %d %s", createRR.Code, createRR.Body.String())
	}

	tenantID := extractXMLID(t, createRR.Body.String())

	cfOK(
		t,
		h,
		http.MethodPut,
		tenantDomainPrefix+"distribution-tenant/"+tenantID+"/associate-web-acl",
		`<WebACLAssociation><WebACLId>arn:aws:wafv2:us-east-1:123:global/webacl/persist/1</WebACLId></WebACLAssociation>`,
	)

	invRR := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"distribution-tenant/"+tenantID+"/invalidation",
		`<InvalidationBatch><CallerReference>ref1</CallerReference>`+
			`<Paths><Quantity>1</Quantity><Items><Path>/*</Path></Items></Paths></InvalidationBatch>`)
	if invRR.Code != http.StatusCreated {
		t.Fatalf("invalidation create failed: %d %s", invRR.Code, invRR.Body.String())
	}

	snap := h.Snapshot(t.Context())
	if len(snap) == 0 {
		t.Fatal("expected non-empty snapshot")
	}

	h2 := newCFHandler(t)
	if err := h2.Restore(t.Context(), snap); err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	// Tenant is retrievable by ID after restore.
	getRR := cfRequest(t, h2, http.MethodGet, tenantDomainPrefix+"distribution-tenant/"+tenantID, "")
	if getRR.Code != http.StatusOK {
		t.Fatalf("expected tenant to survive restore, got %d: %s", getRR.Code, getRR.Body.String())
	}

	// Tenant is retrievable by domain after restore (secondary index rebuilt).
	byDomainRR := cfRequest(t, h2, http.MethodGet,
		tenantDomainPrefix+"distribution-tenant-by-domain?domain=persist.example.com", "")
	if byDomainRR.Code != http.StatusOK || !strings.Contains(byDomainRR.Body.String(), tenantID) {
		t.Errorf(
			"expected tenant resolvable by domain after restore, got %d: %s",
			byDomainRR.Code,
			byDomainRR.Body.String(),
		)
	}

	// Web ACL association survived.
	if !strings.Contains(getRR.Body.String(), "arn:aws:wafv2:us-east-1:123:global/webacl/persist/1") {
		t.Errorf("expected web ACL association to survive restore, got: %s", getRR.Body.String())
	}

	// Invalidation survived.
	listInvRR := cfRequest(
		t,
		h2,
		http.MethodGet,
		tenantDomainPrefix+"distribution-tenant/"+tenantID+"/invalidation",
		"",
	)
	if !strings.Contains(listInvRR.Body.String(), "InvalidationList") {
		t.Errorf("expected invalidation list to survive restore, got: %s", listInvRR.Body.String())
	}

	if strings.Contains(listInvRR.Body.String(), "<Quantity>0</Quantity>") {
		t.Errorf("expected non-empty invalidation list after restore, got: %s", listInvRR.Body.String())
	}
}

// TestGetDistributionTenant_NotFound verifies the not-found path returns NoSuchDistributionTenant.
func TestGetDistributionTenant_NotFound(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)
	rr := cfRequest(t, h, http.MethodGet, tenantDomainPrefix+"distribution-tenant/does-not-exist", "")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rr.Code, rr.Body.String())
	}

	if !strings.Contains(rr.Body.String(), "NoSuchDistributionTenant") {
		t.Errorf("expected NoSuchDistributionTenant in body, got: %s", rr.Body.String())
	}
}

// extractBetween returns the substring in s between the first occurrence of start and end.
func extractBetween(s, start, end string) string {
	i := strings.Index(s, start)
	if i < 0 {
		return ""
	}
	i += len(start)

	j := strings.Index(s[i:], end)
	if j < 0 {
		return ""
	}

	return s[i : i+j]
}
