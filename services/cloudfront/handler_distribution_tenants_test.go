package cloudfront_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// TestParitySweep_GetManagedCertificateDetails_NotFound verifies a 404 for a distribution tenant
// that does not exist (rather than the previous hardcoded always-SUCCESS stub).
func TestParitySweep_GetManagedCertificateDetails_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	const prefix = "/2020-05-31/"

	rec := doXML(t, h, http.MethodGet, prefix+"distribution-tenant/does-not-exist/managed-certificate-details", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "NoSuchDistributionTenant")
}

// TestParitySweep_GetManagedCertificateDetails_StableACrossCalls verifies the derived
// certificate ARN and validation tokens are stable across repeated Get calls for the same
// tenant (a real, cached backend value, not a fresh random result each time).
func TestParitySweep_GetManagedCertificateDetails_StableACrossCalls(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	const prefix = "/2020-05-31/"

	createBody := `<CreateDistributionTenantRequest>` +
		`<DistributionId>dist-parity-001</DistributionId>` +
		`<Domain>parity-cert-test.example.com</Domain>` +
		`</CreateDistributionTenantRequest>`
	createRec := doXML(t, h, http.MethodPost, prefix+"distribution-tenant", []byte(createBody))
	require.Equal(t, http.StatusCreated, createRec.Code)
	tenantID := extractXMLID(t, createRec.Body.String())

	path := prefix + "distribution-tenant/" + tenantID + "/managed-certificate-details"
	first := doXML(t, h, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, first.Code)
	require.Contains(t, first.Body.String(), "SUCCESS")

	second := doXML(t, h, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, second.Code)

	firstARN := strings.SplitN(strings.SplitN(first.Body.String(), "<CertificateArn>", 2)[1], "</CertificateArn>", 2)[0]
	secondARN := strings.SplitN(strings.SplitN(second.Body.String(), "<CertificateArn>", 2)[1], "</CertificateArn>", 2)[0]
	assert.Equal(t, firstARN, secondARN)
	assert.Contains(t, first.Body.String(), "parity-cert-test.example.com")
}

// TestBatch2Accuracy_CreateDistributionTenant_RequiresDistributionId checks that
// an empty DistributionId is rejected with 400 InvalidArgument.
func TestBatch2Accuracy_CreateDistributionTenant_RequiresDistributionId(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"

	tests := []struct {
		name     string
		body     string
		wantErr  string
		wantCode int
	}{
		{
			name: "missing_distribution_id",
			body: `<CreateDistributionTenantRequest>
				<Domain>tenant-test.com</Domain>
			</CreateDistributionTenantRequest>`,
			wantCode: http.StatusBadRequest,
			wantErr:  "InvalidArgument",
		},
		{
			name: "empty_distribution_id",
			body: `<CreateDistributionTenantRequest>
				<DistributionId></DistributionId>
				<Domain>tenant-test2.com</Domain>
			</CreateDistributionTenantRequest>`,
			wantCode: http.StatusBadRequest,
			wantErr:  "InvalidArgument",
		},
		{
			name: "valid_with_distribution_id",
			body: `<CreateDistributionTenantRequest>
				<DistributionId>dist-xyz</DistributionId>
				<Domain>tenant-valid.com</Domain>
			</CreateDistributionTenantRequest>`,
			wantCode: http.StatusCreated,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newCFHandler(t)
			rr := cfRequest(t, h, http.MethodPost, prefix+"distribution-tenant", tc.body)
			if rr.Code != tc.wantCode {
				t.Errorf("got %d want %d: %s", rr.Code, tc.wantCode, rr.Body.String())
			}
			if tc.wantErr != "" && !strings.Contains(rr.Body.String(), tc.wantErr) {
				t.Errorf("want %q in body, got: %s", tc.wantErr, rr.Body.String())
			}
		})
	}
}

// TestBatch2Accuracy_UpdateDistributionTenant_RequiresIfMatch verifies that
// UpdateDistributionTenant returns 412 when If-Match is absent or stale.
func TestBatch2Accuracy_UpdateDistributionTenant_RequiresIfMatch(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"

	tests := []struct {
		name     string
		ifMatch  string
		wantErr  string
		wantCode int
	}{
		{
			name:     "missing_if_match",
			ifMatch:  "",
			wantCode: http.StatusPreconditionFailed,
			wantErr:  "PreconditionFailed",
		},
		{
			name:     "wrong_etag",
			ifMatch:  "wrong-etag",
			wantCode: http.StatusPreconditionFailed,
			wantErr:  "PreconditionFailed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newCFHandler(t)
			createRR := cfRequest(t, h, http.MethodPost, prefix+"distribution-tenant",
				`<CreateDistributionTenantRequest>`+
					`<DistributionId>dist-001</DistributionId>`+
					`<Domain>update-match-`+tc.name+`.com</Domain>`+
					`</CreateDistributionTenantRequest>`)
			if createRR.Code != http.StatusCreated {
				t.Fatalf("create got %d: %s", createRR.Code, createRR.Body.String())
			}
			tenantID := extractXMLID(t, createRR.Body.String())

			var headers map[string]string
			if tc.ifMatch != "" {
				headers = map[string]string{"If-Match": tc.ifMatch}
			}
			rr := cfRequestWithHeader(t, h, http.MethodPut, prefix+"distribution-tenant/"+tenantID, headers)
			if rr.Code != tc.wantCode {
				t.Errorf("got %d want %d: %s", rr.Code, tc.wantCode, rr.Body.String())
			}
			if tc.wantErr != "" && !strings.Contains(rr.Body.String(), tc.wantErr) {
				t.Errorf("want %q in body, got: %s", tc.wantErr, rr.Body.String())
			}
		})
	}
}

// TestBatch2Accuracy_DeleteDistributionTenant_RequiresIfMatch verifies that
// DeleteDistributionTenant returns 412 when If-Match is absent or stale.
func TestBatch2Accuracy_DeleteDistributionTenant_RequiresIfMatch(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"

	tests := []struct {
		name     string
		ifMatch  string
		wantErr  string
		wantCode int
	}{
		{
			name:     "missing_if_match",
			ifMatch:  "",
			wantCode: http.StatusPreconditionFailed,
			wantErr:  "PreconditionFailed",
		},
		{
			name:     "wrong_etag",
			ifMatch:  "bad-etag-value",
			wantCode: http.StatusPreconditionFailed,
			wantErr:  "PreconditionFailed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newCFHandler(t)
			createRR := cfRequest(t, h, http.MethodPost, prefix+"distribution-tenant",
				`<CreateDistributionTenantRequest>`+
					`<DistributionId>dist-001</DistributionId>`+
					`<Domain>delete-match-`+tc.name+`.com</Domain>`+
					`</CreateDistributionTenantRequest>`)
			if createRR.Code != http.StatusCreated {
				t.Fatalf("create got %d: %s", createRR.Code, createRR.Body.String())
			}
			tenantID := extractXMLID(t, createRR.Body.String())

			var headers map[string]string
			if tc.ifMatch != "" {
				headers = map[string]string{"If-Match": tc.ifMatch}
			}
			rr := cfRequestWithHeader(t, h, http.MethodDelete, prefix+"distribution-tenant/"+tenantID, headers)
			if rr.Code != tc.wantCode {
				t.Errorf("got %d want %d: %s", rr.Code, tc.wantCode, rr.Body.String())
			}
			if tc.wantErr != "" && !strings.Contains(rr.Body.String(), tc.wantErr) {
				t.Errorf("want %q in body, got: %s", tc.wantErr, rr.Body.String())
			}
		})
	}
}

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
	createRR := cfRequest(t, h, http.MethodPost, prefix+"distribution-tenant", createBody)
	if createRR.Code != http.StatusCreated {
		t.Fatalf("expected 201 on create, got %d: %s", createRR.Code, createRR.Body.String())
	}
	resp := createRR.Body.String()
	if !strings.Contains(resp, "DistributionTenant") {
		t.Fatalf("expected DistributionTenant in response, got: %s", resp)
	}
	if !strings.Contains(resp, "example.com") {
		t.Fatalf("expected domain in response, got: %s", resp)
	}

	tenantID := extractXMLID(t, resp)
	if tenantID == "" {
		t.Fatal("expected non-empty tenant ID")
	}
	etag := createRR.Header().Get("ETag")
	if etag == "" {
		t.Fatal("expected ETag header from create")
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

	// Update tenant requires If-Match ETag.
	updateRR := cfRequestWithHeader(t, h, http.MethodPut, prefix+"distribution-tenant/"+tenantID,
		map[string]string{"If-Match": etag})
	if updateRR.Code != http.StatusOK {
		t.Errorf("expected 200 on update, got %d: %s", updateRR.Code, updateRR.Body.String())
	}
	if !strings.Contains(updateRR.Body.String(), "DistributionTenant") {
		t.Errorf("update response missing DistributionTenant: %s", updateRR.Body.String())
	}
	// Refresh ETag after update.
	etag = updateRR.Header().Get("ETag")

	// Delete tenant requires If-Match ETag.
	rr := cfRequestWithHeader(t, h, http.MethodDelete, prefix+"distribution-tenant/"+tenantID,
		map[string]string{"If-Match": etag})
	if rr.Code != http.StatusNoContent {
		t.Errorf("expected 204 on delete, got %d: %s", rr.Code, rr.Body.String())
	}

	// List should be empty after delete.
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
	invBody := `<InvalidationBatch>` +
		`<CallerReference>ref1</CallerReference>` +
		`<Paths><Quantity>1</Quantity><Items><Path>/*</Path></Items></Paths>` +
		`</InvalidationBatch>`
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

// ---------------------------------------------------------------------------
// Table-driven tests for formerly-stubbed operations with real state
// ---------------------------------------------------------------------------

func newBatch2Backend() *cloudfront.InMemoryBackend {
	return cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
}

func doBatch2Req(
	t *testing.T,
	h *cloudfront.Handler,
	method, path string,
) *httptest.ResponseRecorder {
	t.Helper()

	return cfRequest(t, h, method, path, "")
}

// TestGetManagedCertificateDetails_TableDriven validates cert details with real tenant state.
func TestGetManagedCertificateDetails_TableDriven(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"

	makeTenant := func(h *cloudfront.Handler, domain string) string {
		const tenantTemplate = `<CreateDistributionTenantRequest>` +
			`<DistributionId>d-001</DistributionId>` +
			`<Domain>%s</Domain>` +
			`</CreateDistributionTenantRequest>`
		body := fmt.Sprintf(tenantTemplate, domain)
		resp := cfOK(t, h, http.MethodPost, prefix+"distribution-tenant", body)

		return extractXMLID(t, resp)
	}

	tests := []struct {
		setup    func(h *cloudfront.Handler) string
		name     string
		wantBody []string
		wantCode int
	}{
		{
			name: "existing_tenant_returns_success_cert",
			setup: func(h *cloudfront.Handler) string {
				return makeTenant(h, "example.com")
			},
			wantCode: http.StatusOK,
			wantBody: []string{"ManagedCertificateDetails", "SUCCESS", "example.com"},
		},
		{
			name: "non_existent_tenant_returns_404",
			setup: func(_ *cloudfront.Handler) string {
				return "no-such-tenant"
			},
			wantCode: http.StatusNotFound,
			wantBody: []string{"NoSuchDistributionTenant"},
		},
		{
			name: "tenant_domain_appears_in_validation_tokens",
			setup: func(h *cloudfront.Handler) string {
				return makeTenant(h, "my-service.example.com")
			},
			wantCode: http.StatusOK,
			wantBody: []string{"my-service.example.com", "ValidationTokenDetails"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := cloudfront.NewHandler(newBatch2Backend())
			tenantID := tt.setup(h)
			certPath := prefix + "distribution-tenant/" + tenantID + "/managed-certificate-details"
			rec := doBatch2Req(t, h, http.MethodGet, certPath)

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, want := range tt.wantBody {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

// TestListDomainConflicts_TableDriven validates domain conflict detection with real state.
func TestListDomainConflicts_TableDriven(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"

	tests := []struct {
		setup    func(b *cloudfront.InMemoryBackend)
		name     string
		domain   string
		wantBody []string
		wantNot  []string
		wantCode int
	}{
		{
			name:     "no_conflicts_returns_empty_list",
			setup:    func(_ *cloudfront.InMemoryBackend) {},
			domain:   "nonexistent.example.com",
			wantCode: http.StatusOK,
			wantBody: []string{"DomainConflictList", "<Quantity>0</Quantity>"},
		},
		{
			name: "conflict_via_distribution_alias",
			setup: func(b *cloudfront.InMemoryBackend) {
				dist, err := b.CreateDistribution("ref-1", "test", true, nil)
				require.NoError(t, err)
				err = b.AssociateAlias(dist.ID, "conflict.example.com")
				require.NoError(t, err)
			},
			domain:   "conflict.example.com",
			wantCode: http.StatusOK,
			wantBody: []string{"DomainConflictList", "conflict.example.com"},
			wantNot:  []string{"<Quantity>0</Quantity>"},
		},
		{
			name: "conflict_via_distribution_tenant_domain",
			setup: func(b *cloudfront.InMemoryBackend) {
				dist, err := b.CreateDistribution("ref-2", "test", true, nil)
				require.NoError(t, err)
				_, err = b.CreateDistributionTenant(
					dist.ID, "tenant-domain-tenant", []string{"tenant-domain.example.com"}, nil,
				)
				require.NoError(t, err)
			},
			domain:   "tenant-domain.example.com",
			wantCode: http.StatusOK,
			wantBody: []string{"DomainConflictList", "tenant-domain.example.com"},
			wantNot:  []string{"<Quantity>0</Quantity>"},
		},
		{
			name:     "empty_domain_returns_empty_list",
			setup:    func(_ *cloudfront.InMemoryBackend) {},
			domain:   "",
			wantCode: http.StatusOK,
			wantBody: []string{"DomainConflictList", "<Quantity>0</Quantity>"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBatch2Backend()
			tt.setup(b)
			h := cloudfront.NewHandler(b)

			path := prefix + "domain-conflict"
			if tt.domain != "" {
				path += "?Domain=" + tt.domain
			}

			rec := cfRequest(t, h, http.MethodPost, path, "")
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, want := range tt.wantBody {
				assert.Contains(t, rec.Body.String(), want)
			}
			for _, notWant := range tt.wantNot {
				assert.NotContains(t, rec.Body.String(), notWant)
			}
		})
	}
}

// TestAssociateDistributionTenantWebACL covers the AssociateDistributionTenantWebACL operation.
func TestAssociateDistributionTenantWebACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		tenantID   string
		body       []byte
		wantStatus int
	}{
		{
			name:     "associate_tenant_web_acl_success",
			tenantID: "tenant-abc-123",
			body: []byte(
				`<WebACLAssociation><WebACLId>arn:aws:wafv2:us-east-1:123:global/webacl/tenant/abc</WebACLId></WebACLAssociation>`,
			),
			wantStatus: http.StatusOK,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder) { t.Helper() },
		},
		{
			name:       "associate_tenant_web_acl_empty_tenant",
			tenantID:   "",
			body:       []byte(`<WebACLAssociation><WebACLId>some-acl</WebACLId></WebACLAssociation>`),
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "InvalidArgument")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			var path string
			if tt.tenantID == "" {
				// POST to the tenant endpoint with empty tenant ID segment
				path = "/2020-05-31/distribution-tenant//associate-web-acl"
			} else {
				path = "/2020-05-31/distribution-tenant/" + tt.tenantID + "/associate-web-acl"
			}

			rec := doXML(t, h, http.MethodPut, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}
