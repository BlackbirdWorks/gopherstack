package cloudfront_test

import (
	"encoding/xml"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
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
// with a domain already claimed by another tenant returns a real 409 CNAMEAlreadyExists.
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

	if !strings.Contains(rr.Body.String(), "CNAMEAlreadyExists") {
		t.Errorf("expected CNAMEAlreadyExists in body, got: %s", rr.Body.String())
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

// domainConflictsList decodes with the real deserializer's element names
// (awsRestxml_deserializeDocumentDomainConflictsList, cloudfront@v1.67.4): the list wrapper AND
// each entry are both named <DomainConflicts>, not <Items>/<DomainConflict>.
type domainConflictsList struct {
	DomainConflicts struct {
		Entries []struct {
			ResourceID string `xml:"ResourceId"`
		} `xml:"DomainConflicts"`
	} `xml:"DomainConflicts"`
}

// listDomainConflictsBody builds a real ListDomainConflictsRequest body, scoped to either a
// distribution or a distribution tenant (DomainControlValidationResource -- both members of
// ListDomainConflictsInput are independently required per api_op_ListDomainConflicts.go:73-77).
func listDomainConflictsBody(domain, distID, tenantID string) string {
	var resource string
	if distID != "" {
		resource = "<DistributionId>" + distID + "</DistributionId>"
	} else {
		resource = "<DistributionTenantId>" + tenantID + "</DistributionTenantId>"
	}

	return `<ListDomainConflictsRequest><Domain>` + domain + `</Domain>` +
		`<DomainControlValidationResource>` + resource + `</DomainControlValidationResource>` +
		`</ListDomainConflictsRequest>`
}

// TestListDomainConflicts_RealConflicts verifies ListDomainConflicts returns actual conflicting
// resources for a claimed domain (scoped to an unrelated resource) and an empty list both for an
// unclaimed domain and when scoped to the very resource that claims the domain -- real AWS
// excludes DomainControlValidationResource's own resource from its conflict list.
func TestListDomainConflicts_RealConflicts(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)
	tenantID := createTestTenant(t, h, "dist-conflicts", "claimed.example.com")

	other, err := h.Backend.CreateDistribution("other-ref", "unrelated", true, nil)
	require.NoError(t, err)

	rr := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"domain-conflicts",
		listDomainConflictsBody("claimed.example.com", other.ID, ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	var parsed domainConflictsList
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &parsed))
	require.Len(t, parsed.DomainConflicts.Entries, 1)
	assert.Equal(t, tenantID, parsed.DomainConflicts.Entries[0].ResourceID)

	rr2 := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"domain-conflicts",
		listDomainConflictsBody("unclaimed.example.com", other.ID, ""))

	var parsed2 domainConflictsList
	require.NoError(t, xml.Unmarshal(rr2.Body.Bytes(), &parsed2))
	assert.Empty(t, parsed2.DomainConflicts.Entries)

	// Scoping the check to the tenant that itself claims the domain excludes it: real AWS
	// interprets DomainControlValidationResource as "the resource with a valid certificate for
	// this domain," not as a resource to flag as a conflict against itself.
	rr3 := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"domain-conflicts",
		listDomainConflictsBody("claimed.example.com", "", tenantID))

	var parsed3 domainConflictsList
	require.NoError(t, xml.Unmarshal(rr3.Body.Bytes(), &parsed3))
	assert.Empty(t, parsed3.DomainConflicts.Entries)
}

// TestListDistributionTenantsByCustomization_FiltersByWebACL verifies that the customization
// listing filters tenants by their associated WAF web ACL ARN. Uses the real wire shape --
// cloudfront@v1.67.4 serializers.go awsRestxml_serializeOpListDistributionTenantsByCustomization
// sends POST to the hyphenated "distribution-tenants-by-customization" path with WebACLArn in
// the XML body, not GET with a query parameter.
func TestListDistributionTenantsByCustomization_FiltersByWebACL(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)
	tenantWithACL := createTestTenant(t, h, "dist-cust-1", "with-acl.example.com")
	tenantWithoutACL := createTestTenant(t, h, "dist-cust-2", "without-acl.example.com")

	cfOK(t, h, http.MethodPut, tenantDomainPrefix+"distribution-tenant/"+tenantWithACL+"/associate-web-acl",
		`<AssociateDistributionTenantWebACLRequest>`+
			`<WebACLArn>arn:aws:wafv2:us-east-1:123:global/webacl/x/1</WebACLArn>`+
			`</AssociateDistributionTenantWebACLRequest>`)

	rr := cfRequest(
		t,
		h,
		http.MethodPost,
		tenantDomainPrefix+"distribution-tenants-by-customization",
		`<ListDistributionTenantsByCustomizationRequest>`+
			`<WebACLArn>arn:aws:wafv2:us-east-1:123:global/webacl/x/1</WebACLArn>`+
			`</ListDistributionTenantsByCustomizationRequest>`,
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

	// The tenant should now resolve by its newly-associated domain. Real
	// GetDistributionTenantByDomain is the bare GET "distribution-tenant"
	// (Domain travels as a "?domain=" query value; cloudfront@v1.67.4
	// serializers.go).
	getResp := cfOK(
		t,
		h,
		http.MethodGet,
		tenantDomainPrefix+"distribution-tenant?domain=secondary.example.com",
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
	// UpdateDomainAssociation's own deserializer (cloudfront@v1.67.4
	// deserializers.go) models no conflict-shaped exception -- this is
	// InvalidArgument (400), not 409.
	rr := cfRequest(t, h, http.MethodPost, tenantDomainPrefix+"domain-association", body)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rr.Code, rr.Body.String())
	}
}

// TestUpdateDomainAssociation_RealClient verifies UpdateDomainAssociationOutput carries a single
// ResourceId (matching the real DistributionId-or-DistributionTenantId union collapse, not two
// separate elements) and a populated ETag header, matching cloudfront@v1.67.4
// api_op_UpdateDomainAssociation.go:60-68.
func TestUpdateDomainAssociation_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	tenant, err := client.CreateDistributionTenant(t.Context(), &cfsdk.CreateDistributionTenantInput{
		DistributionId: aws.String("dist-rc-domain"),
		Name:           aws.String("tenant-rc-domain"),
		Domains:        []types.DomainItem{{Domain: aws.String("primary-rc.example.com")}},
	})
	require.NoError(t, err)
	tenantID := aws.ToString(tenant.DistributionTenant.Id)

	out, err := client.UpdateDomainAssociation(t.Context(), &cfsdk.UpdateDomainAssociationInput{
		Domain: aws.String("secondary-rc.example.com"),
		TargetResource: &types.DistributionResourceId{
			DistributionTenantId: aws.String(tenantID),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "secondary-rc.example.com", aws.ToString(out.Domain))
	assert.Equal(t, tenantID, aws.ToString(out.ResourceId), "ResourceId must carry the target tenant, not be empty")
	assert.NotEmpty(t, aws.ToString(out.ETag), "ETag header must be populated")
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
		`<Tags><Items><Tag><Key>team</Key><Value>edge</Value></Tag></Items></Tags>` +
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
		`<Tags><Items><Tag><Key>owner</Key><Value>team-a</Value></Tag></Items></Tags>` +
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
		`<AssociateDistributionTenantWebACLRequest>`+
			`<WebACLArn>arn:aws:wafv2:us-east-1:123:global/webacl/persist/1</WebACLArn>`+
			`</AssociateDistributionTenantWebACLRequest>`,
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
		tenantDomainPrefix+"distribution-tenant?domain=persist.example.com", "")
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

// TestGetDistributionTenant_NotFound verifies the not-found path returns EntityNotFound.
func TestGetDistributionTenant_NotFound(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)
	rr := cfRequest(t, h, http.MethodGet, tenantDomainPrefix+"distribution-tenant/does-not-exist", "")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rr.Code, rr.Body.String())
	}

	if !strings.Contains(rr.Body.String(), "EntityNotFound") {
		t.Errorf("expected EntityNotFound in body, got: %s", rr.Body.String())
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

// TestListDistributionTenants_ItemShape_RealClient is a regression test for
// gopherstack-21my: ListDistributionTenants' item struct (tenantSummaryXML,
// handler_distribution_tenants.go) omitted ETag, CreatedTime, and LastModifiedTime
// entirely, even though the real DistributionTenantSummary deserializer
// (awsRestxml_deserializeDocumentDistributionTenantSummary) reads all three and they are
// backed by real state (DistributionTenant.ETag/.CreationTime/.LastModifiedTime, set at
// CreateDistributionTenant). Seeds two tenants and asserts every field round-trips
// non-empty.
func TestListDistributionTenants_ItemShape_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	mk := func(distID, name, domain string) *cfsdk.CreateDistributionTenantOutput {
		out, err := client.CreateDistributionTenant(t.Context(), &cfsdk.CreateDistributionTenantInput{
			DistributionId: aws.String(distID),
			Name:           aws.String(name),
			Domains:        []types.DomainItem{{Domain: aws.String(domain)}},
		})
		require.NoError(t, err)

		return out
	}

	first := mk("dist-list-shape-1", "tenant-list-shape-1", "list-shape-1.example.com")
	second := mk("dist-list-shape-2", "tenant-list-shape-2", "list-shape-2.example.com")

	listed, err := client.ListDistributionTenants(t.Context(), &cfsdk.ListDistributionTenantsInput{})
	require.NoError(t, err)
	require.Len(t, listed.DistributionTenantList, 2)

	byID := make(map[string]types.DistributionTenantSummary, 2)
	for _, item := range listed.DistributionTenantList {
		require.NotNil(t, item.Id)
		byID[*item.Id] = item
	}

	item1, ok := byID[*first.DistributionTenant.Id]
	require.True(t, ok)
	assert.NotEmpty(t, aws.ToString(item1.ETag), "ETag must round-trip, not decode empty")
	assert.NotNil(t, item1.CreatedTime, "CreatedTime must round-trip, not decode nil")
	assert.NotNil(t, item1.LastModifiedTime, "LastModifiedTime must round-trip, not decode nil")

	item2, ok := byID[*second.DistributionTenant.Id]
	require.True(t, ok)
	assert.NotEmpty(t, aws.ToString(item2.ETag))
	assert.NotNil(t, item2.CreatedTime)
	assert.NotNil(t, item2.LastModifiedTime)
}
