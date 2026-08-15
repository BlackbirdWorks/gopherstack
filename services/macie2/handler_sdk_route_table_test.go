package macie2_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

// sdkRouteCases is the authoritative method+path for every real Macie2
// operation, extracted from macie2@v1.54.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AcceptInvitation", "POST", "/invitations/accept"},
		{"BatchGetCustomDataIdentifiers", "POST", "/custom-data-identifiers/get"},
		{"BatchUpdateAutomatedDiscoveryAccounts", "PATCH", "/automated-discovery/accounts"},
		{"CreateAllowList", "POST", "/allow-lists"},
		{"CreateClassificationJob", "POST", "/jobs"},
		{"CreateCustomDataIdentifier", "POST", "/custom-data-identifiers"},
		{"CreateFindingsFilter", "POST", "/findingsfilters"},
		{"CreateInvitations", "POST", "/invitations"},
		{"CreateMember", "POST", "/members"},
		{"CreateSampleFindings", "POST", "/findings/sample"},
		{"DeclineInvitations", "POST", "/invitations/decline"},
		{"DeleteAllowList", "DELETE", "/allow-lists/PLACEHOLDER"},
		{"DeleteCustomDataIdentifier", "DELETE", "/custom-data-identifiers/PLACEHOLDER"},
		{"DeleteFindingsFilter", "DELETE", "/findingsfilters/PLACEHOLDER"},
		{"DeleteInvitations", "POST", "/invitations/delete"},
		{"DeleteMember", "DELETE", "/members/PLACEHOLDER"},
		{"DescribeBuckets", "POST", "/datasources/s3"},
		{"DescribeClassificationJob", "GET", "/jobs/PLACEHOLDER"},
		{"DescribeOrganizationConfiguration", "GET", "/admin/configuration"},
		{"DisableMacie", "DELETE", "/macie"},
		{"DisableOrganizationAdminAccount", "DELETE", "/admin"},
		{"DisassociateFromAdministratorAccount", "POST", "/administrator/disassociate"},
		{"DisassociateFromMasterAccount", "POST", "/master/disassociate"},
		{"DisassociateMember", "POST", "/members/disassociate/PLACEHOLDER"},
		{"EnableMacie", "POST", "/macie"},
		{"EnableOrganizationAdminAccount", "POST", "/admin"},
		{"GetAdministratorAccount", "GET", "/administrator"},
		{"GetAllowList", "GET", "/allow-lists/PLACEHOLDER"},
		{"GetAutomatedDiscoveryConfiguration", "GET", "/automated-discovery/configuration"},
		{"GetBucketStatistics", "POST", "/datasources/s3/statistics"},
		{"GetClassificationExportConfiguration", "GET", "/classification-export-configuration"},
		{"GetClassificationScope", "GET", "/classification-scopes/PLACEHOLDER"},
		{"GetCustomDataIdentifier", "GET", "/custom-data-identifiers/PLACEHOLDER"},
		{"GetFindingStatistics", "POST", "/findings/statistics"},
		{"GetFindings", "POST", "/findings/describe"},
		{"GetFindingsFilter", "GET", "/findingsfilters/PLACEHOLDER"},
		{"GetFindingsPublicationConfiguration", "GET", "/findings-publication-configuration"},
		{"GetInvitationsCount", "GET", "/invitations/count"},
		{"GetMacieSession", "GET", "/macie"},
		{"GetMasterAccount", "GET", "/master"},
		{"GetMember", "GET", "/members/PLACEHOLDER"},
		{"GetResourceProfile", "GET", "/resource-profiles"},
		{"GetRevealConfiguration", "GET", "/reveal-configuration"},
		{"GetSensitiveDataOccurrences", "GET", "/findings/PLACEHOLDER/reveal"},
		{"GetSensitiveDataOccurrencesAvailability", "GET", "/findings/PLACEHOLDER/reveal/availability"},
		{"GetSensitivityInspectionTemplate", "GET", "/templates/sensitivity-inspections/PLACEHOLDER"},
		{"GetUsageStatistics", "POST", "/usage/statistics"},
		{"GetUsageTotals", "GET", "/usage"},
		{"ListAllowLists", "GET", "/allow-lists"},
		{"ListAutomatedDiscoveryAccounts", "GET", "/automated-discovery/accounts"},
		{"ListClassificationJobs", "POST", "/jobs/list"},
		{"ListClassificationScopes", "GET", "/classification-scopes"},
		{"ListCustomDataIdentifiers", "POST", "/custom-data-identifiers/list"},
		{"ListFindings", "POST", "/findings"},
		{"ListFindingsFilters", "GET", "/findingsfilters"},
		{"ListInvitations", "GET", "/invitations"},
		{"ListManagedDataIdentifiers", "POST", "/managed-data-identifiers/list"},
		{"ListMembers", "GET", "/members"},
		{"ListOrganizationAdminAccounts", "GET", "/admin"},
		{"ListResourceProfileArtifacts", "GET", "/resource-profiles/artifacts"},
		{"ListResourceProfileDetections", "GET", "/resource-profiles/detections"},
		{"ListSensitivityInspectionTemplates", "GET", "/templates/sensitivity-inspections"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"PutClassificationExportConfiguration", "PUT", "/classification-export-configuration"},
		{"PutFindingsPublicationConfiguration", "PUT", "/findings-publication-configuration"},
		{"SearchResources", "POST", "/datasources/search-resources"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"TestCustomDataIdentifier", "POST", "/custom-data-identifiers/test"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateAllowList", "PUT", "/allow-lists/PLACEHOLDER"},
		{"UpdateAutomatedDiscoveryConfiguration", "PUT", "/automated-discovery/configuration"},
		{"UpdateClassificationJob", "PATCH", "/jobs/PLACEHOLDER"},
		{"UpdateClassificationScope", "PATCH", "/classification-scopes/PLACEHOLDER"},
		{"UpdateFindingsFilter", "PATCH", "/findingsfilters/PLACEHOLDER"},
		{"UpdateMacieSession", "PATCH", "/macie"},
		{"UpdateMemberSession", "PATCH", "/macie/members/PLACEHOLDER"},
		{"UpdateOrganizationConfiguration", "PATCH", "/admin/configuration"},
		{"UpdateResourceProfile", "PATCH", "/resource-profiles"},
		{"UpdateResourceProfileDetections", "PATCH", "/resource-profiles/detections"},
		{"UpdateRevealConfiguration", "PUT", "/reveal-configuration"},
		{"UpdateSensitivityInspectionTemplate", "PUT", "/templates/sensitivity-inspections/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Macie2 op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op, then drives the same
// request through the real Handler() and asserts it did not fall through to
// the unmatched-dispatch fallback: pkgs/service/restdispatch.go's shared
// RESTRouter 404s with an errBody when Parse itself can't name an op, but
// when Parse (== ExtractOperation) DOES resolve a real op name yet no
// dispatcher family in h.dispatch claims it, dispatchTagOps's final default
// (handler_tags.go) silently returns (nil, 404, nil) -- rendered as a bare
// "{}" body with no error at all. That gap is exactly the risk this test
// closes: an op name that resolves correctly but has no matching dispatch
// case (gopherstack-ey26). gopherstack-jqh2 pass
// 2: re-extracted all 81 macie2 ops from the pinned SDK and found the
// existing parseRESTPath table already correct -- no bugs, including on the
// four-way (/macie) and three-way (/admin) same-path/different-method
// collisions this service's routing depends on.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := macie2.NewHandler(macie2.NewInMemoryBackend("000000000000", "us-east-1"))

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))

			unmatched := rec.Code == http.StatusNotFound && strings.TrimSpace(rec.Body.String()) == "{}"
			assert.False(t, unmatched,
				"method=%s path=%s op=%s: dispatched to the unmatched-route fallback (404, empty body)",
				tc.method, tc.path, tc.op)
		})
	}
}
