package cleanrooms_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

// sdkRouteCases is the authoritative method+path for every real Clean Rooms
// operation, extracted from cleanrooms@v1.49.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {...Identifier}/{name}/{type}/{accountId} URI label --
// classifyPath (handler_routing.go) does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op.
// GetCollaborationAnalysisTemplate's real path parameter (analysisTemplateArn)
// spans more than one segment once URL-decoded, but classifyCollabAnalysisTemplates
// matches on ">= 4 segments" specifically to allow that, so a single
// PLACEHOLDER segment (giving exactly 4) still resolves to this op.
//
// A systematic check for a shared method+path template across all 100 ops
// found zero collisions, so no *required dynamic* (non-template) member --
// the s3/glacier vacuity-trap class -- was needed to disambiguate any route
// in this table.
//
// All 100 ops were confirmed wired into the buildOpHandlers map
// (handler.go/handler_*.go) before writing this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"BatchGetCollaborationAnalysisTemplate", "POST", "/collaborations/PLACEHOLDER/batch-analysistemplates"},
		{"BatchGetSchema", "POST", "/collaborations/PLACEHOLDER/batch-schema"},
		{"BatchGetSchemaAnalysisRule", "POST", "/collaborations/PLACEHOLDER/batch-schema-analysis-rule"},
		{"CreateAnalysisTemplate", "POST", "/memberships/PLACEHOLDER/analysistemplates"},
		{"CreateCollaboration", "POST", "/collaborations"},
		{"CreateCollaborationChangeRequest", "POST", "/collaborations/PLACEHOLDER/changeRequests"},
		{
			"CreateConfiguredAudienceModelAssociation",
			"POST",
			"/memberships/PLACEHOLDER/configuredaudiencemodelassociations",
		},
		{"CreateConfiguredTable", "POST", "/configuredTables"},
		{"CreateConfiguredTableAnalysisRule", "POST", "/configuredTables/PLACEHOLDER/analysisRule"},
		{"CreateConfiguredTableAssociation", "POST", "/memberships/PLACEHOLDER/configuredTableAssociations"},
		{
			"CreateConfiguredTableAssociationAnalysisRule",
			"POST",
			"/memberships/PLACEHOLDER/configuredTableAssociations/PLACEHOLDER/analysisRule",
		},
		{"CreateIdMappingTable", "POST", "/memberships/PLACEHOLDER/idmappingtables"},
		{"CreateIdNamespaceAssociation", "POST", "/memberships/PLACEHOLDER/idnamespaceassociations"},
		{"CreateIntermediateTable", "POST", "/memberships/PLACEHOLDER/intermediateTables"},
		{
			"CreateIntermediateTableAnalysisRule",
			"POST",
			"/memberships/PLACEHOLDER/intermediateTables/PLACEHOLDER/analysisRule",
		},
		{"CreateMembership", "POST", "/memberships"},
		{"CreatePrivacyBudgetTemplate", "POST", "/memberships/PLACEHOLDER/privacybudgettemplates"},
		{"DeleteAnalysisTemplate", "DELETE", "/memberships/PLACEHOLDER/analysistemplates/PLACEHOLDER"},
		{"DeleteCollaboration", "DELETE", "/collaborations/PLACEHOLDER"},
		{
			"DeleteConfiguredAudienceModelAssociation",
			"DELETE",
			"/memberships/PLACEHOLDER/configuredaudiencemodelassociations/PLACEHOLDER",
		},
		{"DeleteConfiguredTable", "DELETE", "/configuredTables/PLACEHOLDER"},
		{"DeleteConfiguredTableAnalysisRule", "DELETE", "/configuredTables/PLACEHOLDER/analysisRule/PLACEHOLDER"},
		{
			"DeleteConfiguredTableAssociation",
			"DELETE",
			"/memberships/PLACEHOLDER/configuredTableAssociations/PLACEHOLDER",
		},
		{
			"DeleteConfiguredTableAssociationAnalysisRule",
			"DELETE",
			"/memberships/PLACEHOLDER/configuredTableAssociations/PLACEHOLDER/analysisRule/PLACEHOLDER",
		},
		{"DeleteIdMappingTable", "DELETE", "/memberships/PLACEHOLDER/idmappingtables/PLACEHOLDER"},
		{"DeleteIdNamespaceAssociation", "DELETE", "/memberships/PLACEHOLDER/idnamespaceassociations/PLACEHOLDER"},
		{"DeleteIntermediateTable", "DELETE", "/memberships/PLACEHOLDER/intermediateTables/PLACEHOLDER"},
		{
			"DeleteIntermediateTableAnalysisRule",
			"DELETE",
			"/memberships/PLACEHOLDER/intermediateTables/PLACEHOLDER/analysisRule/PLACEHOLDER",
		},
		{"DeleteMember", "DELETE", "/collaborations/PLACEHOLDER/member/PLACEHOLDER"},
		{"DeleteMembership", "DELETE", "/memberships/PLACEHOLDER"},
		{"DeletePrivacyBudgetTemplate", "DELETE", "/memberships/PLACEHOLDER/privacybudgettemplates/PLACEHOLDER"},
		{"DisallowIntermediateTable", "POST", "/memberships/PLACEHOLDER/disallowIntermediateTable"},
		{"GetAnalysisTemplate", "GET", "/memberships/PLACEHOLDER/analysistemplates/PLACEHOLDER"},
		{"GetCollaboration", "GET", "/collaborations/PLACEHOLDER"},
		{"GetCollaborationAnalysisTemplate", "GET", "/collaborations/PLACEHOLDER/analysistemplates/PLACEHOLDER"},
		{"GetCollaborationChangeRequest", "GET", "/collaborations/PLACEHOLDER/changeRequests/PLACEHOLDER"},
		{
			"GetCollaborationConfiguredAudienceModelAssociation",
			"GET",
			"/collaborations/PLACEHOLDER/configuredaudiencemodelassociations/PLACEHOLDER",
		},
		{
			"GetCollaborationIdNamespaceAssociation",
			"GET",
			"/collaborations/PLACEHOLDER/idnamespaceassociations/PLACEHOLDER",
		},
		{
			"GetCollaborationPrivacyBudgetTemplate",
			"GET",
			"/collaborations/PLACEHOLDER/privacybudgettemplates/PLACEHOLDER",
		},
		{
			"GetConfiguredAudienceModelAssociation",
			"GET",
			"/memberships/PLACEHOLDER/configuredaudiencemodelassociations/PLACEHOLDER",
		},
		{"GetConfiguredTable", "GET", "/configuredTables/PLACEHOLDER"},
		{"GetConfiguredTableAnalysisRule", "GET", "/configuredTables/PLACEHOLDER/analysisRule/PLACEHOLDER"},
		{"GetConfiguredTableAssociation", "GET", "/memberships/PLACEHOLDER/configuredTableAssociations/PLACEHOLDER"},
		{
			"GetConfiguredTableAssociationAnalysisRule",
			"GET",
			"/memberships/PLACEHOLDER/configuredTableAssociations/PLACEHOLDER/analysisRule/PLACEHOLDER",
		},
		{"GetIdMappingTable", "GET", "/memberships/PLACEHOLDER/idmappingtables/PLACEHOLDER"},
		{"GetIdNamespaceAssociation", "GET", "/memberships/PLACEHOLDER/idnamespaceassociations/PLACEHOLDER"},
		{"GetIntermediateTable", "GET", "/memberships/PLACEHOLDER/intermediateTables/PLACEHOLDER"},
		{
			"GetIntermediateTableAnalysisRule",
			"GET",
			"/memberships/PLACEHOLDER/intermediateTables/PLACEHOLDER/analysisRule/PLACEHOLDER",
		},
		{"GetMembership", "GET", "/memberships/PLACEHOLDER"},
		{"GetPrivacyBudgetTemplate", "GET", "/memberships/PLACEHOLDER/privacybudgettemplates/PLACEHOLDER"},
		{"GetProtectedJob", "GET", "/memberships/PLACEHOLDER/protectedJobs/PLACEHOLDER"},
		{"GetProtectedQuery", "GET", "/memberships/PLACEHOLDER/protectedQueries/PLACEHOLDER"},
		{"GetSchema", "GET", "/collaborations/PLACEHOLDER/schemas/PLACEHOLDER"},
		{"GetSchemaAnalysisRule", "GET", "/collaborations/PLACEHOLDER/schemas/PLACEHOLDER/analysisRule/PLACEHOLDER"},
		{"ListAnalysisTemplates", "GET", "/memberships/PLACEHOLDER/analysistemplates"},
		{"ListCollaborationAnalysisTemplates", "GET", "/collaborations/PLACEHOLDER/analysistemplates"},
		{"ListCollaborationChangeRequests", "GET", "/collaborations/PLACEHOLDER/changeRequests"},
		{
			"ListCollaborationConfiguredAudienceModelAssociations",
			"GET",
			"/collaborations/PLACEHOLDER/configuredaudiencemodelassociations",
		},
		{"ListCollaborationIdNamespaceAssociations", "GET", "/collaborations/PLACEHOLDER/idnamespaceassociations"},
		{"ListCollaborationPrivacyBudgetTemplates", "GET", "/collaborations/PLACEHOLDER/privacybudgettemplates"},
		{"ListCollaborationPrivacyBudgets", "GET", "/collaborations/PLACEHOLDER/privacybudgets"},
		{"ListCollaborations", "GET", "/collaborations"},
		{
			"ListConfiguredAudienceModelAssociations",
			"GET",
			"/memberships/PLACEHOLDER/configuredaudiencemodelassociations",
		},
		{"ListConfiguredTableAssociations", "GET", "/memberships/PLACEHOLDER/configuredTableAssociations"},
		{"ListConfiguredTables", "GET", "/configuredTables"},
		{"ListIdMappingTables", "GET", "/memberships/PLACEHOLDER/idmappingtables"},
		{"ListIdNamespaceAssociations", "GET", "/memberships/PLACEHOLDER/idnamespaceassociations"},
		{"ListIntermediateTableVersions", "GET", "/memberships/PLACEHOLDER/intermediateTables/PLACEHOLDER/versions"},
		{"ListIntermediateTables", "GET", "/memberships/PLACEHOLDER/intermediateTables"},
		{"ListMembers", "GET", "/collaborations/PLACEHOLDER/members"},
		{"ListMemberships", "GET", "/memberships"},
		{"ListPrivacyBudgetTemplates", "GET", "/memberships/PLACEHOLDER/privacybudgettemplates"},
		{"ListPrivacyBudgets", "GET", "/memberships/PLACEHOLDER/privacybudgets"},
		{"ListProtectedJobs", "GET", "/memberships/PLACEHOLDER/protectedJobs"},
		{"ListProtectedQueries", "GET", "/memberships/PLACEHOLDER/protectedQueries"},
		{"ListSchemas", "GET", "/collaborations/PLACEHOLDER/schemas"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"PopulateIdMappingTable", "POST", "/memberships/PLACEHOLDER/idmappingtables/PLACEHOLDER/populate"},
		{"PopulateIntermediateTable", "POST", "/memberships/PLACEHOLDER/intermediateTables/PLACEHOLDER/populate"},
		{"PreviewPrivacyImpact", "POST", "/memberships/PLACEHOLDER/previewprivacyimpact"},
		{"StartProtectedJob", "POST", "/memberships/PLACEHOLDER/protectedJobs"},
		{"StartProtectedQuery", "POST", "/memberships/PLACEHOLDER/protectedQueries"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateAnalysisTemplate", "PATCH", "/memberships/PLACEHOLDER/analysistemplates/PLACEHOLDER"},
		{"UpdateCollaboration", "PATCH", "/collaborations/PLACEHOLDER"},
		{"UpdateCollaborationChangeRequest", "PATCH", "/collaborations/PLACEHOLDER/changeRequests/PLACEHOLDER"},
		{
			"UpdateConfiguredAudienceModelAssociation",
			"PATCH",
			"/memberships/PLACEHOLDER/configuredaudiencemodelassociations/PLACEHOLDER",
		},
		{"UpdateConfiguredTable", "PATCH", "/configuredTables/PLACEHOLDER"},
		{"UpdateConfiguredTableAnalysisRule", "PATCH", "/configuredTables/PLACEHOLDER/analysisRule/PLACEHOLDER"},
		{
			"UpdateConfiguredTableAssociation",
			"PATCH",
			"/memberships/PLACEHOLDER/configuredTableAssociations/PLACEHOLDER",
		},
		{
			"UpdateConfiguredTableAssociationAnalysisRule",
			"PATCH",
			"/memberships/PLACEHOLDER/configuredTableAssociations/PLACEHOLDER/analysisRule/PLACEHOLDER",
		},
		{"UpdateIdMappingTable", "PATCH", "/memberships/PLACEHOLDER/idmappingtables/PLACEHOLDER"},
		{"UpdateIdNamespaceAssociation", "PATCH", "/memberships/PLACEHOLDER/idnamespaceassociations/PLACEHOLDER"},
		{"UpdateIntermediateTable", "PATCH", "/memberships/PLACEHOLDER/intermediateTables/PLACEHOLDER"},
		{
			"UpdateIntermediateTableAnalysisRule",
			"PATCH",
			"/memberships/PLACEHOLDER/intermediateTables/PLACEHOLDER/analysisRule/PLACEHOLDER",
		},
		{"UpdateMembership", "PATCH", "/memberships/PLACEHOLDER"},
		{"UpdatePrivacyBudgetTemplate", "PATCH", "/memberships/PLACEHOLDER/privacybudgettemplates/PLACEHOLDER"},
		{"UpdateProtectedJob", "PATCH", "/memberships/PLACEHOLDER/protectedJobs/PLACEHOLDER"},
		{"UpdateProtectedQuery", "PATCH", "/memberships/PLACEHOLDER/protectedQueries/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Clean Rooms op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts classifyPath resolves it to the right op, all 100 ops against
// cleanrooms's real op count. It then drives the same request through the
// real Handler() and asserts the response body is not exactly the literal
// "not found" plain-text sentinel Handler() (handler.go) writes via c.String
// when op == opUnknown -- distinct from every domain not-found error (e.g.
// errMsgNotFound-derived messages), all of which are written through c.JSON
// and so always produce a "{...}" body, never the bare unquoted sentinel. An
// exact-body check (not substring) is used deliberately, matching the same
// class of false-positive risk the xray and iam route tables guard against.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := cleanrooms.NewHandler(cleanrooms.NewInMemoryBackend("123456789012", "us-east-1"))

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
			assert.NotEqual(t, "not found", rec.Body.String(),
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
