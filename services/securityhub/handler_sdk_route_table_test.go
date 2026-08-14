package securityhub_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real SecurityHub
// operation, extracted from securityhub@v1.75.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} or greedy {Param+} URI label -- the router does not
// validate ID shape, so the literal value doesn't matter here, only that the
// path matches Op.
//
// This service leans hard on method-only discrimination at a shared path
// (GET/POST/DELETE/PATCH all on "/accounts" for DescribeHub/EnableSecurityHub/
// DisableSecurityHub/UpdateSecurityHubConfiguration, and similarly at
// "/hubv2") and on prefix-collision-prone families
// ("/securityControl" vs "/securityControls", "/connectors" vs
// "/connectorsv2", "/automationrules" vs "/automationrulesv2",
// "/configurationPolicy" vs "/configurationPolicyAssociation", "/products"
// vs "/productsV2"/"/productSubscriptions") -- all kept in this table as
// separate cases (not deduplicated to one representative per family) so a
// future prefix-ordering regression in classifyPath's pathClassifiers table
// is caught directly rather than assumed safe.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AcceptAdministratorInvitation", "POST", "/administrator"},
		{"AcceptInvitation", "POST", "/master"},
		{"BatchDeleteAutomationRules", "POST", "/automationrules/delete"},
		{"BatchDisableStandards", "POST", "/standards/deregister"},
		{"BatchEnableStandards", "POST", "/standards/register"},
		{"BatchGetAutomationRules", "POST", "/automationrules/get"},
		{"BatchGetConfigurationPolicyAssociations", "POST", "/configurationPolicyAssociation/batchget"},
		{"BatchGetSecurityControls", "POST", "/securityControls/batchGet"},
		{"BatchGetStandardsControlAssociations", "POST", "/associations/batchGet"},
		{"BatchImportFindings", "POST", "/findings/import"},
		{"BatchUpdateAutomationRules", "PATCH", "/automationrules/update"},
		{"BatchUpdateFindings", "PATCH", "/findings/batchupdate"},
		{"BatchUpdateFindingsV2", "PATCH", "/findingsv2/batchupdatev2"},
		{"BatchUpdateStandardsControlAssociations", "PATCH", "/associations"},
		{"CreateActionTarget", "POST", "/actionTargets"},
		{"CreateAggregatorV2", "POST", "/aggregatorv2/create"},
		{"CreateAutomationRule", "POST", "/automationrules/create"},
		{"CreateAutomationRuleV2", "POST", "/automationrulesv2/create"},
		{"CreateConfigurationPolicy", "POST", "/configurationPolicy/create"},
		{"CreateConnector", "POST", "/connectors"},
		{"CreateConnectorV2", "POST", "/connectorsv2"},
		{"CreateFindingAggregator", "POST", "/findingAggregator/create"},
		{"CreateInsight", "POST", "/insights"},
		{"CreateMembers", "POST", "/members"},
		{"CreateTicketV2", "POST", "/ticketsv2"},
		{"DeclineInvitations", "POST", "/invitations/decline"},
		{"DeleteActionTarget", "DELETE", "/actionTargets/PLACEHOLDER"},
		{"DeleteAggregatorV2", "DELETE", "/aggregatorv2/delete/PLACEHOLDER"},
		{"DeleteAutomationRuleV2", "DELETE", "/automationrulesv2/PLACEHOLDER"},
		{"DeleteConfigurationPolicy", "DELETE", "/configurationPolicy/PLACEHOLDER"},
		{"DeleteConnector", "DELETE", "/connectors/PLACEHOLDER"},
		{"DeleteConnectorV2", "DELETE", "/connectorsv2/PLACEHOLDER"},
		{"DeleteFindingAggregator", "DELETE", "/findingAggregator/delete/PLACEHOLDER"},
		{"DeleteInsight", "DELETE", "/insights/PLACEHOLDER"},
		{"DeleteInvitations", "POST", "/invitations/delete"},
		{"DeleteMembers", "POST", "/members/delete"},
		{"DescribeActionTargets", "POST", "/actionTargets/get"},
		{"DescribeHub", "GET", "/accounts"},
		{"DescribeOrganizationConfiguration", "GET", "/organization/configuration"},
		{"DescribeProducts", "GET", "/products"},
		{"DescribeProductsV2", "GET", "/productsV2"},
		{"DescribeSecurityHubV2", "GET", "/hubv2"},
		{"DescribeStandards", "GET", "/standards"},
		{"DescribeStandardsControls", "GET", "/standards/controls/PLACEHOLDER"},
		{"DisableImportFindingsForProduct", "DELETE", "/productSubscriptions/PLACEHOLDER"},
		{"DisableOrganizationAdminAccount", "POST", "/organization/admin/disable"},
		{"DisableSecurityHub", "DELETE", "/accounts"},
		{"DisableSecurityHubFeatureV2", "DELETE", "/hubv2/feature/PLACEHOLDER"},
		{"DisableSecurityHubV2", "DELETE", "/hubv2"},
		{"DisassociateFromAdministratorAccount", "POST", "/administrator/disassociate"},
		{"DisassociateFromMasterAccount", "POST", "/master/disassociate"},
		{"DisassociateMembers", "POST", "/members/disassociate"},
		{"EnableImportFindingsForProduct", "POST", "/productSubscriptions"},
		{"EnableOrganizationAdminAccount", "POST", "/organization/admin/enable"},
		{"EnableSecurityHub", "POST", "/accounts"},
		{"EnableSecurityHubFeatureV2", "POST", "/hubv2/feature/PLACEHOLDER"},
		{"EnableSecurityHubV2", "POST", "/hubv2"},
		{"GenerateRecommendedPolicyV2", "POST", "/recommendedPolicyV2/PLACEHOLDER"},
		{"GetAdministratorAccount", "GET", "/administrator"},
		{"GetAggregatorV2", "GET", "/aggregatorv2/get/PLACEHOLDER"},
		{"GetAutomationRuleV2", "GET", "/automationrulesv2/PLACEHOLDER"},
		{"GetConfigurationPolicy", "GET", "/configurationPolicy/get/PLACEHOLDER"},
		{"GetConfigurationPolicyAssociation", "POST", "/configurationPolicyAssociation/get"},
		{"GetConnector", "GET", "/connectors/PLACEHOLDER"},
		{"GetConnectorV2", "GET", "/connectorsv2/PLACEHOLDER"},
		{"GetEnabledStandards", "POST", "/standards/get"},
		{"GetFindingAggregator", "GET", "/findingAggregator/get/PLACEHOLDER"},
		{"GetFindingHistory", "POST", "/findingHistory/get"},
		{"GetFindingStatisticsV2", "POST", "/findingsv2/statistics"},
		{"GetFindings", "POST", "/findings"},
		{"GetFindingsTrendsV2", "POST", "/findingsTrendsv2"},
		{"GetFindingsV2", "POST", "/findingsv2"},
		{"GetInsightResults", "GET", "/insights/results/PLACEHOLDER"},
		{"GetInsights", "POST", "/insights/get"},
		{"GetInvitationsCount", "GET", "/invitations/count"},
		{"GetMasterAccount", "GET", "/master"},
		{"GetMembers", "POST", "/members/get"},
		{"GetRecommendedPolicyV2", "GET", "/recommendedPolicyV2/PLACEHOLDER"},
		{"GetResourcesStatisticsV2", "POST", "/resourcesv2/statistics"},
		{"GetResourcesTrendsV2", "POST", "/resourcesTrendsv2"},
		{"GetResourcesV2", "POST", "/resourcesv2"},
		{"GetSecurityControlDefinition", "GET", "/securityControl/definition"},
		{"InviteMembers", "POST", "/members/invite"},
		{"ListAggregatorsV2", "GET", "/aggregatorv2/list"},
		{"ListAutomationRules", "GET", "/automationrules/list"},
		{"ListAutomationRulesV2", "GET", "/automationrulesv2/list"},
		{"ListConfigurationPolicies", "GET", "/configurationPolicy/list"},
		{"ListConfigurationPolicyAssociations", "POST", "/configurationPolicyAssociation/list"},
		{"ListConnectors", "GET", "/connectors"},
		{"ListConnectorsV2", "GET", "/connectorsv2"},
		{"ListEnabledProductsForImport", "GET", "/productSubscriptions"},
		{"ListFindingAggregators", "GET", "/findingAggregator/list"},
		{"ListInvitations", "GET", "/invitations"},
		{"ListMembers", "GET", "/members"},
		{"ListOrganizationAdminAccounts", "GET", "/organization/admin"},
		{"ListSecurityControlDefinitions", "GET", "/securityControls/definitions"},
		{"ListStandardsControlAssociations", "GET", "/associations"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"RegisterConnectorV2", "POST", "/connectorsv2/register"},
		{"StartConfigurationPolicyAssociation", "POST", "/configurationPolicyAssociation/associate"},
		{"StartConfigurationPolicyDisassociation", "POST", "/configurationPolicyAssociation/disassociate"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateActionTarget", "PATCH", "/actionTargets/PLACEHOLDER"},
		{"UpdateAggregatorV2", "PATCH", "/aggregatorv2/update/PLACEHOLDER"},
		{"UpdateAutomationRuleV2", "PATCH", "/automationrulesv2/PLACEHOLDER"},
		{"UpdateConfigurationPolicy", "PATCH", "/configurationPolicy/PLACEHOLDER"},
		{"UpdateConnector", "PATCH", "/connectors/PLACEHOLDER"},
		{"UpdateConnectorV2", "PATCH", "/connectorsv2/PLACEHOLDER"},
		{"UpdateFindingAggregator", "PATCH", "/findingAggregator/update"},
		{"UpdateFindings", "PATCH", "/findings"},
		{"UpdateInsight", "PATCH", "/insights/PLACEHOLDER"},
		{"UpdateOrganizationConfiguration", "POST", "/organization/configuration"},
		{"UpdateSecurityControl", "PATCH", "/securityControl/update"},
		{"UpdateSecurityHubConfiguration", "PATCH", "/accounts"},
		{"UpdateStandardsControl", "PATCH", "/standards/control/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real SecurityHub op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts classifyPath resolves it to the right op, all 116 ops against
// securityhub's real op count. It then drives the same request through the
// real Handler() and asserts it did not fall through to the exact
// "unknown operation" text that handleREST's final default case
// (handler.go) emits when no opHandlerGroups table contains the classified
// op -- distinct from every domain-specific error message this service
// writes (e.g. "Insight not found", "SecurityHub is not enabled"), none of
// which contain that substring.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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
			assert.NotContains(t, rec.Body.String(), "unknown operation",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
