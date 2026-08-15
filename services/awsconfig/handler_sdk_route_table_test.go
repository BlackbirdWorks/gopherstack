package awsconfig_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS Config
// operation, extracted from configservice@v1.68.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("StarlingDoveService.<Op>")
// and always request.Request.Method = "POST" against path "/" -- AWS Config
// is JSON-RPC 1.1 (services/_PROTOCOLS.md), so unlike a REST-family service
// there is no path template to get wrong: dispatch is entirely by this one
// header. ExtractOperation and Handler() both derive the action the same way
// (TrimPrefix on "StarlingDoveService."), so the class of bug this table can
// catch is a dispatch-table key that doesn't exactly match the real op name
// (typo, wrong case -- AWS Config is case-sensitive JSON-RPC), not a
// route-template mismatch. "StarlingDoveService" (not e.g. "ConfigService")
// is the real, historical target prefix -- confirmed directly in the pinned
// serializer, not guessed. This directory's SDK package is `configservice`,
// not `awsconfig` -- resolved from go.mod, not the directory name.
//
// This table covers all 102 real AWS Config ops, which is also
// gopherstack's full implemented set (h.GetSupportedOperations(), 102/102)
// as of configservice@v1.68.4 -- confirmed by diffing the actual
// buildDispatchTable() dispatch table (all twelve family builders combined)
// against this exact list, zero mismatches either direction: no dead key,
// no gap.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("StarlingDoveService.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AssociateResourceTypes", "StarlingDoveService.AssociateResourceTypes"},
		{"BatchGetAggregateResourceConfig", "StarlingDoveService.BatchGetAggregateResourceConfig"},
		{"BatchGetResourceConfig", "StarlingDoveService.BatchGetResourceConfig"},
		{"DeleteAggregationAuthorization", "StarlingDoveService.DeleteAggregationAuthorization"},
		{"DeleteConfigRule", "StarlingDoveService.DeleteConfigRule"},
		{"DeleteConfigurationAggregator", "StarlingDoveService.DeleteConfigurationAggregator"},
		{"DeleteConfigurationRecorder", "StarlingDoveService.DeleteConfigurationRecorder"},
		{"DeleteConformancePack", "StarlingDoveService.DeleteConformancePack"},
		{"DeleteConnector", "StarlingDoveService.DeleteConnector"},
		{"DeleteDeliveryChannel", "StarlingDoveService.DeleteDeliveryChannel"},
		{"DeleteEvaluationResults", "StarlingDoveService.DeleteEvaluationResults"},
		{"DeleteOrganizationConfigRule", "StarlingDoveService.DeleteOrganizationConfigRule"},
		{"DeleteOrganizationConformancePack", "StarlingDoveService.DeleteOrganizationConformancePack"},
		{"DeletePendingAggregationRequest", "StarlingDoveService.DeletePendingAggregationRequest"},
		{"DeleteRemediationConfiguration", "StarlingDoveService.DeleteRemediationConfiguration"},
		{"DeleteRemediationExceptions", "StarlingDoveService.DeleteRemediationExceptions"},
		{"DeleteResourceConfig", "StarlingDoveService.DeleteResourceConfig"},
		{"DeleteRetentionConfiguration", "StarlingDoveService.DeleteRetentionConfiguration"},
		{"DeleteServiceLinkedConfigurationRecorder", "StarlingDoveService.DeleteServiceLinkedConfigurationRecorder"},
		{"DeleteStoredQuery", "StarlingDoveService.DeleteStoredQuery"},
		{"DeliverConfigSnapshot", "StarlingDoveService.DeliverConfigSnapshot"},
		{"DescribeAggregateComplianceByConfigRules", "StarlingDoveService.DescribeAggregateComplianceByConfigRules"},
		{
			"DescribeAggregateComplianceByConformancePacks",
			"StarlingDoveService.DescribeAggregateComplianceByConformancePacks",
		},
		{"DescribeAggregationAuthorizations", "StarlingDoveService.DescribeAggregationAuthorizations"},
		{"DescribeComplianceByConfigRule", "StarlingDoveService.DescribeComplianceByConfigRule"},
		{"DescribeComplianceByResource", "StarlingDoveService.DescribeComplianceByResource"},
		{"DescribeConfigRuleEvaluationStatus", "StarlingDoveService.DescribeConfigRuleEvaluationStatus"},
		{"DescribeConfigRules", "StarlingDoveService.DescribeConfigRules"},
		{"DescribeConfigurationAggregators", "StarlingDoveService.DescribeConfigurationAggregators"},
		{
			"DescribeConfigurationAggregatorSourcesStatus",
			"StarlingDoveService.DescribeConfigurationAggregatorSourcesStatus",
		},
		{"DescribeConfigurationRecorders", "StarlingDoveService.DescribeConfigurationRecorders"},
		{"DescribeConfigurationRecorderStatus", "StarlingDoveService.DescribeConfigurationRecorderStatus"},
		{"DescribeConformancePackCompliance", "StarlingDoveService.DescribeConformancePackCompliance"},
		{"DescribeConformancePacks", "StarlingDoveService.DescribeConformancePacks"},
		{"DescribeConformancePackStatus", "StarlingDoveService.DescribeConformancePackStatus"},
		{"DescribeDeliveryChannels", "StarlingDoveService.DescribeDeliveryChannels"},
		{"DescribeDeliveryChannelStatus", "StarlingDoveService.DescribeDeliveryChannelStatus"},
		{"DescribeOrganizationConfigRules", "StarlingDoveService.DescribeOrganizationConfigRules"},
		{"DescribeOrganizationConfigRuleStatuses", "StarlingDoveService.DescribeOrganizationConfigRuleStatuses"},
		{"DescribeOrganizationConformancePacks", "StarlingDoveService.DescribeOrganizationConformancePacks"},
		{
			"DescribeOrganizationConformancePackStatuses",
			"StarlingDoveService.DescribeOrganizationConformancePackStatuses",
		},
		{"DescribePendingAggregationRequests", "StarlingDoveService.DescribePendingAggregationRequests"},
		{"DescribeRemediationConfigurations", "StarlingDoveService.DescribeRemediationConfigurations"},
		{"DescribeRemediationExceptions", "StarlingDoveService.DescribeRemediationExceptions"},
		{"DescribeRemediationExecutionStatus", "StarlingDoveService.DescribeRemediationExecutionStatus"},
		{"DescribeRetentionConfigurations", "StarlingDoveService.DescribeRetentionConfigurations"},
		{"DisassociateResourceTypes", "StarlingDoveService.DisassociateResourceTypes"},
		{"GetAggregateComplianceDetailsByConfigRule", "StarlingDoveService.GetAggregateComplianceDetailsByConfigRule"},
		{"GetAggregateConfigRuleComplianceSummary", "StarlingDoveService.GetAggregateConfigRuleComplianceSummary"},
		{
			"GetAggregateConformancePackComplianceSummary",
			"StarlingDoveService.GetAggregateConformancePackComplianceSummary",
		},
		{"GetAggregateDiscoveredResourceCounts", "StarlingDoveService.GetAggregateDiscoveredResourceCounts"},
		{"GetAggregateResourceConfig", "StarlingDoveService.GetAggregateResourceConfig"},
		{"GetComplianceDetailsByConfigRule", "StarlingDoveService.GetComplianceDetailsByConfigRule"},
		{"GetComplianceDetailsByResource", "StarlingDoveService.GetComplianceDetailsByResource"},
		{"GetComplianceSummaryByConfigRule", "StarlingDoveService.GetComplianceSummaryByConfigRule"},
		{"GetComplianceSummaryByResourceType", "StarlingDoveService.GetComplianceSummaryByResourceType"},
		{"GetConformancePackComplianceDetails", "StarlingDoveService.GetConformancePackComplianceDetails"},
		{"GetConformancePackComplianceSummary", "StarlingDoveService.GetConformancePackComplianceSummary"},
		{"GetConnector", "StarlingDoveService.GetConnector"},
		{"GetCustomRulePolicy", "StarlingDoveService.GetCustomRulePolicy"},
		{"GetDiscoveredResourceCounts", "StarlingDoveService.GetDiscoveredResourceCounts"},
		{"GetOrganizationConfigRuleDetailedStatus", "StarlingDoveService.GetOrganizationConfigRuleDetailedStatus"},
		{
			"GetOrganizationConformancePackDetailedStatus",
			"StarlingDoveService.GetOrganizationConformancePackDetailedStatus",
		},
		{"GetOrganizationCustomRulePolicy", "StarlingDoveService.GetOrganizationCustomRulePolicy"},
		{"GetResourceConfigHistory", "StarlingDoveService.GetResourceConfigHistory"},
		{"GetResourceEvaluationSummary", "StarlingDoveService.GetResourceEvaluationSummary"},
		{"GetStoredQuery", "StarlingDoveService.GetStoredQuery"},
		{"ListAggregateDiscoveredResources", "StarlingDoveService.ListAggregateDiscoveredResources"},
		{"ListConfigurationRecorders", "StarlingDoveService.ListConfigurationRecorders"},
		{"ListConformancePackComplianceScores", "StarlingDoveService.ListConformancePackComplianceScores"},
		{"ListConnectors", "StarlingDoveService.ListConnectors"},
		{"ListDiscoveredResources", "StarlingDoveService.ListDiscoveredResources"},
		{"ListResourceEvaluations", "StarlingDoveService.ListResourceEvaluations"},
		{"ListStoredQueries", "StarlingDoveService.ListStoredQueries"},
		{"ListTagsForResource", "StarlingDoveService.ListTagsForResource"},
		{"PutAggregationAuthorization", "StarlingDoveService.PutAggregationAuthorization"},
		{"PutConfigRule", "StarlingDoveService.PutConfigRule"},
		{"PutConfigurationAggregator", "StarlingDoveService.PutConfigurationAggregator"},
		{"PutConfigurationRecorder", "StarlingDoveService.PutConfigurationRecorder"},
		{"PutConformancePack", "StarlingDoveService.PutConformancePack"},
		{"PutConnector", "StarlingDoveService.PutConnector"},
		{"PutDeliveryChannel", "StarlingDoveService.PutDeliveryChannel"},
		{"PutEvaluations", "StarlingDoveService.PutEvaluations"},
		{"PutExternalEvaluation", "StarlingDoveService.PutExternalEvaluation"},
		{"PutOrganizationConfigRule", "StarlingDoveService.PutOrganizationConfigRule"},
		{"PutOrganizationConformancePack", "StarlingDoveService.PutOrganizationConformancePack"},
		{"PutRemediationConfigurations", "StarlingDoveService.PutRemediationConfigurations"},
		{"PutRemediationExceptions", "StarlingDoveService.PutRemediationExceptions"},
		{"PutResourceConfig", "StarlingDoveService.PutResourceConfig"},
		{"PutRetentionConfiguration", "StarlingDoveService.PutRetentionConfiguration"},
		{"PutServiceLinkedConfigurationRecorder", "StarlingDoveService.PutServiceLinkedConfigurationRecorder"},
		{"PutStoredQuery", "StarlingDoveService.PutStoredQuery"},
		{
			"PutThirdPartyServiceLinkedConfigurationRecorder",
			"StarlingDoveService.PutThirdPartyServiceLinkedConfigurationRecorder",
		},
		{"SelectAggregateResourceConfig", "StarlingDoveService.SelectAggregateResourceConfig"},
		{"SelectResourceConfig", "StarlingDoveService.SelectResourceConfig"},
		{"StartConfigRulesEvaluation", "StarlingDoveService.StartConfigRulesEvaluation"},
		{"StartConfigurationRecorder", "StarlingDoveService.StartConfigurationRecorder"},
		{"StartRemediationExecution", "StarlingDoveService.StartRemediationExecution"},
		{"StartResourceEvaluation", "StarlingDoveService.StartResourceEvaluation"},
		{"StopConfigurationRecorder", "StarlingDoveService.StopConfigurationRecorder"},
		{"TagResource", "StarlingDoveService.TagResource"},
		{"UntagResource", "StarlingDoveService.UntagResource"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real AWS Config
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to the dispatch-miss sentinel a
// dispatch-table key mismatch would produce.
//
// AWS Config's sentinel (errUnknownAction, "unknown action") is not wire-typed
// at all -- handleError's dedicated branch for it returns an untyped
// {"message": err.Error()} body (see handler.go: configservice@v1.68.4 has no
// single error code that fits every operation, so nothing invents one). The
// message text itself ("unknown action") is unique in the package (grepped)
// and produced only at handler.go's single fmt.Errorf("%w: %s",
// errUnknownAction, action) call site, the dispatch() miss.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := awsconfig.NewInMemoryBackend()
			h := awsconfig.NewHandler(backend)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown action",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
