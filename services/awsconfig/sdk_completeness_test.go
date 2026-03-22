package awsconfig_test

import (
	"testing"

	awsconfigsdk "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// configservice client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := awsconfig.NewInMemoryBackend()
	h := awsconfig.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &awsconfigsdk.Client{}, h.GetSupportedOperations(), []string{
		"AssociateResourceTypes",
		"BatchGetAggregateResourceConfig",
		"BatchGetResourceConfig",
		"DeleteAggregationAuthorization",
		"DeleteConfigRule",
		"DeleteConfigurationAggregator",
		"DeleteConformancePack",
		"DeleteEvaluationResults",
		"DeleteOrganizationConfigRule",
		"DeleteOrganizationConformancePack",
		"DeletePendingAggregationRequest",
		"DeleteRemediationConfiguration",
		"DeleteRemediationExceptions",
		"DeleteResourceConfig",
		"DeleteRetentionConfiguration",
		"DeleteServiceLinkedConfigurationRecorder",
		"DeleteStoredQuery",
		"DeliverConfigSnapshot",
		"DescribeAggregateComplianceByConfigRules",
		"DescribeAggregateComplianceByConformancePacks",
		"DescribeAggregationAuthorizations",
		"DescribeComplianceByConfigRule",
		"DescribeComplianceByResource",
		"DescribeConfigRuleEvaluationStatus",
		"DescribeConfigurationAggregatorSourcesStatus",
		"DescribeConfigurationAggregators",
		"DescribeConformancePackCompliance",
		"DescribeConformancePackStatus",
		"DescribeConformancePacks",
		"DescribeDeliveryChannelStatus",
		"DescribeOrganizationConfigRuleStatuses",
		"DescribeOrganizationConfigRules",
		"DescribeOrganizationConformancePackStatuses",
		"DescribeOrganizationConformancePacks",
		"DescribePendingAggregationRequests",
		"DescribeRemediationConfigurations",
		"DescribeRemediationExceptions",
		"DescribeRemediationExecutionStatus",
		"DescribeRetentionConfigurations",
		"DisassociateResourceTypes",
		"GetAggregateComplianceDetailsByConfigRule",
		"GetAggregateConfigRuleComplianceSummary",
		"GetAggregateConformancePackComplianceSummary",
		"GetAggregateDiscoveredResourceCounts",
		"GetAggregateResourceConfig",
		"GetComplianceDetailsByResource",
		"GetComplianceSummaryByConfigRule",
		"GetComplianceSummaryByResourceType",
		"GetConformancePackComplianceDetails",
		"GetConformancePackComplianceSummary",
		"GetCustomRulePolicy",
		"GetDiscoveredResourceCounts",
		"GetOrganizationConfigRuleDetailedStatus",
		"GetOrganizationConformancePackDetailedStatus",
		"GetOrganizationCustomRulePolicy",
		"GetResourceConfigHistory",
		"GetResourceEvaluationSummary",
		"GetStoredQuery",
		"ListAggregateDiscoveredResources",
		"ListConfigurationRecorders",
		"ListConformancePackComplianceScores",
		"ListDiscoveredResources",
		"ListResourceEvaluations",
		"ListStoredQueries",
		"ListTagsForResource",
		"PutAggregationAuthorization",
		"PutConfigRule",
		"PutConfigurationAggregator",
		"PutConformancePack",
		"PutEvaluations",
		"PutExternalEvaluation",
		"PutOrganizationConfigRule",
		"PutOrganizationConformancePack",
		"PutRemediationConfigurations",
		"PutRemediationExceptions",
		"PutResourceConfig",
		"PutRetentionConfiguration",
		"PutServiceLinkedConfigurationRecorder",
		"PutStoredQuery",
		"SelectAggregateResourceConfig",
		"SelectResourceConfig",
		"StartConfigRulesEvaluation",
		"StartRemediationExecution",
		"StartResourceEvaluation",
		"StopConfigurationRecorder",
		"TagResource",
		"UntagResource",
	})
}
