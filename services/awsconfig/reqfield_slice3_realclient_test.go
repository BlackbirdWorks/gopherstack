package awsconfig_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	configservicesdk "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

// TestReqFieldSlice3_AWSConfig_Pagination proves, against the real typed
// aws-sdk-go-v2 configservice client, that every tier-1 Limit/NextToken (and
// the handful of sort/order/time-range) fields flagged by cmd/reqfielddiff
// for awsconfig (gopherstack-xhu2t slice 3) are now read and actually applied
// -- not merely decoded. Each subtest seeds at least two items, requests a
// page of one, and asserts both the truncation and the continuation token
// behave observably.
func TestReqFieldSlice3_AWSConfig_Pagination(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testReqField3AggregationAuthorizationsPagination, "aggregation_authorizations"},
		{testReqField3ConfigurationAggregatorsPagination, "configuration_aggregators"},
		{testReqField3AggregatorSourcesStatusPagination, "aggregator_sources_status"},
		{testReqField3PendingAggregationRequestsPagination, "pending_aggregation_requests"},
		{testReqField3ConfigRuleEvaluationStatusPagination, "config_rule_evaluation_status"},
		{testReqField3ComplianceDetailsByConfigRulePagination, "compliance_details_by_config_rule"},
		{testReqField3ComplianceByResourcePagination, "compliance_by_resource"},
		{testReqField3RemediationExceptionsPagination, "remediation_exceptions"},
		{testReqField3RemediationExecutionStatusPagination, "remediation_execution_status"},
		{testReqField3OrganizationConfigRulesPagination, "organization_config_rules"},
		{testReqField3OrganizationConformancePacksPagination, "organization_conformance_packs"},
		{testReqField3AggregateComplianceByConfigRulesPagination, "aggregate_compliance_by_config_rules"},
		{testReqField3AggregateComplianceByConformancePacksPagination, "aggregate_compliance_by_conformance_packs"},
		{testReqField3ConformancePackComplianceDetailsPagination, "conformance_pack_compliance_details"},
		{testReqField3ConformancePackComplianceScoresSort, "conformance_pack_compliance_scores_sort"},
		{testReqField3ListAggregateDiscoveredResourcesPagination, "list_aggregate_discovered_resources"},
		{testReqField3ListDiscoveredResourcesPagination, "list_discovered_resources"},
		{testReqField3ListResourceEvaluationsPagination, "list_resource_evaluations"},
		{testReqField3ListTagsForResourcePagination, "list_tags_for_resource"},
		{testReqField3ResourceConfigHistoryOrderAndTimeRange, "resource_config_history_order_and_time_range"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func newReqField3Backend(t *testing.T) (*awsconfig.InMemoryBackend, *configservicesdk.Client) {
	t.Helper()

	backend := awsconfig.NewInMemoryBackendWithMeta("000000000000", "us-east-1")
	client := newTestAWSConfigSDKClient(t, awsconfig.NewHandler(backend))

	return backend, client
}

func testReqField3AggregationAuthorizationsPagination(t *testing.T) {
	t.Helper()

	backend, client := newReqField3Backend(t)
	ctx := t.Context()

	require.NoError(t, backend.PutAggregationAuthorization("111111111111", "us-east-1", nil))
	require.NoError(t, backend.PutAggregationAuthorization("222222222222", "us-east-1", nil))

	page1, err := client.DescribeAggregationAuthorizations(
		ctx, &configservicesdk.DescribeAggregationAuthorizationsInput{Limit: 1},
	)
	require.NoError(t, err)
	require.Len(t, page1.AggregationAuthorizations, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.DescribeAggregationAuthorizations(
		ctx, &configservicesdk.DescribeAggregationAuthorizationsInput{
			Limit: 1, NextToken: page1.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.AggregationAuthorizations, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
	assert.NotEqual(t,
		aws.ToString(page1.AggregationAuthorizations[0].AuthorizedAccountId),
		aws.ToString(page2.AggregationAuthorizations[0].AuthorizedAccountId),
	)
}

func testReqField3ConfigurationAggregatorsPagination(t *testing.T) {
	t.Helper()

	backend, client := newReqField3Backend(t)
	ctx := t.Context()

	require.NoError(t, backend.PutConfigurationAggregator("agg-a", nil, nil, nil))
	require.NoError(t, backend.PutConfigurationAggregator("agg-b", nil, nil, nil))

	page1, err := client.DescribeConfigurationAggregators(
		ctx, &configservicesdk.DescribeConfigurationAggregatorsInput{Limit: 1},
	)
	require.NoError(t, err)
	require.Len(t, page1.ConfigurationAggregators, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.DescribeConfigurationAggregators(
		ctx, &configservicesdk.DescribeConfigurationAggregatorsInput{
			Limit: 1, NextToken: page1.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.ConfigurationAggregators, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func testReqField3AggregatorSourcesStatusPagination(t *testing.T) {
	t.Helper()

	backend, client := newReqField3Backend(t)
	ctx := t.Context()

	require.NoError(t, backend.PutConfigurationAggregator(
		"agg-sources",
		[]awsconfig.AccountAggregationSource{{
			AccountIDs: []string{"111111111111"},
			AwsRegions: []string{"us-east-1", "us-west-2"},
		}},
		nil, nil,
	))

	page1, err := client.DescribeConfigurationAggregatorSourcesStatus(
		ctx, &configservicesdk.DescribeConfigurationAggregatorSourcesStatusInput{
			ConfigurationAggregatorName: aws.String("agg-sources"),
			Limit:                       1,
		},
	)
	require.NoError(t, err)
	require.Len(t, page1.AggregatedSourceStatusList, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.DescribeConfigurationAggregatorSourcesStatus(
		ctx, &configservicesdk.DescribeConfigurationAggregatorSourcesStatusInput{
			ConfigurationAggregatorName: aws.String("agg-sources"),
			Limit:                       1,
			NextToken:                   page1.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.AggregatedSourceStatusList, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func testReqField3PendingAggregationRequestsPagination(t *testing.T) {
	t.Helper()

	backend, client := newReqField3Backend(t)
	ctx := t.Context()

	require.NoError(t, backend.PutAggregationAuthorization("111111111111", "us-east-1", nil))
	require.NoError(t, backend.PutAggregationAuthorization("222222222222", "us-east-1", nil))

	page1, err := client.DescribePendingAggregationRequests(
		ctx, &configservicesdk.DescribePendingAggregationRequestsInput{Limit: 1},
	)
	require.NoError(t, err)
	require.Len(t, page1.PendingAggregationRequests, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.DescribePendingAggregationRequests(
		ctx, &configservicesdk.DescribePendingAggregationRequestsInput{
			Limit: 1, NextToken: page1.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.PendingAggregationRequests, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func putReqField3ConfigRule(t *testing.T, backend *awsconfig.InMemoryBackend, name string) {
	t.Helper()

	require.NoError(t, backend.PutConfigRule(&awsconfig.ConfigRule{
		ConfigRuleName: name,
		Source: &awsconfig.ConfigRuleSource{
			Owner:            "AWS",
			SourceIdentifier: "S3_BUCKET_VERSIONING_ENABLED",
		},
	}))
}

func testReqField3ConfigRuleEvaluationStatusPagination(t *testing.T) {
	t.Helper()

	backend, client := newReqField3Backend(t)
	ctx := t.Context()

	putReqField3ConfigRule(t, backend, "rule-a")
	putReqField3ConfigRule(t, backend, "rule-b")
	require.NoError(t, backend.PutEvaluations([]awsconfig.EvaluationResult{
		{
			ConfigRuleName: "rule-a", ComplianceType: "COMPLIANT",
			ResourceType: "AWS::S3::Bucket", ResourceID: "b1",
		},
		{
			ConfigRuleName: "rule-b", ComplianceType: "COMPLIANT",
			ResourceType: "AWS::S3::Bucket", ResourceID: "b2",
		},
	}))

	page1, err := client.DescribeConfigRuleEvaluationStatus(
		ctx, &configservicesdk.DescribeConfigRuleEvaluationStatusInput{Limit: 1},
	)
	require.NoError(t, err)
	require.Len(t, page1.ConfigRulesEvaluationStatus, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.DescribeConfigRuleEvaluationStatus(
		ctx, &configservicesdk.DescribeConfigRuleEvaluationStatusInput{
			Limit: 1, NextToken: page1.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.ConfigRulesEvaluationStatus, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func testReqField3ComplianceDetailsByConfigRulePagination(t *testing.T) {
	t.Helper()

	backend, client := newReqField3Backend(t)
	ctx := t.Context()

	putReqField3ConfigRule(t, backend, "rule-details")
	require.NoError(t, backend.PutEvaluations([]awsconfig.EvaluationResult{
		{
			ConfigRuleName: "rule-details", ComplianceType: "NON_COMPLIANT",
			ResourceType: "AWS::S3::Bucket", ResourceID: "b1",
		},
		{
			ConfigRuleName: "rule-details", ComplianceType: "COMPLIANT",
			ResourceType: "AWS::S3::Bucket", ResourceID: "b2",
		},
	}))

	page1, err := client.GetComplianceDetailsByConfigRule(
		ctx, &configservicesdk.GetComplianceDetailsByConfigRuleInput{
			ConfigRuleName: aws.String("rule-details"), Limit: 1,
		},
	)
	require.NoError(t, err)
	require.Len(t, page1.EvaluationResults, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.GetComplianceDetailsByConfigRule(
		ctx, &configservicesdk.GetComplianceDetailsByConfigRuleInput{
			ConfigRuleName: aws.String("rule-details"), Limit: 1, NextToken: page1.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.EvaluationResults, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func testReqField3ComplianceByResourcePagination(t *testing.T) {
	t.Helper()

	backend, client := newReqField3Backend(t)
	ctx := t.Context()

	putReqField3ConfigRule(t, backend, "rule-by-resource")
	require.NoError(t, backend.PutEvaluations([]awsconfig.EvaluationResult{
		{
			ConfigRuleName: "rule-by-resource", ComplianceType: "COMPLIANT",
			ResourceType: "AWS::S3::Bucket", ResourceID: "b1",
		},
		{
			ConfigRuleName: "rule-by-resource", ComplianceType: "COMPLIANT",
			ResourceType: "AWS::S3::Bucket", ResourceID: "b2",
		},
	}))

	page1, err := client.DescribeComplianceByResource(
		ctx, &configservicesdk.DescribeComplianceByResourceInput{Limit: 1},
	)
	require.NoError(t, err)
	require.Len(t, page1.ComplianceByResources, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.DescribeComplianceByResource(
		ctx, &configservicesdk.DescribeComplianceByResourceInput{Limit: 1, NextToken: page1.NextToken},
	)
	require.NoError(t, err)
	require.Len(t, page2.ComplianceByResources, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func testReqField3RemediationExceptionsPagination(t *testing.T) {
	t.Helper()

	backend, client := newReqField3Backend(t)
	ctx := t.Context()

	putReqField3ConfigRule(t, backend, "rule-exceptions")

	_, err := client.PutRemediationExceptions(ctx, &configservicesdk.PutRemediationExceptionsInput{
		ConfigRuleName: aws.String("rule-exceptions"),
		ResourceKeys: []types.RemediationExceptionResourceKey{
			{ResourceType: aws.String("AWS::S3::Bucket"), ResourceId: aws.String("b1")},
			{ResourceType: aws.String("AWS::S3::Bucket"), ResourceId: aws.String("b2")},
		},
	})
	require.NoError(t, err)

	page1, err := client.DescribeRemediationExceptions(
		ctx, &configservicesdk.DescribeRemediationExceptionsInput{
			ConfigRuleName: aws.String("rule-exceptions"), Limit: 1,
		},
	)
	require.NoError(t, err)
	require.Len(t, page1.RemediationExceptions, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.DescribeRemediationExceptions(
		ctx, &configservicesdk.DescribeRemediationExceptionsInput{
			ConfigRuleName: aws.String("rule-exceptions"), Limit: 1, NextToken: page1.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.RemediationExceptions, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func testReqField3RemediationExecutionStatusPagination(t *testing.T) {
	t.Helper()

	backend, client := newReqField3Backend(t)
	ctx := t.Context()

	putReqField3ConfigRule(t, backend, "rule-exec-status")

	_, err := client.PutRemediationConfigurations(ctx, &configservicesdk.PutRemediationConfigurationsInput{
		RemediationConfigurations: []types.RemediationConfiguration{{
			ConfigRuleName: aws.String("rule-exec-status"),
			TargetType:     types.RemediationTargetTypeSsmDocument,
			TargetId:       aws.String("AWS-EnableS3BucketEncryption"),
		}},
	})
	require.NoError(t, err)

	_, err = client.StartRemediationExecution(ctx, &configservicesdk.StartRemediationExecutionInput{
		ConfigRuleName: aws.String("rule-exec-status"),
		ResourceKeys: []types.ResourceKey{
			{ResourceType: types.ResourceTypeBucket, ResourceId: aws.String("b1")},
			{ResourceType: types.ResourceTypeBucket, ResourceId: aws.String("b2")},
		},
	})
	require.NoError(t, err)

	page1, err := client.DescribeRemediationExecutionStatus(
		ctx, &configservicesdk.DescribeRemediationExecutionStatusInput{
			ConfigRuleName: aws.String("rule-exec-status"), Limit: 1,
		},
	)
	require.NoError(t, err)
	require.Len(t, page1.RemediationExecutionStatuses, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.DescribeRemediationExecutionStatus(
		ctx, &configservicesdk.DescribeRemediationExecutionStatusInput{
			ConfigRuleName: aws.String("rule-exec-status"), Limit: 1, NextToken: page1.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.RemediationExecutionStatuses, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func testReqField3OrganizationConfigRulesPagination(t *testing.T) {
	t.Helper()

	_, client := newReqField3Backend(t)
	ctx := t.Context()

	for _, name := range []string{"org-rule-a", "org-rule-b"} {
		_, err := client.PutOrganizationConfigRule(ctx, &configservicesdk.PutOrganizationConfigRuleInput{
			OrganizationConfigRuleName: aws.String(name),
			OrganizationManagedRuleMetadata: &types.OrganizationManagedRuleMetadata{
				RuleIdentifier: aws.String("S3_BUCKET_VERSIONING_ENABLED"),
			},
		})
		require.NoError(t, err)
	}

	page1, err := client.DescribeOrganizationConfigRules(
		ctx, &configservicesdk.DescribeOrganizationConfigRulesInput{Limit: 1},
	)
	require.NoError(t, err)
	require.Len(t, page1.OrganizationConfigRules, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.DescribeOrganizationConfigRules(
		ctx, &configservicesdk.DescribeOrganizationConfigRulesInput{Limit: 1, NextToken: page1.NextToken},
	)
	require.NoError(t, err)
	require.Len(t, page2.OrganizationConfigRules, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))

	statusPage1, err := client.DescribeOrganizationConfigRuleStatuses(
		ctx, &configservicesdk.DescribeOrganizationConfigRuleStatusesInput{Limit: 1},
	)
	require.NoError(t, err)
	require.Len(t, statusPage1.OrganizationConfigRuleStatuses, 1)
	require.NotEmpty(t, aws.ToString(statusPage1.NextToken))
}

func testReqField3OrganizationConformancePacksPagination(t *testing.T) {
	t.Helper()

	_, client := newReqField3Backend(t)
	ctx := t.Context()

	for _, name := range []string{"org-pack-a", "org-pack-b"} {
		_, err := client.PutOrganizationConformancePack(ctx, &configservicesdk.PutOrganizationConformancePackInput{
			OrganizationConformancePackName: aws.String(name),
			TemplateBody: aws.String(`Resources:
  Rule1:
    Type: AWS::Config::ConfigRule
    Properties:
      ConfigRuleName: dummy
      Source:
        Owner: AWS
        SourceIdentifier: S3_BUCKET_VERSIONING_ENABLED`),
		})
		require.NoError(t, err)
	}

	page1, err := client.DescribeOrganizationConformancePacks(
		ctx, &configservicesdk.DescribeOrganizationConformancePacksInput{Limit: 1},
	)
	require.NoError(t, err)
	require.Len(t, page1.OrganizationConformancePacks, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.DescribeOrganizationConformancePacks(
		ctx, &configservicesdk.DescribeOrganizationConformancePacksInput{
			Limit: 1, NextToken: page1.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.OrganizationConformancePacks, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))

	statusPage1, err := client.DescribeOrganizationConformancePackStatuses(
		ctx, &configservicesdk.DescribeOrganizationConformancePackStatusesInput{Limit: 1},
	)
	require.NoError(t, err)
	require.Len(t, statusPage1.OrganizationConformancePackStatuses, 1)
	require.NotEmpty(t, aws.ToString(statusPage1.NextToken))
}

func testReqField3AggregateComplianceByConfigRulesPagination(t *testing.T) {
	t.Helper()

	backend, client := newReqField3Backend(t)
	ctx := t.Context()

	require.NoError(t, backend.PutConfigurationAggregator("agg-rules", nil, nil, nil))
	putReqField3ConfigRule(t, backend, "agg-rule-a")
	putReqField3ConfigRule(t, backend, "agg-rule-b")
	require.NoError(t, backend.PutEvaluations([]awsconfig.EvaluationResult{
		{
			ConfigRuleName: "agg-rule-a", ComplianceType: "COMPLIANT",
			ResourceType: "AWS::S3::Bucket", ResourceID: "b1",
		},
		{
			ConfigRuleName: "agg-rule-b", ComplianceType: "COMPLIANT",
			ResourceType: "AWS::S3::Bucket", ResourceID: "b2",
		},
	}))

	page1, err := client.DescribeAggregateComplianceByConfigRules(
		ctx, &configservicesdk.DescribeAggregateComplianceByConfigRulesInput{
			ConfigurationAggregatorName: aws.String("agg-rules"), Limit: 1,
		},
	)
	require.NoError(t, err)
	require.Len(t, page1.AggregateComplianceByConfigRules, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.DescribeAggregateComplianceByConfigRules(
		ctx, &configservicesdk.DescribeAggregateComplianceByConfigRulesInput{
			ConfigurationAggregatorName: aws.String("agg-rules"), Limit: 1, NextToken: page1.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.AggregateComplianceByConfigRules, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func testReqField3AggregateComplianceByConformancePacksPagination(t *testing.T) {
	t.Helper()

	backend, client := newReqField3Backend(t)
	ctx := t.Context()

	require.NoError(t, backend.PutConfigurationAggregator("agg-packs", nil, nil, nil))
	require.NoError(t, backend.PutConformancePack("pack-a", "", "", "", "", "", nil))
	require.NoError(t, backend.PutConformancePack("pack-b", "", "", "", "", "", nil))

	page1, err := client.DescribeAggregateComplianceByConformancePacks(
		ctx, &configservicesdk.DescribeAggregateComplianceByConformancePacksInput{
			ConfigurationAggregatorName: aws.String("agg-packs"), Limit: 1,
		},
	)
	require.NoError(t, err)
	require.Len(t, page1.AggregateComplianceByConformancePacks, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.DescribeAggregateComplianceByConformancePacks(
		ctx, &configservicesdk.DescribeAggregateComplianceByConformancePacksInput{
			ConfigurationAggregatorName: aws.String("agg-packs"), Limit: 1, NextToken: page1.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.AggregateComplianceByConformancePacks, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func testReqField3ConformancePackComplianceDetailsPagination(t *testing.T) {
	t.Helper()

	backend, client := newReqField3Backend(t)
	ctx := t.Context()

	template := `Resources:
  Rule1:
    Type: AWS::Config::ConfigRule
    Properties:
      ConfigRuleName: pack-details-rule
      Source:
        Owner: AWS
        SourceIdentifier: S3_BUCKET_VERSIONING_ENABLED`

	_, err := client.PutConformancePack(ctx, &configservicesdk.PutConformancePackInput{
		ConformancePackName: aws.String("pack-details"),
		TemplateBody:        aws.String(template),
	})
	require.NoError(t, err)

	require.NoError(t, backend.PutEvaluations([]awsconfig.EvaluationResult{
		{
			ConfigRuleName: "pack-details-rule", ComplianceType: "NON_COMPLIANT",
			ResourceType: "AWS::S3::Bucket", ResourceID: "b1",
		},
		{
			ConfigRuleName: "pack-details-rule", ComplianceType: "COMPLIANT",
			ResourceType: "AWS::S3::Bucket", ResourceID: "b2",
		},
	}))

	page1, err := client.GetConformancePackComplianceDetails(
		ctx, &configservicesdk.GetConformancePackComplianceDetailsInput{
			ConformancePackName: aws.String("pack-details"), Limit: 1,
		},
	)
	require.NoError(t, err)
	require.Len(t, page1.ConformancePackRuleEvaluationResults, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.GetConformancePackComplianceDetails(
		ctx, &configservicesdk.GetConformancePackComplianceDetailsInput{
			ConformancePackName: aws.String("pack-details"), Limit: 1, NextToken: page1.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.ConformancePackRuleEvaluationResults, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func testReqField3ConformancePackComplianceScoresSort(t *testing.T) {
	t.Helper()

	backend, client := newReqField3Backend(t)
	ctx := t.Context()

	putReqField3ConfigRule(t, backend, "score-rule-low")
	putReqField3ConfigRule(t, backend, "score-rule-high")
	require.NoError(t, backend.PutConformancePack("score-pack-low", "", "", "", "", "", nil))
	require.NoError(t, backend.PutConformancePack("score-pack-high", "", "", "", "", "", nil))

	// score-pack-low has no linked rule (INSUFFICIENT_DATA); score-pack-high
	// links a fully-compliant rule (100.0), forcing distinct sortable scores.
	template := `Resources:
  Rule1:
    Type: AWS::Config::ConfigRule
    Properties:
      ConfigRuleName: score-rule-high
      Source:
        Owner: AWS
        SourceIdentifier: S3_BUCKET_VERSIONING_ENABLED`

	_, err := client.PutConformancePack(ctx, &configservicesdk.PutConformancePackInput{
		ConformancePackName: aws.String("score-pack-high"),
		TemplateBody:        aws.String(template),
	})
	require.NoError(t, err)

	require.NoError(t, backend.PutEvaluations([]awsconfig.EvaluationResult{
		{
			ConfigRuleName: "score-rule-high", ComplianceType: "COMPLIANT",
			ResourceType: "AWS::S3::Bucket", ResourceID: "b1",
		},
	}))

	// INSUFFICIENT_DATA (score-pack-low, never evaluated) sorts first when
	// descending and last when ascending, per ListConformancePackComplianceScoresInput's
	// SortOrder doc comment.
	descOut, err := client.ListConformancePackComplianceScores(
		ctx, &configservicesdk.ListConformancePackComplianceScoresInput{
			SortBy: types.SortByScore, SortOrder: types.SortOrderDescending,
		},
	)
	require.NoError(t, err)
	require.Len(t, descOut.ConformancePackComplianceScores, 2)
	assert.Equal(t, "score-pack-low", aws.ToString(descOut.ConformancePackComplianceScores[0].ConformancePackName))

	ascOut, err := client.ListConformancePackComplianceScores(
		ctx, &configservicesdk.ListConformancePackComplianceScoresInput{
			SortBy: types.SortByScore, SortOrder: types.SortOrderAscending,
		},
	)
	require.NoError(t, err)
	require.Len(t, ascOut.ConformancePackComplianceScores, 2)
	assert.Equal(t, "score-pack-high", aws.ToString(ascOut.ConformancePackComplianceScores[0].ConformancePackName))
}

func testReqField3ListAggregateDiscoveredResourcesPagination(t *testing.T) {
	t.Helper()

	backend, client := newReqField3Backend(t)
	ctx := t.Context()

	require.NoError(t, backend.PutConfigurationAggregator("agg-list", nil, nil, nil))
	require.NoError(t, backend.PutResourceConfig("AWS::S3::Bucket", "b1", "{}"))
	require.NoError(t, backend.PutResourceConfig("AWS::S3::Bucket", "b2", "{}"))

	page1, err := client.ListAggregateDiscoveredResources(
		ctx, &configservicesdk.ListAggregateDiscoveredResourcesInput{
			ConfigurationAggregatorName: aws.String("agg-list"),
			ResourceType:                types.ResourceTypeBucket,
			Limit:                       1,
		},
	)
	require.NoError(t, err)
	require.Len(t, page1.ResourceIdentifiers, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.ListAggregateDiscoveredResources(
		ctx, &configservicesdk.ListAggregateDiscoveredResourcesInput{
			ConfigurationAggregatorName: aws.String("agg-list"),
			ResourceType:                types.ResourceTypeBucket,
			Limit:                       1,
			NextToken:                   page1.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.ResourceIdentifiers, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func testReqField3ListDiscoveredResourcesPagination(t *testing.T) {
	t.Helper()

	backend, client := newReqField3Backend(t)
	ctx := t.Context()

	require.NoError(t, backend.PutResourceConfig("AWS::S3::Bucket", "b1", "{}"))
	require.NoError(t, backend.PutResourceConfig("AWS::S3::Bucket", "b2", "{}"))

	page1, err := client.ListDiscoveredResources(
		ctx, &configservicesdk.ListDiscoveredResourcesInput{
			ResourceType: types.ResourceTypeBucket, Limit: 1,
		},
	)
	require.NoError(t, err)
	require.Len(t, page1.ResourceIdentifiers, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.ListDiscoveredResources(
		ctx, &configservicesdk.ListDiscoveredResourcesInput{
			ResourceType: types.ResourceTypeBucket, Limit: 1, NextToken: page1.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.ResourceIdentifiers, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func testReqField3ListResourceEvaluationsPagination(t *testing.T) {
	t.Helper()

	_, client := newReqField3Backend(t)
	ctx := t.Context()

	for _, id := range []string{"eval-r1", "eval-r2"} {
		_, err := client.StartResourceEvaluation(ctx, &configservicesdk.StartResourceEvaluationInput{
			EvaluationMode: types.EvaluationModeProactive,
			ResourceDetails: &types.ResourceDetails{
				ResourceId:            aws.String(id),
				ResourceType:          aws.String("AWS::S3::Bucket"),
				ResourceConfiguration: aws.String("{}"),
			},
		})
		require.NoError(t, err)
	}

	page1, err := client.ListResourceEvaluations(
		ctx, &configservicesdk.ListResourceEvaluationsInput{Limit: 1},
	)
	require.NoError(t, err)
	require.Len(t, page1.ResourceEvaluations, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.ListResourceEvaluations(
		ctx, &configservicesdk.ListResourceEvaluationsInput{Limit: 1, NextToken: page1.NextToken},
	)
	require.NoError(t, err)
	require.Len(t, page2.ResourceEvaluations, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func testReqField3ListTagsForResourcePagination(t *testing.T) {
	t.Helper()

	backend, client := newReqField3Backend(t)
	ctx := t.Context()

	putReqField3ConfigRule(t, backend, "tagged-rule")

	describeOut, err := client.DescribeConfigRules(
		ctx, &configservicesdk.DescribeConfigRulesInput{ConfigRuleNames: []string{"tagged-rule"}},
	)
	require.NoError(t, err)
	require.Len(t, describeOut.ConfigRules, 1)
	ruleArn := aws.ToString(describeOut.ConfigRules[0].ConfigRuleArn)
	require.NotEmpty(t, ruleArn)

	_, err = client.TagResource(ctx, &configservicesdk.TagResourceInput{
		ResourceArn: aws.String(ruleArn),
		Tags: []types.Tag{
			{Key: aws.String("k1"), Value: aws.String("v1")},
			{Key: aws.String("k2"), Value: aws.String("v2")},
		},
	})
	require.NoError(t, err)

	page1, err := client.ListTagsForResource(
		ctx, &configservicesdk.ListTagsForResourceInput{ResourceArn: aws.String(ruleArn), Limit: 1},
	)
	require.NoError(t, err)
	require.Len(t, page1.Tags, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.ListTagsForResource(
		ctx, &configservicesdk.ListTagsForResourceInput{
			ResourceArn: aws.String(ruleArn), Limit: 1, NextToken: page1.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.Tags, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func testReqField3ResourceConfigHistoryOrderAndTimeRange(t *testing.T) {
	t.Helper()

	backend, client := newReqField3Backend(t)
	ctx := t.Context()

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	backend.SetClock(func() time.Time { return base })
	require.NoError(t, backend.PutResourceConfig("AWS::S3::Bucket", "hist-r1", `{"v":1}`))

	backend.SetClock(func() time.Time { return base.Add(time.Hour) })
	require.NoError(t, backend.PutResourceConfig("AWS::S3::Bucket", "hist-r1", `{"v":2}`))

	backend.SetClock(func() time.Time { return base.Add(2 * time.Hour) })
	require.NoError(t, backend.PutResourceConfig("AWS::S3::Bucket", "hist-r1", `{"v":3}`))

	reverseOut, err := client.GetResourceConfigHistory(ctx, &configservicesdk.GetResourceConfigHistoryInput{
		ResourceType: types.ResourceTypeBucket,
		ResourceId:   aws.String("hist-r1"),
	})
	require.NoError(t, err)
	require.Len(t, reverseOut.ConfigurationItems, 3)
	assert.Equal(t, `{"v":3}`, aws.ToString(reverseOut.ConfigurationItems[0].Configuration))
	assert.Equal(t, `{"v":1}`, aws.ToString(reverseOut.ConfigurationItems[2].Configuration))

	forwardOut, err := client.GetResourceConfigHistory(ctx, &configservicesdk.GetResourceConfigHistoryInput{
		ResourceType:       types.ResourceTypeBucket,
		ResourceId:         aws.String("hist-r1"),
		ChronologicalOrder: types.ChronologicalOrderForward,
	})
	require.NoError(t, err)
	require.Len(t, forwardOut.ConfigurationItems, 3)
	assert.Equal(t, `{"v":1}`, aws.ToString(forwardOut.ConfigurationItems[0].Configuration))
	assert.Equal(t, `{"v":3}`, aws.ToString(forwardOut.ConfigurationItems[2].Configuration))

	rangedOut, err := client.GetResourceConfigHistory(ctx, &configservicesdk.GetResourceConfigHistoryInput{
		ResourceType: types.ResourceTypeBucket,
		ResourceId:   aws.String("hist-r1"),
		EarlierTime:  aws.Time(base.Add(30 * time.Minute)),
		LaterTime:    aws.Time(base.Add(90 * time.Minute)),
	})
	require.NoError(t, err)
	require.Len(t, rangedOut.ConfigurationItems, 1)
	assert.Equal(t, `{"v":2}`, aws.ToString(rangedOut.ConfigurationItems[0].Configuration))
}
