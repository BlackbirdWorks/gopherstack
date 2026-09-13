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

func timeNowAWSConfig() time.Time { return time.Now() }

// TestSlice8_AWSConfig_RealClient covers awsconfig's highest-priority typed-
// client-uncovered op families (gopherstack-n3zi slice 8): recorders/
// delivery channels/status, config rules (+compliance/evaluations),
// conformance packs, remediation, aggregators/authorizations, resource
// config history/select, retention, stored queries, organization rules/
// packs, connectors, and tags. Each subtest creates real state through the
// typed aws-sdk-go-v2 configservice client and asserts decoded response
// values.
func TestSlice8_AWSConfig_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testSlice8RecordersDeliveryRealClient, "recorders_delivery"},
		{testSlice8ConfigRulesComplianceRealClient, "config_rules_compliance"},
		{testSlice8ConformancePacksRealClient, "conformance_packs"},
		{testSlice8RemediationRealClient, "remediation"},
		{testSlice8AggregatorsRealClient, "aggregators_authorizations"},
		{testSlice8ResourceConfigRealClient, "resource_config_history_select"},
		{testSlice8RetentionRealClient, "retention"},
		{testSlice8StoredQueriesRealClient, "stored_queries"},
		{testSlice8OrganizationRealClient, "organization_rules_packs"},
		{testSlice8ConnectorsRealClient, "connectors"},
		{testSlice8TagsRealClient, "tags"},
		{testSlice8ResourceEvaluationRealClient, "resource_evaluation"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func newSlice8AWSConfigBackendAndClient(
	t *testing.T,
) (*awsconfig.InMemoryBackend, *configservicesdk.Client) {
	t.Helper()

	backend := awsconfig.NewInMemoryBackendWithMeta("000000000000", "us-east-1")
	client := newTestAWSConfigSDKClient(t, awsconfig.NewHandler(backend))

	return backend, client
}

func testSlice8RecordersDeliveryRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice8AWSConfigBackendAndClient(t)
	ctx := t.Context()

	require.NoError(t, backend.PutConfigurationRecorder(
		"slice8-recorder", "arn:aws:iam::000000000000:role/config-role", nil,
	))
	require.NoError(t, backend.PutDeliveryChannel("slice8-channel", "slice8-bucket", "", "", nil))

	_, err := client.StartConfigurationRecorder(
		ctx,
		&configservicesdk.StartConfigurationRecorderInput{
			ConfigurationRecorderName: aws.String("slice8-recorder"),
		},
	)
	require.NoError(t, err)

	stopOut, err := client.StopConfigurationRecorder(
		ctx,
		&configservicesdk.StopConfigurationRecorderInput{
			ConfigurationRecorderName: aws.String("slice8-recorder"),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, stopOut)

	listOut, err := client.ListConfigurationRecorders(
		ctx,
		&configservicesdk.ListConfigurationRecordersInput{},
	)
	require.NoError(t, err)
	require.Len(t, listOut.ConfigurationRecorderSummaries, 1)
	assert.Equal(t, "slice8-recorder", aws.ToString(listOut.ConfigurationRecorderSummaries[0].Name))

	chOut, err := client.DescribeDeliveryChannels(
		ctx,
		&configservicesdk.DescribeDeliveryChannelsInput{},
	)
	require.NoError(t, err)
	require.Len(t, chOut.DeliveryChannels, 1)
	assert.Equal(t, "slice8-bucket", aws.ToString(chOut.DeliveryChannels[0].S3BucketName))

	statusOut, err := client.DescribeConfigurationRecorderStatus(
		ctx, &configservicesdk.DescribeConfigurationRecorderStatusInput{},
	)
	require.NoError(t, err)
	require.Len(t, statusOut.ConfigurationRecordersStatus, 1)

	_, err = client.DisassociateResourceTypes(ctx, &configservicesdk.DisassociateResourceTypesInput{
		ConfigurationRecorderArn: statusOut.ConfigurationRecordersStatus[0].Arn,
		ResourceTypes:            []types.ResourceType{types.ResourceTypeInstance},
	})
	require.Error(
		t,
		err,
		"recorder has no recording group / all-supported, so no resource types to disassociate",
	)

	_, err = client.DeleteDeliveryChannel(ctx, &configservicesdk.DeleteDeliveryChannelInput{
		DeliveryChannelName: aws.String("slice8-channel"),
	})
	require.NoError(t, err)

	_, err = client.DeleteConfigurationRecorder(
		ctx,
		&configservicesdk.DeleteConfigurationRecorderInput{
			ConfigurationRecorderName: aws.String("slice8-recorder"),
		},
	)
	require.NoError(t, err)
}

func testSlice8ConfigRulesComplianceRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8AWSConfigBackendAndClient(t)
	ctx := t.Context()

	_, err := client.PutConfigRule(ctx, &configservicesdk.PutConfigRuleInput{
		ConfigRule: &types.ConfigRule{
			ConfigRuleName: aws.String("slice8-rule"),
			Source: &types.Source{
				Owner: types.OwnerCustomLambda,
				SourceIdentifier: aws.String(
					"arn:aws:lambda:us-east-1:000000000000:function:slice8",
				),
			},
			Description: aws.String("slice8 config rule"),
		},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeConfigRules(ctx, &configservicesdk.DescribeConfigRulesInput{
		ConfigRuleNames: []string{"slice8-rule"},
	})
	require.NoError(t, err)
	require.Len(t, descOut.ConfigRules, 1)
	assert.Equal(t, "slice8 config rule", aws.ToString(descOut.ConfigRules[0].Description))

	_, err = client.PutEvaluations(ctx, &configservicesdk.PutEvaluationsInput{
		ResultToken: aws.String("slice8-rule"),
		Evaluations: []types.Evaluation{
			{
				ComplianceResourceType: aws.String("AWS::EC2::Instance"),
				ComplianceResourceId:   aws.String("i-slice8"),
				ComplianceType:         types.ComplianceTypeCompliant,
				OrderingTimestamp:      aws.Time(timeNowAWSConfig()),
			},
		},
	})
	require.NoError(t, err)

	evalStatusOut, err := client.DescribeConfigRuleEvaluationStatus(
		ctx,
		&configservicesdk.DescribeConfigRuleEvaluationStatusInput{
			ConfigRuleNames: []string{"slice8-rule"},
		},
	)
	require.NoError(t, err)
	require.Len(t, evalStatusOut.ConfigRulesEvaluationStatus, 1)

	compByRule, err := client.DescribeComplianceByConfigRule(
		ctx,
		&configservicesdk.DescribeComplianceByConfigRuleInput{
			ConfigRuleNames: []string{"slice8-rule"},
		},
	)
	require.NoError(t, err)
	require.Len(t, compByRule.ComplianceByConfigRules, 1)
	assert.Equal(
		t,
		types.ComplianceTypeCompliant,
		compByRule.ComplianceByConfigRules[0].Compliance.ComplianceType,
	)

	compByResource, err := client.DescribeComplianceByResource(
		ctx,
		&configservicesdk.DescribeComplianceByResourceInput{
			ResourceType: aws.String("AWS::EC2::Instance"),
		},
	)
	require.NoError(t, err)
	require.Len(t, compByResource.ComplianceByResources, 1)

	detailsByRule, err := client.GetComplianceDetailsByConfigRule(
		ctx,
		&configservicesdk.GetComplianceDetailsByConfigRuleInput{
			ConfigRuleName: aws.String("slice8-rule"),
		},
	)
	require.NoError(t, err)
	require.Len(t, detailsByRule.EvaluationResults, 1)

	detailsByResource, err := client.GetComplianceDetailsByResource(
		ctx, &configservicesdk.GetComplianceDetailsByResourceInput{
			ResourceType: aws.String("AWS::EC2::Instance"),
			ResourceId:   aws.String("i-slice8"),
		},
	)
	require.NoError(t, err)
	require.Len(t, detailsByResource.EvaluationResults, 1)

	summaryByType, err := client.GetComplianceSummaryByResourceType(
		ctx,
		&configservicesdk.GetComplianceSummaryByResourceTypeInput{
			ResourceTypes: []string{"AWS::EC2::Instance"},
		},
	)
	require.NoError(t, err)
	require.Len(t, summaryByType.ComplianceSummariesByResourceType, 1)

	_, err = client.PutExternalEvaluation(ctx, &configservicesdk.PutExternalEvaluationInput{
		ConfigRuleName: aws.String("slice8-rule"),
		ExternalEvaluation: &types.ExternalEvaluation{
			ComplianceResourceType: aws.String("AWS::EC2::Instance"),
			ComplianceResourceId:   aws.String("i-slice8-ext"),
			ComplianceType:         types.ComplianceTypeNonCompliant,
			OrderingTimestamp:      aws.Time(timeNowAWSConfig()),
		},
	})
	require.NoError(t, err)

	_, err = client.StartConfigRulesEvaluation(
		ctx,
		&configservicesdk.StartConfigRulesEvaluationInput{ConfigRuleNames: []string{"slice8-rule"}},
	)
	require.NoError(t, err)

	_, err = client.GetCustomRulePolicy(
		ctx, &configservicesdk.GetCustomRulePolicyInput{ConfigRuleName: aws.String("slice8-rule")},
	)
	require.NoError(t, err)

	_, err = client.DeleteEvaluationResults(
		ctx,
		&configservicesdk.DeleteEvaluationResultsInput{ConfigRuleName: aws.String("slice8-rule")},
	)
	require.NoError(t, err)

	_, err = client.DeleteConfigRule(
		ctx, &configservicesdk.DeleteConfigRuleInput{ConfigRuleName: aws.String("slice8-rule")},
	)
	require.NoError(t, err)
}

func testSlice8ConformancePacksRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8AWSConfigBackendAndClient(t)
	ctx := t.Context()

	template := `Resources:
  Rule1:
    Type: AWS::Config::ConfigRule
    Properties:
      ConfigRuleName: slice8-cp-rule
      Source:
        Owner: AWS
        SourceIdentifier: S3_BUCKET_PUBLIC_READ_PROHIBITED`

	_, err := client.PutConformancePack(ctx, &configservicesdk.PutConformancePackInput{
		ConformancePackName: aws.String("slice8-pack"),
		TemplateBody:        aws.String(template),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeConformancePacks(
		ctx,
		&configservicesdk.DescribeConformancePacksInput{
			ConformancePackNames: []string{"slice8-pack"},
		},
	)
	require.NoError(t, err)
	require.Len(t, descOut.ConformancePackDetails, 1)
	assert.Equal(
		t,
		"slice8-pack",
		aws.ToString(descOut.ConformancePackDetails[0].ConformancePackName),
	)

	complianceOut, err := client.GetConformancePackComplianceDetails(
		ctx,
		&configservicesdk.GetConformancePackComplianceDetailsInput{
			ConformancePackName: aws.String("slice8-pack"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice8-pack", aws.ToString(complianceOut.ConformancePackName))

	summaryOut, err := client.GetConformancePackComplianceSummary(
		ctx,
		&configservicesdk.GetConformancePackComplianceSummaryInput{
			ConformancePackNames: []string{"slice8-pack"},
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, summaryOut)

	scoresOut, err := client.ListConformancePackComplianceScores(
		ctx, &configservicesdk.ListConformancePackComplianceScoresInput{},
	)
	require.NoError(t, err)
	assert.NotNil(t, scoresOut)

	_, err = client.DeleteConformancePack(
		ctx,
		&configservicesdk.DeleteConformancePackInput{
			ConformancePackName: aws.String("slice8-pack"),
		},
	)
	require.NoError(t, err)
}

func testSlice8RemediationRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice8AWSConfigBackendAndClient(t)
	ctx := t.Context()

	require.NoError(t, backend.PutConfigRule(&awsconfig.ConfigRule{
		ConfigRuleName: "slice8-remediation-rule",
		Source: &awsconfig.ConfigRuleSource{
			Owner:            "AWS",
			SourceIdentifier: "S3_BUCKET_VERSIONING_ENABLED",
		},
	}))

	_, err := client.PutRemediationConfigurations(
		ctx,
		&configservicesdk.PutRemediationConfigurationsInput{
			RemediationConfigurations: []types.RemediationConfiguration{
				{
					ConfigRuleName: aws.String("slice8-remediation-rule"),
					TargetType:     types.RemediationTargetTypeSsmDocument,
					TargetId:       aws.String("AWS-EnableS3BucketEncryption"),
				},
			},
		},
	)
	require.NoError(t, err)

	descOut, err := client.DescribeRemediationConfigurations(
		ctx,
		&configservicesdk.DescribeRemediationConfigurationsInput{
			ConfigRuleNames: []string{"slice8-remediation-rule"},
		},
	)
	require.NoError(t, err)
	require.Len(t, descOut.RemediationConfigurations, 1)
	assert.Equal(
		t,
		"AWS-EnableS3BucketEncryption",
		aws.ToString(descOut.RemediationConfigurations[0].TargetId),
	)

	_, err = client.PutRemediationExceptions(ctx, &configservicesdk.PutRemediationExceptionsInput{
		ConfigRuleName: aws.String("slice8-remediation-rule"),
		ResourceKeys: []types.RemediationExceptionResourceKey{
			{ResourceType: aws.String("AWS::S3::Bucket"), ResourceId: aws.String("slice8-bucket")},
		},
	})
	require.NoError(t, err)

	excOut, err := client.DescribeRemediationExceptions(
		ctx,
		&configservicesdk.DescribeRemediationExceptionsInput{
			ConfigRuleName: aws.String("slice8-remediation-rule"),
		},
	)
	require.NoError(t, err)
	require.Len(t, excOut.RemediationExceptions, 1)

	_, err = client.StartRemediationExecution(ctx, &configservicesdk.StartRemediationExecutionInput{
		ConfigRuleName: aws.String("slice8-remediation-rule"),
		ResourceKeys: []types.ResourceKey{
			{ResourceType: types.ResourceTypeBucket, ResourceId: aws.String("slice8-bucket")},
		},
	})
	require.NoError(t, err)

	statusOut, err := client.DescribeRemediationExecutionStatus(
		ctx,
		&configservicesdk.DescribeRemediationExecutionStatusInput{
			ConfigRuleName: aws.String("slice8-remediation-rule"),
		},
	)
	require.NoError(t, err)
	require.Len(t, statusOut.RemediationExecutionStatuses, 1)

	_, err = client.DeleteRemediationExceptions(
		ctx,
		&configservicesdk.DeleteRemediationExceptionsInput{
			ConfigRuleName: aws.String("slice8-remediation-rule"),
			ResourceKeys: []types.RemediationExceptionResourceKey{
				{
					ResourceType: aws.String("AWS::S3::Bucket"),
					ResourceId:   aws.String("slice8-bucket"),
				},
			},
		},
	)
	require.NoError(t, err)
}

func testSlice8AggregatorsRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8AWSConfigBackendAndClient(t)
	ctx := t.Context()

	_, err := client.PutAggregationAuthorization(
		ctx,
		&configservicesdk.PutAggregationAuthorizationInput{
			AuthorizedAccountId: aws.String("111111111111"),
			AuthorizedAwsRegion: aws.String("us-west-2"),
		},
	)
	require.NoError(t, err)

	_, err = client.PutConfigurationAggregator(
		ctx,
		&configservicesdk.PutConfigurationAggregatorInput{
			ConfigurationAggregatorName: aws.String("slice8-aggregator"),
			AccountAggregationSources: []types.AccountAggregationSource{
				{AccountIds: []string{"111111111111"}, AllAwsRegions: true},
			},
		},
	)
	require.NoError(t, err)

	descOut, err := client.DescribeConfigurationAggregators(
		ctx, &configservicesdk.DescribeConfigurationAggregatorsInput{},
	)
	require.NoError(t, err)
	require.Len(t, descOut.ConfigurationAggregators, 1)
	assert.Equal(
		t,
		"slice8-aggregator",
		aws.ToString(descOut.ConfigurationAggregators[0].ConfigurationAggregatorName),
	)

	sourcesStatusOut, err := client.DescribeConfigurationAggregatorSourcesStatus(
		ctx, &configservicesdk.DescribeConfigurationAggregatorSourcesStatusInput{
			ConfigurationAggregatorName: aws.String("slice8-aggregator"),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, sourcesStatusOut)

	pendingOut, err := client.DescribePendingAggregationRequests(
		ctx, &configservicesdk.DescribePendingAggregationRequestsInput{},
	)
	require.NoError(t, err)
	assert.Empty(t, pendingOut.PendingAggregationRequests)

	_, err = client.DeletePendingAggregationRequest(
		ctx,
		&configservicesdk.DeletePendingAggregationRequestInput{
			RequesterAccountId: aws.String("111111111111"),
			RequesterAwsRegion: aws.String("us-west-2"),
		},
	)
	require.NoError(t, err)

	_, err = client.DeleteConfigurationAggregator(
		ctx,
		&configservicesdk.DeleteConfigurationAggregatorInput{
			ConfigurationAggregatorName: aws.String("slice8-aggregator"),
		},
	)
	require.NoError(t, err)

	_, err = client.DeleteAggregationAuthorization(
		ctx,
		&configservicesdk.DeleteAggregationAuthorizationInput{
			AuthorizedAccountId: aws.String("111111111111"),
			AuthorizedAwsRegion: aws.String("us-west-2"),
		},
	)
	require.NoError(t, err)
}

func testSlice8ResourceConfigRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice8AWSConfigBackendAndClient(t)
	ctx := t.Context()

	require.NoError(
		t,
		backend.PutResourceConfig("AWS::EC2::Instance", "i-slice8res", `{"foo":"bar"}`),
	)

	_, err := client.DeleteResourceConfig(ctx, &configservicesdk.DeleteResourceConfigInput{
		ResourceType: aws.String("AWS::EC2::Instance"),
		ResourceId:   aws.String("i-slice8res"),
	})
	require.NoError(t, err)

	require.NoError(
		t,
		backend.PutResourceConfig("AWS::EC2::Instance", "i-slice8sel", `{"foo":"bar"}`),
	)

	selOut, err := client.SelectResourceConfig(ctx, &configservicesdk.SelectResourceConfigInput{
		Expression: aws.String("SELECT resourceId WHERE resourceType = 'AWS::EC2::Instance'"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, selOut.Results)

	require.NoError(t, backend.PutConfigurationAggregator(
		"slice8-sel-agg",
		[]awsconfig.AccountAggregationSource{
			{AccountIDs: []string{"000000000000"}, AllAwsRegions: true},
		},
		nil,
		nil,
	))

	selAggOut, err := client.SelectAggregateResourceConfig(
		ctx,
		&configservicesdk.SelectAggregateResourceConfigInput{
			Expression: aws.String(
				"SELECT resourceId WHERE resourceType = 'AWS::EC2::Instance'",
			),
			ConfigurationAggregatorName: aws.String("slice8-sel-agg"),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, selAggOut)

	countsOut, err := client.GetAggregateDiscoveredResourceCounts(
		ctx, &configservicesdk.GetAggregateDiscoveredResourceCountsInput{
			ConfigurationAggregatorName: aws.String("slice8-sel-agg"),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, countsOut)

	listAggOut, err := client.ListAggregateDiscoveredResources(
		ctx, &configservicesdk.ListAggregateDiscoveredResourcesInput{
			ConfigurationAggregatorName: aws.String("slice8-sel-agg"),
			ResourceType:                types.ResourceTypeInstance,
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, listAggOut)
}

func testSlice8RetentionRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8AWSConfigBackendAndClient(t)
	ctx := t.Context()

	_, err := client.PutRetentionConfiguration(
		ctx,
		&configservicesdk.PutRetentionConfigurationInput{
			RetentionPeriodInDays: aws.Int32(2557),
		},
	)
	require.NoError(t, err)

	descOut, err := client.DescribeRetentionConfigurations(
		ctx, &configservicesdk.DescribeRetentionConfigurationsInput{},
	)
	require.NoError(t, err)
	require.Len(t, descOut.RetentionConfigurations, 1)
	assert.Equal(
		t,
		int32(2557),
		aws.ToInt32(descOut.RetentionConfigurations[0].RetentionPeriodInDays),
	)

	_, err = client.DeleteRetentionConfiguration(
		ctx,
		&configservicesdk.DeleteRetentionConfigurationInput{
			RetentionConfigurationName: descOut.RetentionConfigurations[0].Name,
		},
	)
	require.NoError(t, err)
}

func testSlice8StoredQueriesRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8AWSConfigBackendAndClient(t)
	ctx := t.Context()

	putOut, err := client.PutStoredQuery(ctx, &configservicesdk.PutStoredQueryInput{
		StoredQuery: &types.StoredQuery{
			QueryName:   aws.String("slice8-query"),
			Expression:  aws.String("SELECT resourceId WHERE resourceType = 'AWS::EC2::Instance'"),
			Description: aws.String("slice8"),
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(putOut.QueryArn))

	getOut, err := client.GetStoredQuery(
		ctx,
		&configservicesdk.GetStoredQueryInput{QueryName: aws.String("slice8-query")},
	)
	require.NoError(t, err)
	require.NotNil(t, getOut.StoredQuery)
	assert.Equal(t, "slice8", aws.ToString(getOut.StoredQuery.Description))

	listOut, err := client.ListStoredQueries(ctx, &configservicesdk.ListStoredQueriesInput{})
	require.NoError(t, err)
	require.Len(t, listOut.StoredQueryMetadata, 1)

	_, err = client.DeleteStoredQuery(
		ctx,
		&configservicesdk.DeleteStoredQueryInput{QueryName: aws.String("slice8-query")},
	)
	require.NoError(t, err)
}

func testSlice8OrganizationRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8AWSConfigBackendAndClient(t)
	ctx := t.Context()

	putRuleOut, err := client.PutOrganizationConfigRule(
		ctx,
		&configservicesdk.PutOrganizationConfigRuleInput{
			OrganizationConfigRuleName: aws.String("slice8-org-rule"),
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(putRuleOut.OrganizationConfigRuleArn))

	statusOut, err := client.DescribeOrganizationConfigRuleStatuses(
		ctx, &configservicesdk.DescribeOrganizationConfigRuleStatusesInput{
			OrganizationConfigRuleNames: []string{"slice8-org-rule"},
		},
	)
	require.NoError(t, err)
	require.Len(t, statusOut.OrganizationConfigRuleStatuses, 1)

	detailedOut, err := client.GetOrganizationConfigRuleDetailedStatus(
		ctx, &configservicesdk.GetOrganizationConfigRuleDetailedStatusInput{
			OrganizationConfigRuleName: aws.String("slice8-org-rule"),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, detailedOut)

	_, err = client.GetOrganizationCustomRulePolicy(
		ctx,
		&configservicesdk.GetOrganizationCustomRulePolicyInput{
			OrganizationConfigRuleName: aws.String("slice8-org-rule"),
		},
	)
	require.NoError(t, err)

	_, err = client.DeleteOrganizationConfigRule(
		ctx,
		&configservicesdk.DeleteOrganizationConfigRuleInput{
			OrganizationConfigRuleName: aws.String("slice8-org-rule"),
		},
	)
	require.NoError(t, err)

	_, err = client.PutOrganizationConformancePack(
		ctx,
		&configservicesdk.PutOrganizationConformancePackInput{
			OrganizationConformancePackName: aws.String("slice8-org-pack"),
		},
	)
	require.NoError(t, err)

	packStatusOut, err := client.DescribeOrganizationConformancePackStatuses(
		ctx, &configservicesdk.DescribeOrganizationConformancePackStatusesInput{
			OrganizationConformancePackNames: []string{"slice8-org-pack"},
		},
	)
	require.NoError(t, err)
	require.Len(t, packStatusOut.OrganizationConformancePackStatuses, 1)

	packDetailedOut, err := client.GetOrganizationConformancePackDetailedStatus(
		ctx, &configservicesdk.GetOrganizationConformancePackDetailedStatusInput{
			OrganizationConformancePackName: aws.String("slice8-org-pack"),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, packDetailedOut)

	_, err = client.DeleteOrganizationConformancePack(
		ctx, &configservicesdk.DeleteOrganizationConformancePackInput{
			OrganizationConformancePackName: aws.String("slice8-org-pack"),
		},
	)
	require.NoError(t, err)
}

func testSlice8ConnectorsRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8AWSConfigBackendAndClient(t)
	ctx := t.Context()

	putOut, err := client.PutConnector(ctx, &configservicesdk.PutConnectorInput{
		ConnectorConfiguration: &types.ConnectorConfiguration{
			Azure: &types.AzureConnectorConfiguration{
				ClientIdentifier: aws.String("client-1"),
				TenantIdentifier: aws.String("tenant-1"),
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(putOut.Arn))

	getOut, err := client.GetConnector(ctx, &configservicesdk.GetConnectorInput{Arn: putOut.Arn})
	require.NoError(t, err)
	require.NotNil(t, getOut.Connector)

	listOut, err := client.ListConnectors(ctx, &configservicesdk.ListConnectorsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.ConnectorSummaries, 1)

	_, err = client.DeleteConnector(ctx, &configservicesdk.DeleteConnectorInput{Arn: putOut.Arn})
	require.NoError(t, err)
}

func testSlice8TagsRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8AWSConfigBackendAndClient(t)
	ctx := t.Context()

	putOut, err := client.PutRetentionConfiguration(
		ctx,
		&configservicesdk.PutRetentionConfigurationInput{
			RetentionPeriodInDays: aws.Int32(30),
		},
	)
	require.NoError(t, err)

	putRuleOut, err := client.PutConfigRule(ctx, &configservicesdk.PutConfigRuleInput{
		ConfigRule: &types.ConfigRule{
			ConfigRuleName: aws.String("slice8-tag-rule"),
			Source: &types.Source{
				Owner: types.OwnerCustomLambda,
				SourceIdentifier: aws.String(
					"arn:aws:lambda:us-east-1:000000000000:function:slice8",
				),
			},
		},
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)
	_ = putOut
	_ = putRuleOut

	descOut, err := client.DescribeConfigRules(ctx, &configservicesdk.DescribeConfigRulesInput{
		ConfigRuleNames: []string{"slice8-tag-rule"},
	})
	require.NoError(t, err)
	require.Len(t, descOut.ConfigRules, 1)
	arn := aws.ToString(descOut.ConfigRules[0].ConfigRuleArn)
	require.NotEmpty(t, arn)

	listTagsOut, err := client.ListTagsForResource(
		ctx,
		&configservicesdk.ListTagsForResourceInput{ResourceArn: aws.String(arn)},
	)
	require.NoError(t, err)
	require.Len(t, listTagsOut.Tags, 1)
	assert.Equal(t, "env", aws.ToString(listTagsOut.Tags[0].Key))

	_, err = client.TagResource(ctx, &configservicesdk.TagResourceInput{
		ResourceArn: aws.String(arn),
		Tags:        []types.Tag{{Key: aws.String("team"), Value: aws.String("gopherstack")}},
	})
	require.NoError(t, err)

	_, err = client.UntagResource(ctx, &configservicesdk.UntagResourceInput{
		ResourceArn: aws.String(arn),
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err)

	listTagsOut2, err := client.ListTagsForResource(
		ctx,
		&configservicesdk.ListTagsForResourceInput{ResourceArn: aws.String(arn)},
	)
	require.NoError(t, err)
	require.Len(t, listTagsOut2.Tags, 1)
	assert.Equal(t, "team", aws.ToString(listTagsOut2.Tags[0].Key))
}

func testSlice8ResourceEvaluationRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8AWSConfigBackendAndClient(t)
	ctx := t.Context()

	startOut, err := client.StartResourceEvaluation(
		ctx,
		&configservicesdk.StartResourceEvaluationInput{
			ResourceDetails: &types.ResourceDetails{
				ResourceType:          aws.String("AWS::EC2::Instance"),
				ResourceId:            aws.String("i-slice8eval"),
				ResourceConfiguration: aws.String(`{"foo":"bar"}`),
			},
			EvaluationMode: types.EvaluationModeDetective,
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(startOut.ResourceEvaluationId))

	summaryOut, err := client.GetResourceEvaluationSummary(
		ctx,
		&configservicesdk.GetResourceEvaluationSummaryInput{
			ResourceEvaluationId: startOut.ResourceEvaluationId,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "i-slice8eval", aws.ToString(summaryOut.ResourceDetails.ResourceId))

	listOut, err := client.ListResourceEvaluations(
		ctx,
		&configservicesdk.ListResourceEvaluationsInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.ResourceEvaluations)
}
