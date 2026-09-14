package awsconfig_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	configservicesdk "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_ServiceLinkedRecordersAndAggregateCompliance covers awsconfig's last seven typed-
// client-uncovered ops (gopherstack-n3zi): the three service-linked
// configuration recorder ops and the four aggregate-compliance read ops.
func TestRealClient_ServiceLinkedRecordersAndAggregateCompliance(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testServiceLinkedRecordersRealClient, "service_linked_recorders"},
		{testThirdPartyServiceLinkedRecorderRealClient, "third_party_service_linked_recorder"},
		{testAggregateComplianceRealClient, "aggregate_compliance"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testServiceLinkedRecordersRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	put, err := client.PutServiceLinkedConfigurationRecorder(
		ctx,
		&configservicesdk.PutServiceLinkedConfigurationRecorderInput{
			ServicePrincipal: aws.String("guardduty.amazonaws.com"),
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(put.Name))
	assert.NotEmpty(t, aws.ToString(put.Arn))

	deleted, err := client.DeleteServiceLinkedConfigurationRecorder(
		ctx,
		&configservicesdk.DeleteServiceLinkedConfigurationRecorderInput{
			ServicePrincipal: aws.String("guardduty.amazonaws.com"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(put.Name), aws.ToString(deleted.Name))
	assert.Equal(t, aws.ToString(put.Arn), aws.ToString(deleted.Arn))
}

func testThirdPartyServiceLinkedRecorderRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	connOut, err := client.PutConnector(ctx, &configservicesdk.PutConnectorInput{
		ConnectorConfiguration: &types.ConnectorConfiguration{
			Azure: &types.AzureConnectorConfiguration{
				ClientIdentifier: aws.String("slice17-client"),
				TenantIdentifier: aws.String("slice17-tenant"),
			},
		},
	})
	require.NoError(t, err)
	connectorARN := aws.ToString(connOut.Arn)
	require.NotEmpty(t, connectorARN)

	put, err := client.PutThirdPartyServiceLinkedConfigurationRecorder(
		ctx,
		&configservicesdk.PutThirdPartyServiceLinkedConfigurationRecorderInput{
			ServicePrincipal: aws.String("azure.config.amazonaws.com"),
			ConnectorArn:     aws.String(connectorARN),
			ScopeConfiguration: &types.ScopeConfiguration{
				ScopeType:  aws.String("tenant"),
				AllRegions: true,
			},
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(put.Name))
	assert.NotEmpty(t, aws.ToString(put.Arn))
}

func testAggregateComplianceRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	require.NoError(t, backend.PutConfigurationAggregator(
		"slice17-aggregator",
		nil,
		nil,
		nil,
	))

	_, err := client.PutConfigRule(ctx, &configservicesdk.PutConfigRuleInput{
		ConfigRule: &types.ConfigRule{
			ConfigRuleName: aws.String("slice17-rule"),
			Source: &types.Source{
				Owner: types.OwnerCustomLambda,
				SourceIdentifier: aws.String(
					"arn:aws:lambda:us-east-1:000000000000:function:slice17",
				),
			},
		},
	})
	require.NoError(t, err)

	_, err = client.PutEvaluations(ctx, &configservicesdk.PutEvaluationsInput{
		ResultToken: aws.String("slice17-rule"),
		Evaluations: []types.Evaluation{
			{
				ComplianceResourceType: aws.String("AWS::EC2::Instance"),
				ComplianceResourceId:   aws.String("i-slice17"),
				ComplianceType:         types.ComplianceTypeNonCompliant,
				OrderingTimestamp:      aws.Time(timeNowAWSConfig()),
			},
		},
	})
	require.NoError(t, err)

	ruleDetails, err := client.GetAggregateComplianceDetailsByConfigRule(
		ctx,
		&configservicesdk.GetAggregateComplianceDetailsByConfigRuleInput{
			ConfigurationAggregatorName: aws.String("slice17-aggregator"),
			ConfigRuleName:              aws.String("slice17-rule"),
			AccountId:                   aws.String("000000000000"),
			AwsRegion:                   aws.String("us-east-1"),
		},
	)
	require.NoError(t, err)
	require.Len(t, ruleDetails.AggregateEvaluationResults, 1)
	assert.Equal(t, types.ComplianceTypeNonCompliant, ruleDetails.AggregateEvaluationResults[0].ComplianceType)
	assert.Equal(t, "000000000000", aws.ToString(ruleDetails.AggregateEvaluationResults[0].AccountId))

	describeByRule, err := client.DescribeAggregateComplianceByConfigRules(
		ctx,
		&configservicesdk.DescribeAggregateComplianceByConfigRulesInput{
			ConfigurationAggregatorName: aws.String("slice17-aggregator"),
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, describeByRule.AggregateComplianceByConfigRules)

	template := `Resources:
  Rule1:
    Type: AWS::Config::ConfigRule
    Properties:
      ConfigRuleName: slice17-pack-rule
      Source:
        Owner: AWS
        SourceIdentifier: S3_BUCKET_PUBLIC_READ_PROHIBITED`

	_, err = client.PutConformancePack(ctx, &configservicesdk.PutConformancePackInput{
		ConformancePackName: aws.String("slice17-pack"),
		TemplateBody:        aws.String(template),
	})
	require.NoError(t, err)

	packsByAgg, err := client.DescribeAggregateComplianceByConformancePacks(
		ctx,
		&configservicesdk.DescribeAggregateComplianceByConformancePacksInput{
			ConfigurationAggregatorName: aws.String("slice17-aggregator"),
		},
	)
	require.NoError(t, err)
	require.Len(t, packsByAgg.AggregateComplianceByConformancePacks, 1)
	assert.Equal(
		t,
		"slice17-pack",
		aws.ToString(packsByAgg.AggregateComplianceByConformancePacks[0].ConformancePackName),
	)

	packSummary, err := client.GetAggregateConformancePackComplianceSummary(
		ctx,
		&configservicesdk.GetAggregateConformancePackComplianceSummaryInput{
			ConfigurationAggregatorName: aws.String("slice17-aggregator"),
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, packSummary.AggregateConformancePackComplianceSummaries)
	assert.Equal(
		t,
		"000000000000",
		aws.ToString(packSummary.AggregateConformancePackComplianceSummaries[0].GroupName),
	)
}
