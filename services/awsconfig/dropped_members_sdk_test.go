package awsconfig_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	configservicesdk "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDK_DroppedMembersApplied(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, c *configservicesdk.Client)
		name string
	}{
		{
			name: "resource_name_and_tags",
			run: func(t *testing.T, c *configservicesdk.Client) {
				t.Helper()

				_, err := c.PutResourceConfig(t.Context(), &configservicesdk.PutResourceConfigInput{
					ResourceType: aws.String("AWS::EC2::Instance"), ResourceId: aws.String("i-1"),
					ResourceName: aws.String("web"), Configuration: aws.String(`{}`),
					SchemaVersionId: aws.String("1.0"), Tags: map[string]string{"env": "dev"},
				})
				require.NoError(t, err)
				putResources(t, c, "AWS::EC2::Instance", "i-2")

				out, err := c.ListDiscoveredResources(t.Context(), &configservicesdk.ListDiscoveredResourcesInput{
					ResourceType: types.ResourceType("AWS::EC2::Instance"), ResourceName: aws.String("web"),
				})
				require.NoError(t, err)
				require.Len(t, out.ResourceIdentifiers, 1)
				assert.Equal(t, "i-1", aws.ToString(out.ResourceIdentifiers[0].ResourceId))
				assert.Equal(t, "web", aws.ToString(out.ResourceIdentifiers[0].ResourceName))

				hist, err := c.GetResourceConfigHistory(t.Context(), &configservicesdk.GetResourceConfigHistoryInput{
					ResourceType: types.ResourceType("AWS::EC2::Instance"), ResourceId: aws.String("i-1"),
				})
				require.NoError(t, err)
				require.Len(t, hist.ConfigurationItems, 1)
				assert.Equal(t, map[string]string{"env": "dev"}, hist.ConfigurationItems[0].Tags)
			},
		},
		{
			name: "start_rules_evaluation_names",
			run: func(t *testing.T, c *configservicesdk.Client) {
				t.Helper()

				_, err := c.StartConfigRulesEvaluation(t.Context(), &configservicesdk.StartConfigRulesEvaluationInput{
					ConfigRuleNames: []string{"missing"},
				})
				var nf *types.NoSuchConfigRuleException
				require.ErrorAs(t, err, &nf)

				_, err = c.StartConfigRulesEvaluation(t.Context(), &configservicesdk.StartConfigRulesEvaluationInput{})
				require.NoError(t, err)
			},
		},
		{
			name: "organization_conformance_pack",
			run: func(t *testing.T, c *configservicesdk.Client) {
				t.Helper()

				put, err := c.PutOrganizationConformancePack(
					t.Context(), &configservicesdk.PutOrganizationConformancePackInput{
						OrganizationConformancePackName: aws.String("op"),
						DeliveryS3Bucket:                aws.String("awsconfigconforms-bkt"),
						DeliveryS3KeyPrefix:             aws.String("pfx"),
						ExcludedAccounts:                []string{"111111111111"},
						ConformancePackInputParameters: []types.ConformancePackInputParameter{
							{ParameterName: aws.String("p"), ParameterValue: aws.String("v")},
						},
						Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
					})
				require.NoError(t, err)
				require.NotEmpty(t, aws.ToString(put.OrganizationConformancePackArn))

				out, err := c.DescribeOrganizationConformancePacks(
					t.Context(), &configservicesdk.DescribeOrganizationConformancePacksInput{},
				)
				require.NoError(t, err)
				require.Len(t, out.OrganizationConformancePacks, 1)

				p := out.OrganizationConformancePacks[0]
				assert.Equal(t,
					aws.ToString(put.OrganizationConformancePackArn), aws.ToString(p.OrganizationConformancePackArn))
				assert.Equal(t, "awsconfigconforms-bkt", aws.ToString(p.DeliveryS3Bucket))
				assert.Equal(t, "pfx", aws.ToString(p.DeliveryS3KeyPrefix))
				assert.Equal(t, []string{"111111111111"}, p.ExcludedAccounts)
				require.Len(t, p.ConformancePackInputParameters, 1)
				assert.NotNil(t, p.LastUpdateTime)

				tags, err := c.ListTagsForResource(t.Context(), &configservicesdk.ListTagsForResourceInput{
					ResourceArn: put.OrganizationConformancePackArn,
				})
				require.NoError(t, err)
				require.Len(t, tags.Tags, 1)
			},
		},
		{
			name: "conformance_pack_arn_and_parameters",
			run: func(t *testing.T, c *configservicesdk.Client) {
				t.Helper()

				put, err := c.PutConformancePack(t.Context(), &configservicesdk.PutConformancePackInput{
					ConformancePackName: aws.String("cp"),
					TemplateBody:        aws.String("Resources: {}"),
					ConformancePackInputParameters: []types.ConformancePackInputParameter{
						{ParameterName: aws.String("p"), ParameterValue: aws.String("v")},
					},
				})
				require.NoError(t, err)
				require.NotEmpty(t, aws.ToString(put.ConformancePackArn))

				out, err := c.DescribeConformancePacks(t.Context(), &configservicesdk.DescribeConformancePacksInput{})
				require.NoError(t, err)
				require.Len(t, out.ConformancePackDetails, 1)
				detail := out.ConformancePackDetails[0]
				assert.Equal(t, aws.ToString(put.ConformancePackArn), aws.ToString(detail.ConformancePackArn))
				require.Len(t, out.ConformancePackDetails[0].ConformancePackInputParameters, 1)
			},
		},
		{
			name: "organization_custom_policy_rule",
			run: func(t *testing.T, c *configservicesdk.Client) {
				t.Helper()

				put, err := c.PutOrganizationConfigRule(t.Context(), &configservicesdk.PutOrganizationConfigRuleInput{
					OrganizationConfigRuleName: aws.String("ocr"),
					OrganizationCustomPolicyRuleMetadata: &types.OrganizationCustomPolicyRuleMetadata{
						PolicyRuntime: aws.String("guard-2.x.x"), PolicyText: aws.String("rule x {}"),
						OrganizationConfigRuleTriggerTypes: []types.OrganizationConfigRuleTriggerTypeNoSN{
							types.OrganizationConfigRuleTriggerTypeNoSNConfigurationItemChangeNotification,
						},
					},
					Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
				})
				require.NoError(t, err)

				out, err := c.DescribeOrganizationConfigRules(
					t.Context(), &configservicesdk.DescribeOrganizationConfigRulesInput{},
				)
				require.NoError(t, err)
				require.Len(t, out.OrganizationConfigRules, 1)

				m := out.OrganizationConfigRules[0].OrganizationCustomPolicyRuleMetadata
				require.NotNil(t, m)
				assert.Equal(t, "guard-2.x.x", aws.ToString(m.PolicyRuntime))

				pol, err := c.GetOrganizationCustomRulePolicy(
					t.Context(), &configservicesdk.GetOrganizationCustomRulePolicyInput{
						OrganizationConfigRuleName: aws.String("ocr"),
					})
				require.NoError(t, err)
				assert.Equal(t, "rule x {}", aws.ToString(pol.PolicyText))

				tags, err := c.ListTagsForResource(t.Context(), &configservicesdk.ListTagsForResourceInput{
					ResourceArn: put.OrganizationConfigRuleArn,
				})
				require.NoError(t, err)
				require.Len(t, tags.Tags, 1)
			},
		},
		{
			name: "recorder_tags_and_filters",
			run: func(t *testing.T, c *configservicesdk.Client) {
				t.Helper()

				_, err := c.PutConfigurationRecorder(t.Context(), &configservicesdk.PutConfigurationRecorderInput{
					ConfigurationRecorder: &types.ConfigurationRecorder{
						Name: aws.String("rec"), RoleARN: aws.String("arn:aws:iam::000000000000:role/r"),
					},
					Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
				})
				require.NoError(t, err)

				sl, err := c.PutServiceLinkedConfigurationRecorder(
					t.Context(), &configservicesdk.PutServiceLinkedConfigurationRecorderInput{
						ServicePrincipal: aws.String("svc.amazonaws.com"),
					})
				require.NoError(t, err)

				recs, err := c.DescribeConfigurationRecorders(
					t.Context(), &configservicesdk.DescribeConfigurationRecordersInput{Arn: sl.Arn},
				)
				require.NoError(t, err)
				require.Len(t, recs.ConfigurationRecorders, 1)
				assert.Equal(t, aws.ToString(sl.Name), aws.ToString(recs.ConfigurationRecorders[0].Name))

				st, err := c.DescribeConfigurationRecorderStatus(
					t.Context(),
					&configservicesdk.DescribeConfigurationRecorderStatusInput{
						ServicePrincipal: aws.String("svc.amazonaws.com"),
					},
				)
				require.NoError(t, err)
				require.Len(t, st.ConfigurationRecordersStatus, 1)
				assert.Equal(t, aws.ToString(sl.Arn), aws.ToString(st.ConfigurationRecordersStatus[0].Arn))

				none, err := c.DescribeConfigurationRecorders(
					t.Context(),
					&configservicesdk.DescribeConfigurationRecordersInput{
						ServicePrincipal: aws.String("other.amazonaws.com"),
					},
				)
				require.NoError(t, err)
				assert.Empty(t, none.ConfigurationRecorders)

				all, err := c.DescribeConfigurationRecorders(
					t.Context(),
					&configservicesdk.DescribeConfigurationRecordersInput{},
				)
				require.NoError(t, err)
				require.Len(t, all.ConfigurationRecorders, 2)

				var recArn *string

				for _, r := range all.ConfigurationRecorders {
					if aws.ToString(r.Name) == "rec" {
						recArn = r.Arn
					}
				}

				tags, err := c.ListTagsForResource(
					t.Context(),
					&configservicesdk.ListTagsForResourceInput{ResourceArn: recArn},
				)
				require.NoError(t, err)
				require.Len(t, tags.Tags, 1)
			},
		},
		{
			name: "aggregator_filters",
			run: func(t *testing.T, c *configservicesdk.Client) {
				t.Helper()

				for _, n := range []string{"a1", "a2"} {
					_, err := c.PutConfigurationAggregator(
						t.Context(),
						&configservicesdk.PutConfigurationAggregatorInput{
							ConfigurationAggregatorName: aws.String(n),
							AccountAggregationSources: []types.AccountAggregationSource{
								{AccountIds: []string{"111111111111"}, AllAwsRegions: true},
							},
						},
					)
					require.NoError(t, err)
				}

				out, err := c.DescribeConfigurationAggregators(
					t.Context(),
					&configservicesdk.DescribeConfigurationAggregatorsInput{
						ConfigurationAggregatorNames: []string{"a2"},
					},
				)
				require.NoError(t, err)
				require.Len(t, out.ConfigurationAggregators, 1)
				assert.Equal(t, "a2", aws.ToString(out.ConfigurationAggregators[0].ConfigurationAggregatorName))

				_, err = c.DescribeConfigurationAggregators(
					t.Context(),
					&configservicesdk.DescribeConfigurationAggregatorsInput{
						ConfigurationAggregatorNames: []string{"nope"},
					},
				)
				var nf *types.NoSuchConfigurationAggregatorException
				require.ErrorAs(t, err, &nf)

				failed, err := c.DescribeConfigurationAggregatorSourcesStatus(
					t.Context(), &configservicesdk.DescribeConfigurationAggregatorSourcesStatusInput{
						ConfigurationAggregatorName: aws.String("a1"),
						UpdateStatus: []types.AggregatedSourceStatusType{
							types.AggregatedSourceStatusTypeFailed,
						},
					})
				require.NoError(t, err)
				assert.Empty(t, failed.AggregatedSourceStatusList)

				ok, err := c.DescribeConfigurationAggregatorSourcesStatus(
					t.Context(), &configservicesdk.DescribeConfigurationAggregatorSourcesStatusInput{
						ConfigurationAggregatorName: aws.String("a1"),
						UpdateStatus: []types.AggregatedSourceStatusType{
							types.AggregatedSourceStatusTypeSucceeded,
						},
					})
				require.NoError(t, err)
				assert.NotEmpty(t, ok.AggregatedSourceStatusList)

				_, err = c.DescribeAggregateComplianceByConfigRules(
					t.Context(), &configservicesdk.DescribeAggregateComplianceByConfigRulesInput{
						ConfigurationAggregatorName: aws.String("nope"),
					})
				require.ErrorAs(t, err, &nf)

				_, err = c.SelectAggregateResourceConfig(
					t.Context(),
					&configservicesdk.SelectAggregateResourceConfigInput{
						ConfigurationAggregatorName: aws.String("nope"),
						Expression:                  aws.String("SELECT resourceId"),
					},
				)
				require.ErrorAs(t, err, &nf)
			},
		},
		{
			name: "remediation_exception_meta_and_keys",
			run: func(t *testing.T, c *configservicesdk.Client) {
				t.Helper()

				exp := time.Unix(2000000000, 0)
				_, err := c.PutRemediationExceptions(t.Context(), &configservicesdk.PutRemediationExceptionsInput{
					ConfigRuleName: aws.String("r"),
					ResourceKeys: []types.RemediationExceptionResourceKey{
						{ResourceType: aws.String("AWS::S3::Bucket"), ResourceId: aws.String("b1")},
						{ResourceType: aws.String("AWS::S3::Bucket"), ResourceId: aws.String("b2")},
					},
					Message: aws.String("why"), ExpirationTime: &exp,
				})
				require.NoError(t, err)

				out, err := c.DescribeRemediationExceptions(
					t.Context(),
					&configservicesdk.DescribeRemediationExceptionsInput{
						ConfigRuleName: aws.String("r"),
						ResourceKeys: []types.RemediationExceptionResourceKey{
							{ResourceType: aws.String("AWS::S3::Bucket"), ResourceId: aws.String("b2")},
						},
					},
				)
				require.NoError(t, err)
				require.Len(t, out.RemediationExceptions, 1)
				assert.Equal(t, "b2", aws.ToString(out.RemediationExceptions[0].ResourceId))
				assert.Equal(t, "why", aws.ToString(out.RemediationExceptions[0].Message))
				require.NotNil(t, out.RemediationExceptions[0].ExpirationTime)
				assert.Equal(t, exp.Unix(), out.RemediationExceptions[0].ExpirationTime.Unix())
			},
		},
		{
			name: "compliance_details_by_evaluation_id",
			run: func(t *testing.T, c *configservicesdk.Client) {
				t.Helper()

				_, err := c.GetComplianceDetailsByResource(
					t.Context(),
					&configservicesdk.GetComplianceDetailsByResourceInput{ResourceEvaluationId: aws.String("missing")},
				)
				var bad *types.InvalidParameterValueException
				require.ErrorAs(t, err, &bad)

				start, err := c.StartResourceEvaluation(t.Context(), &configservicesdk.StartResourceEvaluationInput{
					ResourceDetails: &types.ResourceDetails{
						ResourceType: aws.String("AWS::S3::Bucket"), ResourceId: aws.String("b1"),
						ResourceConfiguration: aws.String(`{}`),
					},
					EvaluationMode: types.EvaluationModeDetective,
				})
				require.NoError(t, err)

				_, err = c.GetComplianceDetailsByResource(
					t.Context(),
					&configservicesdk.GetComplianceDetailsByResourceInput{
						ResourceEvaluationId: start.ResourceEvaluationId,
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "resource_evaluation_token_and_context",
			run: func(t *testing.T, c *configservicesdk.Client) {
				t.Helper()

				in := func(token, mode string) *configservicesdk.StartResourceEvaluationInput {
					return &configservicesdk.StartResourceEvaluationInput{
						ResourceDetails: &types.ResourceDetails{
							ResourceType: aws.String("AWS::S3::Bucket"), ResourceId: aws.String("b1"),
							ResourceConfiguration: aws.String(`{}`),
						},
						EvaluationMode:    types.EvaluationMode(mode),
						ClientToken:       aws.String(token),
						EvaluationContext: &types.EvaluationContext{EvaluationContextIdentifier: aws.String("ctx-1")},
					}
				}

				first, err := c.StartResourceEvaluation(t.Context(), in("tok", "DETECTIVE"))
				require.NoError(t, err)

				again, err := c.StartResourceEvaluation(t.Context(), in("tok", "DETECTIVE"))
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(first.ResourceEvaluationId), aws.ToString(again.ResourceEvaluationId))

				_, err = c.StartResourceEvaluation(t.Context(), in("tok", "PROACTIVE"))
				require.ErrorContains(t, err, "IdempotentParameterMismatch")

				sum, err := c.GetResourceEvaluationSummary(
					t.Context(),
					&configservicesdk.GetResourceEvaluationSummaryInput{
						ResourceEvaluationId: first.ResourceEvaluationId,
					},
				)
				require.NoError(t, err)
				require.NotNil(t, sum.EvaluationContext)
				assert.Equal(t, "ctx-1", aws.ToString(sum.EvaluationContext.EvaluationContextIdentifier))

				list, err := c.ListResourceEvaluations(t.Context(), &configservicesdk.ListResourceEvaluationsInput{
					Filters: &types.ResourceEvaluationFilters{EvaluationContextIdentifier: aws.String("other")},
				})
				require.NoError(t, err)
				assert.Empty(t, list.ResourceEvaluations)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newOpenItemsClient(t)
			tt.run(t, client)
		})
	}
}
