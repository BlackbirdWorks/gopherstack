package awsconfig_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	configservicesdk "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

// TestListOps_PageAndRejectBadTokens covers Limit/MaxResults + NextToken on the previously ignored ops.
func TestListOps_PageAndRejectBadTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		list func(ctx context.Context, c *configservicesdk.Client, size int32, tok *string) (int, *string, error)
		name string
	}{
		{
			name: "conformance_packs",
			list: func(ctx context.Context, c *configservicesdk.Client, sz int32, tok *string) (int, *string, error) {
				out, err := c.DescribeConformancePacks(
					ctx,
					&configservicesdk.DescribeConformancePacksInput{Limit: sz, NextToken: tok},
				)
				if err != nil {
					return 0, nil, err
				}

				return len(out.ConformancePackDetails), out.NextToken, nil
			},
		},
		{
			name: "conformance_pack_status",
			list: func(ctx context.Context, c *configservicesdk.Client, sz int32, tok *string) (int, *string, error) {
				out, err := c.DescribeConformancePackStatus(
					ctx,
					&configservicesdk.DescribeConformancePackStatusInput{Limit: sz, NextToken: tok},
				)
				if err != nil {
					return 0, nil, err
				}

				return len(out.ConformancePackStatusDetails), out.NextToken, nil
			},
		},
		{
			name: "conformance_pack_compliance_summary",
			list: func(ctx context.Context, c *configservicesdk.Client, sz int32, tok *string) (int, *string, error) {
				out, err := c.GetConformancePackComplianceSummary(
					ctx,
					&configservicesdk.GetConformancePackComplianceSummaryInput{
						ConformancePackNames: []string{"p1", "p2", "p3"}, Limit: sz, NextToken: tok,
					},
				)
				if err != nil {
					return 0, nil, err
				}

				return len(out.ConformancePackComplianceSummaryList), out.NextToken, nil
			},
		},
		{
			name: "stored_queries",
			list: func(ctx context.Context, c *configservicesdk.Client, sz int32, tok *string) (int, *string, error) {
				out, err := c.ListStoredQueries(
					ctx,
					&configservicesdk.ListStoredQueriesInput{MaxResults: aws.Int32(sz), NextToken: tok},
				)
				if err != nil {
					return 0, nil, err
				}

				return len(out.StoredQueryMetadata), out.NextToken, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, c := newBackend(t)

			for _, n := range []string{"p1", "p2", "p3"} {
				require.NoError(t, b.PutConformancePack(n, "", "", "", "", "", nil))
				_, err := b.PutStoredQuery("q-"+n, "", "SELECT resourceId", nil)
				require.NoError(t, err)
			}

			total, next, err := tt.list(t.Context(), c, 0, nil)
			require.NoError(t, err)
			assert.Equal(t, 3, total)
			assert.Nil(t, next)

			n, next, err := tt.list(t.Context(), c, 2, nil)
			require.NoError(t, err)
			assert.Equal(t, 2, n)
			require.NotNil(t, next)

			rest, _, err := tt.list(t.Context(), c, 2, next)
			require.NoError(t, err)
			assert.Equal(t, 1, rest)

			_, _, err = tt.list(t.Context(), c, 0, aws.String("%%bogus%%"))
			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Contains(t, []string{"InvalidNextTokenException", "ValidationException"}, apiErr.ErrorCode())
		})
	}
}

// TestListOps_Filters covers the Filters / name-list members of the describe/list ops.
func TestListOps_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(ctx context.Context, c *configservicesdk.Client) (int, error)
		name string
		want int
	}{
		{
			name: "conformance_pack_names_match",
			want: 1,
			run: func(ctx context.Context, c *configservicesdk.Client) (int, error) {
				out, err := c.DescribeConformancePacks(
					ctx,
					&configservicesdk.DescribeConformancePacksInput{ConformancePackNames: []string{"p2"}},
				)
				if err != nil {
					return 0, err
				}

				return len(out.ConformancePackDetails), nil
			},
		},
		{
			name: "recorders_scope_paid_excludes_internal",
			want: 0,
			run: func(ctx context.Context, c *configservicesdk.Client) (int, error) {
				out, err := c.ListConfigurationRecorders(ctx, &configservicesdk.ListConfigurationRecordersInput{
					Filters: []types.ConfigurationRecorderFilter{{
						FilterName:  types.ConfigurationRecorderFilterNameRecordingScope,
						FilterValue: []string{"PAID"},
					}},
				})
				if err != nil {
					return 0, err
				}

				return len(out.ConfigurationRecorderSummaries), nil
			},
		},
		{
			name: "recorders_scope_internal_includes",
			want: 1,
			run: func(ctx context.Context, c *configservicesdk.Client) (int, error) {
				out, err := c.ListConfigurationRecorders(ctx, &configservicesdk.ListConfigurationRecordersInput{
					Filters: []types.ConfigurationRecorderFilter{{
						FilterName:  types.ConfigurationRecorderFilterNameRecordingScope,
						FilterValue: []string{"INTERNAL"},
					}},
				})
				if err != nil {
					return 0, err
				}

				return len(out.ConfigurationRecorderSummaries), nil
			},
		},
		{
			name: "aggregate_rule_summary_other_account",
			want: 0,
			run: func(ctx context.Context, c *configservicesdk.Client) (int, error) {
				out, err := c.GetAggregateConfigRuleComplianceSummary(
					ctx,
					&configservicesdk.GetAggregateConfigRuleComplianceSummaryInput{
						ConfigurationAggregatorName: aws.String("agg"),
						Filters: &types.ConfigRuleComplianceSummaryFilters{
							AccountId: aws.String("999999999999"),
						},
					},
				)
				if err != nil {
					return 0, err
				}

				return len(out.AggregateComplianceCounts), nil
			},
		},
		{
			name: "aggregate_rule_compliance_other_region",
			want: 0,
			run: func(ctx context.Context, c *configservicesdk.Client) (int, error) {
				out, err := c.DescribeAggregateComplianceByConfigRules(
					ctx,
					&configservicesdk.DescribeAggregateComplianceByConfigRulesInput{
						ConfigurationAggregatorName: aws.String("agg"),
						Filters: &types.ConfigRuleComplianceFilters{
							AwsRegion: aws.String("eu-west-1"),
						},
					},
				)
				if err != nil {
					return 0, err
				}

				return len(out.AggregateComplianceByConfigRules), nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, c := newBackend(t)

			for _, n := range []string{"p1", "p2"} {
				require.NoError(t, b.PutConformancePack(n, "", "", "", "", "", nil))
			}

			require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/r", nil))
			require.NoError(t, b.PutConfigurationAggregator("agg", nil, nil, nil))
			require.NoError(t, b.PutEvaluations([]awsconfig.EvaluationResult{{
				ConfigRuleName: "rule", ResourceType: "AWS::S3::Bucket", ResourceID: "b", ComplianceType: "COMPLIANT",
			}}))

			got, err := tt.run(t.Context(), c)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestConformancePacks_UnknownNameRejected(t *testing.T) {
	t.Parallel()

	_, c := newBackend(t)
	_, err := c.DescribeConformancePacks(
		t.Context(),
		&configservicesdk.DescribeConformancePacksInput{ConformancePackNames: []string{"nope"}},
	)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NoSuchConformancePackException", apiErr.ErrorCode())
}
