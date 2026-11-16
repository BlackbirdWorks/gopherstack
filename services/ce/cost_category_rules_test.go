package ce_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	costexplorersdk "github.com/aws/aws-sdk-go-v2/service/costexplorer"
	cetypes "github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

func TestCostCategoryRulesAssignValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  string
		entry ce.CostEntry
	}{
		{name: "service_rule", entry: ce.CostEntry{Service: "Amazon S3"}, want: "storage"},
		{name: "and_not_rule", entry: ce.CostEntry{Service: "Amazon EC2", Region: "us-west-2"}, want: "compute-west"},
		{
			name:  "inherited_tag",
			entry: ce.CostEntry{Service: "AWS Lambda", Tags: map[string]string{"team": "blue"}},
			want:  "blue",
		},
		{name: "default", entry: ce.CostEntry{Service: "AWS Lambda"}, want: "other"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ce.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestCEClient(t, ce.NewHandler(b))

			dim := func(key string, vals ...string) *cetypes.Expression {
				return &cetypes.Expression{Dimensions: &cetypes.DimensionValues{
					Key: cetypes.Dimension(key), Values: vals,
				}}
			}

			_, err := client.CreateCostCategoryDefinition(
				t.Context(),
				&costexplorersdk.CreateCostCategoryDefinitionInput{
					Name: aws.String("Team"), RuleVersion: cetypes.CostCategoryRuleVersionCostCategoryExpressionV1,
					DefaultValue: aws.String("other"),
					Rules: []cetypes.CostCategoryRule{
						{
							Value: aws.String("storage"),
							Type:  cetypes.CostCategoryRuleTypeRegular,
							Rule:  dim("SERVICE", "Amazon S3"),
						},
						{
							Value: aws.String("compute-west"),
							Type:  cetypes.CostCategoryRuleTypeRegular,
							Rule: &cetypes.Expression{
								And: []cetypes.Expression{
									*dim("SERVICE", "Amazon EC2"),
									{Not: dim("REGION", "us-east-1")},
								},
							},
						},
						{
							Type: cetypes.CostCategoryRuleTypeInheritedValue,
							InheritedValue: &cetypes.CostCategoryInheritedValueDimension{
								DimensionName: cetypes.CostCategoryInheritedValueDimensionNameTag,
								DimensionKey:  aws.String("team"),
							},
						},
					},
				},
			)
			require.NoError(t, err)

			assert.Equal(t, tt.want, b.CostCategoryValue("Team", &tt.entry))

			out, err := client.ListCostCategoryDefinitions(
				t.Context(),
				&costexplorersdk.ListCostCategoryDefinitionsInput{},
			)
			require.NoError(t, err)
			require.Len(t, out.CostCategoryReferences, 1)

			desc, err := client.DescribeCostCategoryDefinition(
				t.Context(),
				&costexplorersdk.DescribeCostCategoryDefinitionInput{
					CostCategoryArn: out.CostCategoryReferences[0].CostCategoryArn,
				},
			)
			require.NoError(t, err)
			require.Len(t, desc.CostCategory.Rules, 3)
			assert.Equal(t, []string{"Amazon S3"}, desc.CostCategory.Rules[0].Rule.Dimensions.Values)
			assert.Equal(t, cetypes.CostCategoryRuleTypeInheritedValue, desc.CostCategory.Rules[2].Type)
		})
	}
}
