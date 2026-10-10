package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartDataQualityRuleRecommendationRun_Fields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		clientToken string
		repeat      bool
	}{
		{name: "echoed_fields"},
		{name: "repeated_token_returns_same_run", clientToken: "tok", repeat: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			in := &gluesdk.StartDataQualityRuleRecommendationRunInput{
				DataSource: &types.DataSource{GlueTable: &types.GlueTable{
					DatabaseName: aws.String("db"), TableName: aws.String("tbl"),
				}},
				Role:                             aws.String("arn:aws:iam::123456789012:role/r"),
				CreatedRulesetName:               aws.String("rs"),
				DataQualitySecurityConfiguration: aws.String("sec"),
				AdditionalRunOptions: &types.DataQualityRuleRecommendationRunAdditionalRunOptions{
					CustomLogGroupPrefix: aws.String("pfx"),
				},
			}
			if tt.clientToken != "" {
				in.ClientToken = aws.String(tt.clientToken)
			}

			first, err := client.StartDataQualityRuleRecommendationRun(ctx, in)
			require.NoError(t, err)

			if tt.repeat {
				second, repeatErr := client.StartDataQualityRuleRecommendationRun(ctx, in)
				require.NoError(t, repeatErr)
				assert.Equal(t, aws.ToString(first.RunId), aws.ToString(second.RunId))
			}

			got, err := client.GetDataQualityRuleRecommendationRun(
				ctx,
				&gluesdk.GetDataQualityRuleRecommendationRunInput{
					RunId: first.RunId,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "arn:aws:iam::123456789012:role/r", aws.ToString(got.Role))
			assert.Equal(t, "rs", aws.ToString(got.CreatedRulesetName))
			assert.Equal(t, "sec", aws.ToString(got.DataQualitySecurityConfiguration))
			require.NotNil(t, got.DataSource.GlueTable)
			assert.Equal(t, "tbl", aws.ToString(got.DataSource.GlueTable.TableName))
			require.NotNil(t, got.AdditionalRunOptions)
			assert.Equal(t, "pfx", aws.ToString(got.AdditionalRunOptions.CustomLogGroupPrefix))
		})
	}
}
