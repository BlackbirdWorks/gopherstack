package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	securityhubtypes "github.com/aws/aws-sdk-go-v2/service/securityhub/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createSecurityHubClient returns a Security Hub client pointed at the shared test container.
func createSecurityHubClient(t *testing.T) *securityhubsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return securityhubsdk.NewFromConfig(cfg, func(o *securityhubsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_SecurityHub_InsightLifecycle enables the hub and drives create→get→delete of
// a custom insight. The hub-enable is shared account state, so an "already enabled" conflict is
// tolerated to stay parallel-safe.
func TestIntegration_SecurityHub_InsightLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name        string
		insightName string
		groupBy     string
	}{
		{name: "full_lifecycle", insightName: "integ-insight", groupBy: "ResourceType"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createSecurityHubClient(t)

			// Hub-enable is global; ignore an "already enabled" conflict from a sibling test.
			_, _ = client.EnableSecurityHub(ctx, &securityhubsdk.EnableSecurityHubInput{
				EnableDefaultStandards: aws.Bool(false),
			})

			createOut, err := client.CreateInsight(ctx, &securityhubsdk.CreateInsightInput{
				Name:             aws.String(tt.insightName),
				GroupByAttribute: aws.String(tt.groupBy),
				Filters: &securityhubtypes.AwsSecurityFindingFilters{
					RecordState: []securityhubtypes.StringFilter{
						{Comparison: securityhubtypes.StringFilterComparisonEquals, Value: aws.String("ACTIVE")},
					},
				},
			})
			require.NoError(t, err, "CreateInsight should succeed")
			insightArn := aws.ToString(createOut.InsightArn)
			require.NotEmpty(t, insightArn, "insight ARN must be returned")

			t.Cleanup(func() {
				_, _ = client.DeleteInsight(ctx, &securityhubsdk.DeleteInsightInput{InsightArn: aws.String(insightArn)})
			})

			getOut, err := client.GetInsights(ctx, &securityhubsdk.GetInsightsInput{
				InsightArns: []string{insightArn},
			})
			require.NoError(t, err, "GetInsights should succeed")
			require.Len(t, getOut.Insights, 1)
			assert.Equal(t, tt.insightName, aws.ToString(getOut.Insights[0].Name))

			_, err = client.DeleteInsight(ctx, &securityhubsdk.DeleteInsightInput{InsightArn: aws.String(insightArn)})
			require.NoError(t, err, "DeleteInsight should succeed")
		})
	}
}
