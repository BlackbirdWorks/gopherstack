package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	aasdk "github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	aatypes "github.com/aws/aws-sdk-go-v2/service/accessanalyzer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createAccessAnalyzerClient returns an IAM Access Analyzer client pointed at the shared test container.
func createAccessAnalyzerClient(t *testing.T) *aasdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return aasdk.NewFromConfig(cfg, func(o *aasdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_AccessAnalyzer_AnalyzerLifecycle drives create→get→list→delete of an analyzer.
func TestIntegration_AccessAnalyzer_AnalyzerLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name         string
		analyzerName string
		analyzerType aatypes.Type
	}{
		{name: "account_analyzer", analyzerName: "integ-analyzer", analyzerType: aatypes.TypeAccount},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createAccessAnalyzerClient(t)

			createOut, err := client.CreateAnalyzer(ctx, &aasdk.CreateAnalyzerInput{
				AnalyzerName: aws.String(tt.analyzerName),
				Type:         tt.analyzerType,
			})
			require.NoError(t, err, "CreateAnalyzer should succeed")
			assert.NotEmpty(t, aws.ToString(createOut.Arn), "analyzer ARN must be returned")

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteAnalyzer(
					cleanupCtx,
					&aasdk.DeleteAnalyzerInput{AnalyzerName: aws.String(tt.analyzerName)},
				)
			})

			getOut, err := client.GetAnalyzer(ctx, &aasdk.GetAnalyzerInput{AnalyzerName: aws.String(tt.analyzerName)})
			require.NoError(t, err, "GetAnalyzer should succeed")
			require.NotNil(t, getOut.Analyzer)
			assert.Equal(t, tt.analyzerName, aws.ToString(getOut.Analyzer.Name))
			assert.Equal(t, tt.analyzerType, getOut.Analyzer.Type)
			assert.Equal(t, aatypes.AnalyzerStatusActive, getOut.Analyzer.Status)

			listOut, err := client.ListAnalyzers(ctx, &aasdk.ListAnalyzersInput{})
			require.NoError(t, err, "ListAnalyzers should succeed")

			found := false
			for _, a := range listOut.Analyzers {
				if aws.ToString(a.Name) == tt.analyzerName {
					found = true

					break
				}
			}

			assert.True(t, found, "created analyzer should appear in list")

			_, err = client.DeleteAnalyzer(ctx, &aasdk.DeleteAnalyzerInput{AnalyzerName: aws.String(tt.analyzerName)})
			require.NoError(t, err, "DeleteAnalyzer should succeed")
		})
	}
}

// TestIntegration_AccessAnalyzer_ArchiveRuleLifecycle drives create→get→list→delete of an
// archive rule nested under an analyzer.
func TestIntegration_AccessAnalyzer_ArchiveRuleLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name         string
		analyzerName string
		ruleName     string
	}{
		{name: "full_lifecycle", analyzerName: "integ-rule-analyzer", ruleName: "integ-rule"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createAccessAnalyzerClient(t)

			_, err := client.CreateAnalyzer(ctx, &aasdk.CreateAnalyzerInput{
				AnalyzerName: aws.String(tt.analyzerName),
				Type:         aatypes.TypeAccount,
			})
			require.NoError(t, err, "CreateAnalyzer should succeed")

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteAnalyzer(
					cleanupCtx,
					&aasdk.DeleteAnalyzerInput{AnalyzerName: aws.String(tt.analyzerName)},
				)
			})

			_, err = client.CreateArchiveRule(ctx, &aasdk.CreateArchiveRuleInput{
				AnalyzerName: aws.String(tt.analyzerName),
				RuleName:     aws.String(tt.ruleName),
				Filter: map[string]aatypes.Criterion{
					"resourceType": {Eq: []string{"AWS::S3::Bucket"}},
				},
			})
			require.NoError(t, err, "CreateArchiveRule should succeed")

			getOut, err := client.GetArchiveRule(ctx, &aasdk.GetArchiveRuleInput{
				AnalyzerName: aws.String(tt.analyzerName),
				RuleName:     aws.String(tt.ruleName),
			})
			require.NoError(t, err, "GetArchiveRule should succeed")
			require.NotNil(t, getOut.ArchiveRule)
			assert.Equal(t, tt.ruleName, aws.ToString(getOut.ArchiveRule.RuleName))

			listOut, err := client.ListArchiveRules(ctx, &aasdk.ListArchiveRulesInput{
				AnalyzerName: aws.String(tt.analyzerName),
			})
			require.NoError(t, err, "ListArchiveRules should succeed")
			assert.NotEmpty(t, listOut.ArchiveRules, "archive rule should be listed")

			_, err = client.DeleteArchiveRule(ctx, &aasdk.DeleteArchiveRuleInput{
				AnalyzerName: aws.String(tt.analyzerName),
				RuleName:     aws.String(tt.ruleName),
			})
			require.NoError(t, err, "DeleteArchiveRule should succeed")
		})
	}
}
