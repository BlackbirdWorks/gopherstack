package accessanalyzer_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	aasdk "github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	aatypes "github.com/aws/aws-sdk-go-v2/service/accessanalyzer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// TestListSummaryShapes proves this pass's over-wide-response audit
// (gopherstack, 2026-09-19) for accessanalyzer's five flagged List ops:
// ListAnalyzedResources and ListArchiveRules already matched their real
// *Summary member sets exactly (verified via cmd/structfielddiff against
// accessanalyzer@v1.51.4); ListAnalyzers and ListAccessPreviews match with
// no leaks; ListFindings had a real leak (types.FindingSummary has no
// analyzerArn member, unlike types.Finding) which this pass fixed with a
// narrow findingSummaryToJSON builder.
func TestListSummaryShapes(t *testing.T) {
	t.Parallel()

	t.Run("analyzed resources exact", func(t *testing.T) {
		t.Parallel()

		b := accessanalyzer.NewInMemoryBackend("123456789012", "us-east-1")
		h := accessanalyzer.NewHandler(b)
		client := newTestAccessAnalyzerClient(t, h)
		ctx := t.Context()

		arn := mustAnalyzer(t, b, "a1")

		_, err := b.AddAnalyzedResource(arn, "arn:aws:s3:::bucket", "AWS::S3::Bucket", false)
		require.NoError(t, err)

		out, err := client.ListAnalyzedResources(ctx, &aasdk.ListAnalyzedResourcesInput{
			AnalyzerArn: aws.String(arn),
		})
		require.NoError(t, err)
		require.Len(t, out.AnalyzedResources, 1)
		r := out.AnalyzedResources[0]
		assert.Equal(t, "arn:aws:s3:::bucket", aws.ToString(r.ResourceArn))
		assert.Equal(t, "123456789012", aws.ToString(r.ResourceOwnerAccount))
		assert.NotEmpty(t, r.ResourceType)

		rec := doRequest(t, h, "POST", "/analyzed-resource", map[string]any{
			"analyzerArn": "arn:aws:access-analyzer:us-east-1:123456789012:analyzer/a1",
		})
		assert.NotContains(t, rec.Body.String(), "isPublic")
		assert.NotContains(t, rec.Body.String(), "createdAt")
	})

	t.Run("archive rules exact", func(t *testing.T) {
		t.Parallel()

		b := accessanalyzer.NewInMemoryBackend("123456789012", "us-east-1")
		h := accessanalyzer.NewHandler(b)
		client := newTestAccessAnalyzerClient(t, h)
		ctx := t.Context()

		_ = mustAnalyzer(t, b, "a1")

		_, err := client.CreateArchiveRule(ctx, &aasdk.CreateArchiveRuleInput{
			AnalyzerName: aws.String("a1"),
			RuleName:     aws.String("r1"),
			Filter: map[string]aatypes.Criterion{
				"resource": {Eq: []string{"arn:aws:s3:::bucket"}},
			},
		})
		require.NoError(t, err)

		out, err := client.ListArchiveRules(ctx, &aasdk.ListArchiveRulesInput{
			AnalyzerName: aws.String("a1"),
		})
		require.NoError(t, err)
		require.Len(t, out.ArchiveRules, 1)
		rule := out.ArchiveRules[0]
		assert.Equal(t, "r1", aws.ToString(rule.RuleName))
		assert.Contains(t, rule.Filter, "resource")
		assert.NotNil(t, rule.CreatedAt)
		assert.NotNil(t, rule.UpdatedAt)
	})

	t.Run("analyzers no leak", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestAccessAnalyzerClient(t, h)
		ctx := t.Context()

		_, err := client.CreateAnalyzer(ctx, &aasdk.CreateAnalyzerInput{
			AnalyzerName: aws.String("a1"),
			Type:         aatypes.TypeAccount,
			Tags:         map[string]string{"k": "v"},
		})
		require.NoError(t, err)

		out, err := client.ListAnalyzers(ctx, &aasdk.ListAnalyzersInput{})
		require.NoError(t, err)
		require.Len(t, out.Analyzers, 1)
		a := out.Analyzers[0]
		assert.Equal(t, "a1", aws.ToString(a.Name))
		assert.Equal(t, map[string]string{"k": "v"}, a.Tags)

		rec := doRequest(t, h, "GET", "/analyzer", nil)
		assert.NotContains(t, rec.Body.String(), "configuration")
	})

	t.Run("access previews no leak", func(t *testing.T) {
		t.Parallel()

		b := accessanalyzer.NewInMemoryBackend("123456789012", "us-east-1")
		h := accessanalyzer.NewHandler(b)
		client := newTestAccessAnalyzerClient(t, h)
		ctx := t.Context()

		arn := mustAnalyzer(t, b, "a1")

		_, err := client.CreateAccessPreview(ctx, &aasdk.CreateAccessPreviewInput{
			AnalyzerArn: aws.String(arn),
			Configurations: map[string]aatypes.Configuration{
				"s3Bucket": &aatypes.ConfigurationMemberS3Bucket{},
			},
		})
		require.NoError(t, err)

		out, err := client.ListAccessPreviews(ctx, &aasdk.ListAccessPreviewsInput{
			AnalyzerArn: aws.String(arn),
		})
		require.NoError(t, err)
		require.Len(t, out.AccessPreviews, 1)
		p := out.AccessPreviews[0]
		assert.Equal(t, arn, aws.ToString(p.AnalyzerArn))
		assert.NotEmpty(t, p.Status)
		assert.NotNil(t, p.CreatedAt)

		rec := doRequest(t, h, "GET", "/access-preview?analyzerArn="+arn, nil)
		assert.NotContains(t, rec.Body.String(), "configurations")
	})

	t.Run("findings analyzerArn was leaking", func(t *testing.T) {
		t.Parallel()

		b := accessanalyzer.NewInMemoryBackend("123456789012", "us-east-1")
		h := accessanalyzer.NewHandler(b)
		client := newTestAccessAnalyzerClient(t, h)
		ctx := t.Context()

		arn := mustAnalyzer(t, b, "a1")
		_, err := b.AddFinding(
			"a1", "AWS::S3::Bucket", "arn:aws:s3:::bucket", nil, nil, nil,
		)
		require.NoError(t, err)

		out, err := client.ListFindings(ctx, &aasdk.ListFindingsInput{
			AnalyzerArn: aws.String(arn),
		})
		require.NoError(t, err)
		require.Len(t, out.Findings, 1)
		f := out.Findings[0]
		assert.NotEmpty(t, f.Id)
		assert.NotNil(t, f.CreatedAt)
		assert.NotNil(t, f.Condition)

		rec := doRequest(t, h, "POST", "/finding", map[string]any{"analyzerArn": arn})
		assert.NotContains(t, rec.Body.String(), "analyzerArn")
	})
}
