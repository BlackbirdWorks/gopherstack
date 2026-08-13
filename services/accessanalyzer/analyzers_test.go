package accessanalyzer_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

func TestCreateAnalyzer_Success(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	a, err := b.CreateAnalyzer("my-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)
	require.NoError(t, err)

	assert.Equal(t, "my-analyzer", a.Name)
	assert.Equal(t, accessanalyzer.AnalyzerTypeAccount, a.Type)
	assert.Equal(t, accessanalyzer.AnalyzerStatusActive, a.Status)
	assert.Contains(t, a.Arn, "arn:aws:access-analyzer:")
	assert.Contains(t, a.Arn, "my-analyzer")
}

func TestCreateAnalyzer_DuplicateRejected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateAnalyzer("dup-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)
	require.NoError(t, err)

	_, err = b.CreateAnalyzer("dup-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)
	require.Error(t, err)
}

func TestGetAnalyzer_NotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.GetAnalyzer("nonexistent")
	require.Error(t, err)
}

func TestListAnalyzers_Empty(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	analyzers, err := b.ListAnalyzers("")
	require.NoError(t, err)
	assert.Empty(t, analyzers)
}

func TestListAnalyzers_FilterByType(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateAnalyzer("account-a", accessanalyzer.AnalyzerTypeAccount, nil)
	_, _ = b.CreateAnalyzer("org-a", accessanalyzer.AnalyzerTypeOrganization, nil)

	accounts, err := b.ListAnalyzers(string(accessanalyzer.AnalyzerTypeAccount))
	require.NoError(t, err)
	assert.Len(t, accounts, 1)
	assert.Equal(t, "account-a", accounts[0].Name)
}

func TestDeleteAnalyzer_RemovesFindings(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateAnalyzer("del-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)
	isPublicFinding := true
	_, _ = b.AddFinding("del-analyzer", "AWS::S3::Bucket", "arn:aws:s3:::my-bucket",
		[]string{"s3:GetObject"}, nil, &isPublicFinding)

	require.NoError(t, b.DeleteAnalyzer("del-analyzer"))

	_, err := b.GetAnalyzer("del-analyzer")
	require.Error(t, err)
}

// TestDeleteAnalyzer_CascadesGhostRows verifies DeleteAnalyzer leaves no
// ghost rows behind in tags, finding recommendations, analyzed resources, or
// access previews -- all of which are keyed off the analyzer (by ARN or, for
// finding recommendations, by finding ID) but live in separate tables/maps
// that DeleteAnalyzer must sweep explicitly.
func TestDeleteAnalyzer_CascadesGhostRows(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	a, err := b.CreateAnalyzer("cascade-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)
	require.NoError(t, err)

	require.NoError(t, b.TagResource(a.Arn, map[string]string{"env": "test"}))

	finding, err := b.AddFinding("cascade-analyzer", "AWS::S3::Bucket", "arn:aws:s3:::bucket", nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.GenerateFindingRecommendation(a.Arn, finding.ID))

	_, err = b.AddAnalyzedResource(a.Arn, "arn:aws:s3:::analyzed-bucket", "AWS::S3::Bucket", false)
	require.NoError(t, err)

	preview, err := b.CreateAccessPreview(a.Arn, map[string]json.RawMessage{
		"arn:aws:s3:::analyzed-bucket": json.RawMessage(`{"s3Bucket":{"bucketPolicy":"{}"}}`),
	})
	require.NoError(t, err)

	require.NoError(t, b.DeleteAnalyzer("cascade-analyzer"))

	tags, err := b.ListTagsForResource(a.Arn)
	require.NoError(t, err)
	assert.Empty(t, tags, "tags for the deleted analyzer's ARN must not survive")

	_, err = b.GetFindingRecommendation(a.Arn, finding.ID)
	require.Error(t, err, "finding recommendations for a deleted finding must not survive")

	resources, _, err := b.ListAnalyzedResources(a.Arn, "", 0, "")
	require.NoError(t, err)
	assert.Empty(t, resources, "analyzed resources for the deleted analyzer must not survive")

	_, err = b.GetAccessPreview(preview.ID)
	require.Error(t, err, "access previews for the deleted analyzer must not survive")
}
