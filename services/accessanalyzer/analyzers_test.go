package accessanalyzer_test

import (
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
