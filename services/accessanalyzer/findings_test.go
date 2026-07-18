package accessanalyzer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

func TestAddFinding_ThenGet(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateAnalyzer("find-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)

	isPublic := true
	f, err := b.AddFinding("find-analyzer", "AWS::S3::Bucket", "arn:aws:s3:::public-bucket",
		[]string{"s3:GetObject"}, nil, &isPublic)
	require.NoError(t, err)
	require.NotEmpty(t, f.ID)

	got, err := b.GetFinding("find-analyzer", f.ID)
	require.NoError(t, err)
	assert.Equal(t, accessanalyzer.FindingStatusActive, got.Status)
	assert.True(t, *got.IsPublic)
}

func TestListFindings_FilterByStatus(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateAnalyzer("list-find-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)

	_, _ = b.AddFinding("list-find-analyzer", "AWS::S3::Bucket", "arn:aws:s3:::a", nil, nil, nil)
	f2, _ := b.AddFinding("list-find-analyzer", "AWS::IAM::Role", "arn:aws:iam:::role/r", nil, nil, nil)

	// Archive one finding.
	require.NoError(t, b.UpdateFindings("list-find-analyzer", []string{f2.ID}, accessanalyzer.FindingStatusArchived))

	active, _, err := b.ListFindings("list-find-analyzer", nil, "ACTIVE", 0, "")
	require.NoError(t, err)
	assert.Len(t, active, 1)

	archived, _, err := b.ListFindings("list-find-analyzer", nil, "ARCHIVED", 0, "")
	require.NoError(t, err)
	assert.Len(t, archived, 1)
}

func TestUpdateFindings_ChangesStatus(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateAnalyzer("upd-find-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)

	f, _ := b.AddFinding("upd-find-analyzer", "AWS::S3::Bucket", "arn:aws:s3:::b", nil, nil, nil)

	require.NoError(t, b.UpdateFindings("upd-find-analyzer", []string{f.ID}, accessanalyzer.FindingStatusArchived))

	got, err := b.GetFinding("upd-find-analyzer", f.ID)
	require.NoError(t, err)
	assert.Equal(t, accessanalyzer.FindingStatusArchived, got.Status)
}
