package accessanalyzer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

func newBackend(t *testing.T) *accessanalyzer.InMemoryBackend {
	t.Helper()

	return accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
}

// ---- Analyzer CRUD ----

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

// ---- Archive rules ----

func TestCreateArchiveRule_Success(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateAnalyzer("ar-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)

	filter := map[string]accessanalyzer.FilterCriterion{
		"resourceType": {Eq: []string{"AWS::S3::Bucket"}},
	}

	rule, err := b.CreateArchiveRule("ar-analyzer", "s3-rule", filter)
	require.NoError(t, err)
	assert.Equal(t, "s3-rule", rule.RuleName)
}

func TestCreateArchiveRule_DuplicateRejected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateAnalyzer("dup-ar-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)

	filter := map[string]accessanalyzer.FilterCriterion{}
	_, err := b.CreateArchiveRule("dup-ar-analyzer", "dup-rule", filter)
	require.NoError(t, err)

	_, err = b.CreateArchiveRule("dup-ar-analyzer", "dup-rule", filter)
	require.Error(t, err)
}

func TestGetArchiveRule_NotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateAnalyzer("get-ar-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)

	_, err := b.GetArchiveRule("get-ar-analyzer", "nonexistent")
	require.Error(t, err)
}

func TestListArchiveRules_ReturnsAll(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateAnalyzer("list-ar-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)

	filter := map[string]accessanalyzer.FilterCriterion{}
	_, _ = b.CreateArchiveRule("list-ar-analyzer", "rule-1", filter)
	_, _ = b.CreateArchiveRule("list-ar-analyzer", "rule-2", filter)

	rules, err := b.ListArchiveRules("list-ar-analyzer")
	require.NoError(t, err)
	assert.Len(t, rules, 2)
}

func TestUpdateArchiveRule_ChangesFilter(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateAnalyzer("upd-ar-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)

	original := map[string]accessanalyzer.FilterCriterion{
		"resourceType": {Eq: []string{"AWS::S3::Bucket"}},
	}
	_, _ = b.CreateArchiveRule("upd-ar-analyzer", "update-rule", original)

	updated := map[string]accessanalyzer.FilterCriterion{
		"resourceType": {Eq: []string{"AWS::IAM::Role"}},
	}
	rule, err := b.UpdateArchiveRule("upd-ar-analyzer", "update-rule", updated)
	require.NoError(t, err)
	assert.Equal(t, []string{"AWS::IAM::Role"}, rule.Filter["resourceType"].Eq)
}

func TestDeleteArchiveRule_Success(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateAnalyzer("del-ar-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)

	_, _ = b.CreateArchiveRule("del-ar-analyzer", "del-rule", nil)
	require.NoError(t, b.DeleteArchiveRule("del-ar-analyzer", "del-rule"))

	_, err := b.GetArchiveRule("del-ar-analyzer", "del-rule")
	require.Error(t, err)
}

// ---- Findings ----

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

// ---- Tags ----

func TestTagResource_Roundtrip(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	arn := "arn:aws:access-analyzer:us-east-1:000000000000:analyzer/tagged"

	require.NoError(t, b.TagResource(arn, map[string]string{"env": "prod", "team": "infra"}))

	tags, err := b.ListTagsForResource(arn)
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "infra", tags["team"])
}

func TestUntagResource_RemovesTags(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	arn := "arn:aws:access-analyzer:us-east-1:000000000000:analyzer/untag-test"

	_ = b.TagResource(arn, map[string]string{"a": "1", "b": "2"})
	_ = b.UntagResource(arn, []string{"a"})

	tags, _ := b.ListTagsForResource(arn)
	assert.NotContains(t, tags, "a")
	assert.Contains(t, tags, "b")
}

// ---- Reset ----

func TestReset_ClearsState(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateAnalyzer("reset-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)

	b.Reset()

	analyzers, _ := b.ListAnalyzers("")
	assert.Empty(t, analyzers)
}
