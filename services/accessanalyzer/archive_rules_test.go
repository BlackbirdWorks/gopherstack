package accessanalyzer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

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

func TestCreateArchiveRule_AutoArchivesExistingActiveFindings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantArchived  int
		wantActive    int
		preFindingCnt int
	}{
		{
			name:          "archives_all_active_findings_on_rule_creation",
			preFindingCnt: 3,
			wantArchived:  3,
			wantActive:    0,
		},
		{
			name:          "no_findings_no_error",
			preFindingCnt: 0,
			wantArchived:  0,
			wantActive:    0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, _ = b.CreateAnalyzer("auto-arc-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)

			for i := range tc.preFindingCnt {
				_, _ = b.AddFinding("auto-arc-analyzer", "AWS::S3::Bucket",
					"arn:aws:s3:::bucket-"+string(rune('a'+i)), nil, nil, nil)
			}

			_, err := b.CreateArchiveRule("auto-arc-analyzer", "auto-rule", nil)
			require.NoError(t, err)

			archived, _, _ := b.ListFindings("auto-arc-analyzer", nil, "ARCHIVED", nil, 0, "")
			active, _, _ := b.ListFindings("auto-arc-analyzer", nil, "ACTIVE", nil, 0, "")
			assert.Len(t, archived, tc.wantArchived)
			assert.Len(t, active, tc.wantActive)
		})
	}
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
