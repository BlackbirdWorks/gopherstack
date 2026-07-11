package accessanalyzer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := accessanalyzer.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := accessanalyzer.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	_, err := b.CreateAnalyzer("seed-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)
	require.NoError(t, err)

	// A syntactically valid but version-mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	analyzers, err := b.ListAnalyzers("")
	require.NoError(t, err)
	assert.Empty(t, analyzers)

	_, err = b.GetAnalyzer("seed-analyzer")
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all decodes with Version == 0, which
// mismatches accessanalyzerSnapshotVersion and is discarded the same way any
// other incompatible version is -- not partially applied. This also covers
// the pre-Phase-3.3 snapshot shape: it was never actually reachable (see
// persistence.go's Handler.Snapshot doc comment), so there is no real
// on-disk data in this shape to be compatible with, but a hand-built
// snapshot in the old shape must still be discarded safely rather than
// misinterpreted as the new one.
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := accessanalyzer.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	_, err := b.CreateAnalyzer("seed-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)
	require.NoError(t, err)

	// A shape entirely lacking "version"/"tables" keys -- the pre-refactor
	// on-disk format.
	err = b.Restore(t.Context(), []byte(
		`{"analyzers":{"seed-analyzer":{"name":"seed-analyzer"}},"archiveRules":{},"findings":{},"tags":{}}`,
	))
	require.NoError(t, err)

	analyzers, err := b.ListAnalyzers("")
	require.NoError(t, err)
	assert.Empty(t, analyzers)
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every store.Table-backed resource family the Phase 3.3
// conversion touched (analyzers; the composite-key archiveRules table with
// its byAnalyzer index and hidden AnalyzerName field; the findings table
// keyed directly by ID with its byAnalyzer index derived from AnalyzerArn;
// the composite-key analyzedResources table; and the policyGenerations,
// accessPreviews, and findingRecommendations tables, none of which were
// persisted pre-refactor) plus the one plain map left un-converted (tags).
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := accessanalyzer.NewInMemoryBackend("111122223333", "us-west-2")

	analyzer, err := original.CreateAnalyzer(
		"analyzer-1", accessanalyzer.AnalyzerTypeAccount, map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	rule, err := original.CreateArchiveRule("analyzer-1", "rule-1", map[string]accessanalyzer.FilterCriterion{
		"resourceType": {Eq: []string{"AWS::S3::Bucket"}},
	})
	require.NoError(t, err)

	isPublic := true
	finding, err := original.AddFinding(
		"analyzer-1", "AWS::S3::Bucket", "arn:aws:s3:::bucket-1",
		[]string{"s3:GetObject"}, map[string]string{"AWS": "*"}, &isPublic,
	)
	require.NoError(t, err)

	require.NoError(t, original.TagResource(analyzer.Arn, map[string]string{"team": "platform"}))

	pg, err := original.StartPolicyGeneration("arn:aws:iam::111122223333:role/example")
	require.NoError(t, err)

	ap, err := original.CreateAccessPreview(analyzer.Arn)
	require.NoError(t, err)

	ar, err := original.AddAnalyzedResource(analyzer.Arn, "arn:aws:s3:::bucket-2", "AWS::S3::Bucket", false)
	require.NoError(t, err)

	require.NoError(t, original.GenerateFindingRecommendation(analyzer.Arn, finding.ID))

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := accessanalyzer.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Scalars: accountID/region surface indirectly through a freshly created
	// resource (there is no direct accessor).
	newAnalyzer, err := fresh.CreateAnalyzer("analyzer-2", accessanalyzer.AnalyzerTypeAccount, nil)
	require.NoError(t, err)
	assert.Contains(t, newAnalyzer.Arn, "111122223333")
	assert.Contains(t, newAnalyzer.Arn, "us-west-2")

	// analyzers table + Tags.
	gotAnalyzer, err := fresh.GetAnalyzer("analyzer-1")
	require.NoError(t, err)
	assert.Equal(t, "test", gotAnalyzer.Tags["env"])
	tagVals, err := fresh.ListTagsForResource(analyzer.Arn)
	require.NoError(t, err)
	assert.Equal(t, "platform", tagVals["team"])

	// archiveRules table (composite key + byAnalyzer index; AnalyzerName hidden field).
	gotRule, err := fresh.GetArchiveRule("analyzer-1", "rule-1")
	require.NoError(t, err)
	assert.Equal(t, rule.RuleName, gotRule.RuleName)
	rules, err := fresh.ListArchiveRules("analyzer-1")
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.Equal(t, "rule-1", rules[0].RuleName)

	// findings table (direct key + byAnalyzer index derived from AnalyzerArn).
	gotFinding, err := fresh.GetFinding("analyzer-1", finding.ID)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:s3:::bucket-1", gotFinding.ResourceArn)
	findings, _, err := fresh.ListFindings("analyzer-1", nil, "", 0, "")
	require.NoError(t, err)
	require.Len(t, findings, 1)
	assert.Equal(t, finding.ID, findings[0].ID)

	// policyGenerations table (not persisted pre-refactor).
	gotPG, err := fresh.GetPolicyGeneration(pg.JobID)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::111122223333:role/example", gotPG.PrincipalArn)

	// accessPreviews table (not persisted pre-refactor).
	gotAP, err := fresh.GetAccessPreview(ap.ID)
	require.NoError(t, err)
	assert.Equal(t, analyzer.Arn, gotAP.AnalyzerArn)

	// analyzedResources table (composite key from two real fields; not persisted pre-refactor).
	gotAR, err := fresh.GetAnalyzedResource(analyzer.Arn, ar.ResourceArn)
	require.NoError(t, err)
	assert.Equal(t, "AWS::S3::Bucket", gotAR.ResourceType)

	// findingRecommendations table (not persisted pre-refactor).
	gotRec, err := fresh.GetFindingRecommendation(analyzer.Arn, finding.ID)
	require.NoError(t, err)
	assert.Equal(t, "UNUSED_PERMISSION", gotRec.RecommendationType)

	// DeleteAnalyzer must still cascade-delete the restored archiveRules/findings
	// groups via their rebuilt byAnalyzer indexes.
	require.NoError(t, fresh.DeleteAnalyzer("analyzer-1"))
	_, err = fresh.GetArchiveRule("analyzer-1", "rule-1")
	require.Error(t, err)
	_, err = fresh.GetFinding("analyzer-1", finding.ID)
	require.Error(t, err)
}

// TestHandler_SnapshotRestoreDelegate verifies the Handler-level Snapshot and
// Restore -- which the persistence layer actually calls, per
// setupPersistence in cli.go -- correctly delegate to the backend. Before
// this Phase 3.3 conversion Handler had no Snapshot/Restore methods at all,
// so setupPersistence's duck-type check never matched Access Analyzer's
// Handler and its state was silently never persisted; this test guards
// against that dead-wiring regressing.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := accessanalyzer.NewHandler(accessanalyzer.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion))

	_, err := h.Backend.CreateAnalyzer("delegate-analyzer", accessanalyzer.AnalyzerTypeAccount, nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := accessanalyzer.NewHandler(accessanalyzer.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion))
	require.NoError(t, h2.Restore(t.Context(), snap))

	got, err := h2.Backend.GetAnalyzer("delegate-analyzer")
	require.NoError(t, err)
	assert.Equal(t, "delegate-analyzer", got.Name)
}

// TestInMemoryBackend_Snapshot_EmptyBackend verifies Snapshot/Restore round
// trips cleanly on a backend with no data at all -- every plain map and
// registered table must decode back to empty, not nil-panic or error.
func TestInMemoryBackend_Snapshot_EmptyBackend(t *testing.T) {
	t.Parallel()

	original := accessanalyzer.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := accessanalyzer.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	analyzers, err := fresh.ListAnalyzers("")
	require.NoError(t, err)
	assert.Empty(t, analyzers)
}
