package accessanalyzer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

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

// TestCreateAnalyzer_InitialTags_VisibleViaListTagsForResource verifies tags
// supplied at CreateAnalyzer reach ListTagsForResource. CreateAnalyzer only
// wrote the Analyzer's own Tags field (read by GetAnalyzer/ListAnalyzers);
// ListTagsForResource reads a separate ARN-keyed b.tags map that TagResource
// writes to, so creation-time tags were invisible to it.
func TestCreateAnalyzer_InitialTags_VisibleViaListTagsForResource(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	a, err := b.CreateAnalyzer("with-tags", accessanalyzer.AnalyzerTypeAccount, map[string]string{"env": "prod"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(a.Arn)
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"])
}

// TestTagResource_ReflectedInGetAnalyzer verifies tags applied via
// TagResource show up in GetAnalyzer's response. GetAnalyzer renders
// Analyzer.Tags, a field distinct from the b.tags map TagResource writes to,
// so a tag applied after creation was invisible to GetAnalyzer/ListAnalyzers.
func TestTagResource_ReflectedInGetAnalyzer(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	a, err := b.CreateAnalyzer("post-tag", accessanalyzer.AnalyzerTypeAccount, nil)
	require.NoError(t, err)

	require.NoError(t, b.TagResource(a.Arn, map[string]string{"added": "yes"}))

	got, err := b.GetAnalyzer("post-tag")
	require.NoError(t, err)
	assert.Equal(t, "yes", got.Tags["added"])
}
