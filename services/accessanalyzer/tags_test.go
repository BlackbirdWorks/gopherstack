package accessanalyzer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
