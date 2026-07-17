package kinesisanalyticsv2_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

func TestBackend_Tags(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)
	app, err := b.CreateApplication(ctx, "tagged-app", "FLINK-1_18", "", "", "", []kinesisanalyticsv2.Tag{
		{Key: "env", Value: "test"},
	})
	require.NoError(t, err)

	appARN := app.ApplicationARN

	// ListTagsForResource.
	tags, err := b.ListTagsForResource(ctx, appARN)
	require.NoError(t, err)
	assert.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].Key)
	assert.Equal(t, "test", tags[0].Value)

	// TagResource - add new tag.
	err = b.TagResource(ctx, appARN, []kinesisanalyticsv2.Tag{{Key: "team", Value: "platform"}})
	require.NoError(t, err)

	tags, err = b.ListTagsForResource(ctx, appARN)
	require.NoError(t, err)
	assert.Len(t, tags, 2)

	// TagResource - update existing tag.
	err = b.TagResource(ctx, appARN, []kinesisanalyticsv2.Tag{{Key: "env", Value: "prod"}})
	require.NoError(t, err)

	tags, err = b.ListTagsForResource(ctx, appARN)
	require.NoError(t, err)
	tagMap := kinesisanalyticsv2.TagsToMapForTest(tags)
	assert.Equal(t, "prod", tagMap["env"])

	// UntagResource.
	err = b.UntagResource(ctx, appARN, []string{"team"})
	require.NoError(t, err)

	tags, err = b.ListTagsForResource(ctx, appARN)
	require.NoError(t, err)
	assert.Len(t, tags, 1)
}

func TestBackend_Tags_NotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)

	_, err := b.ListTagsForResource(ctx, "arn:aws:kinesisanalytics:us-east-1:000000000000:application/missing")
	require.Error(t, err)

	err = b.TagResource(ctx, "arn:aws:kinesisanalytics:us-east-1:000000000000:application/missing", nil)
	require.Error(t, err)

	err = b.UntagResource(ctx, "arn:aws:kinesisanalytics:us-east-1:000000000000:application/missing", nil)
	require.Error(t, err)
}

// TestBackend_UntagResource_NoSliceAliasing verifies UntagResource's
// filtered-tags slice does not alias the original backing array.
func TestBackend_UntagResource_NoSliceAliasing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)
	app, err := b.CreateApplication(ctx, "untag-app", "FLINK-1_18", "", "", "", []kinesisanalyticsv2.Tag{
		{Key: "a", Value: "1"},
		{Key: "b", Value: "2"},
		{Key: "c", Value: "3"},
	})
	require.NoError(t, err)

	err = b.UntagResource(ctx, app.ApplicationARN, []string{"b"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(ctx, app.ApplicationARN)
	require.NoError(t, err)
	assert.Len(t, tags, 2)
	keys := []string{tags[0].Key, tags[1].Key}
	assert.NotContains(t, keys, "b")
}

// TestBackend_ListTagsForResource_Sorted verifies tags are returned sorted
// by key regardless of insertion order.
func TestBackend_ListTagsForResource_Sorted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)
	app, err := b.CreateApplication(ctx, "sorted-tag-app", "FLINK-1_18", "", "", "", []kinesisanalyticsv2.Tag{
		{Key: "z", Value: "last"},
		{Key: "a", Value: "first"},
		{Key: "m", Value: "middle"},
	})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(ctx, app.ApplicationARN)
	require.NoError(t, err)
	require.Len(t, tags, 3)
	assert.Equal(t, "a", tags[0].Key)
	assert.Equal(t, "m", tags[1].Key)
	assert.Equal(t, "z", tags[2].Key)
}

// TestBackend_ListTagsForResource_NonNilWhenEmpty verifies ListTagsForResource
// returns a non-nil (but empty) slice for an untagged application.
func TestBackend_ListTagsForResource_NonNilWhenEmpty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)
	app, err := b.CreateApplication(ctx, "no-tag-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(ctx, app.ApplicationARN)
	require.NoError(t, err)
	assert.NotNil(t, tags)
	assert.Empty(t, tags)
}
