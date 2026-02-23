package sqs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/sqs"
)

func TestTagQueue_Basic(t *testing.T) {
	t.Parallel()

	b := newBackend()
	url := createTestQueue(t, b, "tag-test-queue")

	err := b.TagQueue(&sqs.TagQueueInput{
		QueueURL: url,
		Tags:     map[string]string{"env": "test", "team": "platform"},
	})
	require.NoError(t, err)

	out, err := b.ListQueueTags(&sqs.ListQueueTagsInput{QueueURL: url})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "test", "team": "platform"}, out.Tags)
}

func TestUntagQueue_Basic(t *testing.T) {
	t.Parallel()

	b := newBackend()
	url := createTestQueue(t, b, "untag-test-queue")

	err := b.TagQueue(&sqs.TagQueueInput{
		QueueURL: url,
		Tags:     map[string]string{"env": "test", "team": "platform"},
	})
	require.NoError(t, err)

	err = b.UntagQueue(&sqs.UntagQueueInput{
		QueueURL: url,
		TagKeys:  []string{"team"},
	})
	require.NoError(t, err)

	out, err := b.ListQueueTags(&sqs.ListQueueTagsInput{QueueURL: url})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "test"}, out.Tags)
}

func TestListQueueTags_Empty(t *testing.T) {
	t.Parallel()

	b := newBackend()
	url := createTestQueue(t, b, "empty-tags-queue")

	out, err := b.ListQueueTags(&sqs.ListQueueTagsInput{QueueURL: url})
	require.NoError(t, err)
	assert.Empty(t, out.Tags)
}

func TestTagQueue_QueueNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend()

	err := b.TagQueue(&sqs.TagQueueInput{
		QueueURL: "http://localhost:4566/000000000000/nonexistent",
		Tags:     map[string]string{"key": "value"},
	})
	assert.ErrorIs(t, err, sqs.ErrQueueNotFound)
}
