package firehose_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/firehose"
)

func TestFirehose_CreateDeliveryStream(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	s, err := b.CreateDeliveryStream("my-stream")
	require.NoError(t, err)
	assert.Equal(t, "my-stream", s.Name)
	assert.Equal(t, "ACTIVE", s.Status)
	assert.Contains(t, s.ARN, "arn:aws:firehose:")
}

func TestFirehose_CreateDeliveryStream_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream("my-stream")
	require.NoError(t, err)

	_, err = b.CreateDeliveryStream("my-stream")
	require.Error(t, err)
	assert.ErrorIs(t, err, firehose.ErrAlreadyExists)
}

func TestFirehose_DeleteDeliveryStream(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateDeliveryStream("my-stream")

	err := b.DeleteDeliveryStream("my-stream")
	require.NoError(t, err)

	names := b.ListDeliveryStreams()
	assert.Empty(t, names)
}

func TestFirehose_DeleteDeliveryStream_NotFound(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.DeleteDeliveryStream("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, firehose.ErrNotFound)
}

func TestFirehose_DescribeDeliveryStream(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateDeliveryStream("my-stream")

	s, err := b.DescribeDeliveryStream("my-stream")
	require.NoError(t, err)
	assert.Equal(t, "my-stream", s.Name)
}

func TestFirehose_DescribeDeliveryStream_NotFound(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.DescribeDeliveryStream("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, firehose.ErrNotFound)
}

func TestFirehose_PutRecord(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateDeliveryStream("my-stream")

	err := b.PutRecord("my-stream", []byte("hello world"))
	require.NoError(t, err)
}

func TestFirehose_PutRecordBatch(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateDeliveryStream("my-stream")

	failed, err := b.PutRecordBatch("my-stream", [][]byte{[]byte("a"), []byte("b")})
	require.NoError(t, err)
	assert.Equal(t, 0, failed)
}

func TestFirehose_ListDeliveryStreams(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateDeliveryStream("s1")
	_, _ = b.CreateDeliveryStream("s2")

	names := b.ListDeliveryStreams()
	assert.Len(t, names, 2)
}

func TestFirehose_TagDeliveryStream(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateDeliveryStream("tagged-stream")

	err := b.TagDeliveryStream("tagged-stream", map[string]string{"env": "prod", "team": "platform"})
	require.NoError(t, err)

	tags, err := b.ListTagsForDeliveryStream("tagged-stream")
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "platform", tags["team"])
}

func TestFirehose_TagDeliveryStream_Overwrite(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateDeliveryStream("overwrite-stream")

	_ = b.TagDeliveryStream("overwrite-stream", map[string]string{"env": "dev"})
	_ = b.TagDeliveryStream("overwrite-stream", map[string]string{"env": "prod"})

	tags, err := b.ListTagsForDeliveryStream("overwrite-stream")
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"])
}

func TestFirehose_TagDeliveryStream_NotFound(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.TagDeliveryStream("nonexistent", map[string]string{"k": "v"})
	require.Error(t, err)
	assert.ErrorIs(t, err, firehose.ErrNotFound)
}

func TestFirehose_UntagDeliveryStream(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateDeliveryStream("untag-stream")
	_ = b.TagDeliveryStream("untag-stream", map[string]string{"env": "prod", "team": "platform"})

	err := b.UntagDeliveryStream("untag-stream", []string{"env"})
	require.NoError(t, err)

	tags, err := b.ListTagsForDeliveryStream("untag-stream")
	require.NoError(t, err)
	assert.NotContains(t, tags, "env")
	assert.Equal(t, "platform", tags["team"])
}

func TestFirehose_UntagDeliveryStream_NotFound(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.UntagDeliveryStream("nonexistent", []string{"k"})
	require.Error(t, err)
	assert.ErrorIs(t, err, firehose.ErrNotFound)
}

func TestFirehose_ListTagsForDeliveryStream_NotFound(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.ListTagsForDeliveryStream("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, firehose.ErrNotFound)
}

func TestFirehose_ListTagsForDeliveryStream_Empty(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateDeliveryStream("empty-tags")

	tags, err := b.ListTagsForDeliveryStream("empty-tags")
	require.NoError(t, err)
	assert.Empty(t, tags)
}
