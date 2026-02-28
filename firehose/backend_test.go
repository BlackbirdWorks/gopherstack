package firehose_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/firehose"
)

func TestFirehoseBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, b *firehose.InMemoryBackend)
	}{
		{name: "CreateDeliveryStream/success", run: func(t *testing.T, b *firehose.InMemoryBackend) {
			s, err := b.CreateDeliveryStream("my-stream")
			require.NoError(t, err)
			assert.Equal(t, "my-stream", s.Name)
			assert.Equal(t, "ACTIVE", s.Status)
			assert.Contains(t, s.ARN, "arn:aws:firehose:")
		}},
		{name: "CreateDeliveryStream/already_exists", run: func(t *testing.T, b *firehose.InMemoryBackend) {
			_, err := b.CreateDeliveryStream("my-stream")
			require.NoError(t, err)

			_, err = b.CreateDeliveryStream("my-stream")
			require.Error(t, err)
			assert.ErrorIs(t, err, firehose.ErrAlreadyExists)
		}},
		{name: "DeleteDeliveryStream/success", run: func(t *testing.T, b *firehose.InMemoryBackend) {
			_, _ = b.CreateDeliveryStream("my-stream")

			err := b.DeleteDeliveryStream("my-stream")
			require.NoError(t, err)

			names := b.ListDeliveryStreams()
			assert.Empty(t, names)
		}},
		{name: "DeleteDeliveryStream/not_found", run: func(t *testing.T, b *firehose.InMemoryBackend) {
			err := b.DeleteDeliveryStream("nonexistent")
			require.Error(t, err)
			assert.ErrorIs(t, err, firehose.ErrNotFound)
		}},
		{name: "DescribeDeliveryStream/success", run: func(t *testing.T, b *firehose.InMemoryBackend) {
			_, _ = b.CreateDeliveryStream("my-stream")

			s, err := b.DescribeDeliveryStream("my-stream")
			require.NoError(t, err)
			assert.Equal(t, "my-stream", s.Name)
		}},
		{name: "DescribeDeliveryStream/not_found", run: func(t *testing.T, b *firehose.InMemoryBackend) {
			_, err := b.DescribeDeliveryStream("nonexistent")
			require.Error(t, err)
			assert.ErrorIs(t, err, firehose.ErrNotFound)
		}},
		{name: "PutRecord/success", run: func(t *testing.T, b *firehose.InMemoryBackend) {
			_, _ = b.CreateDeliveryStream("my-stream")

			err := b.PutRecord("my-stream", []byte("hello world"))
			require.NoError(t, err)
		}},
		{name: "PutRecordBatch/success", run: func(t *testing.T, b *firehose.InMemoryBackend) {
			_, _ = b.CreateDeliveryStream("my-stream")

			failed, err := b.PutRecordBatch("my-stream", [][]byte{[]byte("a"), []byte("b")})
			require.NoError(t, err)
			assert.Equal(t, 0, failed)
		}},
		{name: "ListDeliveryStreams/multiple", run: func(t *testing.T, b *firehose.InMemoryBackend) {
			_, _ = b.CreateDeliveryStream("s1")
			_, _ = b.CreateDeliveryStream("s2")

			names := b.ListDeliveryStreams()
			assert.Len(t, names, 2)
		}},
		{name: "TagDeliveryStream/success", run: func(t *testing.T, b *firehose.InMemoryBackend) {
			_, _ = b.CreateDeliveryStream("tagged-stream")

			err := b.TagDeliveryStream("tagged-stream", map[string]string{"env": "prod", "team": "platform"})
			require.NoError(t, err)

			tags, err := b.ListTagsForDeliveryStream("tagged-stream")
			require.NoError(t, err)
			assert.Equal(t, "prod", tags["env"])
			assert.Equal(t, "platform", tags["team"])
		}},
		{name: "TagDeliveryStream/overwrite", run: func(t *testing.T, b *firehose.InMemoryBackend) {
			_, _ = b.CreateDeliveryStream("overwrite-stream")

			_ = b.TagDeliveryStream("overwrite-stream", map[string]string{"env": "dev"})
			_ = b.TagDeliveryStream("overwrite-stream", map[string]string{"env": "prod"})

			tags, err := b.ListTagsForDeliveryStream("overwrite-stream")
			require.NoError(t, err)
			assert.Equal(t, "prod", tags["env"])
		}},
		{name: "TagDeliveryStream/not_found", run: func(t *testing.T, b *firehose.InMemoryBackend) {
			err := b.TagDeliveryStream("nonexistent", map[string]string{"k": "v"})
			require.Error(t, err)
			assert.ErrorIs(t, err, firehose.ErrNotFound)
		}},
		{name: "UntagDeliveryStream/success", run: func(t *testing.T, b *firehose.InMemoryBackend) {
			_, _ = b.CreateDeliveryStream("untag-stream")
			_ = b.TagDeliveryStream("untag-stream", map[string]string{"env": "prod", "team": "platform"})

			err := b.UntagDeliveryStream("untag-stream", []string{"env"})
			require.NoError(t, err)

			tags, err := b.ListTagsForDeliveryStream("untag-stream")
			require.NoError(t, err)
			assert.NotContains(t, tags, "env")
			assert.Equal(t, "platform", tags["team"])
		}},
		{name: "UntagDeliveryStream/not_found", run: func(t *testing.T, b *firehose.InMemoryBackend) {
			err := b.UntagDeliveryStream("nonexistent", []string{"k"})
			require.Error(t, err)
			assert.ErrorIs(t, err, firehose.ErrNotFound)
		}},
		{name: "ListTagsForDeliveryStream/not_found", run: func(t *testing.T, b *firehose.InMemoryBackend) {
			_, err := b.ListTagsForDeliveryStream("nonexistent")
			require.Error(t, err)
			assert.ErrorIs(t, err, firehose.ErrNotFound)
		}},
		{name: "ListTagsForDeliveryStream/empty", run: func(t *testing.T, b *firehose.InMemoryBackend) {
			_, _ = b.CreateDeliveryStream("empty-tags")

			tags, err := b.ListTagsForDeliveryStream("empty-tags")
			require.NoError(t, err)
			assert.Empty(t, tags)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			tt.run(t, b)
		})
	}
}
