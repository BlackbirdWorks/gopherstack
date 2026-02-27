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
