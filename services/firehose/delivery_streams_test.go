package firehose_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

func TestCreateDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(b *firehose.InMemoryBackend)
		name       string
		streamName string
	}{
		{
			name:       "success",
			streamName: "my-stream",
		},
		{
			name:       "already_exists",
			streamName: "my-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "my-stream"})
			},
			wantErr: firehose.ErrAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			s, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: tt.streamName})
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.streamName, s.Name)
			assert.Equal(t, "ACTIVE", s.Status)
			assert.Contains(t, s.ARN, "arn:aws:firehose:")
		})
	}
}

// TestCreateDeliveryStream_EmptyName verifies that whitespace-only stream names are
// rejected with ErrValidation.
func TestCreateDeliveryStream_EmptyName(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "   "})
	require.Error(t, err)
	assert.ErrorIs(t, err, firehose.ErrValidation)
}

// TestCreateDeliveryStream_SetsVersionAndType verifies the default DeliveryStreamType and
// initial VersionID assigned at creation.
func TestCreateDeliveryStream_SetsVersionAndType(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "typed-stream"})
	require.NoError(t, err)

	s, err := b.DescribeDeliveryStream(context.TODO(), "typed-stream")
	require.NoError(t, err)
	assert.Equal(t, "DirectPut", s.DeliveryStreamType)
	assert.Equal(t, "1", s.VersionID)
}

// TestCreateDeliveryStream_SetsDestinationID verifies that a default DestinationId is
// stamped onto a newly created S3 destination.
func TestCreateDeliveryStream_SetsDestinationID(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name: "dest-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::my-bucket",
		},
	})
	require.NoError(t, err)

	s, err := b.DescribeDeliveryStream(context.TODO(), "dest-stream")
	require.NoError(t, err)
	require.NotNil(t, s.S3Destination)
	assert.Equal(t, "destinationId-000000000001", s.S3Destination.DestinationID)
}

func TestDeleteDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(b *firehose.InMemoryBackend)
		name       string
		streamName string
	}{
		{
			name:       "success",
			streamName: "my-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "my-stream"})
			},
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			wantErr:    firehose.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			err := b.DeleteDeliveryStream(context.TODO(), tt.streamName)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			names := b.ListDeliveryStreams(context.TODO())
			assert.Empty(t, names)
		})
	}
}

func TestDescribeDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(b *firehose.InMemoryBackend)
		name       string
		streamName string
	}{
		{
			name:       "success",
			streamName: "my-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "my-stream"})
			},
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			wantErr:    firehose.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			s, err := b.DescribeDeliveryStream(context.TODO(), tt.streamName)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.streamName, s.Name)
		})
	}
}

func TestListDeliveryStreams(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "s1"})
	_, _ = b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "s2"})

	names := b.ListDeliveryStreams(context.TODO())
	assert.Len(t, names, 2)
}

// TestListDeliveryStreams_SortedOrder verifies that ListDeliveryStreams returns names
// in alphabetical order, matching AWS Firehose API behaviour.
func TestListDeliveryStreams_SortedOrder(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	for _, name := range []string{"zebra-stream", "alpha-stream", "middle-stream"} {
		_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: name})
		require.NoError(t, err)
	}

	names := b.ListDeliveryStreams(context.TODO())
	require.Len(t, names, 3)
	assert.Equal(t, []string{"alpha-stream", "middle-stream", "zebra-stream"}, names)
}

// TestListDeliveryStreamsByType_Filter verifies the DeliveryStreamType filter on the
// backend list.
func TestListDeliveryStreamsByType_Filter(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "dp"})
	require.NoError(t, err)
	_, err = b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name:               "ks",
		DeliveryStreamType: "KinesisStreamAsSource",
		Source: &firehose.SourceDescription{
			KinesisStreamSourceDescription: &firehose.KinesisStreamSourceDescription{
				KinesisStreamARN: "arn:aws:kinesis:us-east-1:000000000000:stream/s",
			},
		},
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"dp"}, b.ListDeliveryStreamsByType(context.TODO(), "DirectPut"))
	assert.Equal(t, []string{"ks"}, b.ListDeliveryStreamsByType(context.TODO(), "KinesisStreamAsSource"))
	assert.ElementsMatch(t, []string{"dp", "ks"}, b.ListDeliveryStreamsByType(context.TODO(), ""))
}

// TestListDeliveryStreams_CacheConsistentAfterMutations verifies the cached sorted-name
// list stays correct across create/delete (the cache is invalidated on mutation).
func TestListDeliveryStreams_CacheConsistentAfterMutations(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	for _, n := range []string{"c", "a", "b"} {
		_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: n})
		require.NoError(t, err)
	}

	// First call populates the cache; second call must return the same sorted result.
	assert.Equal(t, []string{"a", "b", "c"}, b.ListDeliveryStreams(context.TODO()))
	assert.Equal(t, []string{"a", "b", "c"}, b.ListDeliveryStreams(context.TODO()))

	require.NoError(t, b.DeleteDeliveryStream(context.TODO(), "b"))
	assert.Equal(t, []string{"a", "c"}, b.ListDeliveryStreams(context.TODO()))

	// Mutating the returned slice must not corrupt the cache.
	got := b.ListDeliveryStreams(context.TODO())
	got[0] = "mutated"
	assert.Equal(t, []string{"a", "c"}, b.ListDeliveryStreams(context.TODO()))
}
