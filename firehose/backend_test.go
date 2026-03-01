package firehose_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/firehose"
)

func TestCreateDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(b *firehose.InMemoryBackend)
		streamName string
		wantErr    error
	}{
		{
			name:       "success",
			streamName: "my-stream",
		},
		{
			name:       "already_exists",
			streamName: "my-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream("my-stream")
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
			s, err := b.CreateDeliveryStream(tt.streamName)
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

func TestDeleteDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(b *firehose.InMemoryBackend)
		streamName string
		wantErr    error
	}{
		{
			name:       "success",
			streamName: "my-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream("my-stream")
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
			err := b.DeleteDeliveryStream(tt.streamName)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			names := b.ListDeliveryStreams()
			assert.Empty(t, names)
		})
	}
}

func TestDescribeDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(b *firehose.InMemoryBackend)
		streamName string
		wantErr    error
	}{
		{
			name:       "success",
			streamName: "my-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream("my-stream")
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
			s, err := b.DescribeDeliveryStream(tt.streamName)
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

func TestPutRecord(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateDeliveryStream("my-stream")

	err := b.PutRecord("my-stream", []byte("hello world"))
	require.NoError(t, err)
}

func TestPutRecordBatch(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateDeliveryStream("my-stream")

	failed, err := b.PutRecordBatch("my-stream", [][]byte{[]byte("a"), []byte("b")})
	require.NoError(t, err)
	assert.Equal(t, 0, failed)
}

func TestListDeliveryStreams(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateDeliveryStream("s1")
	_, _ = b.CreateDeliveryStream("s2")

	names := b.ListDeliveryStreams()
	assert.Len(t, names, 2)
}

func TestTagDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(b *firehose.InMemoryBackend)
		streamName string
		tags       map[string]string
		wantErr    error
		wantTags   map[string]string
	}{
		{
			name:       "success",
			streamName: "tagged-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream("tagged-stream")
			},
			tags:     map[string]string{"env": "prod", "team": "platform"},
			wantTags: map[string]string{"env": "prod", "team": "platform"},
		},
		{
			name:       "overwrite",
			streamName: "overwrite-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream("overwrite-stream")
				_ = b.TagDeliveryStream("overwrite-stream", map[string]string{"env": "dev"})
			},
			tags:     map[string]string{"env": "prod"},
			wantTags: map[string]string{"env": "prod"},
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			tags:       map[string]string{"k": "v"},
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
			err := b.TagDeliveryStream(tt.streamName, tt.tags)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			tags, err := b.ListTagsForDeliveryStream(tt.streamName)
			require.NoError(t, err)
			for k, v := range tt.wantTags {
				assert.Equal(t, v, tags[k])
			}
		})
	}
}

func TestUntagDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(b *firehose.InMemoryBackend)
		streamName     string
		keysToRemove   []string
		wantErr        error
		wantAbsentKeys []string
		wantTags       map[string]string
	}{
		{
			name:       "success",
			streamName: "untag-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream("untag-stream")
				_ = b.TagDeliveryStream("untag-stream", map[string]string{"env": "prod", "team": "platform"})
			},
			keysToRemove:   []string{"env"},
			wantAbsentKeys: []string{"env"},
			wantTags:       map[string]string{"team": "platform"},
		},
		{
			name:         "not_found",
			streamName:   "nonexistent",
			keysToRemove: []string{"k"},
			wantErr:      firehose.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			err := b.UntagDeliveryStream(tt.streamName, tt.keysToRemove)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			tags, err := b.ListTagsForDeliveryStream(tt.streamName)
			require.NoError(t, err)
			for _, k := range tt.wantAbsentKeys {
				assert.NotContains(t, tags, k)
			}
			for k, v := range tt.wantTags {
				assert.Equal(t, v, tags[k])
			}
		})
	}
}

func TestListTagsForDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(b *firehose.InMemoryBackend)
		streamName string
		wantErr    error
		wantEmpty  bool
	}{
		{
			name:       "not_found",
			streamName: "nonexistent",
			wantErr:    firehose.ErrNotFound,
		},
		{
			name:       "empty",
			streamName: "empty-tags",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream("empty-tags")
			},
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			tags, err := b.ListTagsForDeliveryStream(tt.streamName)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			if tt.wantEmpty {
				assert.Empty(t, tags)
			}
		})
	}
}
