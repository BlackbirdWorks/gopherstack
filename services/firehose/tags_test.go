package firehose_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

func TestTagDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(b *firehose.InMemoryBackend)
		tags       map[string]string
		wantTags   map[string]string
		name       string
		streamName string
	}{
		{
			name:       "success",
			streamName: "tagged-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "tagged-stream"})
			},
			tags:     map[string]string{"env": "prod", "team": "platform"},
			wantTags: map[string]string{"env": "prod", "team": "platform"},
		},
		{
			name:       "overwrite",
			streamName: "overwrite-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream(
					context.TODO(),
					firehose.CreateDeliveryStreamInput{Name: "overwrite-stream"},
				)
				_ = b.TagDeliveryStream(context.TODO(), "overwrite-stream", map[string]string{"env": "dev"})
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
			err := b.TagDeliveryStream(context.TODO(), tt.streamName, tt.tags)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			tags, err := b.ListTagsForDeliveryStream(context.TODO(), tt.streamName)
			require.NoError(t, err)
			for k, v := range tt.wantTags {
				assert.Equal(t, v, tags[k])
			}
		})
	}
}

// TestDeleteDeliveryStream_ClosesTags verifies that Tags resources are released when a
// stream is deleted, preventing Prometheus registry leaks.
func TestDeleteDeliveryStream_ClosesTags(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name: "tag-leak-stream",
	})
	require.NoError(t, err)

	// Tag the stream so the Tags collection is active.
	require.NoError(t, b.TagDeliveryStream(context.TODO(), "tag-leak-stream", map[string]string{"env": "test"}))

	// Delete must succeed without panicking; tags are closed internally.
	require.NoError(t, b.DeleteDeliveryStream(context.TODO(), "tag-leak-stream"))

	// Subsequent lookup must return not-found.
	_, descErr := b.DescribeDeliveryStream(context.TODO(), "tag-leak-stream")
	require.Error(t, descErr)
	assert.ErrorIs(t, descErr, firehose.ErrNotFound)
}

func TestUntagDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr        error
		setup          func(b *firehose.InMemoryBackend)
		wantTags       map[string]string
		name           string
		streamName     string
		keysToRemove   []string
		wantAbsentKeys []string
	}{
		{
			name:       "success",
			streamName: "untag-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "untag-stream"})
				_ = b.TagDeliveryStream(
					context.TODO(),
					"untag-stream",
					map[string]string{"env": "prod", "team": "platform"},
				)
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
			err := b.UntagDeliveryStream(context.TODO(), tt.streamName, tt.keysToRemove)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			tags, err := b.ListTagsForDeliveryStream(context.TODO(), tt.streamName)
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
		wantErr    error
		setup      func(b *firehose.InMemoryBackend)
		name       string
		streamName string
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
				_, _ = b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "empty-tags"})
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
			tags, err := b.ListTagsForDeliveryStream(context.TODO(), tt.streamName)
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
