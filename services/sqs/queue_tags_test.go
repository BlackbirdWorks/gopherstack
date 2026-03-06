package sqs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/sqs"
)

func TestTagQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		inputTags map[string]string
		wantTags  map[string]string
		name      string
		queueName string
		queueURL  string
	}{
		{
			name:      "tags queue successfully",
			queueName: "tag-test-queue",
			inputTags: map[string]string{"env": "test", "team": "platform"},
			wantTags:  map[string]string{"env": "test", "team": "platform"},
		},
		{
			name:      "queue not found",
			queueURL:  "http://localhost:4566/000000000000/nonexistent",
			inputTags: map[string]string{"key": "value"},
			wantErr:   sqs.ErrQueueNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			url := tt.queueURL
			if tt.queueName != "" {
				url = createTestQueue(t, b, tt.queueName)
			}

			err := b.TagQueue(&sqs.TagQueueInput{
				QueueURL: url,
				Tags:     tags.FromMap("test", tt.inputTags),
			})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			out, err := b.ListQueueTags(&sqs.ListQueueTagsInput{QueueURL: url})
			require.NoError(t, err)
			assert.Equal(t, tt.wantTags, out.Tags.Clone())
		})
	}
}

func TestUntagQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		initialTags map[string]string
		wantTags    map[string]string
		name        string
		queueName   string
		removeKeys  []string
	}{
		{
			name:        "removes specified tag",
			queueName:   "untag-test-queue",
			initialTags: map[string]string{"env": "test", "team": "platform"},
			removeKeys:  []string{"team"},
			wantTags:    map[string]string{"env": "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			url := createTestQueue(t, b, tt.queueName)

			err := b.TagQueue(&sqs.TagQueueInput{
				QueueURL: url,
				Tags:     tags.FromMap("test", tt.initialTags),
			})
			require.NoError(t, err)

			err = b.UntagQueue(&sqs.UntagQueueInput{
				QueueURL: url,
				TagKeys:  tt.removeKeys,
			})
			require.NoError(t, err)

			out, err := b.ListQueueTags(&sqs.ListQueueTagsInput{QueueURL: url})
			require.NoError(t, err)
			assert.Equal(t, tt.wantTags, out.Tags.Clone())
		})
	}
}

func TestListQueueTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queueName string
		wantEmpty bool
	}{
		{
			name:      "returns empty tags for untagged queue",
			queueName: "empty-tags-queue",
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			url := createTestQueue(t, b, tt.queueName)

			out, err := b.ListQueueTags(&sqs.ListQueueTagsInput{QueueURL: url})
			require.NoError(t, err)

			if tt.wantEmpty {
				assert.Empty(t, out.Tags.Clone())
			}
		})
	}
}
