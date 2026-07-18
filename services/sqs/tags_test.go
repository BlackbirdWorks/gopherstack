package sqs_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTags_CreateWithTags(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "tagged-queue",
		Endpoint:  "localhost",
		Tags:      map[string]string{"env": "test", "team": "platform"},
	})
	require.NoError(t, err)

	tagsOut, err := b.ListQueueTags(&sqs.ListQueueTagsInput{QueueURL: out.QueueURL})
	require.NoError(t, err)
	assert.Equal(t, "test", tagsOut.Tags.Clone()["env"])
	assert.Equal(t, "platform", tagsOut.Tags.Clone()["team"])
}

func TestTags_TagAndUntag(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "tag-untag")

	require.NoError(t, b.TagQueue(&sqs.TagQueueInput{
		QueueURL: qURL,
		Tags:     tagsFromMap(map[string]string{"k1": "v1", "k2": "v2"}),
	}))

	out, err := b.ListQueueTags(&sqs.ListQueueTagsInput{QueueURL: qURL})
	require.NoError(t, err)
	m := out.Tags.Clone()
	assert.Equal(t, "v1", m["k1"])
	assert.Equal(t, "v2", m["k2"])

	require.NoError(t, b.UntagQueue(&sqs.UntagQueueInput{QueueURL: qURL, TagKeys: []string{"k1"}}))

	out2, err := b.ListQueueTags(&sqs.ListQueueTagsInput{QueueURL: qURL})
	require.NoError(t, err)
	m2 := out2.Tags.Clone()
	_, hasK1 := m2["k1"]
	assert.False(t, hasK1)
	assert.Equal(t, "v2", m2["k2"])
}

func TestTags_ListQueueTags_Empty(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "no-tags")
	out, err := b.ListQueueTags(&sqs.ListQueueTagsInput{QueueURL: qURL})
	require.NoError(t, err)
	assert.Empty(t, out.Tags.Clone())
}

func TestTags_TagQueue_NotFound(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	err := b.TagQueue(&sqs.TagQueueInput{
		QueueURL: "http://localhost/000000000000/ghost",
		Tags:     tagsFromMap(map[string]string{"k": "v"}),
	})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestTags_UntagQueue_NotFound(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	err := b.UntagQueue(&sqs.UntagQueueInput{
		QueueURL: "http://localhost/000000000000/ghost",
		TagKeys:  []string{"k"},
	})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestTags_OverwriteExisting(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "tag-overwrite")

	require.NoError(t, b.TagQueue(&sqs.TagQueueInput{
		QueueURL: qURL,
		Tags:     tagsFromMap(map[string]string{"env": "dev"}),
	}))
	require.NoError(t, b.TagQueue(&sqs.TagQueueInput{
		QueueURL: qURL,
		Tags:     tagsFromMap(map[string]string{"env": "prod"}),
	}))

	out, err := b.ListQueueTags(&sqs.ListQueueTagsInput{QueueURL: qURL})
	require.NoError(t, err)
	assert.Equal(t, "prod", out.Tags.Clone()["env"])
}

func TestUntagQueue_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		name     string
		queueURL string
	}{
		{
			name:     "queue_not_found",
			queueURL: queueURL("nonexistent-untag"),
			wantErr:  sqs.ErrQueueNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			err := b.UntagQueue(&sqs.UntagQueueInput{
				QueueURL: tt.queueURL,
				TagKeys:  []string{"env"},
			})
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestTaggedQueues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantLen  int
		withTags bool
	}{
		{name: "no_queues", wantLen: 0, withTags: false},
		{name: "queue_with_tags_included", wantLen: 1, withTags: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			if tt.withTags {
				qURL := createTestQueue(t, b, "tagged-queue")
				require.NoError(t, b.TagQueue(&sqs.TagQueueInput{
					QueueURL: qURL,
				}))
			}

			result := b.TaggedQueues()
			assert.Len(t, result, tt.wantLen)
		})
	}
}

func TestTagQueueByARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		arn     string
	}{
		{
			name:    "not_found_by_arn",
			arn:     "arn:aws:sqs:us-east-1:000000000000:nonexistent",
			wantErr: sqs.ErrQueueNotFound,
		},
		{
			name:    "tags_applied_by_arn",
			arn:     "", // set after queue creation
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qARN := tt.arn

			if qARN == "" {
				createTestQueue(t, b, "arn-tag-queue")
				attrs, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
					QueueURL:       queueURL("arn-tag-queue"),
					AttributeNames: []string{"QueueArn"},
				})
				require.NoError(t, err)
				qARN = attrs.Attributes["QueueArn"]
			}

			err := b.TagQueueByARN(qARN, map[string]string{"env": "test"})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUntagQueueByARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		arn     string
	}{
		{
			name:    "not_found_by_arn",
			arn:     "arn:aws:sqs:us-east-1:000000000000:nonexistent",
			wantErr: sqs.ErrQueueNotFound,
		},
		{
			name:    "tags_removed_by_arn",
			arn:     "", // set after queue creation
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qARN := tt.arn

			if qARN == "" {
				createTestQueue(t, b, "arn-untag-queue")
				require.NoError(t, b.TagQueue(&sqs.TagQueueInput{QueueURL: queueURL("arn-untag-queue")}))
				attrs, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
					QueueURL:       queueURL("arn-untag-queue"),
					AttributeNames: []string{"QueueArn"},
				})
				require.NoError(t, err)
				qARN = attrs.Attributes["QueueArn"]
			}

			err := b.UntagQueueByARN(qARN, []string{"env"})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

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

			b := newBackend(t)

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

			b := newBackend(t)
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

			b := newBackend(t)
			url := createTestQueue(t, b, tt.queueName)

			out, err := b.ListQueueTags(&sqs.ListQueueTagsInput{QueueURL: url})
			require.NoError(t, err)

			if tt.wantEmpty {
				assert.Empty(t, out.Tags.Clone())
			}
		})
	}
}
