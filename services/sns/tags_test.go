package sns_test

import (
	"net/http"
	"net/url"
	"testing"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSNSHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		form             url.Values
		name             string
		wantBodyContains []string
		wantStatus       int
	}{
		{
			name: "tag_resource",
			form: url.Values{
				"Action":              {"TagResource"},
				"ResourceArn":         {"arn:aws:sns:us-east-1:000000000000:tag-topic"},
				"Tags.member.1.Key":   {"env"},
				"Tags.member.1.Value": {"prod"},
				"Tags.member.2.Key":   {"team"},
				"Tags.member.2.Value": {"infra"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"TagResourceResponse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sns.NewInMemoryBackend()
			b.CreateTopic("tag-topic", nil)
			h := sns.NewHandler(b)
			rec := snsPost(t, h, tt.form)
			assert.Equal(t, tt.wantStatus, rec.Code)
			for _, want := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestSNSHandler_UntagResource(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	b.CreateTopic("untag-topic", nil)
	topicArn := "arn:aws:sns:us-east-1:000000000000:untag-topic"
	b.SetTopicTags(topicArn, svcTags.FromMap("test.sns.untag", map[string]string{"env": "prod", "team": "infra"}))

	h := sns.NewHandler(b)

	rec := snsPost(t, h, url.Values{
		"Action":           {"UntagResource"},
		"ResourceArn":      {topicArn},
		"TagKeys.member.1": {"team"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "UntagResourceResponse")

	// Verify only "env" remains.
	remaining := b.GetTopicTags(topicArn)
	assert.Len(t, remaining, 1)
	assert.Equal(t, "prod", remaining["env"])
}

func TestSNSHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	b.CreateTopic("listtag-topic", nil)
	topicArn := "arn:aws:sns:us-east-1:000000000000:listtag-topic"
	b.SetTopicTags(topicArn, svcTags.FromMap("test.sns.list", map[string]string{"env": "staging"}))

	h := sns.NewHandler(b)

	rec := snsPost(t, h, url.Values{
		"Action":      {"ListTagsForResource"},
		"ResourceArn": {topicArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListTagsForResourceResponse")
	assert.Contains(t, rec.Body.String(), "env")
	assert.Contains(t, rec.Body.String(), "staging")
}

// TestTaggedTopics verifies TaggedTopics returns topic info.
func TestTaggedTopics(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	top, err := b.CreateTopic("my-topic", nil)
	require.NoError(t, err)

	topics := b.TaggedTopics()
	require.Len(t, topics, 1)
	assert.Equal(t, top.TopicArn, topics[0].ARN)
}

// TestTagTopicByARN verifies TagTopicByARN.
func TestTagTopicByARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*sns.InMemoryBackend) string
		tags    map[string]string
		name    string
		wantErr bool
	}{
		{
			name: "existing_topic",
			setup: func(b *sns.InMemoryBackend) string {
				top, err := b.CreateTopic("tag-me", nil)
				require.NoError(t, err)

				return top.TopicArn
			},
			tags: map[string]string{"env": "prod"},
		},
		{
			name: "not_found",
			setup: func(*sns.InMemoryBackend) string {
				return "arn:aws:sns:us-east-1:000000000000:no-such"
			},
			tags:    map[string]string{"env": "prod"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			topicARN := tt.setup(b)

			err := b.TagTopicByARN(topicARN, tt.tags)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestUntagTopicByARN verifies UntagTopicByARN.
func TestUntagTopicByARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(*sns.InMemoryBackend) string
		keys    []string
		wantErr bool
	}{
		{
			name: "existing_topic_with_tags",
			setup: func(b *sns.InMemoryBackend) string {
				top, err := b.CreateTopic("untag-me", nil)
				require.NoError(t, err)
				require.NoError(t, b.TagTopicByARN(top.TopicArn, map[string]string{"env": "prod"}))

				return top.TopicArn
			},
			keys: []string{"env"},
		},
		{
			name: "existing_topic_no_tags",
			setup: func(b *sns.InMemoryBackend) string {
				top, err := b.CreateTopic("no-tags", nil)
				require.NoError(t, err)

				return top.TopicArn
			},
			keys: []string{"env"},
		},
		{
			name: "not_found",
			setup: func(*sns.InMemoryBackend) string {
				return "arn:aws:sns:us-east-1:000000000000:no-such"
			},
			keys:    []string{"env"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			topicARN := tt.setup(b)

			err := b.UntagTopicByARN(topicARN, tt.keys)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
