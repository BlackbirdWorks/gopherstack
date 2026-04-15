package sns_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sns"
)

func newR1Backend(t *testing.T) *sns.InMemoryBackend {
	t.Helper()

	return sns.NewInMemoryBackendWithContext(t.Context(), "000000000000", "us-east-1")
}

// TestRefinement1_ErrNilAppContext verifies the provider nil guard.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &sns.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, sns.ErrNilAppContext)
}

// TestRefinement1_ProviderInit verifies normal provider init.
func TestRefinement1_ProviderInit(t *testing.T) {
	t.Parallel()

	p := &sns.Provider{}
	reg, err := p.Init(&service.AppContext{JanitorCtx: context.Background()})
	require.NoError(t, err)
	assert.NotNil(t, reg)
}

// TestRefinement1_SetPublishEmitter verifies the emitter can be set.
func TestRefinement1_SetPublishEmitter(t *testing.T) {
	t.Parallel()

	b := newR1Backend(t)
	// SetPublishEmitter accepts nil - just exercises the covered path.
	b.SetPublishEmitter(nil)
}

// TestRefinement1_TaggedTopics verifies TaggedTopics returns topic info.
func TestRefinement1_TaggedTopics(t *testing.T) {
	t.Parallel()

	b := newR1Backend(t)
	top, err := b.CreateTopic("my-topic", nil)
	require.NoError(t, err)

	topics := b.TaggedTopics()
	require.Len(t, topics, 1)
	assert.Equal(t, top.TopicArn, topics[0].ARN)
}

// TestRefinement1_TagTopicByARN verifies TagTopicByARN.
func TestRefinement1_TagTopicByARN(t *testing.T) {
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

			b := newR1Backend(t)
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

// TestRefinement1_UntagTopicByARN verifies UntagTopicByARN.
func TestRefinement1_UntagTopicByARN(t *testing.T) {
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

			b := newR1Backend(t)
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

// TestRefinement1_ListAllPlatformApplications verifies list works.
func TestRefinement1_ListAllPlatformApplications(t *testing.T) {
	t.Parallel()

	b := newR1Backend(t)
	apps := b.ListAllPlatformApplications()
	assert.Empty(t, apps)

	_, err := b.CreatePlatformApplication("my-app", "GCM", map[string]string{
		"PlatformCredential": "fake-cred",
	})
	require.NoError(t, err)

	apps = b.ListAllPlatformApplications()
	assert.Len(t, apps, 1)
}

// TestRefinement1_StorageBackendInterface verifies var_ assertion compiles.
func TestRefinement1_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ sns.StorageBackend = (*sns.InMemoryBackend)(nil)
}

// TestRefinement1_HandlerOpsLen verifies GetSupportedOperations count.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	b := newR1Backend(t)
	h := sns.NewHandler(b)
	assert.Len(t, h.GetSupportedOperations(), 42)
}

// TestRefinement1_SDKOpsSorted verifies GetSupportedOperations is sorted.
func TestRefinement1_SDKOpsSorted(t *testing.T) {
	t.Parallel()

	b := newR1Backend(t)
	h := sns.NewHandler(b)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}
