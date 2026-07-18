package appsync_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestTagResource_GraphqlAPI_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	err = b.TagResource(api.APIID, map[string]string{
		"env":  "prod",
		"team": "platform",
	})
	require.NoError(t, err)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	require.NotNil(t, got.Tags)
	v, _ := got.Tags.Get("env")
	assert.Equal(t, "prod", v)
}

func TestUntagResource_GraphqlAPI(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, map[string]string{
		"env":  "prod",
		"team": "platform",
	}, nil)
	require.NoError(t, err)

	err = b.UntagResource(api.APIID, []string{"env"})
	require.NoError(t, err)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	_, ok := got.Tags.Get("env")
	assert.False(t, ok)
	v, ok := got.Tags.Get("team")
	assert.True(t, ok)
	assert.Equal(t, "platform", v)
}

func TestInMemoryBackend_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*appsync.InMemoryBackend) string
		name       string
		wantErr    bool
		wantTagLen int
	}{
		{
			name: "tag_and_untag",
			setup: func(b *appsync.InMemoryBackend) string {
				api, _ := b.CreateGraphqlAPI("T", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

				return api.APIID
			},
			wantTagLen: 1,
		},
		{
			name: "tag_not_found",
			setup: func(_ *appsync.InMemoryBackend) string {
				return "nonexistent"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			apiID := tt.setup(b)

			err := b.TagResource(apiID, map[string]string{"key": "val", "env": "prod"})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			err = b.UntagResource(apiID, []string{"env"})
			require.NoError(t, err)

			tags, err := b.ListTagsForResource(apiID)
			require.NoError(t, err)
			assert.Len(t, tags, tt.wantTagLen)
			assert.Equal(t, "val", tags["key"])
			assert.NotContains(t, tags, "env")
		})
	}
}

func TestInMemoryBackend_ListTagsForResource_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.ListTagsForResource("nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_ListTagsForResource_EmptyTags(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// No tags set yet — should return empty map.
	tagMap, err := b.ListTagsForResource(api.APIID)
	require.NoError(t, err)
	assert.Empty(t, tagMap)
}
