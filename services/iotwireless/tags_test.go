package iotwireless_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

func TestInMemoryBackend_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupTags  map[string]string
		addTags    map[string]string
		wantTags   map[string]string
		name       string
		removeTags []string
	}{
		{
			name:      "add_tags",
			setupTags: nil,
			addTags:   map[string]string{"env": "prod", "team": "platform"},
			wantTags:  map[string]string{"env": "prod", "team": "platform"},
		},
		{
			name:       "remove_tags",
			setupTags:  map[string]string{"env": "prod", "team": "platform"},
			removeTags: []string{"team"},
			wantTags:   map[string]string{"env": "prod"},
		},
		{
			name:      "list_tags_empty_arn",
			setupTags: nil,
			addTags:   nil,
			wantTags:  map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := iotwireless.NewInMemoryBackend()

			sp, err := bk.CreateServiceProfile(testAccountID, testRegion, "sp-tag-test", nil, tt.setupTags)
			require.NoError(t, err)

			if tt.addTags != nil {
				err = bk.TagResource(sp.ARN, tt.addTags)
				require.NoError(t, err)
			}

			if tt.removeTags != nil {
				err = bk.UntagResource(sp.ARN, tt.removeTags)
				require.NoError(t, err)
			}

			tags, err := bk.ListTagsForResource(sp.ARN)
			require.NoError(t, err)

			for k, v := range tt.wantTags {
				assert.Equal(t, v, tags[k])
			}
		})
	}
}

func TestInMemoryBackend_ListTagsForResource_UnknownARN(t *testing.T) {
	t.Parallel()

	bk := iotwireless.NewInMemoryBackend()

	tags, err := bk.ListTagsForResource(
		"arn:aws:iotwireless:us-east-1:000000000000:ServiceProfile/unknown",
	)
	require.NoError(t, err)
	assert.Empty(t, tags)
}

func TestInMemoryBackend_UntagResource_CleansEmptyMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setupTags    map[string]string
		removeTags   []string
		wantTagsLeft int
	}{
		{
			name:         "removing_all_tags_cleans_map",
			setupTags:    map[string]string{"env": "prod", "team": "platform"},
			removeTags:   []string{"env", "team"},
			wantTagsLeft: 0,
		},
		{
			name:         "removing_some_tags_leaves_rest",
			setupTags:    map[string]string{"env": "prod", "team": "platform"},
			removeTags:   []string{"team"},
			wantTagsLeft: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := iotwireless.NewInMemoryBackend()

			sp, err := bk.CreateServiceProfile(testAccountID, testRegion, "sp-cleanup", nil, tt.setupTags)
			require.NoError(t, err)

			err = bk.UntagResource(sp.ARN, tt.removeTags)
			require.NoError(t, err)

			tags, err := bk.ListTagsForResource(sp.ARN)
			require.NoError(t, err)
			assert.Len(t, tags, tt.wantTagsLeft)
		})
	}
}
