package iotanalytics_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

func TestInMemoryBackend_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		channelName string
		tags        map[string]string
		removeTags  []string
		wantCount   int
	}{
		{
			name:        "tag_and_list",
			channelName: "tagged_channel",
			tags:        map[string]string{"env": "test", "team": "ops"},
			wantCount:   2,
		},
		{
			name:        "tag_and_untag",
			channelName: "untagged_channel",
			tags:        map[string]string{"env": "test", "team": "ops"},
			removeTags:  []string{"env"},
			wantCount:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotanalytics.NewInMemoryBackend()

			ch, err := b.CreateChannel(context.Background(), tt.channelName, nil, nil, nil)
			require.NoError(t, err)

			tagList := make([]iotanalytics.ExportedTagDTO, 0, len(tt.tags))
			for k, v := range tt.tags {
				tagList = append(tagList, iotanalytics.ExportedTagDTO{Key: k, Value: v})
			}

			err = b.TagResource(ch.ARN, tagList)
			require.NoError(t, err)

			if len(tt.removeTags) > 0 {
				err = b.UntagResource(ch.ARN, tt.removeTags)
				require.NoError(t, err)
			}

			got, err := b.ListTagsForResource(ch.ARN)
			require.NoError(t, err)
			assert.Len(t, got, tt.wantCount)
		})
	}
}

func TestResolveARNResource_CrossRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		arn     string
		wantErr bool
	}{
		{
			name:    "channel default region resolves",
			arn:     "arn:aws:iotanalytics:us-east-1:000000000000:channel/chxr",
			wantErr: false,
		},
		{
			name:    "channel eu-west-1 cross-region resolves",
			arn:     "arn:aws:iotanalytics:eu-west-1:111122223333:channel/chxr",
			wantErr: false,
		},
		{
			name:    "datastore ap-southeast-2 cross-region resolves",
			arn:     "arn:aws:iotanalytics:ap-southeast-2:999988887777:datastore/dsxr",
			wantErr: false,
		},
		{
			name:    "unknown resource type not found",
			arn:     "arn:aws:iotanalytics:us-east-1:000000000000:unknown/chxr",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotanalytics.NewInMemoryBackend()
			ctx := context.Background()

			_, err := b.CreateChannel(ctx, "chxr", nil, nil, nil)
			require.NoError(t, err)
			_, err = b.CreateDatastore(ctx, "dsxr", nil, nil, nil, nil, nil)
			require.NoError(t, err)

			_, err = b.ListTagsForResource(tt.arn)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestInMemoryBackend_SortedListTagsForResource verifies ListTagsForResource returns tags
// sorted by key.
func TestInMemoryBackend_SortedListTagsForResource(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()
	ch, err := b.CreateChannel(context.Background(), "tagged_ch", map[string]string{
		"zzz": "last",
		"aaa": "first",
		"mmm": "middle",
	}, nil, nil)
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(ch.ARN)
	require.NoError(t, err)
	require.Len(t, tags, 3)
	assert.Equal(t, "aaa", tags[0].Key)
	assert.Equal(t, "mmm", tags[1].Key)
	assert.Equal(t, "zzz", tags[2].Key)
}

// TestInMemoryBackend_ResourceNotFoundForTags verifies ListTagsForResource returns
// ErrResourceNotFound for an ARN that does not resolve to any known resource.
func TestInMemoryBackend_ResourceNotFoundForTags(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()

	_, err := b.ListTagsForResource("arn:aws:iotanalytics:us-east-1:000000000000:channel/no-such")
	require.Error(t, err)
	assert.ErrorIs(t, err, iotanalytics.ErrResourceNotFound)
}
