package autoscaling_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

func TestInMemoryBackend_CreateOrUpdateTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		wantTag autoscaling.Tag
		name    string
		tags    []autoscaling.ResourceTag
		wantErr bool
	}{
		{
			name: "create_tag",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "tag-g",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			tags: []autoscaling.ResourceTag{
				{ResourceID: "tag-g", ResourceType: "auto-scaling-group", Key: "env", Value: "prod"},
			},
			wantTag: autoscaling.Tag{Key: "env", Value: "prod"},
		},
		{
			name: "update_existing_tag",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "upd-tag-g",
					MinSize:              0,
					MaxSize:              5,
					Tags:                 []autoscaling.Tag{{Key: "env", Value: "dev"}},
				})
			},
			tags: []autoscaling.ResourceTag{
				{ResourceID: "upd-tag-g", ResourceType: "auto-scaling-group", Key: "env", Value: "prod"},
			},
			wantTag: autoscaling.Tag{Key: "env", Value: "prod"},
		},
		{
			name: "group_not_found",
			tags: []autoscaling.ResourceTag{
				{ResourceID: "no-such", ResourceType: "auto-scaling-group", Key: "k", Value: "v"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.CreateOrUpdateTags(tt.tags)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tt.wantTag.Key != "" {
				groups, gErr := b.DescribeAutoScalingGroups([]string{tt.tags[0].ResourceID})
				require.NoError(t, gErr)
				found := false
				for _, tag := range groups[0].Tags {
					if tag.Key == tt.wantTag.Key {
						assert.Equal(t, tt.wantTag.Value, tag.Value)
						found = true

						break
					}
				}
				assert.True(t, found, "expected tag %q not found", tt.wantTag.Key)
			}
		})
	}
}

func TestInMemoryBackend_DeleteAndDescribeTags(t *testing.T) {
	t.Parallel()

	b := autoscaling.NewInMemoryBackend()
	_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "tag-asg",
		MinSize:              0,
		MaxSize:              5,
		Tags:                 []autoscaling.Tag{{Key: "env", Value: "prod"}, {Key: "team", Value: "platform"}},
	})
	require.NoError(t, err)

	// DescribeTags returns all tags
	tags, err := b.DescribeTags(nil)
	require.NoError(t, err)
	assert.Len(t, tags, 2)

	// DeleteTags removes the env tag
	err = b.DeleteTags([]autoscaling.ResourceTag{
		{ResourceID: "tag-asg", ResourceType: "auto-scaling-group", Key: "env"},
	})
	require.NoError(t, err)

	remaining, err := b.DescribeTags(nil)
	require.NoError(t, err)
	assert.Len(t, remaining, 1)
	assert.Equal(t, "team", remaining[0].Key)
}

func TestInMemoryBackend_DescribeTags_WithFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *autoscaling.InMemoryBackend)
		name      string
		filters   []autoscaling.TagFilter
		wantCount int
	}{
		{
			name: "no_filters_returns_all",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "tfilter-asg",
					MinSize:              0,
					MaxSize:              5,
					Tags: []autoscaling.Tag{
						{Key: "env", Value: "prod"},
						{Key: "team", Value: "platform"},
					},
				})
			},
			filters:   nil,
			wantCount: 2,
		},
		{
			name: "filter_by_key",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "tfilter2-asg",
					MinSize:              0,
					MaxSize:              5,
					Tags: []autoscaling.Tag{
						{Key: "env", Value: "prod"},
						{Key: "team", Value: "platform"},
					},
				})
			},
			filters:   []autoscaling.TagFilter{{Name: "key", Values: []string{"env"}}},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			tags, err := b.DescribeTags(tt.filters)
			require.NoError(t, err)
			assert.Len(t, tags, tt.wantCount)
		})
	}
}
