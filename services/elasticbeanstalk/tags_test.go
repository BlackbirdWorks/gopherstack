package elasticbeanstalk_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

func TestInMemoryBackend_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs  error
		name       string
		wantErr    bool
		useRealARN bool
	}{
		{
			name:       "list tags for app",
			useRealARN: true,
		},
		{
			name:      "list tags for nonexistent",
			wantErr:   true,
			wantErrIs: awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			app, _ := b.CreateApplication(context.Background(), "tag-app", "", map[string]string{"key1": "val1"})

			resourceARN := "nonexistent-arn"
			if tt.useRealARN {
				resourceARN = app.ApplicationARN
			}

			tags, err := b.ListTagsForResource(context.Background(), resourceARN)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "val1", tags["key1"])
		})
	}
}

func TestInMemoryBackend_UpdateTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addTags    map[string]string
		removeTags map[string]string
		wantTags   map[string]string
		name       string
		wantErr    bool
	}{
		{
			name:     "add tags",
			addTags:  map[string]string{"k2": "v2"},
			wantTags: map[string]string{"k1": "v1", "k2": "v2"},
		},
		{
			name:       "remove tags",
			removeTags: map[string]string{"k1": ""},
			wantTags:   map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			app, _ := b.CreateApplication(context.Background(), "tag-app", "", map[string]string{"k1": "v1"})

			err := b.UpdateTagsForResource(context.Background(), app.ApplicationARN, tt.addTags, tt.removeTags)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			tags, _ := b.ListTagsForResource(context.Background(), app.ApplicationARN)

			for k, v := range tt.wantTags {
				assert.Equal(t, v, tags[k])
			}

			for k := range tt.removeTags {
				_, exists := tags[k]
				assert.False(t, exists, "removed tag should not exist")
			}
		})
	}
}

// TestInMemoryBackend_TagsReachConfigTemplateAndPlatformVersion verifies that tags
// supplied to CreateConfigurationTemplate and CreatePlatformVersion (real AWS:
// "Elastic Beanstalk supports tagging of all of its resources") are actually
// reachable through List/UpdateTagsForResource, not just stored and orphaned.
func TestInMemoryBackend_TagsReachConfigTemplateAndPlatformVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		buildARN func(b *elasticbeanstalk.InMemoryBackend) string
		name     string
	}{
		{
			name: "configuration template",
			buildARN: func(b *elasticbeanstalk.InMemoryBackend) string {
				ctx := context.Background()
				_, _ = b.CreateApplication(ctx, "tmpl-app", "", nil)
				tmpl, err := b.CreateConfigurationTemplate(
					ctx, "tmpl-app", "tmpl1", "", "", map[string]string{"k1": "v1"},
				)
				require.NoError(t, err)

				return "arn:aws:elasticbeanstalk:us-east-1:123456789012:configurationtemplate/tmpl-app/" +
					tmpl.TemplateName
			},
		},
		{
			name: "platform version",
			buildARN: func(b *elasticbeanstalk.InMemoryBackend) string {
				pv, err := b.CreatePlatformVersion(
					context.Background(), "my-platform", "1.0.0", map[string]string{"k1": "v1"},
				)
				require.NoError(t, err)

				return pv.PlatformArn
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			resourceARN := tt.buildARN(b)

			tags, err := b.ListTagsForResource(context.Background(), resourceARN)
			require.NoError(t, err)
			assert.Equal(t, "v1", tags["k1"])

			err = b.UpdateTagsForResource(
				context.Background(), resourceARN, map[string]string{"k2": "v2"}, map[string]string{"k1": ""},
			)
			require.NoError(t, err)

			tags, err = b.ListTagsForResource(context.Background(), resourceARN)
			require.NoError(t, err)
			assert.Equal(t, "v2", tags["k2"])
			_, hasK1 := tags["k1"]
			assert.False(t, hasK1, "removed tag should not exist")
		})
	}
}
