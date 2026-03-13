package pinpoint_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

func newTestBackend() *pinpoint.InMemoryBackend {
	return pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
}

func TestBackend_CreateApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		appName   string
		tags      map[string]string
		wantName  string
		wantTagKV [2]string
		wantErr   bool
	}{
		{
			name:     "creates_app",
			appName:  "my-app",
			wantName: "my-app",
		},
		{
			name:      "creates_app_with_tags",
			appName:   "tagged-app",
			tags:      map[string]string{"env": "test"},
			wantName:  "tagged-app",
			wantTagKV: [2]string{"env", "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			app, err := b.CreateApp("us-east-1", "123456789012", tt.appName, tt.tags)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, app)
			assert.Equal(t, tt.wantName, app.Name)
			assert.NotEmpty(t, app.ID)
			assert.NotEmpty(t, app.ARN)
			assert.NotEmpty(t, app.CreationDate)

			if tt.wantTagKV[0] != "" {
				assert.Equal(t, tt.wantTagKV[1], app.Tags[tt.wantTagKV[0]])
			}
		})
	}
}

func TestBackend_GetApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appName string
		wantErr bool
	}{
		{
			name:    "gets_existing_app",
			appName: "my-app",
			wantErr: false,
		},
		{
			name:    "returns_error_for_missing_app",
			appName: "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var appID string

			if tt.appName != "" {
				app, err := b.CreateApp("us-east-1", "123456789012", tt.appName, nil)
				require.NoError(t, err)

				appID = app.ID
			} else {
				appID = "nonexistent-id"
			}

			got, err := b.GetApp(appID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, tt.appName, got.Name)
		})
	}
}

func TestBackend_DeleteApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appName string
		wantErr bool
	}{
		{
			name:    "deletes_existing_app",
			appName: "to-delete",
			wantErr: false,
		},
		{
			name:    "returns_error_for_missing_app",
			appName: "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var appID string

			if tt.appName != "" {
				app, err := b.CreateApp("us-east-1", "123456789012", tt.appName, nil)
				require.NoError(t, err)

				appID = app.ID
			} else {
				appID = "nonexistent-id"
			}

			deleted, err := b.DeleteApp(appID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.appName, deleted.Name)

			_, err = b.GetApp(appID)
			require.Error(t, err)
		})
	}
}

func TestBackend_GetApps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		appNames  []string
		wantCount int
	}{
		{
			name:      "returns_empty_list",
			appNames:  nil,
			wantCount: 0,
		},
		{
			name:      "returns_all_apps",
			appNames:  []string{"app-a", "app-b", "app-c"},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			for _, n := range tt.appNames {
				_, err := b.CreateApp("us-east-1", "123456789012", n, nil)
				require.NoError(t, err)
			}

			apps, err := b.GetApps()

			require.NoError(t, err)
			assert.Len(t, apps, tt.wantCount)
		})
	}
}

func TestBackend_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tagKey   string
		tagValue string
	}{
		{
			name:     "tag_and_untag_resource",
			tagKey:   "owner",
			tagValue: "team-a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			app, err := b.CreateApp("us-east-1", "123456789012", "tagtest", nil)
			require.NoError(t, err)

			err = b.TagResource(app.ARN, map[string]string{tt.tagKey: tt.tagValue})
			require.NoError(t, err)

			tags, err := b.ListTagsForResource(app.ARN)
			require.NoError(t, err)
			assert.Equal(t, tt.tagValue, tags[tt.tagKey])

			err = b.UntagResource(app.ARN, []string{tt.tagKey})
			require.NoError(t, err)

			tags, err = b.ListTagsForResource(app.ARN)
			require.NoError(t, err)
			assert.Empty(t, tags[tt.tagKey])
		})
	}
}
