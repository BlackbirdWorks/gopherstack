package amplify_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/amplify"
)

func TestInMemoryBackend_TagResource_App(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		setup   func(*amplify.InMemoryBackend) string
		tagMap  map[string]string
		name    string
		wantErr bool
	}{
		{
			name: "tags_existing_app",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.ARN
			},
			tagMap: map[string]string{"env": "prod", "team": "backend"},
		},
		{
			name: "returns_not_found_for_invalid_arn",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "arn:aws:amplify:us-east-1:000000000000:apps/nonexistent"
			},
			tagMap:  map[string]string{"env": "test"},
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
		{
			name: "returns_not_found_for_malformed_arn",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "invalid-arn"
			},
			tagMap:  map[string]string{"env": "test"},
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			arn := tt.setup(b)
			err := b.TagResource(arn, tt.tagMap)

			if tt.wantErr {
				require.Error(t, err)

				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)

			tags, listErr := b.ListTagsForResource(arn)
			require.NoError(t, listErr)

			for k, v := range tt.tagMap {
				assert.Equal(t, v, tags[k])
			}
		})
	}
}

func TestInMemoryBackend_TagResource_Branch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		setup   func(*amplify.InMemoryBackend) string
		name    string
		wantErr bool
	}{
		{
			name: "tags_existing_branch",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)
				branch, _ := b.CreateBranch(app.AppID, "main", "", "", false, nil)

				return branch.BranchARN
			},
		},
		{
			name: "returns_not_found_for_nonexistent_branch_arn",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return "arn:aws:amplify:us-east-1:000000000000:apps/" + app.AppID + "/branches/nonexistent"
			},
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			branchARN := tt.setup(b)
			err := b.TagResource(branchARN, map[string]string{"tagged": "yes"})

			if tt.wantErr {
				require.Error(t, err)

				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		setup   func(*amplify.InMemoryBackend) string
		name    string
		tagKeys []string
		wantErr bool
	}{
		{
			name: "removes_specified_tags",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", map[string]string{
					"env":  "prod",
					"team": "backend",
				})

				return app.ARN
			},
			tagKeys: []string{"env"},
		},
		{
			name: "returns_not_found_for_invalid_arn",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "arn:aws:amplify:us-east-1:000000000000:apps/nonexistent"
			},
			tagKeys: []string{"env"},
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			arn := tt.setup(b)
			err := b.UntagResource(arn, tt.tagKeys)

			if tt.wantErr {
				require.Error(t, err)

				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)

			tags, listErr := b.ListTagsForResource(arn)
			require.NoError(t, listErr)

			for _, k := range tt.tagKeys {
				_, exists := tags[k]
				assert.False(t, exists, "tag %s should have been removed", k)
			}
		})
	}
}

func TestInMemoryBackend_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs    error
		setup    func(*amplify.InMemoryBackend) string
		wantTags map[string]string
		name     string
		wantErr  bool
	}{
		{
			name: "returns_tags_for_app",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", map[string]string{"env": "test"})

				return app.ARN
			},
			wantTags: map[string]string{"env": "test"},
		},
		{
			name: "returns_empty_tags",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.ARN
			},
			wantTags: map[string]string{},
		},
		{
			name: "returns_not_found_for_invalid_arn",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "arn:aws:amplify:us-east-1:000000000000:apps/nonexistent"
			},
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
		{
			name: "returns_not_found_for_unsupported_arn",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "arn:aws:amplify:us-east-1:000000000000:domains/example"
			},
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			arn := tt.setup(b)
			tags, err := b.ListTagsForResource(arn)

			if tt.wantErr {
				require.Error(t, err)

				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantTags, tags)
		})
	}
}

func TestInMemoryBackend_TagResource_BranchMissingApp(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	err := b.TagResource(
		"arn:aws:amplify:us-east-1:000000000000:apps/nonexistent/branches/main",
		map[string]string{"k": "v"},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}
