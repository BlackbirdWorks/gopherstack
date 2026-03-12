package mwaa_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

func newTestBackend() *mwaa.InMemoryBackend {
	return mwaa.NewInMemoryBackend("us-east-1", "123456789012")
}

func newCreateReq() *mwaa.ExportedCreateEnvironmentRequest {
	return &mwaa.ExportedCreateEnvironmentRequest{
		DagS3Path:        "dags/",
		ExecutionRoleArn: "arn:aws:iam::123456789012:role/mwaa-role",
		SourceBucketArn:  "arn:aws:s3:::my-bucket",
	}
}

func TestBackend_CreateEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		envName     string
		req         *mwaa.ExportedCreateEnvironmentRequest
		wantStatus  string
		wantVersion string
		wantClass   string
		wantErr     bool
	}{
		{
			name:        "creates_with_defaults",
			envName:     "my-env",
			req:         newCreateReq(),
			wantStatus:  "AVAILABLE",
			wantVersion: "2.10.3",
			wantClass:   "mw1.small",
		},
		{
			name:    "creates_with_custom_version",
			envName: "custom-env",
			req: &mwaa.ExportedCreateEnvironmentRequest{
				DagS3Path:        "dags/",
				ExecutionRoleArn: "arn:aws:iam::123456789012:role/mwaa-role",
				SourceBucketArn:  "arn:aws:s3:::my-bucket",
				AirflowVersion:   "2.8.1",
				EnvironmentClass: "mw1.medium",
			},
			wantStatus:  "AVAILABLE",
			wantVersion: "2.8.1",
			wantClass:   "mw1.medium",
		},
		{
			name:    "duplicate_returns_error",
			envName: "dupe-env",
			req:     newCreateReq(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.name == "duplicate_returns_error" {
				_, err := b.CreateEnvironment("us-east-1", "123456789012", tt.envName, newCreateReq())
				require.NoError(t, err)
			}

			env, err := b.CreateEnvironment("us-east-1", "123456789012", tt.envName, tt.req)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.envName, env.Name)
			assert.Equal(t, tt.wantStatus, env.Status)
			assert.Equal(t, tt.wantVersion, env.AirflowVersion)
			assert.Equal(t, tt.wantClass, env.EnvironmentClass)
			assert.NotEmpty(t, env.ARN)
			assert.NotEmpty(t, env.WebserverURL)
		})
	}
}

func TestBackend_GetEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		envName string
		seed    bool
		wantErr bool
	}{
		{
			name:    "found",
			envName: "existing-env",
			seed:    true,
		},
		{
			name:    "not_found",
			envName: "missing-env",
			seed:    false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.seed {
				_, err := b.CreateEnvironment("us-east-1", "123456789012", tt.envName, newCreateReq())
				require.NoError(t, err)
			}

			env, err := b.GetEnvironment(tt.envName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.envName, env.Name)
		})
	}
}

func TestBackend_DeleteEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		envName string
		seed    bool
		wantErr bool
	}{
		{
			name:    "deletes_existing",
			envName: "to-delete",
			seed:    true,
		},
		{
			name:    "not_found",
			envName: "missing",
			seed:    false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.seed {
				_, err := b.CreateEnvironment("us-east-1", "123456789012", tt.envName, newCreateReq())
				require.NoError(t, err)
			}

			deleted, err := b.DeleteEnvironment(tt.envName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.envName, deleted.Name)

			_, err = b.GetEnvironment(tt.envName)
			require.Error(t, err, "environment should be gone after delete")
		})
	}
}

func TestBackend_ListEnvironments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seedNames []string
		wantCount int
	}{
		{
			name:      "empty",
			seedNames: []string{},
			wantCount: 0,
		},
		{
			name:      "multiple",
			seedNames: []string{"env-a", "env-b", "env-c"},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			for _, n := range tt.seedNames {
				_, err := b.CreateEnvironment("us-east-1", "123456789012", n, newCreateReq())
				require.NoError(t, err)
			}

			names, err := b.ListEnvironments()
			require.NoError(t, err)
			assert.Len(t, names, tt.wantCount)
		})
	}
}

func TestBackend_UpdateEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update    *mwaa.ExportedUpdateEnvironmentRequest
		name      string
		envName   string
		wantClass string
		seed      bool
		wantErr   bool
	}{
		{
			name:    "updates_class",
			envName: "update-env",
			seed:    true,
			update: &mwaa.ExportedUpdateEnvironmentRequest{
				EnvironmentClass: "mw1.large",
			},
			wantClass: "mw1.large",
		},
		{
			name:    "not_found",
			envName: "missing",
			seed:    false,
			update:  &mwaa.ExportedUpdateEnvironmentRequest{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.seed {
				_, err := b.CreateEnvironment("us-east-1", "123456789012", tt.envName, newCreateReq())
				require.NoError(t, err)
			}

			env, err := b.UpdateEnvironment(tt.envName, tt.update)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantClass, env.EnvironmentClass)
		})
	}
}

func TestBackend_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tagsToAdd    map[string]string
		wantTags     map[string]string
		name         string
		envName      string
		keysToRemove []string
		wantErr      bool
	}{
		{
			name:      "tag_and_list",
			envName:   "tag-env",
			tagsToAdd: map[string]string{"env": "test", "owner": "team"},
			wantTags:  map[string]string{"env": "test", "owner": "team"},
		},
		{
			name:         "tag_and_untag",
			envName:      "untag-env",
			tagsToAdd:    map[string]string{"env": "test", "owner": "team"},
			keysToRemove: []string{"owner"},
			wantTags:     map[string]string{"env": "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			env, err := b.CreateEnvironment("us-east-1", "123456789012", tt.envName, newCreateReq())
			require.NoError(t, err)

			err = b.TagResource(env.ARN, tt.tagsToAdd)
			require.NoError(t, err)

			if len(tt.keysToRemove) > 0 {
				err = b.UntagResource(env.ARN, tt.keysToRemove)
				require.NoError(t, err)
			}

			tags, err := b.ListTagsForResource(env.ARN)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTags, tags)
		})
	}
}
