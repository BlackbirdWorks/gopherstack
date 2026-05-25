package elasticbeanstalk_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

func TestElasticBeanstalk_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *elasticbeanstalk.InMemoryBackend)
		verify func(t *testing.T, b *elasticbeanstalk.InMemoryBackend)
		name   string
	}{
		{
			name:  "empty",
			setup: func(_ *elasticbeanstalk.InMemoryBackend) {},
			verify: func(t *testing.T, b *elasticbeanstalk.InMemoryBackend) {
				t.Helper()

				assert.Empty(t, b.DescribeApplications(nil))
			},
		},
		{
			name: "application_preserved",
			setup: func(b *elasticbeanstalk.InMemoryBackend) {
				_, _ = b.CreateApplication("my-app", "desc", map[string]string{"env": "prod"})
			},
			verify: func(t *testing.T, b *elasticbeanstalk.InMemoryBackend) {
				t.Helper()

				apps := b.DescribeApplications(nil)
				require.Len(t, apps, 1)
				assert.Equal(t, "my-app", apps[0].ApplicationName)
				assert.Equal(t, "prod", apps[0].Tags["env"])
				// Verify ARN index rebuilt - tag ops should work
				gotTags, err := b.ListTagsForResource(apps[0].ApplicationARN)
				require.NoError(t, err)
				assert.Equal(t, "prod", gotTags["env"])
			},
		},
		{
			name: "batch1_environment_and_version_state_preserved",
			setup: func(b *elasticbeanstalk.InMemoryBackend) {
				_, _ = b.CreateEnvironment("app", "env", "stack", "", nil, elasticbeanstalk.CreateEnvironmentParams{
					VersionLabel: "v1",
					OptionSettings: []elasticbeanstalk.OptionSetting{
						{Namespace: "aws:ec2:vpc", OptionName: "VPCId", Value: "vpc-1"},
					},
				})
				_, _ = b.CreateApplicationVersionWithParams("app", "v1", elasticbeanstalk.ApplicationVersionParams{
					Process: true,
					SourceBuildInformation: &elasticbeanstalk.SourceBuildInformation{
						SourceType: "CodeCommit", SourceRepository: "repo", SourceLocation: "main",
					},
				})
			},
			verify: func(t *testing.T, b *elasticbeanstalk.InMemoryBackend) {
				t.Helper()

				envs := b.DescribeEnvironments("app", []string{"env"}, nil)
				require.Len(t, envs, 1)
				assert.Equal(t, "v1", envs[0].VersionLabel)
				assert.Equal(t, "vpc-1", envs[0].OptionSettings[0].Value)

				versions := b.DescribeApplicationVersions("app", []string{"v1"})
				require.Len(t, versions, 1)
				assert.Equal(t, "CodeCommit", versions[0].SourceBuildInformation.SourceType)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")
			tt.setup(b)

			snap := b.Snapshot()
			require.NotNil(t, snap)

			b2 := elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")
			err := b2.Restore(snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}
