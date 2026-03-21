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
