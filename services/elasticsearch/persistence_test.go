package elasticsearch_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

func TestElasticsearch_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *elasticsearch.InMemoryBackend)
		verify func(t *testing.T, b *elasticsearch.InMemoryBackend)
		name   string
	}{
		{
			name:  "empty_backend",
			setup: func(_ *testing.T, _ *elasticsearch.InMemoryBackend) {},
			verify: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				names := b.ListDomainNames(context.Background())
				assert.Empty(t, names)
			},
		},
		{
			name: "domain_preserved",
			setup: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateDomain(
					context.Background(),
					"my-domain",
					"7.10",
					elasticsearch.ClusterConfig{InstanceType: "t3.small.elasticsearch", InstanceCount: 1},
					elasticsearch.EBSOptions{EBSEnabled: true, VolumeType: "gp2", VolumeSize: 10},
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				names := b.ListDomainNames(context.Background())
				require.Len(t, names, 1)
				assert.Equal(t, "my-domain", names[0])

				d, err := b.DescribeDomain(context.Background(), "my-domain")
				require.NoError(t, err)
				assert.Equal(t, "7.10", d.ElasticsearchVersion)
				assert.Equal(t, "Active", d.Status)
				assert.NotEmpty(t, d.ARN)
			},
		},
		{
			name: "tags_preserved_via_arn",
			setup: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				d, err := b.CreateDomain(
					context.Background(),
					"tagged-domain",
					"",
					elasticsearch.ClusterConfig{},
					elasticsearch.EBSOptions{},
				)
				require.NoError(t, err)

				require.NoError(t, b.AddTags(context.Background(), d.ARN, map[string]string{"team": "platform"}))
			},
			verify: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				d, err := b.DescribeDomain(context.Background(), "tagged-domain")
				require.NoError(t, err)

				tagMap, err := b.ListTags(context.Background(), d.ARN)
				require.NoError(t, err)
				assert.Equal(t, "platform", tagMap["team"])

				// ARN index must be rebuilt: ARN lookup must work.
				tagMap2, err := b.ListTags(context.Background(), d.ARN)
				require.NoError(t, err)
				assert.Equal(t, tagMap, tagMap2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
			tt.setup(t, b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
			err := b2.Restore(t.Context(), snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}
