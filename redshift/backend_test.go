package redshift_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/redshift"
)

func TestRedshiftBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, b *redshift.InMemoryBackend)
	}{
		{name: "CreateCluster/success", run: func(t *testing.T, b *redshift.InMemoryBackend) {
			c, err := b.CreateCluster("my-cluster", "dc2.large", "mydb", "admin")
			require.NoError(t, err)
			assert.Equal(t, "my-cluster", c.ClusterIdentifier)
			assert.Equal(t, "dc2.large", c.NodeType)
			assert.Equal(t, "mydb", c.DBName)
			assert.Equal(t, "admin", c.MasterUsername)
			assert.Equal(t, "available", c.Status)
			assert.Contains(t, c.Endpoint, "my-cluster")
		}},
		{name: "CreateCluster/empty_id", run: func(t *testing.T, b *redshift.InMemoryBackend) {
			_, err := b.CreateCluster("", "", "", "")
			require.Error(t, err)
			assert.ErrorIs(t, err, redshift.ErrInvalidParameter)
		}},
		{name: "CreateCluster/already_exists", run: func(t *testing.T, b *redshift.InMemoryBackend) {
			_, err := b.CreateCluster("dup-cluster", "", "", "")
			require.NoError(t, err)

			_, err = b.CreateCluster("dup-cluster", "", "", "")
			require.Error(t, err)
			assert.ErrorIs(t, err, redshift.ErrClusterAlreadyExists)
		}},
		{name: "DeleteCluster/success", run: func(t *testing.T, b *redshift.InMemoryBackend) {
			_, err := b.CreateCluster("del-cluster", "", "", "")
			require.NoError(t, err)

			deleted, err := b.DeleteCluster("del-cluster")
			require.NoError(t, err)
			assert.Equal(t, "del-cluster", deleted.ClusterIdentifier)

			_, err = b.DescribeClusters("del-cluster")
			require.Error(t, err)
			assert.ErrorIs(t, err, redshift.ErrClusterNotFound)
		}},
		{name: "DescribeClusters/multiple", run: func(t *testing.T, b *redshift.InMemoryBackend) {
			_, err := b.CreateCluster("cluster-1", "", "", "")
			require.NoError(t, err)
			_, err = b.CreateCluster("cluster-2", "", "", "")
			require.NoError(t, err)

			clusters, err := b.DescribeClusters("")
			require.NoError(t, err)
			assert.Len(t, clusters, 2)
		}},
		{name: "DescribeClusters/not_found", run: func(t *testing.T, b *redshift.InMemoryBackend) {
			_, err := b.DescribeClusters("nonexistent")
			require.Error(t, err)
			assert.ErrorIs(t, err, redshift.ErrClusterNotFound)
		}},
		{name: "CreateTags/success", run: func(t *testing.T, b *redshift.InMemoryBackend) {
			_, err := b.CreateCluster("tagged-cluster", "dc2.large", "mydb", "admin")
			require.NoError(t, err)

			err = b.CreateTags("tagged-cluster", map[string]string{"env": "prod", "team": "platform"})
			require.NoError(t, err)

			allTags := b.DescribeTags()
			tags, ok := allTags["tagged-cluster"]
			require.True(t, ok)
			assert.Equal(t, "prod", tags["env"])
			assert.Equal(t, "platform", tags["team"])
		}},
		{name: "CreateTags/overwrite", run: func(t *testing.T, b *redshift.InMemoryBackend) {
			_, _ = b.CreateCluster("overwrite-cluster", "", "", "")
			_ = b.CreateTags("overwrite-cluster", map[string]string{"env": "dev"})
			_ = b.CreateTags("overwrite-cluster", map[string]string{"env": "prod"})

			allTags := b.DescribeTags()
			assert.Equal(t, "prod", allTags["overwrite-cluster"]["env"])
		}},
		{name: "CreateTags/not_found", run: func(t *testing.T, b *redshift.InMemoryBackend) {
			err := b.CreateTags("nonexistent", map[string]string{"k": "v"})
			require.Error(t, err)
			assert.ErrorIs(t, err, redshift.ErrClusterNotFound)
		}},
		{name: "DeleteTags/success", run: func(t *testing.T, b *redshift.InMemoryBackend) {
			_, _ = b.CreateCluster("del-tags-cluster", "", "", "")
			_ = b.CreateTags("del-tags-cluster", map[string]string{"env": "prod", "team": "platform"})

			err := b.DeleteTags("del-tags-cluster", []string{"env"})
			require.NoError(t, err)

			allTags := b.DescribeTags()
			tags := allTags["del-tags-cluster"]
			assert.NotContains(t, tags, "env")
			assert.Equal(t, "platform", tags["team"])
		}},
		{name: "DeleteTags/not_found", run: func(t *testing.T, b *redshift.InMemoryBackend) {
			err := b.DeleteTags("nonexistent", []string{"k"})
			require.Error(t, err)
			assert.ErrorIs(t, err, redshift.ErrClusterNotFound)
		}},
		{name: "DescribeTags/empty", run: func(t *testing.T, b *redshift.InMemoryBackend) {
			_, _ = b.CreateCluster("empty-tags-cluster", "", "", "")

			allTags := b.DescribeTags()
			tags, ok := allTags["empty-tags-cluster"]
			require.True(t, ok)
			assert.Empty(t, tags)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			tt.run(t, b)
		})
	}
}
