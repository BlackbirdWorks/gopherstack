package redshift_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/redshift"
)

func TestRedshiftCreateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(b *redshift.InMemoryBackend)
		name       string
		clusterID  string
		nodeType   string
		dbName     string
		masterUser string
	}{
		{
			name:       "success",
			clusterID:  "my-cluster",
			nodeType:   "dc2.large",
			dbName:     "mydb",
			masterUser: "admin",
		},
		{
			name:      "empty_id",
			clusterID: "",
			wantErr:   redshift.ErrInvalidParameter,
		},
		{
			name:      "already_exists",
			clusterID: "dup-cluster",
			setup: func(b *redshift.InMemoryBackend) {
				_, _ = b.CreateCluster("dup-cluster", "", "", "")
			},
			wantErr: redshift.ErrClusterAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			c, err := b.CreateCluster(tt.clusterID, tt.nodeType, tt.dbName, tt.masterUser)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.clusterID, c.ClusterIdentifier)
			assert.Equal(t, tt.nodeType, c.NodeType)
			assert.Equal(t, tt.dbName, c.DBName)
			assert.Equal(t, tt.masterUser, c.MasterUsername)
			assert.Equal(t, "available", c.Status)
			assert.Contains(t, c.Endpoint, tt.clusterID)
		})
	}
}

func TestRedshiftDeleteCluster(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateCluster("del-cluster", "", "", "")
	require.NoError(t, err)

	deleted, err := b.DeleteCluster("del-cluster")
	require.NoError(t, err)
	assert.Equal(t, "del-cluster", deleted.ClusterIdentifier)

	_, err = b.DescribeClusters("del-cluster")
	require.Error(t, err)
	assert.ErrorIs(t, err, redshift.ErrClusterNotFound)
}

func TestRedshiftDescribeClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(b *redshift.InMemoryBackend)
		name      string
		clusterID string
		wantCount int
	}{
		{
			name: "multiple",
			setup: func(b *redshift.InMemoryBackend) {
				_, _ = b.CreateCluster("cluster-1", "", "", "")
				_, _ = b.CreateCluster("cluster-2", "", "", "")
			},
			clusterID: "",
			wantCount: 2,
		},
		{
			name:      "not_found",
			clusterID: "nonexistent",
			wantErr:   redshift.ErrClusterNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			clusters, err := b.DescribeClusters(tt.clusterID)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Len(t, clusters, tt.wantCount)
		})
	}
}

func TestRedshiftCreateTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(b *redshift.InMemoryBackend)
		tags      map[string]string
		wantTags  map[string]string
		name      string
		clusterID string
	}{
		{
			name:      "success",
			clusterID: "tagged-cluster",
			setup: func(b *redshift.InMemoryBackend) {
				_, _ = b.CreateCluster("tagged-cluster", "dc2.large", "mydb", "admin")
			},
			tags:     map[string]string{"env": "prod", "team": "platform"},
			wantTags: map[string]string{"env": "prod", "team": "platform"},
		},
		{
			name:      "overwrite",
			clusterID: "overwrite-cluster",
			setup: func(b *redshift.InMemoryBackend) {
				_, _ = b.CreateCluster("overwrite-cluster", "", "", "")
				_ = b.CreateTags("overwrite-cluster", map[string]string{"env": "dev"})
			},
			tags:     map[string]string{"env": "prod"},
			wantTags: map[string]string{"env": "prod"},
		},
		{
			name:      "not_found",
			clusterID: "nonexistent",
			tags:      map[string]string{"k": "v"},
			wantErr:   redshift.ErrClusterNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			err := b.CreateTags(tt.clusterID, tt.tags)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			allTags := b.DescribeTags()
			tags, ok := allTags[tt.clusterID]
			require.True(t, ok)
			for k, v := range tt.wantTags {
				assert.Equal(t, v, tags[k])
			}
		})
	}
}

func TestRedshiftDeleteTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr        error
		setup          func(b *redshift.InMemoryBackend)
		wantTags       map[string]string
		name           string
		clusterID      string
		keysToRemove   []string
		wantAbsentKeys []string
	}{
		{
			name:      "success",
			clusterID: "del-tags-cluster",
			setup: func(b *redshift.InMemoryBackend) {
				_, _ = b.CreateCluster("del-tags-cluster", "", "", "")
				_ = b.CreateTags("del-tags-cluster", map[string]string{"env": "prod", "team": "platform"})
			},
			keysToRemove:   []string{"env"},
			wantAbsentKeys: []string{"env"},
			wantTags:       map[string]string{"team": "platform"},
		},
		{
			name:         "not_found",
			clusterID:    "nonexistent",
			keysToRemove: []string{"k"},
			wantErr:      redshift.ErrClusterNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			err := b.DeleteTags(tt.clusterID, tt.keysToRemove)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			allTags := b.DescribeTags()
			tags := allTags[tt.clusterID]
			for _, k := range tt.wantAbsentKeys {
				assert.NotContains(t, tags, k)
			}
			for k, v := range tt.wantTags {
				assert.Equal(t, v, tags[k])
			}
		})
	}
}

func TestRedshiftDescribeTags(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateCluster("empty-tags-cluster", "", "", "")

	allTags := b.DescribeTags()
	tags, ok := allTags["empty-tags-cluster"]
	require.True(t, ok)
	assert.Empty(t, tags)
}
