package redshift_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/redshift"
)

func TestRedshiftBackend_CreateCluster(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	c, err := b.CreateCluster("my-cluster", "dc2.large", "mydb", "admin")
	require.NoError(t, err)
	assert.Equal(t, "my-cluster", c.ClusterIdentifier)
	assert.Equal(t, "dc2.large", c.NodeType)
	assert.Equal(t, "mydb", c.DBName)
	assert.Equal(t, "admin", c.MasterUsername)
	assert.Equal(t, "available", c.Status)
	assert.Contains(t, c.Endpoint, "my-cluster")
}

func TestRedshiftBackend_CreateCluster_EmptyID(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateCluster("", "", "", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, redshift.ErrInvalidParameter)
}

func TestRedshiftBackend_CreateCluster_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateCluster("dup-cluster", "", "", "")
	require.NoError(t, err)

	_, err = b.CreateCluster("dup-cluster", "", "", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, redshift.ErrClusterAlreadyExists)
}

func TestRedshiftBackend_DeleteCluster(t *testing.T) {
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

func TestRedshiftBackend_DescribeClusters(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateCluster("cluster-1", "", "", "")
	require.NoError(t, err)
	_, err = b.CreateCluster("cluster-2", "", "", "")
	require.NoError(t, err)

	clusters, err := b.DescribeClusters("")
	require.NoError(t, err)
	assert.Len(t, clusters, 2)
}

func TestRedshiftBackend_DescribeClusters_NotFound(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.DescribeClusters("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, redshift.ErrClusterNotFound)
}
