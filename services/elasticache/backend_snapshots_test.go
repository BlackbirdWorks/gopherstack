package elasticache_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_CreateSnapshot_FromCluster(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateClusterWithOptions(
		context.Background(),
		"snap-src-cluster",
		"redis",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)

	snap, err := b.CreateSnapshot(context.Background(), "my-snapshot", "snap-src-cluster", "")
	require.NoError(t, err)
	assert.Equal(t, "my-snapshot", snap.SnapshotName)
	assert.Equal(t, "snap-src-cluster", snap.CacheClusterID)
	assert.Equal(t, "available", snap.Status)
	assert.Contains(t, snap.ARN, "arn:aws:elasticache:")
}

func TestBackend_CreateSnapshot_FromReplicationGroup(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID:          "snap-rg-src",
		Description: "for snapshot",
	})
	require.NoError(t, err)

	snap, err := b.CreateSnapshot(context.Background(), "rg-snapshot", "", "snap-rg-src")
	require.NoError(t, err)
	assert.Equal(t, "snap-rg-src", snap.ReplicationGroupID)
}

func TestBackend_CreateSnapshot_InvalidSource(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateSnapshot(context.Background(), "bad-snap", "", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrInvalidSnapshotSource)
}

func TestBackend_DescribeSnapshots_FilterByName(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateClusterWithOptions(
		context.Background(),
		"desc-snap-cl",
		"redis",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)

	for i := range 3 {
		_, err = b.CreateSnapshot(context.Background(), "snap-"+string(rune('a'+i)), "desc-snap-cl", "")
		require.NoError(t, err)
	}

	p, err := b.DescribeSnapshots(context.Background(), "snap-a", "", "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "snap-a", p.Data[0].SnapshotName)
}

func TestBackend_DeleteSnapshot(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateClusterWithOptions(
		context.Background(),
		"del-snap-cl",
		"redis",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)

	_, err = b.CreateSnapshot(context.Background(), "to-delete-snap", "del-snap-cl", "")
	require.NoError(t, err)

	deleted, err := b.DeleteSnapshot(context.Background(), "to-delete-snap")
	require.NoError(t, err)
	assert.Equal(t, "to-delete-snap", deleted.SnapshotName)

	// Should be gone now.
	_, err = b.DescribeSnapshots(context.Background(), "to-delete-snap", "", "", "", "", 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrSnapshotNotFound)
}

func TestBackend_CopySnapshot(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateClusterWithOptions(
		context.Background(),
		"copy-snap-src-cl",
		"redis",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)

	_, err = b.CreateSnapshot(context.Background(), "copy-src-snap", "copy-snap-src-cl", "")
	require.NoError(t, err)

	copied, err := b.CopySnapshot(context.Background(), "copy-src-snap", "copy-dst-snap")
	require.NoError(t, err)
	assert.Equal(t, "copy-dst-snap", copied.SnapshotName)

	// Both exist.
	p, err := b.DescribeSnapshots(context.Background(), "", "", "", "", "", 0)
	require.NoError(t, err)
	assert.Len(t, p.Data, 2)
}

// ----------------------------------------
// User ACL (Redis 6+ / Valkey) CRUD (issue)
// ----------------------------------------

func TestBackend_CopySnapshotFull_WithKmsKey(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID: "kms-rg", Description: "for copy",
	})
	require.NoError(t, err)

	_, err = b.CreateSnapshot(context.Background(), "original-snap", "", "kms-rg")
	require.NoError(t, err)

	copied, err := b.CopySnapshotFull(
		context.Background(),
		"original-snap",
		"encrypted-copy",
		"arn:aws:kms:us-east-1:000000000000:key/key-1",
	)
	require.NoError(t, err)
	assert.Equal(t, "encrypted-copy", copied.SnapshotName)
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/key-1", copied.KmsKeyID)
	assert.NotEmpty(t, copied.ARN)
}

func TestBackend_CopySnapshotFull_SourceNotFound(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CopySnapshotFull(context.Background(), "no-such-snap", "target", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrSnapshotNotFound)
}

func TestBackend_CopySnapshotFull_TargetAlreadyExists(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID: "dup-copy-rg", Description: "dup",
	})
	require.NoError(t, err)

	_, err = b.CreateSnapshot(context.Background(), "src-snap", "", "dup-copy-rg")
	require.NoError(t, err)

	_, err = b.CreateSnapshot(context.Background(), "dst-snap", "", "dup-copy-rg")
	require.NoError(t, err)

	_, err = b.CopySnapshotFull(context.Background(), "src-snap", "dst-snap", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrSnapshotAlreadyExists)
}

// ----------------------------------------
// CreateUserGroupValidated
// ----------------------------------------

func TestBackend_DescribeSnapshots_SnapshotSource_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		snapshotSource string
		wantCount      int
	}{
		{name: "no_filter", snapshotSource: "", wantCount: 2},
		{name: "user", snapshotSource: "user", wantCount: 1},
		{name: "system", snapshotSource: "system", wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
			elasticache.AddSnapshotInternal(b, "snap-manual", "rg-x", "manual")
			elasticache.AddSnapshotInternal(b, "snap-auto", "rg-x", "automated")

			p, err := b.DescribeSnapshots(context.Background(), "", "", "", tt.snapshotSource, "", 0)
			require.NoError(t, err)
			assert.Len(t, p.Data, tt.wantCount)
		})
	}
}
