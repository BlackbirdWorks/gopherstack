package elasticache_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_CreateSubnetGroup(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	sg, err := b.CreateSubnetGroup(context.Background(), "my-sng", "my subnet group", []string{"subnet-1", "subnet-2"})
	require.NoError(t, err)
	assert.Equal(t, "my-sng", sg.Name)
	assert.Len(t, sg.SubnetIDs, 2)
	assert.Contains(t, sg.ARN, "arn:aws:elasticache:")
}

func TestBackend_ModifySubnetGroup_AddSubnet(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateSubnetGroup(context.Background(), "add-sng", "original", []string{"subnet-1"})
	require.NoError(t, err)

	sg, err := b.ModifySubnetGroup(
		context.Background(),
		"add-sng",
		"updated",
		[]string{"subnet-1", "subnet-2", "subnet-3"},
	)
	require.NoError(t, err)
	assert.Len(t, sg.SubnetIDs, 3)
	assert.Equal(t, "updated", sg.Description)
}

func TestBackend_DeleteSubnetGroup_NotFound(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	err := b.DeleteSubnetGroup(context.Background(), "nonexistent-sng")
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrSubnetGroupNotFound)
}

// ----------------------------------------
// Snapshot CRUD + ExportServerlessCacheSnapshot (issue)
// ----------------------------------------

func TestBackend_CreateSubnetGroupFull_WithVpcId(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	sg, err := b.CreateSubnetGroupFull(
		context.Background(),
		"sng-vpc",
		"with vpc",
		"vpc-0abc123",
		[]string{"subnet-1", "subnet-2"},
	)
	require.NoError(t, err)
	assert.Equal(t, "sng-vpc", sg.Name)
	assert.Equal(t, "vpc-0abc123", sg.VpcID)
	assert.Len(t, sg.SubnetIDs, 2)
	assert.NotEmpty(t, sg.ARN)
}

func TestBackend_CreateSubnetGroupFull_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateSubnetGroupFull(context.Background(), "dup-sng", "dup", "vpc-111", nil)
	require.NoError(t, err)

	_, err = b.CreateSubnetGroupFull(context.Background(), "dup-sng", "dup", "vpc-111", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrSubnetGroupAlreadyExists)
}
