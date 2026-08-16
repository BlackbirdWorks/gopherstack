package elasticache_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

func TestBackend_DescribeReservedCacheNodes_Empty(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	p, err := b.DescribeReservedCacheNodes(context.Background(), "", "", "", "", 0)
	require.NoError(t, err)
	assert.NotNil(t, p.Data)
}

func TestBackend_DescribeReservedCacheNodesOfferings_NonEmpty(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	p, err := b.DescribeReservedCacheNodesOfferings(context.Background(), "", "", "", "", 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(p.Data), 1)
}

func TestBackend_PurchaseReservedCacheNodesOffering(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	offerings, err := b.DescribeReservedCacheNodesOfferings(context.Background(), "", "", "", "", 0)
	require.NoError(t, err)
	require.NotEmpty(t, offerings.Data)

	offeringID := offerings.Data[0].OfferingID
	rcn, err := b.PurchaseReservedCacheNodesOffering(context.Background(), offeringID, "my-reserved-node-id", 1)
	require.NoError(t, err)
	assert.Equal(t, "my-reserved-node-id", rcn.ReservedCacheNodeID)
	assert.Equal(t, int32(1), rcn.CacheNodeCount)
}

// ----------------------------------------
// AllowedNodeTypeModifications
// ----------------------------------------

func TestBackend_PurchaseReservedCacheNode_HasARN(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	rcn, err := b.PurchaseReservedCacheNodesOffering(
		context.Background(),
		"31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa",
		"",
		1,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, rcn.ARN)
	assert.Contains(t, rcn.ARN, "arn:aws:elasticache")
	assert.Contains(t, rcn.ARN, "reserved-instance")
}

func TestBackend_PurchaseReservedCacheNode_AutoIDUnique(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	rcn1, err := b.PurchaseReservedCacheNodesOffering(
		context.Background(),
		"31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa",
		"",
		1,
	)
	require.NoError(t, err)

	rcn2, err := b.PurchaseReservedCacheNodesOffering(
		context.Background(),
		"31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa",
		"",
		2,
	)
	require.NoError(t, err)

	assert.NotEqual(t, rcn1.ReservedCacheNodeID, rcn2.ReservedCacheNodeID)
}
