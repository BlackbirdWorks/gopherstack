package kafka

import (
	"context"
)

// ListClusterOperations returns all cluster operations for a cluster.
func (b *InMemoryBackend) ListClusterOperations(_ context.Context, clusterArn string) ([]*ClusterOperation, error) {
	b.mu.RLock("ListClusterOperations")
	defer b.mu.RUnlock()

	return collectClusterChildrenLocked(
		b.clusters,
		b.clusterOperationsByCluster,
		clusterArn,
		cloneClusterOperation,
		func(op *ClusterOperation) string { return op.ClusterOperationArn },
	)
}

// DescribeClusterOperationV2 retrieves a cluster operation (V2) by ARN.
func (b *InMemoryBackend) DescribeClusterOperationV2(
	ctx context.Context,
	clusterOperationArn string,
) (*ClusterOperation, error) {
	return b.DescribeClusterOperation(ctx, clusterOperationArn)
}

// ListClusterOperationsV2 returns all cluster operations for a cluster (V2).
func (b *InMemoryBackend) ListClusterOperationsV2(ctx context.Context, clusterArn string) ([]*ClusterOperation, error) {
	return b.ListClusterOperations(ctx, clusterArn)
}

// newClusterOperationLocked creates and stores a cluster operation.
// MUST be called with b.mu write lock held.
func (b *InMemoryBackend) newClusterOperationLocked(
	region, clusterArn, operationType string, source, target *MutableClusterInfo,
) *ClusterOperation {
	clusterOperationArn := b.clusterOperationARN(region, clusterArn)
	op := &ClusterOperation{
		ClusterOperationArn: clusterOperationArn,
		ClusterArn:          clusterArn,
		OperationType:       operationType,
		OperationState:      ClusterOperationStateUpdateComplete,
		SourceClusterInfo:   source,
		TargetClusterInfo:   target,
	}
	b.clusterOperations.Put(op)

	return cloneClusterOperation(op)
}

// DescribeClusterOperation retrieves a cluster operation by ARN.
func (b *InMemoryBackend) DescribeClusterOperation(
	_ context.Context,
	clusterOperationArn string,
) (*ClusterOperation, error) {
	b.mu.RLock("DescribeClusterOperation")
	defer b.mu.RUnlock()

	op, ok := b.clusterOperations.Get(clusterOperationArn)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneClusterOperation(op), nil
}

// AddClusterOperationInternal adds a cluster operation for testing purposes.
func (b *InMemoryBackend) AddClusterOperationInternal(
	clusterArn, operationType string,
) *ClusterOperation {
	b.mu.Lock("AddClusterOperationInternal")
	defer b.mu.Unlock()

	region := regionFromARN(clusterArn, b.region)
	clusterOperationArn := b.clusterOperationARN(region, clusterArn)
	op := &ClusterOperation{
		ClusterOperationArn: clusterOperationArn,
		ClusterArn:          clusterArn,
		OperationType:       operationType,
		OperationState:      ClusterOperationStateUpdateComplete,
	}
	b.clusterOperations.Put(op)

	return cloneClusterOperation(op)
}

// cloneClusterOperation creates a deep copy of a ClusterOperation.
func cloneClusterOperation(op *ClusterOperation) *ClusterOperation {
	return &ClusterOperation{
		ClusterOperationArn: op.ClusterOperationArn,
		ClusterArn:          op.ClusterArn,
		OperationType:       op.OperationType,
		OperationState:      op.OperationState,
		SourceClusterInfo:   cloneMutableClusterInfo(op.SourceClusterInfo),
		TargetClusterInfo:   cloneMutableClusterInfo(op.TargetClusterInfo),
	}
}
