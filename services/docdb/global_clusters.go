package docdb

import (
	"context"
	"fmt"
	"sort"
)

// CreateGlobalCluster creates a global cluster.
func (b *InMemoryBackend) CreateGlobalCluster(
	_ context.Context,
	id, sourceDBClusterID, engine, engineVersion string,
) (*GlobalCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateGlobalCluster")
	defer b.mu.Unlock()
	if b.globalClusters.Has(id) {
		return nil, fmt.Errorf("%w: global cluster %s already exists", ErrGlobalClusterAlreadyExists, id)
	}
	if engine == "" {
		engine = docDBEngine
	}
	if engineVersion == "" {
		engineVersion = defaultEngineVersion
	}
	gc := &GlobalCluster{
		GlobalClusterIdentifier: id,
		SourceDBClusterID:       sourceDBClusterID,
		Status:                  statusAvailable,
		Engine:                  engine,
		EngineVersion:           engineVersion,
		GlobalClusterArn:        b.globalClusterARN(id),
	}
	b.globalClusters.Put(gc)
	cp := *gc

	return &cp, nil
}

// DeleteGlobalCluster deletes a global cluster.
func (b *InMemoryBackend) DeleteGlobalCluster(_ context.Context, id string) (*GlobalCluster, error) {
	b.mu.Lock("DeleteGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, id)
	}
	cp := *gc
	b.globalClusters.Delete(id)

	return &cp, nil
}

// DescribeGlobalClusters returns global clusters, optionally filtered by ID, sorted by identifier.
func (b *InMemoryBackend) DescribeGlobalClusters(_ context.Context, id string) []GlobalCluster {
	b.mu.RLock("DescribeGlobalClusters")
	defer b.mu.RUnlock()
	if id != "" {
		gc, exists := b.globalClusters.Get(id)
		if !exists {
			return []GlobalCluster{}
		}
		cp := *gc

		return []GlobalCluster{cp}
	}
	globalClusters := b.globalClusters.All()
	result := make([]GlobalCluster, 0, len(globalClusters))
	for _, gc := range globalClusters {
		result = append(result, *gc)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].GlobalClusterIdentifier < result[j].GlobalClusterIdentifier
	})

	return result
}

// ModifyGlobalCluster modifies a global cluster.
func (b *InMemoryBackend) ModifyGlobalCluster(
	_ context.Context,
	id, newID string,
	deletionProtection *bool,
) (*GlobalCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("ModifyGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, id)
	}
	if deletionProtection != nil {
		gc.DeletionProtection = *deletionProtection
	}
	if newID != "" && newID != id {
		b.globalClusters.Delete(id)
		gc.GlobalClusterIdentifier = newID
		gc.GlobalClusterArn = b.globalClusterARN(newID)
		b.globalClusters.Put(gc)
	}
	cp := *gc

	return &cp, nil
}

// FailoverGlobalCluster initiates a failover for a global cluster.
func (b *InMemoryBackend) FailoverGlobalCluster(_ context.Context, id, _ string) (*GlobalCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("FailoverGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, id)
	}
	gc.Status = "failing-over"
	cp := *gc

	return &cp, nil
}

// RemoveFromGlobalCluster removes a DB cluster from a global cluster.
func (b *InMemoryBackend) RemoveFromGlobalCluster(
	_ context.Context,
	globalClusterID, _ string,
) (*GlobalCluster, error) {
	if globalClusterID == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("RemoveFromGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters.Get(globalClusterID)
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, globalClusterID)
	}
	cp := *gc

	return &cp, nil
}

// SwitchoverGlobalCluster initiates a switchover for a global cluster.
func (b *InMemoryBackend) SwitchoverGlobalCluster(_ context.Context, id, _ string) (*GlobalCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("SwitchoverGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, id)
	}
	gc.Status = "switching-over"
	cp := *gc

	return &cp, nil
}
