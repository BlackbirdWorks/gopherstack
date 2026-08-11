package rds

import (
	"fmt"
	"slices"
)

// CreateGlobalCluster creates a new global cluster.
func (b *InMemoryBackend) CreateGlobalCluster(
	id, engine, engineVersion string,
	storageEncrypted, deletionProtection bool,
) (*GlobalCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("CreateGlobalCluster")
	defer b.mu.Unlock()

	if _, exists := b.globalClusters.Get(id); exists {
		return nil, fmt.Errorf("%w: global cluster %s already exists", ErrGlobalClusterAlreadyExists, id)
	}

	if engine == "" {
		engine = "aurora-postgresql"
	}

	gc := &GlobalCluster{
		GlobalClusterIdentifier: id,
		GlobalClusterArn:        b.rdsARN("global-cluster", id),
		Engine:                  engine,
		EngineVersion:           engineVersion,
		Status:                  instanceStatusAvailable,
		StorageEncrypted:        storageEncrypted,
		DeletionProtection:      deletionProtection,
	}
	b.globalClusters.Put(gc)
	cp := *gc

	return &cp, nil
}

// DescribeGlobalClusters returns global clusters, optionally filtered by identifier.
func (b *InMemoryBackend) DescribeGlobalClusters(id string) ([]GlobalCluster, error) {
	b.mu.RLock("DescribeGlobalClusters")
	defer b.mu.RUnlock()

	if id != "" {
		gc, exists := b.globalClusters.Get(id)
		if !exists {
			return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, id)
		}
		cp := *gc

		return []GlobalCluster{cp}, nil
	}

	result := make([]GlobalCluster, 0, b.globalClusters.Len())
	for _, gc := range b.globalClusters.All() {
		result = append(result, *gc)
	}

	return result, nil
}

// DeleteGlobalCluster removes the given global cluster.
func (b *InMemoryBackend) DeleteGlobalCluster(id string) (*GlobalCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteGlobalCluster")
	defer b.mu.Unlock()

	gc, exists := b.globalClusters.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, id)
	}

	if gc.DeletionProtection {
		return nil, fmt.Errorf(
			"%w: cannot delete protected global cluster %s, disable deletion protection first",
			ErrInvalidGlobalClusterState, id,
		)
	}

	cp := *gc
	b.globalClusters.Delete(id)

	return &cp, nil
}

// ModifyGlobalCluster modifies properties of a global cluster.
func (b *InMemoryBackend) ModifyGlobalCluster(
	id, newGlobalClusterID, engineVersion string,
	deletionProtection *bool,
) (*GlobalCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyGlobalCluster")
	defer b.mu.Unlock()

	gc, exists := b.globalClusters.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, id)
	}

	if newGlobalClusterID != "" && newGlobalClusterID != id {
		if _, alreadyExists := b.globalClusters.Get(newGlobalClusterID); alreadyExists {
			return nil, fmt.Errorf(
				"%w: global cluster %s already exists",
				ErrGlobalClusterAlreadyExists,
				newGlobalClusterID,
			)
		}
		b.globalClusters.Delete(id)
		gc.GlobalClusterIdentifier = newGlobalClusterID
		b.globalClusters.Put(gc)
	}
	if engineVersion != "" {
		gc.EngineVersion = engineVersion
	}
	if deletionProtection != nil {
		gc.DeletionProtection = *deletionProtection
	}

	cp := *gc

	return &cp, nil
}

// RemoveFromGlobalCluster removes a DB cluster from a global cluster.
func (b *InMemoryBackend) RemoveFromGlobalCluster(globalClusterID, dbClusterARN string) (*GlobalCluster, error) {
	b.mu.Lock("RemoveFromGlobalCluster")
	defer b.mu.Unlock()
	gc, ok := b.globalClusters.Get(globalClusterID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrGlobalClusterNotFound, globalClusterID)
	}
	gc.ClusterARNs = slices.DeleteFunc(gc.ClusterARNs, func(arn string) bool {
		return arn == dbClusterARN
	})
	cp := *gc

	return &cp, nil
}

// FailoverGlobalCluster initiates a failover for a global cluster.
func (b *InMemoryBackend) FailoverGlobalCluster(
	globalClusterID, _ string,
) (*GlobalCluster, error) {
	b.mu.Lock("FailoverGlobalCluster")
	defer b.mu.Unlock()
	gc, ok := b.globalClusters.Get(globalClusterID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrGlobalClusterNotFound, globalClusterID)
	}
	gc.Status = instanceStatusAvailable
	cp := *gc

	return &cp, nil
}

// SwitchoverGlobalCluster initiates a switchover for a global cluster.
func (b *InMemoryBackend) SwitchoverGlobalCluster(
	globalClusterID, _ string,
) (*GlobalCluster, error) {
	b.mu.Lock("SwitchoverGlobalCluster")
	defer b.mu.Unlock()
	gc, ok := b.globalClusters.Get(globalClusterID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrGlobalClusterNotFound, globalClusterID)
	}
	gc.Status = instanceStatusAvailable
	cp := *gc

	return &cp, nil
}
