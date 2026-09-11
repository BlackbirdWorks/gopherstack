package rds

import (
	"fmt"
	"net/url"
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
	slices.SortFunc(result, func(a, b GlobalCluster) int {
		if a.GlobalClusterIdentifier < b.GlobalClusterIdentifier {
			return -1
		}
		if a.GlobalClusterIdentifier > b.GlobalClusterIdentifier {
			return 1
		}

		return 0
	})

	return result, nil
}

// isKnownGlobalClusterFilterName reports whether name is a
// Filters.Filter.N.Name value AWS recognizes for DescribeGlobalClusters.
// Its own doc comment (rds@v1.124.1 api_op_DescribeGlobalClusters.go:38-45)
// says "Currently, the only supported filter is region".
func isKnownGlobalClusterFilterName(name string) bool {
	return name == filterNameRegion
}

// applyGlobalClusterFilters validates the DescribeGlobalClusters Filters
// contract but does not narrow gcs: "region" is a real, documented filter
// (unlike the 22 rds ops whose Filters doc says "This parameter isn't
// currently supported"), but no gopherstack API path ever populates
// GlobalCluster.PrimaryRegion or GlobalClusterMembers -- CreateGlobalCluster
// leaves PrimaryRegion empty, and handler_global_clusters.go's own comment
// on AddGlobalClusterMemberInternal says membership is a test-only seam. With
// no real region data to match against, "region" is accepted (an
// unrecognized filter name still returns InvalidParameterValue, matching
// real AWS) but matches vacuously, the same treatment as the existing
// DescribeDBInstances "domain" precedent (db_instances.go).
func applyGlobalClusterFilters(vals url.Values, gcs []GlobalCluster) ([]GlobalCluster, error) {
	filters := parseDescribeFilters(vals)
	if len(filters) == 0 {
		return gcs, nil
	}

	for name := range filters {
		if !isKnownGlobalClusterFilterName(name) {
			return nil, fmt.Errorf("%w: Unrecognized filter name: %s", ErrInvalidParameter, name)
		}
	}

	return gcs, nil
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
