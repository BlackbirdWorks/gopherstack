package neptune

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// globalClusterARN returns the partition-scoped ARN for a Neptune global cluster.
func (b *InMemoryBackend) globalClusterARN(id string) string {
	return arn.Build("rds", "", b.accountID, "global-cluster:"+id)
}

// CreateGlobalCluster creates a Neptune global cluster.
// Global clusters are partition-scoped (not region-isolated), but the optional
// source DB cluster is looked up in the ctx region where it resides.
func (b *InMemoryBackend) CreateGlobalCluster(
	ctx context.Context, globalClusterID, sourceDBClusterID string,
) (*GlobalCluster, error) {
	if globalClusterID == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateGlobalCluster")
	defer b.mu.Unlock()
	if b.globalClusters.Has(globalClusterID) {
		return nil, fmt.Errorf(
			"%w: global cluster %s already exists",
			ErrGlobalClusterAlreadyExists,
			globalClusterID,
		)
	}
	gc := &GlobalCluster{
		GlobalClusterIdentifier: globalClusterID,
		GlobalClusterArn:        b.globalClusterARN(globalClusterID),
		GlobalClusterResourceID: fmt.Sprintf("cluster-%s", globalClusterID),
		Status:                  clusterStatusAvailable,
		Engine:                  neptuneEngine,
		EngineVersion:           defaultEngineVersion,
	}
	if sourceDBClusterID != "" {
		if cl, exists := b.clusterGet(region, sourceDBClusterID); exists {
			gc.GlobalClusterMembers = []GlobalClusterMember{
				{
					DBClusterARN: b.clusterARN(region, cl.DBClusterIdentifier),
					IsWriter:     true,
				},
			}
			gc.EngineVersion = cl.EngineVersion
			gc.StorageEncrypted = cl.StorageEncrypted
		}
	}
	b.globalClusters.Put(gc)
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// DescribeGlobalClusters returns all Neptune global clusters.
// Global clusters are partition-scoped, so all are returned regardless of region.
func (b *InMemoryBackend) DescribeGlobalClusters(_ context.Context) []GlobalCluster {
	b.mu.RLock("DescribeGlobalClusters")
	defer b.mu.RUnlock()
	globalClusters := b.globalClusters.All()
	result := make([]GlobalCluster, 0, len(globalClusters))
	for _, gc := range globalClusters {
		cp := *gc
		cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
		copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)
		result = append(result, cp)
	}
	slices.SortFunc(result, func(a, b GlobalCluster) int {
		return strings.Compare(a.GlobalClusterIdentifier, b.GlobalClusterIdentifier)
	})

	return result
}

// DeleteGlobalCluster deletes a Neptune global cluster (partition-scoped).
func (b *InMemoryBackend) DeleteGlobalCluster(
	_ context.Context,
	globalClusterID string,
) (*GlobalCluster, error) {
	b.mu.Lock("DeleteGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters.Get(globalClusterID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: global cluster %s not found",
			ErrGlobalClusterNotFound,
			globalClusterID,
		)
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)
	b.globalClusters.Delete(globalClusterID)

	return &cp, nil
}

// FailoverGlobalCluster performs a failover for a Neptune global cluster (partition-scoped).
// targetDBClusterID is accepted for API compatibility but not used in the in-memory backend.
func (b *InMemoryBackend) FailoverGlobalCluster(
	_ context.Context, globalClusterID, _ string,
) (*GlobalCluster, error) {
	b.mu.Lock("FailoverGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters.Get(globalClusterID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: global cluster %s not found",
			ErrGlobalClusterNotFound,
			globalClusterID,
		)
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// ModifyGlobalCluster modifies a Neptune global cluster (partition-scoped).
func (b *InMemoryBackend) ModifyGlobalCluster(
	_ context.Context,
	globalClusterID string,
) (*GlobalCluster, error) {
	b.mu.Lock("ModifyGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters.Get(globalClusterID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: global cluster %s not found",
			ErrGlobalClusterNotFound,
			globalClusterID,
		)
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// RemoveFromGlobalCluster removes a DB cluster from a Neptune global cluster (partition-scoped).
func (b *InMemoryBackend) RemoveFromGlobalCluster(
	_ context.Context, globalClusterID, dbClusterARN string,
) (*GlobalCluster, error) {
	b.mu.Lock("RemoveFromGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters.Get(globalClusterID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: global cluster %s not found",
			ErrGlobalClusterNotFound,
			globalClusterID,
		)
	}
	kept := make([]GlobalClusterMember, 0, len(gc.GlobalClusterMembers))
	for _, m := range gc.GlobalClusterMembers {
		if m.DBClusterARN != dbClusterARN {
			kept = append(kept, m)
		}
	}
	gc.GlobalClusterMembers = kept
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// SwitchoverGlobalCluster switches over a Neptune global cluster to a new primary (partition-scoped).
// targetDBClusterID is accepted for API compatibility but not used in the in-memory backend.
func (b *InMemoryBackend) SwitchoverGlobalCluster(
	_ context.Context, globalClusterID, _ string,
) (*GlobalCluster, error) {
	b.mu.Lock("SwitchoverGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters.Get(globalClusterID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: global cluster %s not found",
			ErrGlobalClusterNotFound,
			globalClusterID,
		)
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}
