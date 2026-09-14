package neptune

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// isNeptuneARN reports whether s looks like an AWS ARN, as opposed to a bare
// resource identifier -- both forms are accepted for DbClusterIdentifier/
// TargetDbClusterIdentifier parameters throughout this file, matching this
// backend's existing leniency elsewhere (see e.g. validateResourceARN's
// callers, which also tolerate either form).
func isNeptuneARN(s string) bool { return strings.HasPrefix(s, "arn:") }

// globalClusterARN returns the partition-scoped ARN for a Neptune global cluster.
func (b *InMemoryBackend) globalClusterARN(id string) string {
	return arn.Build("rds", "", b.accountID, "global-cluster:"+id)
}

// CreateGlobalCluster creates a Neptune global cluster.
// Global clusters are partition-scoped (not region-isolated), but the optional
// source DB cluster is looked up in the ctx region where it resides.
func (b *InMemoryBackend) CreateGlobalCluster(
	ctx context.Context, globalClusterID, sourceDBClusterID, databaseName string,
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
		DatabaseName:            databaseName,
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
			cl.GlobalClusterIdentifier = globalClusterID
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
	ctx context.Context,
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

	if gc.DeletionProtection {
		return nil, fmt.Errorf(
			"%w: cannot delete protected global cluster %s, disable deletion protection first",
			ErrInvalidGlobalClusterState, globalClusterID,
		)
	}

	if len(gc.GlobalClusterMembers) > 0 {
		return nil, fmt.Errorf(
			"%w: global cluster %s still has attached member clusters; detach or delete them first",
			ErrInvalidGlobalClusterState, globalClusterID,
		)
	}

	cp := *gc
	b.globalClusters.Delete(globalClusterID)
	region := regionFromARN(gc.GlobalClusterArn, getRegion(ctx, b.region))
	delete(b.tagsStore(region), gc.GlobalClusterArn)

	return &cp, nil
}

// FailoverGlobalCluster fails a Neptune global cluster over to
// targetDBClusterID, promoting it to the new primary/writer -- see
// promoteGlobalClusterWriter for the member-promotion logic and error
// semantics shared with SwitchoverGlobalCluster (partition-scoped, but the
// target is looked up in the ctx region when it names an existing DB cluster
// rather than an ARN).
func (b *InMemoryBackend) FailoverGlobalCluster(
	ctx context.Context, globalClusterID, targetDBClusterID string,
) (*GlobalCluster, error) {
	region := getRegion(ctx, b.region)
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
	if err := b.promoteGlobalClusterWriter(region, gc, targetDBClusterID); err != nil {
		return nil, err
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// promoteGlobalClusterWriter flips IsWriter on gc.GlobalClusterMembers so
// that targetDBClusterID becomes the sole writer, mirroring the real
// FailoverGlobalCluster/SwitchoverGlobalCluster member promotion -- both
// operations declare the identical error set (DBClusterNotFoundFault,
// GlobalClusterNotFoundFault, InvalidDBClusterStateFault,
// InvalidGlobalClusterStateFault; neptune@v1.48.4 deserializers.go:5338
// awsAwsquery_deserializeOpErrorFailoverGlobalCluster and :8147
// ...Switchover..., both switching on exactly those four codes).
// TargetDbClusterIdentifier's doc comment
// (api_op_FailoverGlobalCluster.go:53) names it "the ARN of the secondary
// Neptune DB cluster" to promote: a target that resolves to no DB cluster
// this backend tracks at all is DBClusterNotFoundFault; one that resolves to
// a real cluster but isn't a member of THIS global cluster, or is already
// its writer (nothing to fail over to), is InvalidDBClusterStateFault --
// real AWS rejects both cases since the argument must name an existing
// secondary. Caller must hold b.mu (write lock).
func (b *InMemoryBackend) promoteGlobalClusterWriter(
	region string, gc *GlobalCluster, targetDBClusterID string,
) error {
	if targetDBClusterID == "" {
		return fmt.Errorf("%w: TargetDbClusterIdentifier is required", ErrInvalidParameter)
	}
	targetARN := targetDBClusterID
	if !isNeptuneARN(targetDBClusterID) {
		cl, ok := b.clusterGet(region, targetDBClusterID)
		if !ok {
			return fmt.Errorf("%w: DB cluster %s not found", ErrClusterNotFound, targetDBClusterID)
		}
		targetARN = b.clusterARN(region, cl.DBClusterIdentifier)
	} else if _, ok := b.clusterByARNLocked(targetARN, region); !ok {
		return fmt.Errorf("%w: DB cluster %s not found", ErrClusterNotFound, targetDBClusterID)
	}

	idx := -1
	for i := range gc.GlobalClusterMembers {
		if gc.GlobalClusterMembers[i].DBClusterARN == targetARN {
			idx = i

			break
		}
	}
	if idx == -1 {
		return fmt.Errorf(
			"%w: DB cluster %s is not a member of global cluster %s",
			ErrInvalidDBClusterStateFault, targetDBClusterID, gc.GlobalClusterIdentifier,
		)
	}
	if gc.GlobalClusterMembers[idx].IsWriter {
		return fmt.Errorf(
			"%w: DB cluster %s is already the primary of global cluster %s",
			ErrInvalidDBClusterStateFault, targetDBClusterID, gc.GlobalClusterIdentifier,
		)
	}
	for i := range gc.GlobalClusterMembers {
		gc.GlobalClusterMembers[i].IsWriter = i == idx
	}

	return nil
}

// ModifyGlobalCluster applies deletion-protection/engine-version/rename
// changes to a Neptune global cluster (partition-scoped) -- previously a
// disguised no-op: the interface didn't even accept the new values to apply.
func (b *InMemoryBackend) ModifyGlobalCluster(
	_ context.Context,
	globalClusterID string,
	opts GlobalClusterModifyOptions,
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
	if opts.DeletionProtectionSet {
		gc.DeletionProtection = opts.DeletionProtection
	}
	if opts.EngineVersion != "" {
		gc.EngineVersion = opts.EngineVersion
	}
	if opts.NewGlobalClusterIdentifier != "" && opts.NewGlobalClusterIdentifier != globalClusterID {
		if b.globalClusters.Has(opts.NewGlobalClusterIdentifier) {
			return nil, fmt.Errorf(
				"%w: global cluster %s already exists",
				ErrGlobalClusterAlreadyExists,
				opts.NewGlobalClusterIdentifier,
			)
		}
		gc.GlobalClusterIdentifier = opts.NewGlobalClusterIdentifier
		gc.GlobalClusterArn = b.globalClusterARN(opts.NewGlobalClusterIdentifier)
		b.globalClusters.Delete(globalClusterID)
		b.globalClusters.Put(gc)
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// RemoveFromGlobalCluster removes a DB cluster from a Neptune global cluster (partition-scoped).
func (b *InMemoryBackend) RemoveFromGlobalCluster(
	ctx context.Context, globalClusterID, dbClusterARN string,
) (*GlobalCluster, error) {
	region := getRegion(ctx, b.region)
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
	if cl, ok := b.clusterByARNLocked(dbClusterARN, region); ok && cl.GlobalClusterIdentifier == globalClusterID {
		cl.GlobalClusterIdentifier = ""
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// SwitchoverGlobalCluster switches a Neptune global cluster over to a new
// primary (partition-scoped), promoting targetDBClusterID the same way
// FailoverGlobalCluster does -- the two AWS operations differ in
// data-loss guarantees (switchover is graceful, failover is not), a
// distinction this synchronous in-memory backend has no failure window to
// model, so both perform the same real member promotion.
func (b *InMemoryBackend) SwitchoverGlobalCluster(
	ctx context.Context, globalClusterID, targetDBClusterID string,
) (*GlobalCluster, error) {
	region := getRegion(ctx, b.region)
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
	if err := b.promoteGlobalClusterWriter(region, gc, targetDBClusterID); err != nil {
		return nil, err
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}
