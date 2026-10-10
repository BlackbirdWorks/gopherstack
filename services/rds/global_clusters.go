package rds

import (
	"cmp"
	"fmt"
	"net/url"
	"slices"
	"strings"
)

// CreateGlobalCluster creates a new global cluster.
func (b *InMemoryBackend) CreateGlobalCluster(
	id, engine, engineVersion, engineLifecycleSupport string,
	storageEncrypted, deletionProtection bool,
) (*GlobalCluster, error) {
	return b.CreateGlobalClusterFromSource(
		id, engine, engineVersion, engineLifecycleSupport, "", "", storageEncrypted, deletionProtection,
	)
}

// CreateGlobalClusterFromSource is CreateGlobalCluster plus SourceDBClusterIdentifier (id or ARN, becomes the
// writer and lends engine, version and encryption) and DatabaseName.
func (b *InMemoryBackend) CreateGlobalClusterFromSource(
	id, engine, engineVersion, engineLifecycleSupport, sourceCluster, databaseName string,
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

	var source *DBCluster

	if sourceCluster != "" {
		var srcExists bool
		if source, srcExists = b.clusters.Get(normalizeID(rdsIDFromARN(sourceCluster))); !srcExists {
			return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, sourceCluster)
		}

		engine = cmp.Or(engine, source.Engine)
		engineVersion = cmp.Or(engineVersion, source.EngineVersion)
		storageEncrypted = storageEncrypted || source.StorageEncrypted
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
		EngineLifecycleSupport:  engineLifecycleSupport,
		DatabaseName:            databaseName,
	}
	if source != nil {
		joinGlobalCluster(gc, source.DBClusterArn)
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

// applyGlobalClusterFilters validates the Filters contract; "region" is accepted but matches
// vacuously because PrimaryRegion is never populated.
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

// majorVersion returns the leading dot-separated segment of an engine
// version string (e.g. "15.4" -> "15"), this backend's simplified stand-in
// for AWS's per-engine major-version compatibility matrix.
func majorVersion(v string) string {
	if before, _, found := strings.Cut(v, "."); found {
		return before
	}

	return v
}

// ModifyGlobalCluster modifies properties of a global cluster.
func (b *InMemoryBackend) ModifyGlobalCluster(
	id, newGlobalClusterID, engineVersion string,
	deletionProtection *bool,
	allowMajorVersionUpgrade bool,
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
		if !allowMajorVersionUpgrade && gc.EngineVersion != "" &&
			majorVersion(engineVersion) != majorVersion(gc.EngineVersion) {
			return nil, fmt.Errorf(
				"%w: engine version upgrade from %s to %s changes the major version; "+
					"set AllowMajorVersionUpgrade to upgrade",
				ErrInvalidParameterCombination, gc.EngineVersion, engineVersion,
			)
		}
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
	gc.GlobalClusterMembers = slices.DeleteFunc(gc.GlobalClusterMembers, func(m GlobalClusterMember) bool {
		return m.DBClusterArn == dbClusterARN
	})
	cp := *gc

	return &cp, nil
}

// FailoverGlobalCluster promotes the target member cluster to writer.
func (b *InMemoryBackend) FailoverGlobalCluster(globalClusterID, target string) (*GlobalCluster, error) {
	b.mu.Lock("FailoverGlobalCluster")
	defer b.mu.Unlock()

	return b.promoteGlobalWriterLocked(globalClusterID, target)
}

// SwitchoverGlobalCluster promotes the target member cluster to writer.
func (b *InMemoryBackend) SwitchoverGlobalCluster(globalClusterID, target string) (*GlobalCluster, error) {
	b.mu.Lock("SwitchoverGlobalCluster")
	defer b.mu.Unlock()

	return b.promoteGlobalWriterLocked(globalClusterID, target)
}

// promoteGlobalWriterLocked makes target (id or ARN of an existing member cluster) the sole writer.
func (b *InMemoryBackend) promoteGlobalWriterLocked(globalClusterID, target string) (*GlobalCluster, error) {
	gc, ok := b.globalClusters.Get(globalClusterID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrGlobalClusterNotFound, globalClusterID)
	}
	if target == "" {
		return nil, fmt.Errorf("%w: TargetDbClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	cluster, ok := b.clusters.Get(normalizeID(rdsIDFromARN(target)))
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, target)
	}
	idx := slices.IndexFunc(gc.GlobalClusterMembers, func(m GlobalClusterMember) bool {
		return m.DBClusterArn == cluster.DBClusterArn
	})
	if idx < 0 {
		return nil, fmt.Errorf(
			"%w: cluster %s is not a member of global cluster %s",
			ErrInvalidGlobalClusterState, cluster.DBClusterIdentifier, globalClusterID,
		)
	}
	for i := range gc.GlobalClusterMembers {
		gc.GlobalClusterMembers[i].IsWriter = i == idx
	}
	gc.Status = instanceStatusAvailable
	cp := *gc
	cp.GlobalClusterMembers = slices.Clone(gc.GlobalClusterMembers)

	return &cp, nil
}

// joinGlobalCluster adds clusterARN as a member; the first member is the writer.
func joinGlobalCluster(gc *GlobalCluster, clusterARN string) {
	gc.GlobalClusterMembers = append(gc.GlobalClusterMembers, GlobalClusterMember{
		DBClusterArn: clusterARN,
		IsWriter:     len(gc.GlobalClusterMembers) == 0,
	})
	gc.ClusterARNs = append(gc.ClusterARNs, clusterARN)
}

// leaveGlobalClustersLocked drops clusterARN from every global cluster. Caller holds b.mu.
func (b *InMemoryBackend) leaveGlobalClustersLocked(clusterARN string) {
	for _, gc := range b.globalClusters.All() {
		gc.GlobalClusterMembers = slices.DeleteFunc(gc.GlobalClusterMembers, func(m GlobalClusterMember) bool {
			return m.DBClusterArn == clusterARN
		})
		gc.ClusterARNs = slices.DeleteFunc(gc.ClusterARNs, func(a string) bool { return a == clusterARN })
	}
}
