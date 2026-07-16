package rds

import (
	"fmt"
	"slices"
	"time"
)

// CreateDBClusterSnapshot creates a snapshot of the given cluster.
func (b *InMemoryBackend) CreateDBClusterSnapshot(snapshotID, clusterID string) (*DBClusterSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBClusterSnapshot")
	defer b.mu.Unlock()
	if _, exists := b.clusterSnapshots.Get(snapshotID); exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s already exists", ErrClusterSnapshotAlreadyExists, snapshotID)
	}
	cluster, exists := b.clusters.Get(clusterID)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	snap := b.newManualClusterSnapshotLocked(snapshotID, cluster)
	b.clusterSnapshots.Put(snap)
	cp := *snap

	return &cp, nil
}

// newManualClusterSnapshotLocked builds a manual DB cluster snapshot record
// for cluster. It does not check for an existing snapshot with the same ID or
// insert into b.clusterSnapshots — callers must do both under b.mu.
func (b *InMemoryBackend) newManualClusterSnapshotLocked(snapshotID string, cluster *DBCluster) *DBClusterSnapshot {
	return &DBClusterSnapshot{
		SnapshotCreateTime:          time.Now().UTC(),
		DBClusterSnapshotIdentifier: snapshotID,
		DBClusterIdentifier:         cluster.DBClusterIdentifier,
		Engine:                      cluster.Engine,
		EngineVersion:               cluster.EngineVersion,
		Status:                      instanceStatusAvailable,
		PercentProgress:             percentProgressComplete,
		StorageEncrypted:            cluster.StorageEncrypted,
	}
}

// DescribeDBClusterSnapshots returns cluster snapshots.
// If clusterID is non-empty, only snapshots whose DBClusterIdentifier matches are returned.
func (b *InMemoryBackend) DescribeDBClusterSnapshots(snapshotID, clusterID string) ([]DBClusterSnapshot, error) {
	b.mu.RLock("DescribeDBClusterSnapshots")
	defer b.mu.RUnlock()
	if snapshotID != "" {
		snap, exists := b.clusterSnapshots.Get(snapshotID)
		if !exists {
			return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
		}
		cp := *snap

		return []DBClusterSnapshot{cp}, nil
	}
	result := make([]DBClusterSnapshot, 0, b.clusterSnapshots.Len())
	for _, snap := range b.clusterSnapshots.All() {
		if clusterID != "" && snap.DBClusterIdentifier != clusterID {
			continue
		}
		result = append(result, *snap)
	}
	slices.SortFunc(result, func(a, b DBClusterSnapshot) int {
		if a.DBClusterSnapshotIdentifier < b.DBClusterSnapshotIdentifier {
			return -1
		}
		if a.DBClusterSnapshotIdentifier > b.DBClusterSnapshotIdentifier {
			return 1
		}

		return 0
	})

	return result, nil
}

// DeleteDBClusterSnapshot removes the given cluster snapshot.
func (b *InMemoryBackend) DeleteDBClusterSnapshot(snapshotID string) (*DBClusterSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("DeleteDBClusterSnapshot")
	defer b.mu.Unlock()
	snap, exists := b.clusterSnapshots.Get(snapshotID)
	if !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	cp := *snap
	b.clusterSnapshots.Delete(snapshotID)
	delete(b.tags, b.rdsARN("cluster-snapshot", snapshotID))

	return &cp, nil
}

// CopyDBClusterSnapshot creates a copy of the given cluster snapshot.
func (b *InMemoryBackend) CopyDBClusterSnapshot(sourceSnapshotID, targetSnapshotID string) (*DBClusterSnapshot, error) {
	if sourceSnapshotID == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	if targetSnapshotID == "" {
		return nil, fmt.Errorf("%w: TargetDBClusterSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CopyDBClusterSnapshot")
	defer b.mu.Unlock()
	source, srcExists := b.clusterSnapshots.Get(sourceSnapshotID)
	if !srcExists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, sourceSnapshotID)
	}
	if _, dstExists := b.clusterSnapshots.Get(targetSnapshotID); dstExists {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s already exists",
			ErrClusterSnapshotAlreadyExists,
			targetSnapshotID,
		)
	}
	snap := &DBClusterSnapshot{
		DBClusterSnapshotIdentifier: targetSnapshotID,
		DBClusterIdentifier:         source.DBClusterIdentifier,
		Engine:                      source.Engine,
		Status:                      instanceStatusAvailable,
	}
	b.clusterSnapshots.Put(snap)
	cp := *snap

	return &cp, nil
}

// DescribeDBClusterSnapshotAttributes returns attributes for a DB cluster snapshot.
func (b *InMemoryBackend) DescribeDBClusterSnapshotAttributes(
	snapshotID string,
) (*DBClusterSnapshotAttributesResult, error) {
	b.mu.RLock("DescribeDBClusterSnapshotAttributes")
	defer b.mu.RUnlock()
	if _, ok := b.clusterSnapshots.Get(snapshotID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	if attrs, ok := b.clusterSnapshotAttributes.Get(snapshotID); ok {
		cp := *attrs

		return &cp, nil
	}
	result := &DBClusterSnapshotAttributesResult{
		DBClusterSnapshotIdentifier: snapshotID,
		DBClusterSnapshotAttributes: []DBSnapshotAttribute{},
	}
	cp := *result

	return &cp, nil
}

// ModifyDBClusterSnapshotAttribute adds or removes attribute values for a cluster snapshot.
func (b *InMemoryBackend) ModifyDBClusterSnapshotAttribute(
	snapshotID, attributeName string,
	valuesToAdd, valuesToRemove []string,
) (*DBClusterSnapshotAttributesResult, error) {
	b.mu.Lock("ModifyDBClusterSnapshotAttribute")
	defer b.mu.Unlock()
	if _, ok := b.clusterSnapshots.Get(snapshotID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	result, ok := b.clusterSnapshotAttributes.Get(snapshotID)
	if !ok {
		result = &DBClusterSnapshotAttributesResult{
			DBClusterSnapshotIdentifier: snapshotID,
			DBClusterSnapshotAttributes: []DBSnapshotAttribute{},
		}
		b.clusterSnapshotAttributes.Put(result)
	}
	applySnapshotAttributeChange(&result.DBClusterSnapshotAttributes, attributeName, valuesToAdd, valuesToRemove)
	cp := *result

	return &cp, nil
}
