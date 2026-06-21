package redshift

import (
	"fmt"
	"time"
)

// CreateClusterSnapshot creates a manual snapshot of the specified cluster.
func (b *InMemoryBackend) CreateClusterSnapshot(snapshotID, clusterID string) (*Snapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: SnapshotIdentifier is required", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateClusterSnapshot")
	defer b.mu.Unlock()

	if _, exists := b.clusters[clusterID]; !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	if _, exists := b.snapshots[snapshotID]; exists {
		return nil, fmt.Errorf("%w: snapshot %s already exists", ErrSnapshotAlreadyExists, snapshotID)
	}

	snap := &Snapshot{
		SnapshotIdentifier:            snapshotID,
		ClusterIdentifier:             clusterID,
		SnapshotType:                  "manual",
		Status:                        "available",
		AccountsWithRestoreAccess:     []AccountWithRestoreAccess{},
		ManualSnapshotRetentionPeriod: -1,
		SnapshotCreateTime:            time.Now(),
	}
	b.snapshots[snapshotID] = snap

	return cloneSnapshot(snap), nil
}

// DeleteClusterSnapshot removes a cluster snapshot.
func (b *InMemoryBackend) DeleteClusterSnapshot(snapshotID string) (*Snapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: SnapshotIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteClusterSnapshot")
	defer b.mu.Unlock()

	snap, exists := b.snapshots[snapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)
	}

	cp := cloneSnapshot(snap)
	delete(b.snapshots, snapshotID)

	return cp, nil
}

// DescribeClusterSnapshots returns snapshots, optionally filtered by snapshotID, clusterID, or
// snapshotType ("manual" or "automated"). An empty snapshotType matches all types.
func (b *InMemoryBackend) DescribeClusterSnapshots(snapshotID, clusterID, snapshotType string) ([]Snapshot, error) {
	b.mu.RLock("DescribeClusterSnapshots")
	defer b.mu.RUnlock()

	if snapshotID != "" {
		snap, exists := b.snapshots[snapshotID]
		if !exists {
			return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)
		}

		return []Snapshot{*cloneSnapshot(snap)}, nil
	}

	result := make([]Snapshot, 0, len(b.snapshots))

	for _, snap := range b.snapshots {
		if clusterID != "" && snap.ClusterIdentifier != clusterID {
			continue
		}
		if snapshotType != "" && snap.SnapshotType != snapshotType {
			continue
		}
		result = append(result, *cloneSnapshot(snap))
	}

	return result, nil
}

// CopyClusterSnapshot copies a snapshot to a new identifier, optionally to a different region.
func (b *InMemoryBackend) CopyClusterSnapshot(
	sourceSnapshotID, destinationSnapshotID string,
) (*Snapshot, error) {
	if sourceSnapshotID == "" {
		return nil, fmt.Errorf("%w: SourceSnapshotIdentifier is required", ErrInvalidParameter)
	}
	if destinationSnapshotID == "" {
		return nil, fmt.Errorf("%w: TargetSnapshotIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CopyClusterSnapshot")
	defer b.mu.Unlock()

	src, exists := b.snapshots[sourceSnapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, sourceSnapshotID)
	}

	if _, dstExists := b.snapshots[destinationSnapshotID]; dstExists {
		return nil, fmt.Errorf("%w: snapshot %s already exists", ErrSnapshotAlreadyExists, destinationSnapshotID)
	}

	cp := cloneSnapshot(src)
	cp.SnapshotIdentifier = destinationSnapshotID
	cp.SnapshotType = "manual"
	b.snapshots[destinationSnapshotID] = cp

	result := cloneSnapshot(cp)

	return result, nil
}

// RestoreFromClusterSnapshot creates a new cluster from an existing snapshot.
func (b *InMemoryBackend) RestoreFromClusterSnapshot(clusterID, snapshotID string) (*Cluster, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: SnapshotIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("RestoreFromClusterSnapshot")
	defer b.mu.Unlock()

	snap, exists := b.snapshots[snapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)
	}

	if _, clusterExists := b.clusters[clusterID]; clusterExists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, clusterID)
	}

	endpoint := fmt.Sprintf("%s.%s.%s.redshift.amazonaws.com", clusterID, b.accountID, b.region)
	cluster := &Cluster{
		ClusterIdentifier: clusterID,
		NodeType:          defaultNodeType,
		ClusterType:       clusterTypeMultiNode,
		Endpoint:          endpoint,
		Status:            "restoring",
		DBName:            snap.ClusterIdentifier,
		MasterUsername:    defaultMasterUsername,
		Port:              defaultPort,
		NumberOfNodes:     1,
	}

	b.clusters[clusterID] = cluster

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	cp := cloneCluster(cluster)

	return &cp, nil
}
