package redshift

import "fmt"

// ---- Snapshot Copy Grants ----

// CreateSnapshotCopyGrant creates a KMS key grant used for cross-region snapshot copy.
func (b *InMemoryBackend) CreateSnapshotCopyGrant(
	name, kmsKeyID string,
	tagMap map[string]string,
) (*SnapshotCopyGrant, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: SnapshotCopyGrantName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSnapshotCopyGrant")
	defer b.mu.Unlock()

	if _, exists := b.snapshotCopyGrants.Get(name); exists {
		return nil, fmt.Errorf("%w: grant %s already exists", ErrSnapshotCopyGrantAlreadyExists, name)
	}

	grant := &SnapshotCopyGrant{
		SnapshotCopyGrantName: name,
		KMSKeyID:              kmsKeyID,
		Tags:                  tagMap,
	}
	b.snapshotCopyGrants.Put(grant)

	cp := *grant

	return &cp, nil
}

// DeleteSnapshotCopyGrant deletes the named snapshot copy grant.
func (b *InMemoryBackend) DeleteSnapshotCopyGrant(name string) error {
	if name == "" {
		return fmt.Errorf("%w: SnapshotCopyGrantName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteSnapshotCopyGrant")
	defer b.mu.Unlock()

	if _, exists := b.snapshotCopyGrants.Get(name); !exists {
		return fmt.Errorf("%w: grant %s not found", ErrSnapshotCopyGrantNotFound, name)
	}

	b.snapshotCopyGrants.Delete(name)

	return nil
}

// DescribeSnapshotCopyGrants returns snapshot copy grants, optionally filtered by name.
func (b *InMemoryBackend) DescribeSnapshotCopyGrants(name string) ([]SnapshotCopyGrant, error) {
	b.mu.RLock("DescribeSnapshotCopyGrants")
	defer b.mu.RUnlock()

	if name != "" {
		g, exists := b.snapshotCopyGrants.Get(name)
		if !exists {
			return nil, fmt.Errorf("%w: grant %s not found", ErrSnapshotCopyGrantNotFound, name)
		}

		cp := *g

		return []SnapshotCopyGrant{cp}, nil
	}

	result := make([]SnapshotCopyGrant, 0, b.snapshotCopyGrants.Len())

	for _, g := range b.snapshotCopyGrants.All() {
		result = append(result, *g)
	}

	return result, nil
}

// ---- Snapshot Copy (cluster-level) ----

// EnableSnapshotCopy enables cross-region snapshot copy for a cluster.
func (b *InMemoryBackend) EnableSnapshotCopy(
	clusterID, destinationRegion, grantName string,
	retentionPeriod int,
) (*Cluster, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	if destinationRegion == "" {
		return nil, fmt.Errorf("%w: DestinationRegion is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableSnapshotCopy")
	defer b.mu.Unlock()

	cluster, exists := b.clusters.Get(clusterID)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	if _, already := b.snapshotCopyConfigs[clusterID]; already {
		return nil, fmt.Errorf(
			"%w: snapshot copy already enabled for cluster %s",
			ErrSnapshotCopyAlreadyEnabled,
			clusterID,
		)
	}

	if retentionPeriod <= 0 {
		retentionPeriod = 7
	}

	b.snapshotCopyConfigs[clusterID] = &SnapshotCopyConfig{
		DestinationRegion:     destinationRegion,
		SnapshotCopyGrantName: grantName,
		RetentionPeriod:       retentionPeriod,
	}

	cp := cloneCluster(cluster)

	return &cp, nil
}

// DisableSnapshotCopy disables cross-region snapshot copy for a cluster.
func (b *InMemoryBackend) DisableSnapshotCopy(clusterID string) (*Cluster, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableSnapshotCopy")
	defer b.mu.Unlock()

	cluster, exists := b.clusters.Get(clusterID)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	if _, enabled := b.snapshotCopyConfigs[clusterID]; !enabled {
		return nil, fmt.Errorf("%w: snapshot copy not enabled for cluster %s", ErrSnapshotCopyNotEnabled, clusterID)
	}

	delete(b.snapshotCopyConfigs, clusterID)

	cp := cloneCluster(cluster)

	return &cp, nil
}

// ModifySnapshotCopyRetentionPeriod modifies the retention period for cross-region snapshot copy.
func (b *InMemoryBackend) ModifySnapshotCopyRetentionPeriod(clusterID string, retentionPeriod int) (*Cluster, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifySnapshotCopyRetentionPeriod")
	defer b.mu.Unlock()

	cluster, exists := b.clusters.Get(clusterID)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	cfg, enabled := b.snapshotCopyConfigs[clusterID]
	if !enabled {
		return nil, fmt.Errorf("%w: snapshot copy not enabled for cluster %s", ErrSnapshotCopyNotEnabled, clusterID)
	}

	cfg.RetentionPeriod = retentionPeriod
	cp := cloneCluster(cluster)

	return &cp, nil
}
