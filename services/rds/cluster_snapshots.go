package rds

import (
	"cmp"
	"fmt"
	"net/url"
	"slices"
	"time"
)

// CreateDBClusterSnapshot creates a snapshot of the given cluster.
func (b *InMemoryBackend) CreateDBClusterSnapshot(snapshotID, clusterID string) (*DBClusterSnapshot, error) {
	return b.CreateDBClusterSnapshotAs("", snapshotID, clusterID)
}

// CreateDBClusterSnapshotAs is CreateDBClusterSnapshot owned by caller's account.
func (b *InMemoryBackend) CreateDBClusterSnapshotAs(caller, snapshotID, clusterID string) (*DBClusterSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	data, err := b.captureClusterData(clusterID)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateDBClusterSnapshot")
	defer b.mu.Unlock()
	if _, exists := b.clusterSnapshots.Get(normalizeID(snapshotID)); exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s already exists", ErrClusterSnapshotAlreadyExists, snapshotID)
	}
	cluster, exists := b.clusters.Get(normalizeID(clusterID))
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	snap := b.newManualClusterSnapshotLocked(snapshotID, cluster)
	snap.OwnerAccountID = caller
	snap.DBClusterSnapshotArn = b.ownedSnapshotARN(caller, "cluster-snapshot", snapshotID)
	b.clusterSnapshots.Put(snap)
	b.storeSnapDataLocked(snapKindCluster, snapshotID, data)
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
		DBClusterSnapshotArn:        b.rdsARN("cluster-snapshot", snapshotID),
		DBClusterIdentifier:         cluster.DBClusterIdentifier,
		DBClusterResourceID:         cluster.DBClusterResourceID,
		Engine:                      cluster.Engine,
		EngineVersion:               cluster.EngineVersion,
		Status:                      instanceStatusAvailable,
		SnapshotType:                snapshotTypeManual,
		PercentProgress:             percentProgressComplete,
		StorageEncrypted:            cluster.StorageEncrypted,
		KmsKeyID:                    cluster.KmsKeyID,
		ClusterCreateTime:           cluster.ClusterCreateTime,
		MasterUsername:              cluster.MasterUsername,
		StorageType:                 cluster.StorageType,
		Port:                        cluster.Port,
		AllocatedStorage:            cluster.AllocatedStorage,
		Iops:                        cluster.Iops,
		BackupRetentionPeriod:       cluster.BackupRetentionPeriod,
		IAMDatabaseAuthEnabled:      cluster.IAMDatabaseAuthenticationEnabled,
	}
}

// DescribeDBClusterSnapshots returns cluster snapshots.
// If clusterID is non-empty, only snapshots whose DBClusterIdentifier matches are returned.
func (b *InMemoryBackend) DescribeDBClusterSnapshots(snapshotID, clusterID string) ([]DBClusterSnapshot, error) {
	return b.DescribeDBClusterSnapshotsAs(SnapshotQuery{SnapshotID: snapshotID, ParentID: clusterID})
}

// DescribeDBClusterSnapshotsAs is DescribeDBClusterSnapshots from the caller's point of view.
func (b *InMemoryBackend) DescribeDBClusterSnapshotsAs(q SnapshotQuery) ([]DBClusterSnapshot, error) {
	b.mu.RLock("DescribeDBClusterSnapshots")
	defer b.mu.RUnlock()
	if q.SnapshotID != "" {
		id, byARN := snapshotRef(q.SnapshotID)
		snap, exists := b.clusterSnapshots.Get(normalizeID(id))
		if !exists || !b.snapshotReachableLocked(
			q.CallerAccount, snap.OwnerAccountID, snapKindCluster, snap.DBClusterSnapshotIdentifier, byARN,
		) {
			return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, q.SnapshotID)
		}
		cp := *snap

		return []DBClusterSnapshot{cp}, nil
	}
	result := make([]DBClusterSnapshot, 0, b.clusterSnapshots.Len())
	for _, snap := range b.clusterSnapshots.All() {
		if q.ParentID != "" && !idEqual(snap.DBClusterIdentifier, q.ParentID) {
			continue
		}
		if !b.snapshotListedLocked(
			q, snapKindCluster, snap.DBClusterSnapshotIdentifier, snap.OwnerAccountID, snap.SnapshotType,
		) {
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

// isKnownDBClusterSnapshotFilterName reports whether name is a
// Filters.Filter.N.Name value AWS recognizes for DescribeDBClusterSnapshots.
func isKnownDBClusterSnapshotFilterName(name string) bool {
	switch name {
	case filterNameDBClusterID, "db-cluster-snapshot-id", filterNameSnapshotType, filterNameEngine:
		return true
	default:
		return false
	}
}

// applyDBClusterSnapshotFilters narrows snaps per the AWS
// DescribeDBClusterSnapshots Filters contract: each filter ANDs together,
// and a filter's Values list is OR-matched against the corresponding
// snapshot field. An unrecognized filter name returns InvalidParameterValue,
// matching real AWS.
func applyDBClusterSnapshotFilters(vals url.Values, snaps []DBClusterSnapshot) ([]DBClusterSnapshot, error) {
	filters := parseDescribeFilters(vals)
	if len(filters) == 0 {
		return snaps, nil
	}

	for name := range filters {
		if !isKnownDBClusterSnapshotFilterName(name) {
			return nil, fmt.Errorf("%w: Unrecognized filter name: %s", ErrInvalidParameter, name)
		}
	}

	filtered := make([]DBClusterSnapshot, 0, len(snaps))
	for _, s := range snaps {
		if matchesAllDBClusterSnapshotFilters(s, filters) {
			filtered = append(filtered, s)
		}
	}

	return filtered, nil
}

func matchesAllDBClusterSnapshotFilters(s DBClusterSnapshot, filters map[string][]string) bool {
	for name, values := range filters {
		switch name {
		case filterNameDBClusterID:
			if !containsFoldIDOrARN(values, s.DBClusterIdentifier) {
				return false
			}
		case "db-cluster-snapshot-id":
			if !containsFold(values, s.DBClusterSnapshotIdentifier) {
				return false
			}
		case filterNameSnapshotType:
			if !slices.Contains(values, s.SnapshotType) {
				return false
			}
		case filterNameEngine:
			if !slices.Contains(values, s.Engine) {
				return false
			}
		}
	}

	return true
}

// DeleteDBClusterSnapshot removes the given cluster snapshot.
func (b *InMemoryBackend) DeleteDBClusterSnapshot(snapshotID string) (*DBClusterSnapshot, error) {
	return b.DeleteDBClusterSnapshotAs("", snapshotID)
}

// DeleteDBClusterSnapshotAs deletes a cluster snapshot the caller owns.
func (b *InMemoryBackend) DeleteDBClusterSnapshotAs(caller, snapshotID string) (*DBClusterSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("DeleteDBClusterSnapshot")
	defer b.mu.Unlock()
	snap, exists := b.clusterSnapshots.Get(normalizeID(snapshotID))
	if !exists || !ownsSnapshot(caller, snap.OwnerAccountID) {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	cp := *snap
	b.clusterSnapshots.Delete(normalizeID(snapshotID))
	b.dropSnapDataLocked(snapKindCluster, snapshotID)
	// Use snap.DBClusterSnapshotIdentifier (the stored, creation-time casing)
	// rather than the raw snapshotID argument -- see normalizeID.
	delete(b.tags, cmp.Or(snap.DBClusterSnapshotArn, b.rdsARN("cluster-snapshot", snap.DBClusterSnapshotIdentifier)))

	return &cp, nil
}

// CopyDBClusterSnapshot creates a copy of the given cluster snapshot.
// copyTags mirrors CopyDBSnapshot's CopyTags contract (rds@v1.124.1
// api_op_CopyDBClusterSnapshot.go's CopyTags: "By default, tags are not
// copied"): when true, the source snapshot's tags are copied onto the new
// target snapshot.
func (b *InMemoryBackend) CopyDBClusterSnapshot(
	sourceSnapshotID, targetSnapshotID string, copyTags bool,
) (*DBClusterSnapshot, error) {
	return b.CopyDBClusterSnapshotAs("", sourceSnapshotID, targetSnapshotID, copyTags, "")
}

// CopyDBClusterSnapshotWithKMS is CopyDBClusterSnapshot plus KmsKeyId, which re-encrypts the copy.
func (b *InMemoryBackend) CopyDBClusterSnapshotWithKMS(
	sourceSnapshotID, targetSnapshotID string, copyTags bool, kmsKeyID string,
) (*DBClusterSnapshot, error) {
	return b.CopyDBClusterSnapshotAs("", sourceSnapshotID, targetSnapshotID, copyTags, kmsKeyID)
}

// CopyDBClusterSnapshotAs copies a snapshot reachable by caller into caller's account.
func (b *InMemoryBackend) CopyDBClusterSnapshotAs(
	caller, sourceSnapshotID, targetSnapshotID string, copyTags bool, kmsKeyID string,
) (*DBClusterSnapshot, error) {
	if sourceSnapshotID == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	if targetSnapshotID == "" {
		return nil, fmt.Errorf("%w: TargetDBClusterSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CopyDBClusterSnapshot")
	defer b.mu.Unlock()
	// SourceDBClusterSnapshotIdentifier accepts either a bare identifier or a
	// full ARN (real AWS docs: required when copying an encrypted snapshot);
	// previously only a bare identifier resolved, so the ARN form always 404ed.
	srcID, byARN := snapshotRef(sourceSnapshotID)
	source, srcExists := b.clusterSnapshots.Get(normalizeID(srcID))
	if !srcExists || !b.snapshotReachableLocked(
		caller, source.OwnerAccountID, snapKindCluster, source.DBClusterSnapshotIdentifier, byARN,
	) {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, sourceSnapshotID)
	}
	if _, dstExists := b.clusterSnapshots.Get(normalizeID(targetSnapshotID)); dstExists {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s already exists",
			ErrClusterSnapshotAlreadyExists,
			targetSnapshotID,
		)
	}
	targetArn := b.ownedSnapshotARN(caller, "cluster-snapshot", targetSnapshotID)
	snap := &DBClusterSnapshot{
		SnapshotCreateTime:          time.Now().UTC(),
		DBClusterSnapshotIdentifier: targetSnapshotID,
		DBClusterSnapshotArn:        targetArn,
		OwnerAccountID:              caller,
		DBClusterIdentifier:         source.DBClusterIdentifier,
		DBClusterResourceID:         source.DBClusterResourceID,
		Engine:                      source.Engine,
		EngineVersion:               source.EngineVersion,
		Status:                      instanceStatusAvailable,
		SnapshotType:                snapshotTypeManual,
		PercentProgress:             percentProgressComplete,
		StorageEncrypted:            source.StorageEncrypted || kmsKeyID != "",
		KmsKeyID:                    cmp.Or(kmsKeyID, source.KmsKeyID),
		CopyTagsToSnapshot:          copyTags,
		SourceDBClusterSnapshotArn:  source.DBClusterSnapshotArn,
		ClusterCreateTime:           source.ClusterCreateTime,
		MasterUsername:              source.MasterUsername,
		StorageType:                 source.StorageType,
		Port:                        source.Port,
		AllocatedStorage:            source.AllocatedStorage,
		Iops:                        source.Iops,
		BackupRetentionPeriod:       source.BackupRetentionPeriod,
		IAMDatabaseAuthEnabled:      source.IAMDatabaseAuthEnabled,
	}
	b.clusterSnapshots.Put(snap)
	b.copySnapDataLocked(snapKindCluster, source.DBClusterSnapshotIdentifier, targetSnapshotID)
	if copyTags {
		if srcTags := b.tags[source.DBClusterSnapshotArn]; len(srcTags) > 0 {
			cp := make([]Tag, len(srcTags))
			copy(cp, srcTags)
			b.tags[targetArn] = cp
		}
	}
	cp := *snap

	return &cp, nil
}

// DescribeDBClusterSnapshotAttributes returns attributes for a DB cluster snapshot.
func (b *InMemoryBackend) DescribeDBClusterSnapshotAttributes(
	snapshotID string,
) (*DBClusterSnapshotAttributesResult, error) {
	return b.DescribeDBClusterSnapshotAttributesAs("", snapshotID)
}

// DescribeDBClusterSnapshotAttributesAs describes the attributes of a cluster snapshot the caller owns.
func (b *InMemoryBackend) DescribeDBClusterSnapshotAttributesAs(
	caller, snapshotID string,
) (*DBClusterSnapshotAttributesResult, error) {
	b.mu.RLock("DescribeDBClusterSnapshotAttributes")
	defer b.mu.RUnlock()
	snap, ok := b.clusterSnapshots.Get(normalizeID(snapshotID))
	if !ok || !ownsSnapshot(caller, snap.OwnerAccountID) {
		return nil, fmt.Errorf("%w: %s", ErrClusterSnapshotNotFound, snapshotID)
	}
	if stored, storedOK := b.clusterSnapshotAttributes.Get(normalizeID(snapshotID)); storedOK {
		cp := *stored

		return &cp, nil
	}
	// Real AWS always includes a "restore" entry (empty AttributeValues) for
	// a manual snapshot even before any ModifyDBClusterSnapshotAttribute
	// call; terraform-provider-aws's aws_rds_cluster_snapshot_copy Read
	// panics with a nil pointer dereference when this list comes back
	// empty. Automated snapshots cannot be shared and get no entry.
	attrs := []DBSnapshotAttribute{}
	if snap.SnapshotType == snapshotTypeManual {
		attrs = []DBSnapshotAttribute{{AttributeName: "restore", AttributeValues: []string{}}}
	}

	result := &DBClusterSnapshotAttributesResult{
		DBClusterSnapshotIdentifier: snapshotID,
		DBClusterSnapshotAttributes: attrs,
	}
	cp := *result

	return &cp, nil
}

// ModifyDBClusterSnapshotAttribute adds or removes attribute values for a cluster snapshot.
func (b *InMemoryBackend) ModifyDBClusterSnapshotAttribute(
	snapshotID, attributeName string,
	valuesToAdd, valuesToRemove []string,
) (*DBClusterSnapshotAttributesResult, error) {
	return b.ModifyDBClusterSnapshotAttributeAs("", snapshotID, attributeName, valuesToAdd, valuesToRemove)
}

// ModifyDBClusterSnapshotAttributeAs modifies the attributes of a cluster snapshot the caller owns.
func (b *InMemoryBackend) ModifyDBClusterSnapshotAttributeAs(
	caller, snapshotID, attributeName string,
	valuesToAdd, valuesToRemove []string,
) (*DBClusterSnapshotAttributesResult, error) {
	b.mu.Lock("ModifyDBClusterSnapshotAttribute")
	defer b.mu.Unlock()
	if snap, ok := b.clusterSnapshots.Get(normalizeID(snapshotID)); !ok || !ownsSnapshot(caller, snap.OwnerAccountID) {
		return nil, fmt.Errorf("%w: %s", ErrClusterSnapshotNotFound, snapshotID)
	}
	result, ok := b.clusterSnapshotAttributes.Get(normalizeID(snapshotID))
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
