package rds

import (
	"fmt"
	"net/url"
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
	if _, exists := b.clusterSnapshots.Get(normalizeID(snapshotID)); exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s already exists", ErrClusterSnapshotAlreadyExists, snapshotID)
	}
	cluster, exists := b.clusters.Get(normalizeID(clusterID))
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
		DBClusterSnapshotArn:        b.rdsARN("cluster-snapshot", snapshotID),
		DBClusterIdentifier:         cluster.DBClusterIdentifier,
		DBClusterResourceID:         cluster.DBClusterResourceID,
		Engine:                      cluster.Engine,
		EngineVersion:               cluster.EngineVersion,
		Status:                      instanceStatusAvailable,
		SnapshotType:                snapshotTypeManual,
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
		snap, exists := b.clusterSnapshots.Get(normalizeID(snapshotID))
		if !exists {
			return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
		}
		cp := *snap

		return []DBClusterSnapshot{cp}, nil
	}
	result := make([]DBClusterSnapshot, 0, b.clusterSnapshots.Len())
	for _, snap := range b.clusterSnapshots.All() {
		if clusterID != "" && !idEqual(snap.DBClusterIdentifier, clusterID) {
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
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("DeleteDBClusterSnapshot")
	defer b.mu.Unlock()
	snap, exists := b.clusterSnapshots.Get(normalizeID(snapshotID))
	if !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	cp := *snap
	b.clusterSnapshots.Delete(normalizeID(snapshotID))
	// Use snap.DBClusterSnapshotIdentifier (the stored, creation-time casing)
	// rather than the raw snapshotID argument -- see normalizeID.
	delete(b.tags, b.rdsARN("cluster-snapshot", snap.DBClusterSnapshotIdentifier))

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
	source, srcExists := b.clusterSnapshots.Get(normalizeID(rdsIDFromARN(sourceSnapshotID)))
	if !srcExists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, sourceSnapshotID)
	}
	if _, dstExists := b.clusterSnapshots.Get(normalizeID(targetSnapshotID)); dstExists {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s already exists",
			ErrClusterSnapshotAlreadyExists,
			targetSnapshotID,
		)
	}
	targetArn := b.rdsARN("cluster-snapshot", targetSnapshotID)
	snap := &DBClusterSnapshot{
		SnapshotCreateTime:          time.Now().UTC(),
		DBClusterSnapshotIdentifier: targetSnapshotID,
		DBClusterSnapshotArn:        targetArn,
		DBClusterIdentifier:         source.DBClusterIdentifier,
		DBClusterResourceID:         source.DBClusterResourceID,
		Engine:                      source.Engine,
		EngineVersion:               source.EngineVersion,
		Status:                      instanceStatusAvailable,
		SnapshotType:                snapshotTypeManual,
		PercentProgress:             percentProgressComplete,
		StorageEncrypted:            source.StorageEncrypted,
		CopyTagsToSnapshot:          copyTags,
		SourceDBClusterSnapshotArn:  source.DBClusterSnapshotArn,
	}
	b.clusterSnapshots.Put(snap)
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
	b.mu.RLock("DescribeDBClusterSnapshotAttributes")
	defer b.mu.RUnlock()
	snap, ok := b.clusterSnapshots.Get(normalizeID(snapshotID))
	if !ok {
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
	b.mu.Lock("ModifyDBClusterSnapshotAttribute")
	defer b.mu.Unlock()
	if _, ok := b.clusterSnapshots.Get(normalizeID(snapshotID)); !ok {
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
