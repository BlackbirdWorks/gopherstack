package neptune

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) clusterSnapshotGet(region, id string) (*DBClusterSnapshot, bool) {
	return b.clusterSnapshots.Get(regionKey(region, id))
}

func (b *InMemoryBackend) clusterSnapshotHas(region, id string) bool {
	return b.clusterSnapshots.Has(regionKey(region, id))
}

func (b *InMemoryBackend) clusterSnapshotPut(v *DBClusterSnapshot) { b.clusterSnapshots.Put(v) }

func (b *InMemoryBackend) clusterSnapshotDelete(region, id string) {
	b.clusterSnapshots.Delete(regionKey(region, id))
}

func (b *InMemoryBackend) clusterSnapshotsInRegion(region string) []*DBClusterSnapshot {
	return b.clusterSnapshotsByRegion.Get(region)
}

// cloneClusterSnapshot returns a deep copy of a cluster snapshot (with its
// RestoreAttributeValues slice copied, so a caller mutating the returned copy
// -- or a later ModifyDBClusterSnapshotAttribute call mutating the stored
// original -- cannot alias the other's backing array).
func cloneClusterSnapshot(snap *DBClusterSnapshot) DBClusterSnapshot {
	cp := *snap
	cp.RestoreAttributeValues = make([]string, len(snap.RestoreAttributeValues))
	copy(cp.RestoreAttributeValues, snap.RestoreAttributeValues)

	return cp
}

// clusterSnapshotARN returns the region-scoped ARN for a Neptune DB cluster snapshot.
func (b *InMemoryBackend) clusterSnapshotARN(region, id string) string {
	return arn.Build("rds", region, b.accountID, "cluster-snapshot:"+id)
}

// CreateDBClusterSnapshot creates a Neptune DB cluster snapshot.
func (b *InMemoryBackend) CreateDBClusterSnapshot(
	ctx context.Context, snapshotID, clusterID string,
) (*DBClusterSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBClusterSnapshot")
	defer b.mu.Unlock()
	if b.clusterSnapshotHas(region, snapshotID) {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s already exists",
			ErrClusterSnapshotAlreadyExists,
			snapshotID,
		)
	}
	cl, exists := b.clusterGet(region, clusterID)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	snap := &DBClusterSnapshot{
		region:                           region,
		DBClusterSnapshotIdentifier:      snapshotID,
		DBClusterSnapshotArn:             b.clusterSnapshotARN(region, snapshotID),
		DBClusterIdentifier:              clusterID,
		Engine:                           neptuneEngine,
		EngineVersion:                    cl.EngineVersion,
		Status:                           snapshotStatusAvailable,
		StorageEncrypted:                 cl.StorageEncrypted,
		KmsKeyID:                         cl.KmsKeyID,
		IAMDatabaseAuthenticationEnabled: cl.EnableIAMDatabaseAuthentication,
		Port:                             cl.Port,
		PercentProgress:                  percentProgressComplete,
		AllocatedStorage:                 cl.AllocatedStorage,
		SnapshotType:                     snapshotSourceManual,
		SnapshotCreateTime:               nowISO8601(),
		ClusterCreateTime:                cl.ClusterCreateTime,
	}
	b.clusterSnapshotPut(snap)
	cp := cloneClusterSnapshot(snap)

	return &cp, nil
}

// DescribeDBClusterSnapshots returns all Neptune cluster snapshots or a specific one.
// If clusterID is set, results are filtered to that cluster.
func (b *InMemoryBackend) DescribeDBClusterSnapshots(
	ctx context.Context, snapshotID, clusterID, snapshotTypeFilter string,
) ([]DBClusterSnapshot, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBClusterSnapshots")
	defer b.mu.RUnlock()
	if snapshotID != "" {
		snap, exists := b.clusterSnapshotGet(region, snapshotID)
		if !exists {
			return nil, fmt.Errorf(
				"%w: cluster snapshot %s not found",
				ErrClusterSnapshotNotFound,
				snapshotID,
			)
		}
		cp := cloneClusterSnapshot(snap)

		return []DBClusterSnapshot{cp}, nil
	}
	snapshots := b.clusterSnapshotsInRegion(region)
	result := make([]DBClusterSnapshot, 0, len(snapshots))
	for _, snap := range snapshots {
		if clusterID != "" && snap.DBClusterIdentifier != clusterID {
			continue
		}
		if snapshotTypeFilter != "" && snap.SnapshotType != snapshotTypeFilter {
			continue
		}
		result = append(result, cloneClusterSnapshot(snap))
	}
	slices.SortFunc(result, func(a, b DBClusterSnapshot) int {
		return strings.Compare(a.DBClusterSnapshotIdentifier, b.DBClusterSnapshotIdentifier)
	})

	return result, nil
}

// DeleteDBClusterSnapshot deletes a Neptune DB cluster snapshot.
func (b *InMemoryBackend) DeleteDBClusterSnapshot(
	ctx context.Context,
	snapshotID string,
) (*DBClusterSnapshot, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBClusterSnapshot")
	defer b.mu.Unlock()
	snap, exists := b.clusterSnapshotGet(region, snapshotID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s not found",
			ErrClusterSnapshotNotFound,
			snapshotID,
		)
	}
	cp := cloneClusterSnapshot(snap)
	b.clusterSnapshotDelete(region, snapshotID)
	delete(b.tagsStore(region), b.clusterSnapshotARN(region, snapshotID))

	return &cp, nil
}

// dbClusterSnapshotRestoreAttribute is the only DB cluster snapshot attribute
// name Neptune's API models: it holds the account IDs (or "all") authorized
// to copy/restore a manual snapshot. See ModifyDBClusterSnapshotAttribute /
// DescribeDBClusterSnapshotAttributes.
const dbClusterSnapshotRestoreAttribute = "restore"

// ModifyDBClusterSnapshotAttribute adds and/or removes values from a Neptune
// DB cluster snapshot's "restore" attribute (the list of accounts authorized
// to copy/restore the snapshot). It returns the updated snapshot so callers
// can render the DBClusterSnapshotAttributesResult AWS includes in both the
// Modify and Describe responses.
func (b *InMemoryBackend) ModifyDBClusterSnapshotAttribute(
	ctx context.Context,
	snapshotID, attributeName string,
	valuesToAdd, valuesToRemove []string,
) (*DBClusterSnapshot, error) {
	if attributeName != dbClusterSnapshotRestoreAttribute {
		return nil, fmt.Errorf(
			"%w: AttributeName must be %q",
			ErrInvalidParameter,
			dbClusterSnapshotRestoreAttribute,
		)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBClusterSnapshotAttribute")
	defer b.mu.Unlock()
	snap, exists := b.clusterSnapshotGet(region, snapshotID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s not found",
			ErrClusterSnapshotNotFound,
			snapshotID,
		)
	}
	remove := make(map[string]bool, len(valuesToRemove))
	for _, v := range valuesToRemove {
		remove[v] = true
	}
	kept := make([]string, 0, len(snap.RestoreAttributeValues))
	for _, v := range snap.RestoreAttributeValues {
		if !remove[v] {
			kept = append(kept, v)
		}
	}
	for _, v := range valuesToAdd {
		if !slices.Contains(kept, v) {
			kept = append(kept, v)
		}
	}
	snap.RestoreAttributeValues = kept
	cp := cloneClusterSnapshot(snap)

	return &cp, nil
}

// CopyDBClusterSnapshot copies a Neptune DB cluster snapshot.
func (b *InMemoryBackend) CopyDBClusterSnapshot(
	ctx context.Context, sourceSnapshotID, targetSnapshotID string,
) (*DBClusterSnapshot, error) {
	if sourceSnapshotID == "" {
		return nil, fmt.Errorf(
			"%w: SourceDBClusterSnapshotIdentifier is required",
			ErrInvalidParameter,
		)
	}
	if targetSnapshotID == "" {
		return nil, fmt.Errorf(
			"%w: TargetDBClusterSnapshotIdentifier is required",
			ErrInvalidParameter,
		)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CopyDBClusterSnapshot")
	defer b.mu.Unlock()
	src, exists := b.clusterSnapshotGet(region, sourceSnapshotID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s not found",
			ErrClusterSnapshotNotFound,
			sourceSnapshotID,
		)
	}
	if b.clusterSnapshotHas(region, targetSnapshotID) {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s already exists",
			ErrClusterSnapshotAlreadyExists,
			targetSnapshotID,
		)
	}
	snap := &DBClusterSnapshot{
		region:                           region,
		DBClusterSnapshotIdentifier:      targetSnapshotID,
		DBClusterSnapshotArn:             b.clusterSnapshotARN(region, targetSnapshotID),
		DBClusterIdentifier:              src.DBClusterIdentifier,
		Engine:                           src.Engine,
		EngineVersion:                    src.EngineVersion,
		Status:                           snapshotStatusAvailable,
		StorageEncrypted:                 src.StorageEncrypted,
		KmsKeyID:                         src.KmsKeyID,
		VpcID:                            src.VpcID,
		IAMDatabaseAuthenticationEnabled: src.IAMDatabaseAuthenticationEnabled,
		Port:                             src.Port,
		AllocatedStorage:                 src.AllocatedStorage,
		PercentProgress:                  percentProgressComplete,
		SnapshotType:                     snapshotSourceManual,
		SnapshotCreateTime:               nowISO8601(),
		ClusterCreateTime:                src.ClusterCreateTime,
	}
	b.clusterSnapshotPut(snap)
	cp := cloneClusterSnapshot(snap)

	return &cp, nil
}

// AddSnapshotInternal creates a snapshot directly, bypassing normal validation. Used for seeding tests.
func (b *InMemoryBackend) AddSnapshotInternal(snapshotID, clusterID string) *DBClusterSnapshot {
	b.mu.Lock("AddSnapshotInternal")
	defer b.mu.Unlock()
	snap := &DBClusterSnapshot{
		region:                      b.region,
		DBClusterSnapshotIdentifier: snapshotID,
		DBClusterSnapshotArn:        b.clusterSnapshotARN(b.region, snapshotID),
		DBClusterIdentifier:         clusterID,
		Engine:                      neptuneEngine,
		EngineVersion:               defaultEngineVersion,
		Status:                      clusterStatusAvailable,
		SnapshotType:                snapshotSourceManual,
	}
	b.clusterSnapshotPut(snap)
	cp := *snap

	return &cp
}
