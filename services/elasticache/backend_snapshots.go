package elasticache

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func (b *InMemoryBackend) snapshotARN(region, name string) string {
	return arn.Build("elasticache", region, b.accountID, "snapshot:"+name)
}

// CreateSnapshot creates a manual snapshot of a cluster or replication group.
func (b *InMemoryBackend) CreateSnapshot(
	ctx context.Context,
	snapshotName, clusterID, replicationGroupID string,
) (*CacheSnapshot, error) {
	b.mu.Lock("CreateSnapshot")
	defer b.mu.Unlock()

	// Exactly one source identifier must be provided.
	if (clusterID == "") == (replicationGroupID == "") {
		return nil, ErrInvalidSnapshotSource
	}

	region := getRegion(ctx, b.region)
	snapStore := b.snapshotsStore(region)
	if _, exists := snapStore.Get(snapshotName); exists {
		return nil, ErrSnapshotAlreadyExists
	}

	snap := &CacheSnapshot{
		SnapshotName:       snapshotName,
		CacheClusterID:     clusterID,
		ReplicationGroupID: replicationGroupID,
		Status:             statusAvailable,
		ARN:                b.snapshotARN(region, snapshotName),
		SnapshotSource:     snapshotSourceManual,
		CreatedAt:          time.Now(),
		Tags:               tags.New("elasticache.snapshot." + snapshotName + ".tags"),
	}
	b.markCreatingLocked(&snap.PendingStatus, &snap.AvailableAt)

	if clusterID != "" {
		c, ok := b.clustersStore(region).Get(clusterID)
		if !ok {
			return nil, ErrClusterNotFound
		}
		snap.Engine = c.Engine
		snap.EngineVersion = c.EngineVersion
		snap.NodeType = c.NodeType
	}

	if replicationGroupID != "" {
		rg, ok := b.replicationGroupsStore(region).Get(replicationGroupID)
		if !ok {
			return nil, ErrReplicationGroupNotFound
		}
		snap.Engine = engineRedis
		ev := rg.EngineVersion
		if ev == "" {
			ev = defaultEngineVersion(engineRedis)
		}
		snap.EngineVersion = ev
		snap.ReplicationGroupID = rg.ReplicationGroupID
	}

	snapStore.Put(snap)
	sourceID := clusterID
	if sourceID == "" {
		sourceID = replicationGroupID
	}
	b.appendEventLocked(sourceID, "cache-cluster", "snapshot "+snapshotName+" created")

	cp := *snap
	cp.Status = overlayStatus(b.now(), snap.Status, snap.PendingStatus, snap.AvailableAt)

	return &cp, nil
}

// DeleteSnapshot removes a snapshot and returns the deleted snapshot.
func (b *InMemoryBackend) DeleteSnapshot(ctx context.Context, snapshotName string) (*CacheSnapshot, error) {
	b.mu.Lock("DeleteSnapshot")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.pruneRegionLocked(region)
	tbl := b.snapshotsStore(region)
	snap, exists := tbl.Get(snapshotName)
	if !exists || isReaped(b.now(), snap.PendingStatus, snap.AvailableAt) {
		return nil, ErrSnapshotNotFound
	}

	if d := b.pendingUntil(); !d.IsZero() {
		snap.PendingStatus = statusDeleting
		snap.AvailableAt = d
		b.appendEventLocked(snapshotName, "cache-snapshot", "snapshot deleting")
		cp := *snap
		cp.Status = statusDeleting

		return &cp, nil
	}

	cp := *snap
	snap.Tags.Close()
	tbl.Delete(snapshotName)
	b.appendEventLocked(snapshotName, "cache-snapshot", "snapshot deleted")

	return &cp, nil
}

// DescribeSnapshots returns one snapshot by name, or a paginated list filtered by cluster/rg/source.
// snapshotSource mirrors the real AWS filter values: "system" matches automated snapshots,
// "user" matches manual snapshots, and "" returns all.
func (b *InMemoryBackend) DescribeSnapshots(
	ctx context.Context,
	snapshotName, clusterID, replicationGroupID, snapshotSource, marker string,
	maxRecords int,
) (page.Page[CacheSnapshot], error) {
	b.mu.RLock("DescribeSnapshots")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	// Map AWS filter values ("system"/"user") to stored values ("automated"/"manual").
	wantSource := ""
	switch snapshotSource {
	case "system":
		wantSource = snapshotSourceAutomated
	case "user":
		wantSource = snapshotSourceManual
	}

	p, err := describePaged(b.snapshotsStore(region), snapshotName, ErrSnapshotNotFound, func(s CacheSnapshot) bool {
		return (clusterID == "" || s.CacheClusterID == clusterID) &&
			(replicationGroupID == "" || s.ReplicationGroupID == replicationGroupID) &&
			(wantSource == "" || s.SnapshotSource == wantSource)
	},
		func(s CacheSnapshot) string { return s.SnapshotName }, marker, maxRecords)

	return b.finalizeSnapshotPage(snapshotName, p, err)
}

// CopySnapshot copies an existing snapshot to a new name.
func (b *InMemoryBackend) CopySnapshot(
	ctx context.Context,
	sourceSnapshotName, targetSnapshotName string,
) (*CacheSnapshot, error) {
	b.mu.Lock("CopySnapshot")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.snapshotsStore(region)

	src, ok := tbl.Get(sourceSnapshotName)
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	if _, targetExists := tbl.Get(targetSnapshotName); targetExists {
		return nil, ErrSnapshotAlreadyExists
	}

	cp := *src
	cp.SnapshotName = targetSnapshotName
	cp.ARN = b.snapshotARN(region, targetSnapshotName)
	cp.CreatedAt = time.Now()
	cp.Tags = tags.New("elasticache.snapshot." + targetSnapshotName + ".tags")
	tbl.Put(&cp)
	b.appendEventLocked(targetSnapshotName, "cache-snapshot", "snapshot copied from "+sourceSnapshotName)

	result := cp

	return &result, nil
}

// CopySnapshotFull copies a snapshot and optionally re-encrypts with a different KMS key.
func (b *InMemoryBackend) CopySnapshotFull(
	ctx context.Context,
	sourceSnapshotName, targetSnapshotName, kmsKeyID string,
) (*CacheSnapshot, error) {
	b.mu.Lock("CopySnapshotFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.snapshotsStore(region)

	src, ok := tbl.Get(sourceSnapshotName)
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	if _, exists := tbl.Get(targetSnapshotName); exists {
		return nil, ErrSnapshotAlreadyExists
	}

	cp := *src
	cp.SnapshotName = targetSnapshotName
	cp.ARN = b.snapshotARN(region, targetSnapshotName)
	cp.CreatedAt = time.Now()
	cp.SnapshotSource = snapshotSourceManual
	cp.Tags = tags.New("elasticache.snapshot." + targetSnapshotName + ".tags")

	if kmsKeyID != "" {
		cp.KmsKeyID = kmsKeyID
	}

	tbl.Put(&cp)
	b.appendEventLocked(targetSnapshotName, "snapshot", "snapshot copied from "+sourceSnapshotName)

	result := cp

	return &result, nil
}

// ----------------------------------------
// CreateUserGroupValidated — validates users exist
// ----------------------------------------
