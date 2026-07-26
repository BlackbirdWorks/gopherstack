package memorydb

import (
	"context"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateSnapshot creates a snapshot of a cluster.
func (b *InMemoryBackend) CreateSnapshot(ctx context.Context, req *createSnapshotRequest) (*Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	c, ok := b.clustersStore(region).Get(req.ClusterName)
	if !ok {
		return nil, ErrClusterNotFound
	}

	if _, exists := b.snapshotsStore(region).Get(req.SnapshotName); exists {
		return nil, ErrSnapshotAlreadyExists
	}

	snapshotARN := arn.Build("memorydb", region, b.accountID, "snapshot/"+req.SnapshotName)

	s := &Snapshot{
		Name:        req.SnapshotName,
		ARN:         snapshotARN,
		ClusterName: req.ClusterName,
		Status:      snapshotStatusAvailable,
		KmsKeyID:    req.KmsKeyID,
		Source:      snapshotSourceManual,
		DataTiering: c.DataTiering,
		Tags:        tagsFromSlice(req.Tags),
		CreatedAt:   time.Now(),
		ClusterConfiguration: snapshotClusterConfig{
			Name:                   c.Name,
			NodeType:               c.NodeType,
			EngineVersion:          c.EngineVersion,
			Description:            c.Description,
			Port:                   c.Port,
			NumShards:              c.NumShards,
			Engine:                 c.Engine,
			MaintenanceWindow:      c.MaintenanceWindow,
			TopicArn:               c.SnsTopicArn,
			ParameterGroupName:     c.ParameterGroupName,
			SubnetGroupName:        c.SubnetGroupName,
			SnapshotRetentionLimit: c.SnapshotRetentionLimit,
			SnapshotWindow:         c.SnapshotWindow,
		},
	}

	b.snapshotsStore(region).Put(s)
	b.arnToResourceStore(region)[snapshotARN] = resourceRef{Kind: resourceKindSnapshot, Name: req.SnapshotName}

	b.appendEventLocked(region, &Event{
		Date:       time.Now(),
		SourceName: req.SnapshotName,
		SourceType: resourceKindSnapshot,

		Message: "Snapshot " + req.SnapshotName + " created for cluster " + req.ClusterName,
	})

	// Clone: s stays in the table and its Tags map can be mutated in place by
	// a concurrent TagResource/UntagResource call after this method returns
	// and b.mu is released.
	return cloneSnapshot(s), nil
}

// normalizeSnapshotSource maps DescribeSnapshotsInput's real Source filter
// values ("system"/"user", per api_op_DescribeSnapshots.go's doc comment) to
// this backend's internal Source storage convention ("automated"/"manual",
// matching types.Snapshot.Source's own doc comment: "automated" or "manual").
// Also leniently accepts "automated"/"manual" directly, since a caller may
// reasonably pass the response-side value back in as a filter.
func normalizeSnapshotSource(source string) string {
	switch source {
	case "system":
		return snapshotSourceAutomated
	case "user":
		return snapshotSourceManual
	default:
		return source
	}
}

// DescribeSnapshots returns snapshots, optionally filtered by name, cluster name, or source.
func (b *InMemoryBackend) DescribeSnapshots(
	ctx context.Context,
	name, clusterName, source string,
) ([]*Snapshot, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	t := b.snapshots[region]

	if name != "" {
		s, ok := tableGet(t, name)
		if !ok {
			return nil, ErrSnapshotNotFound
		}

		return []*Snapshot{cloneSnapshot(s)}, nil
	}

	source = normalizeSnapshotSource(source)

	all := tableAll(t)
	result := make([]*Snapshot, 0, len(all))
	for _, s := range all {
		if clusterName != "" && s.ClusterName != clusterName {
			continue
		}
		if source != "" && s.Source != source {
			continue
		}
		result = append(result, cloneSnapshot(s))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// CopySnapshot copies an existing snapshot to a new name.
func (b *InMemoryBackend) CopySnapshot(ctx context.Context, req *copySnapshotRequest) (*Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	src, ok := b.snapshotsStore(region).Get(req.SourceSnapshotName)
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	if req.TargetBucket != "" {
		return cloneSnapshot(src), nil
	}

	if _, exists := b.snapshotsStore(region).Get(req.TargetSnapshotName); exists {
		return nil, ErrSnapshotAlreadyExists
	}

	targetARN := arn.Build("memorydb", region, b.accountID, "snapshot/"+req.TargetSnapshotName)

	kmsKeyID := req.KmsKeyID
	if kmsKeyID == "" {
		kmsKeyID = src.KmsKeyID
	}

	var tags map[string]string
	if len(req.Tags) > 0 {
		tags = tagsFromSlice(req.Tags)
	} else {
		tags = maps.Clone(src.Tags)
	}

	dst := &Snapshot{
		Name:                 req.TargetSnapshotName,
		ARN:                  targetARN,
		ClusterName:          src.ClusterName,
		Status:               snapshotStatusAvailable,
		KmsKeyID:             kmsKeyID,
		Source:               snapshotSourceManual,
		DataTiering:          src.DataTiering,
		Tags:                 tags,
		CreatedAt:            time.Now(),
		ClusterConfiguration: src.ClusterConfiguration,
	}

	b.snapshotsStore(region).Put(dst)
	b.arnToResourceStore(region)[targetARN] = resourceRef{Kind: resourceKindSnapshot, Name: req.TargetSnapshotName}

	// Clone for the same reason as CreateSnapshot: dst remains live in the table.
	return cloneSnapshot(dst), nil
}

// DeleteSnapshot removes a snapshot.
func (b *InMemoryBackend) DeleteSnapshot(ctx context.Context, name string) (*Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	s, ok := b.snapshotsStore(region).Get(name)
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	b.snapshotsStore(region).Delete(name)
	delete(b.arnToResourceStore(region), s.ARN)

	b.appendEventLocked(region, &Event{
		Date:       time.Now(),
		SourceName: name,
		SourceType: resourceKindSnapshot,
		Message:    "Snapshot " + name + " deleted",
	})

	return s, nil
}

// ExportSnapshot validates the snapshot exists and returns it (export to S3 is a no-op in the mock).
func (b *InMemoryBackend) ExportSnapshot(ctx context.Context, req *exportSnapshotRequest) (*Snapshot, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	// tableGet reads b.snapshots[region] directly (nil-safe) rather than
	// through snapshotsStore, which lazily creates the region's table -- a
	// data race when called from a read-only method holding only
	// b.mu.RLock(). Matches DescribeSnapshots above.
	s, ok := tableGet(b.snapshots[region], req.SnapshotName)
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	// Clone: s is the live table entry and its Tags map can be mutated in
	// place by a concurrent TagResource/UntagResource call after this method
	// returns and the RLock is released.
	return cloneSnapshot(s), nil
}

// -- EngineVersion operations ---------------------------------------------------

// cloneSnapshot returns a shallow copy of the snapshot with a separate tags map.
func cloneSnapshot(s *Snapshot) *Snapshot {
	if s == nil {
		return nil
	}

	cp := *s
	cp.Tags = maps.Clone(s.Tags)

	return &cp
}

// AddSnapshotInternal inserts a snapshot directly into the backend for testing.
func (b *InMemoryBackend) AddSnapshotInternal(name, clusterName string) *Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()

	snapshotARN := arn.Build("memorydb", b.defaultRegion, b.accountID, "snapshot/"+name)
	s := &Snapshot{
		Name:        name,
		ARN:         snapshotARN,
		ClusterName: clusterName,
		Status:      snapshotStatusAvailable,
		Tags:        make(map[string]string),
		CreatedAt:   time.Now(),
	}
	b.snapshotsStore(b.defaultRegion).Put(s)
	b.arnToResourceStore(b.defaultRegion)[snapshotARN] = resourceRef{Kind: resourceKindSnapshot, Name: name}

	return s
}
