package kinesisanalyticsv2

import (
	"context"
	"sort"
	"time"
)

// CreateApplicationSnapshot creates a snapshot for an application.
func (b *InMemoryBackend) CreateApplicationSnapshot(
	ctx context.Context,
	appName, snapshotName string,
) (*Snapshot, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateApplicationSnapshot")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, appName)
	if !ok {
		return nil, ErrNotFound
	}

	// Real AWS requires application to be RUNNING before snapshot creation.
	if app.effectiveStatus() != ApplicationStatusRunning {
		return nil, ErrAlreadyExists
	}

	if b.snapshots.Has(snapshotKey(region, appName, snapshotName)) {
		return nil, ErrAlreadyExists
	}

	snap := &Snapshot{
		ApplicationARN:     app.ApplicationARN,
		SnapshotName:       snapshotName,
		SnapshotStatus:     "READY",
		Region:             region,
		AppName:            appName,
		ApplicationVersion: app.ApplicationVersionID,
		SnapshotCreation:   time.Now().UTC(),
	}
	if lj, managed := b.flinkManagedLocked(applicationKey(region, appName)); managed && lj.jobID != "" {
		b.startFlinkSnapshotLocked(lj, snap)
	}

	b.snapshots.Put(snap)

	return snap, nil
}

// DescribeApplicationSnapshot retrieves a snapshot by application name and snapshot name.
func (b *InMemoryBackend) DescribeApplicationSnapshot(
	ctx context.Context,
	appName, snapshotName string,
) (*Snapshot, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("DescribeApplicationSnapshot")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationKey(region, appName)) {
		return nil, ErrNotFound
	}

	s, ok := b.snapshots.Get(snapshotKey(region, appName, snapshotName))
	if !ok {
		return nil, ErrNotFound
	}

	cp := *s

	return &cp, nil
}

// ListApplicationSnapshots returns snapshots for an application with optional pagination, sorted by creation time.
func (b *InMemoryBackend) ListApplicationSnapshots(
	ctx context.Context,
	appName, nextToken string,
	limit int,
) ([]*Snapshot, string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListApplicationSnapshots")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationKey(region, appName)) {
		return nil, "", ErrNotFound
	}

	live := b.snapshotsByApp.Get(appParentKey(region, appName))
	out := make([]*Snapshot, len(live))

	for i, s := range live {
		cp := *s
		out[i] = &cp
	}

	sort.Slice(out, func(i, j int) bool {
		if !out[i].SnapshotCreation.Equal(out[j].SnapshotCreation) {
			return out[i].SnapshotCreation.Before(out[j].SnapshotCreation)
		}

		return out[i].SnapshotName < out[j].SnapshotName
	})

	page, outToken := pageSlice(out, nextToken, limit)

	return page, outToken, nil
}

// DeleteApplicationSnapshot deletes a snapshot.
func (b *InMemoryBackend) DeleteApplicationSnapshot(ctx context.Context, appName, snapshotName string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplicationSnapshot")
	defer b.mu.Unlock()

	if !b.applications.Has(applicationKey(region, appName)) {
		return ErrNotFound
	}

	key := snapshotKey(region, appName, snapshotName)

	s, ok := b.snapshots.Get(key)
	if !ok {
		return ErrNotFound
	}

	if b.flink != nil {
		b.removeSavepointLocked(applicationKey(region, appName), s)
	}

	b.snapshots.Delete(key)

	return nil
}
