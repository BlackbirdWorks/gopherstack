package elasticache

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func (b *InMemoryBackend) serverlessCacheARN(region, name string) string {
	return arn.Build("elasticache", region, b.accountID, "serverlesscache:"+name)
}

func (b *InMemoryBackend) serverlessCacheSnapshotARN(region, name string) string {
	return arn.Build("elasticache", region, b.accountID, "serverlesssnapshot:"+name)
}

// CreateServerlessCache creates a new serverless cache.
func (b *InMemoryBackend) CreateServerlessCache(
	ctx context.Context,
	name, description, engine string,
) (*ServerlessCache, error) {
	b.mu.Lock("CreateServerlessCache")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.serverlessCachesStore(region)
	if _, exists := tbl.Get(name); exists {
		return nil, ErrServerlessCacheAlreadyExists
	}

	if engine == "" {
		engine = engineRedis
	}

	suffix := randomSuffix()
	host := fmt.Sprintf("%s.serverless.%s.%s.cache.amazonaws.com", name, suffix, region)
	readerHost := fmt.Sprintf("%s.serverless.%s.%s.cache.amazonaws.com", name+"-ro", suffix, region)
	port := 6379
	if engine == engineMemcached {
		port = 11211
	}

	ep := &ServerlessCacheEndpoint{Address: host, Port: port}
	readerEp := &ServerlessCacheEndpoint{Address: readerHost, Port: port}

	sc := &ServerlessCache{
		Name:           name,
		Description:    description,
		Status:         statusServerlessAvailable,
		ARN:            b.serverlessCacheARN(region, name),
		Engine:         engine,
		CreatedAt:      time.Now(),
		Tags:           tags.New("elasticache.serverless." + name + ".tags"),
		Endpoint:       ep,
		ReaderEndpoint: readerEp,
	}
	b.markCreatingLocked(&sc.PendingStatus, &sc.AvailableAt)
	tbl.Put(sc)
	b.appendEventLocked(name, "serverless-cache", "serverless cache created")

	return b.serverlessCacheView(sc), nil
}

// ----------------------------------------
// CreateServerlessCacheSnapshot
// ----------------------------------------

// CreateServerlessCacheSnapshot creates a manual snapshot of a serverless cache.
func (b *InMemoryBackend) CreateServerlessCacheSnapshot(
	ctx context.Context,
	snapshotName, serverlessCacheName string,
) (*ServerlessCacheSnapshot, error) {
	b.mu.Lock("CreateServerlessCacheSnapshot")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	snapStore := b.serverlessCacheSnapshotsStore(region)
	if _, exists := snapStore.Get(snapshotName); exists {
		return nil, ErrServerlessCacheSnapshotExists
	}

	if _, ok := b.serverlessCachesStore(region).Get(serverlessCacheName); !ok {
		return nil, ErrServerlessCacheNotFound
	}

	snap := &ServerlessCacheSnapshot{
		Name:                snapshotName,
		Status:              statusAvailable,
		ARN:                 b.serverlessCacheSnapshotARN(region, snapshotName),
		ServerlessCacheName: serverlessCacheName,
		SnapshotType:        snapshotSourceManual,
		CreatedAt:           time.Now(),
		Tags:                tags.New("elasticache.serverlesssnap." + snapshotName + ".tags"),
	}
	snapStore.Put(snap)

	return snap, nil
}

// ----------------------------------------
// CopyServerlessCacheSnapshot
// ----------------------------------------

// CopyServerlessCacheSnapshot copies a serverless cache snapshot to a new name.
func (b *InMemoryBackend) CopyServerlessCacheSnapshot(
	ctx context.Context,
	sourceSnapshotName, targetSnapshotName string,
) (*ServerlessCacheSnapshot, error) {
	b.mu.Lock("CopyServerlessCacheSnapshot")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.serverlessCacheSnapshotsStore(region)

	src, ok := tbl.Get(sourceSnapshotName)
	if !ok {
		return nil, ErrServerlessCacheSnapshotNotFound
	}

	if _, exists := tbl.Get(targetSnapshotName); exists {
		return nil, ErrServerlessCacheSnapshotExists
	}

	cp := *src
	cp.Name = targetSnapshotName
	cp.ARN = b.serverlessCacheSnapshotARN(region, targetSnapshotName)
	cp.CreatedAt = time.Now()
	cp.Tags = tags.New("elasticache.serverlesssnap." + targetSnapshotName + ".tags")
	tbl.Put(&cp)

	result := cp

	return &result, nil
}

// ----------------------------------------
// CreateUser
// ----------------------------------------

// AddServerlessCacheInternal seeds a serverless cache for testing.
func (b *InMemoryBackend) AddServerlessCacheInternal(sc *ServerlessCache) {
	b.mu.Lock("AddServerlessCacheInternal")
	defer b.mu.Unlock()
	b.serverlessCachesStore(b.region).Put(sc)
}

// AddServerlessCacheSnapshotInternal seeds a serverless cache snapshot for testing.
func (b *InMemoryBackend) AddServerlessCacheSnapshotInternal(snap *ServerlessCacheSnapshot) {
	b.mu.Lock("AddServerlessCacheSnapshotInternal")
	defer b.mu.Unlock()
	b.serverlessCacheSnapshotsStore(b.region).Put(snap)
}

// CreateServerlessCacheFull creates a serverless cache with the full set of options.
func (b *InMemoryBackend) CreateServerlessCacheFull(
	ctx context.Context,
	opts ServerlessCreateOpts,
) (*ServerlessCache, error) {
	b.mu.Lock("CreateServerlessCacheFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.serverlessCachesStore(region)
	if _, exists := tbl.Get(opts.Name); exists {
		return nil, ErrServerlessCacheAlreadyExists
	}

	engine := opts.Engine
	if engine == "" {
		engine = engineRedis
	}

	suffix := randomSuffix()
	host := fmt.Sprintf("%s.serverless.%s.%s.cache.amazonaws.com", opts.Name, suffix, region)
	readerHost := fmt.Sprintf("%s.serverless.%s.%s.cache.amazonaws.com", opts.Name+"-ro", suffix, region)
	port := 6379
	if engine == engineMemcached {
		port = 11211
	}

	ep := &ServerlessCacheEndpoint{Address: host, Port: port}
	readerEp := &ServerlessCacheEndpoint{Address: readerHost, Port: port}

	majorVer := opts.MajorEngineVersion
	if majorVer == "" {
		majorVer = majorVersionStr(engine)
	}

	sc := &ServerlessCache{
		Name:                   opts.Name,
		Description:            opts.Description,
		Status:                 statusServerlessAvailable,
		ARN:                    b.serverlessCacheARN(region, opts.Name),
		Engine:                 engine,
		KmsKeyID:               opts.KmsKeyID,
		UserGroupID:            opts.UserGroupID,
		SubnetGroupName:        opts.SubnetGroupName,
		DailySnapshotTime:      opts.DailySnapshotTime,
		MajorEngineVersion:     majorVer,
		SubnetIDs:              opts.SubnetIDs,
		SecurityGroupIDs:       opts.SecurityGroupIDs,
		SnapshotRetentionLimit: opts.SnapshotRetentionLimit,
		CreatedAt:              time.Now(),
		Tags:                   tags.New("elasticache.serverless." + opts.Name + ".tags"),
		Endpoint:               ep,
		ReaderEndpoint:         readerEp,
	}

	if len(opts.Tags) > 0 {
		for k, v := range opts.Tags {
			sc.Tags.Set(k, v)
		}
	}

	b.markCreatingLocked(&sc.PendingStatus, &sc.AvailableAt)
	tbl.Put(sc)
	b.appendEventLocked(opts.Name, "serverless-cache", "serverless cache created")

	return b.serverlessCacheView(sc), nil
}

// majorVersionStr returns a human-readable major version string for an engine.
func majorVersionStr(engine string) string {
	switch engine {
	case engineMemcached:
		return "1"
	case engineValkey:
		return "8"
	default:
		return "7"
	}
}

// ----------------------------------------
// ModifyServerlessCacheFull
// ----------------------------------------

// ModifyServerlessCacheFull modifies a serverless cache with the full set of options.
func (b *InMemoryBackend) ModifyServerlessCacheFull(
	ctx context.Context,
	name string,
	opts ServerlessModifyOpts,
) (*ServerlessCache, error) {
	b.mu.Lock("ModifyServerlessCacheFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	sc, ok := b.serverlessCachesStore(region).Get(name)
	if !ok {
		return nil, ErrServerlessCacheNotFound
	}

	if opts.Description != "" {
		sc.Description = opts.Description
	}

	if opts.UserGroupID != "" {
		sc.UserGroupID = opts.UserGroupID
	} else if opts.RemoveUserGroup {
		sc.UserGroupID = ""
	}

	if opts.DailySnapshotTime != "" {
		sc.DailySnapshotTime = opts.DailySnapshotTime
	}

	if opts.SnapshotRetentionLimit != nil {
		sc.SnapshotRetentionLimit = *opts.SnapshotRetentionLimit
	}

	if len(opts.SecurityGroupIDs) > 0 {
		sc.SecurityGroupIDs = opts.SecurityGroupIDs
	}

	b.markTransitionLocked(&sc.PendingStatus, &sc.AvailableAt, statusModifying)

	return b.serverlessCacheView(sc), nil
}

// ----------------------------------------
// CreateSubnetGroupFull — with VpcId
// ----------------------------------------

// DeleteServerlessCache deletes a serverless cache.
func (b *InMemoryBackend) DeleteServerlessCache(ctx context.Context, name string) (*ServerlessCache, error) {
	b.mu.Lock("DeleteServerlessCache")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.pruneRegionLocked(region)
	tbl := b.serverlessCachesStore(region)
	sc, ok := tbl.Get(name)
	if !ok || isReaped(b.now(), sc.PendingStatus, sc.AvailableAt) {
		return nil, ErrServerlessCacheNotFound
	}

	if d := b.pendingUntil(); !d.IsZero() {
		sc.PendingStatus = statusDeleting
		sc.AvailableAt = d
		b.appendEventLocked(name, "serverless-cache", "serverless cache deleting")

		return b.serverlessCacheView(sc), nil
	}

	result := *sc
	sc.Tags.Close()
	tbl.Delete(name)
	b.appendEventLocked(name, "serverless-cache", "serverless cache deleted")

	return &result, nil
}

// DeleteServerlessCacheSnapshot deletes a serverless cache snapshot.
func (b *InMemoryBackend) DeleteServerlessCacheSnapshot(
	ctx context.Context,
	name string,
) (*ServerlessCacheSnapshot, error) {
	b.mu.Lock("DeleteServerlessCacheSnapshot")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.serverlessCacheSnapshotsStore(region)
	snap, ok := tbl.Get(name)
	if !ok {
		return nil, ErrServerlessCacheSnapshotNotFound
	}

	result := *snap
	tbl.Delete(name)
	b.appendEventLocked(name, "serverless-cache-snapshot", "serverless cache snapshot deleted")

	return &result, nil
}

// DescribeServerlessCaches returns a paginated list of serverless caches.
func (b *InMemoryBackend) DescribeServerlessCaches(
	ctx context.Context,
	name, marker string,
	maxRecords int,
) (page.Page[ServerlessCache], error) {
	b.mu.RLock("DescribeServerlessCaches")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	p, err := describePaged(b.serverlessCachesStore(region), name, ErrServerlessCacheNotFound, nil,
		func(sc ServerlessCache) string { return sc.Name }, marker, maxRecords)

	return b.finalizeServerlessCachePage(name, p, err)
}

// DescribeServerlessCacheSnapshots returns a paginated list of serverless cache snapshots.
func (b *InMemoryBackend) DescribeServerlessCacheSnapshots(
	ctx context.Context,
	serverlessCacheName, snapshotName, marker string,
	maxRecords int,
) (page.Page[ServerlessCacheSnapshot], error) {
	b.mu.RLock("DescribeServerlessCacheSnapshots")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	tbl := b.serverlessCacheSnapshotsStore(region)

	if snapshotName != "" {
		snap, ok := tbl.Get(snapshotName)
		if !ok {
			return page.Page[ServerlessCacheSnapshot]{}, ErrServerlessCacheSnapshotNotFound
		}

		return page.Page[ServerlessCacheSnapshot]{Data: []ServerlessCacheSnapshot{*snap}}, nil
	}

	all := tbl.All()
	out := make([]ServerlessCacheSnapshot, 0, len(all))
	for _, snap := range all {
		if serverlessCacheName != "" && snap.ServerlessCacheName != serverlessCacheName {
			continue
		}

		out = append(out, *snap)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// ExportServerlessCacheSnapshot exports a serverless cache snapshot to S3.
func (b *InMemoryBackend) ExportServerlessCacheSnapshot(
	ctx context.Context,
	snapshotName, _ string,
) (*ServerlessCacheSnapshot, error) {
	b.mu.Lock("ExportServerlessCacheSnapshot")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	snap, ok := b.serverlessCacheSnapshotsStore(region).Get(snapshotName)
	if !ok {
		return nil, ErrServerlessCacheSnapshotNotFound
	}

	result := *snap

	return &result, nil
}

// ModifyServerlessCache modifies a serverless cache.
func (b *InMemoryBackend) ModifyServerlessCache(
	ctx context.Context,
	name, description string,
) (*ServerlessCache, error) {
	b.mu.Lock("ModifyServerlessCache")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	sc, ok := b.serverlessCachesStore(region).Get(name)
	if !ok {
		return nil, ErrServerlessCacheNotFound
	}

	if description != "" {
		sc.Description = description
	}

	b.markTransitionLocked(&sc.PendingStatus, &sc.AvailableAt, statusModifying)

	return b.serverlessCacheView(sc), nil
}
