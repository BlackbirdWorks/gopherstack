package memorydb

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// Purge removes all MemoryDB resources created before the cutoff time.
func (b *InMemoryBackend) Purge(ctx context.Context, cutoff time.Time) {
	if ctx.Err() != nil {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for region, t := range b.clusters {
		purgeTable(ctx, t, cutoff, clusterKeyFn,
			func(c *Cluster) time.Time { return c.CreatedAt },
			func(c *Cluster) { delete(b.arnToResource[region], c.ARN) },
		)
	}
	for region, t := range b.acls {
		purgeTableFiltered(ctx, t, cutoff, aclKeyFn,
			func(a *ACL) bool { return a.Name == openAccessACL },
			func(a *ACL) time.Time { return a.CreatedAt },
			func(a *ACL) { delete(b.arnToResource[region], a.ARN) },
		)
	}
	for region, t := range b.subnetGroups {
		purgeTable(ctx, t, cutoff, subnetGroupKeyFn,
			func(sg *SubnetGroup) time.Time { return sg.CreatedAt },
			func(sg *SubnetGroup) { delete(b.arnToResource[region], sg.ARN) },
		)
	}
	for region, t := range b.users {
		purgeTable(ctx, t, cutoff, userKeyFn,
			func(u *User) time.Time { return u.CreatedAt },
			func(u *User) { delete(b.arnToResource[region], u.ARN) },
		)
	}
	for region, t := range b.parameterGroups {
		purgeTable(ctx, t, cutoff, parameterGroupKeyFn,
			func(pg *ParameterGroup) time.Time { return pg.CreatedAt },
			func(pg *ParameterGroup) { delete(b.arnToResource[region], pg.ARN) },
		)
	}
	for region, t := range b.snapshots {
		purgeTable(ctx, t, cutoff, snapshotKeyFn,
			func(s *Snapshot) time.Time { return s.CreatedAt },
			func(s *Snapshot) { delete(b.arnToResource[region], s.ARN) },
		)
	}
	purgeTable(ctx, b.multiRegionClusters, cutoff, multiRegionClusterKeyFn,
		func(mrc *MultiRegionCluster) time.Time { return mrc.CreatedAt },
		func(_ *MultiRegionCluster) {},
	)

	if ctx.Err() != nil {
		return
	}

	for region, evs := range b.events {
		filtered := evs[:0]
		for _, ev := range evs {
			if !ev.Date.IsZero() && ev.Date.Before(cutoff) {
				continue
			}
			filtered = append(filtered, ev)
		}
		b.events[region] = filtered
	}
}

// purgeTable deletes entries from t that were created before cutoff, calling
// cleanup for each deleted entry. keyFn must be the same key function t was
// constructed with (see store_setup.go).
func purgeTable[V any](
	ctx context.Context,
	t *store.Table[V],
	cutoff time.Time,
	keyFn func(*V) string,
	getTime func(*V) time.Time,
	cleanup func(*V),
) {
	for _, v := range t.All() {
		if ctx.Err() != nil {
			return
		}
		if getTime(v).Before(cutoff) {
			cleanup(v)
			t.Delete(keyFn(v))
		}
	}
}

// purgeTableFiltered is like purgeTable but skips entries where skip returns true.
func purgeTableFiltered[V any](
	ctx context.Context,
	t *store.Table[V],
	cutoff time.Time,
	keyFn func(*V) string,
	skip func(*V) bool,
	getTime func(*V) time.Time,
	cleanup func(*V),
) {
	for _, v := range t.All() {
		if ctx.Err() != nil {
			return
		}
		if skip(v) {
			continue
		}
		if getTime(v).Before(cutoff) {
			cleanup(v)
			t.Delete(keyFn(v))
		}
	}
}

// -- Deep-copy helpers -----------------------------------------------------------
