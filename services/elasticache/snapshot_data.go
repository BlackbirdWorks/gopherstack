package elasticache

import (
	"context"
	"fmt"
	"maps"

	"github.com/alicebob/miniredis/v2"

	"github.com/blackbirdworks/gopherstack/pkgs/redisengine"
)

const dataKeyStandalone = "cluster"

// miniForLocked returns the embedded server behind c, which a replica may share with its shard primary.
func (b *InMemoryBackend) miniForLocked(region string, c *Cluster) *miniredis.Miniredis {
	if c.mini != nil {
		return c.mini
	}

	if c.ReplicationGroupID == "" {
		return nil
	}

	for _, other := range b.clustersStore(region).All() {
		if other.mini != nil && other.ReplicationGroupID == c.ReplicationGroupID &&
			other.ConnectAddress == c.ConnectAddress {
			return other.mini
		}
	}

	return nil
}

func (b *InMemoryBackend) captureClusterDataLocked(region string, c *Cluster) (map[string][]byte, error) {
	blob, err := redisengine.DumpKeyspace(b.miniForLocked(region, c))
	if err != nil || blob == nil {
		return nil, err
	}

	return map[string][]byte{dataKeyStandalone: blob}, nil
}

func (b *InMemoryBackend) captureGroupDataLocked(region string, rg *ReplicationGroup) (map[string][]byte, error) {
	data := map[string][]byte{}

	for i := range rg.NodeGroups {
		ng := &rg.NodeGroups[i]
		if ng.PrimaryNode == nil {
			continue
		}

		c, ok := b.clustersStore(region).Get(ng.PrimaryNode.CacheClusterID)
		if !ok {
			continue
		}

		blob, err := redisengine.DumpKeyspace(b.miniForLocked(region, c))
		if err != nil {
			return nil, fmt.Errorf("snapshot shard %s: %w", ng.NodeGroupID, err)
		}

		if blob != nil {
			data[ng.NodeGroupID] = blob
		}
	}

	return data, nil
}

// clusterBlob picks the data a standalone cluster restores from a snapshot.
func clusterBlob(snap *CacheSnapshot) []byte {
	if blob, ok := snap.Data[dataKeyStandalone]; ok {
		return blob
	}

	if len(snap.Data) == 1 {
		for _, blob := range snap.Data {
			return blob
		}
	}

	return nil
}

// RestoreClusterData loads the keyspace captured in snapshotName into the running cluster id.
func (b *InMemoryBackend) RestoreClusterData(ctx context.Context, id, snapshotName string) error {
	b.mu.Lock("RestoreClusterData")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	snap, ok := b.snapshotsStore(region).Get(snapshotName)
	if !ok {
		return ErrSnapshotNotFound
	}

	c, ok := b.clustersStore(region).Get(id)
	if !ok {
		return ErrClusterNotFound
	}

	mr := b.miniForLocked(region, c)
	if mr == nil {
		return nil
	}

	return redisengine.LoadKeyspace(mr, clusterBlob(snap))
}

// restoreShardData loads each shard's captured keyspace into the matching new shard engine.
func restoreShardData(snap *CacheSnapshot, plans []shardPlan, engines []*clusterEngine, adopted bool) error {
	next := 0

	for si, plan := range plans {
		if si == 0 && adopted {
			continue
		}

		eng := engines[next]
		next++

		if eng == nil || eng.mini == nil {
			continue
		}

		if err := redisengine.LoadKeyspace(eng.mini, snap.Data[plan.id]); err != nil {
			return fmt.Errorf("restore shard %s: %w", plan.id, err)
		}
	}

	return nil
}

func (b *InMemoryBackend) restoreGroupDataLocked(
	region string, opts ReplicationGroupCreateOpts, prep preparedGroup, engines []*clusterEngine, adopted bool,
) error {
	if opts.SnapshotName == "" {
		return nil
	}

	snap, ok := b.snapshotsStore(region).Get(opts.SnapshotName)
	if !ok || len(snap.Data) == 0 {
		return nil
	}

	return restoreShardData(snap, prep.plans, engines, adopted)
}

func cloneSnapshotData(data map[string][]byte) map[string][]byte {
	return maps.Clone(data)
}
