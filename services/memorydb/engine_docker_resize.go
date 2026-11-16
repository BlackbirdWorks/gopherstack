package memorydb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/redisdocker"
)

const dockerResizeTimeout = 10 * time.Minute

// resizeDockerReplicasLocked grows or shrinks every shard of c's containers to want replicas in the background;
// c reports "updating" until the group has converged. Must hold b.mu.
func (b *InMemoryBackend) resizeDockerReplicasLocked(region string, c *Cluster, want int) {
	d := c.dock
	if d == nil || d.group == nil {
		return
	}

	g, shards := d.group, int(c.NumShards)
	c.Status = statusUpdating

	b.dockerWG.Go(func() {
		ctx, cancel := context.WithTimeout(d.ctx, dockerResizeTimeout)
		defer cancel()

		var err error

		for i := range shards {
			if err = g.SetReplicas(ctx, i, want); err != nil {
				break
			}
		}

		b.finishResize(region, c, d, err)
	})
}

func (b *InMemoryBackend) finishResize(region string, c *Cluster, d *dockState, err error) {
	b.mu.Lock("finishResize")
	defer b.mu.Unlock()

	if cur, ok := tableGet(b.clusters[region], c.Name); !ok || cur != c || c.dock != d {
		return
	}

	c.endpoints = d.group.Endpoints()
	c.Status = clusterStatusAvailable

	if err != nil {
		logger.Load(context.Background()).
			Warn("memorydb: docker replica resize failed", "cluster", c.Name, "error", err)
		b.appendEventLocked(region, &Event{
			Date: time.Now(), SourceName: c.Name, SourceType: resourceKindCluster,
			Message: "Cluster " + c.Name + " could not be resized: " + err.Error(),
		})

		return
	}

	b.syncDockerACLLocked(c)
}

// failoverDocker promotes the replica of one shard of d's containers; the caller must not hold b.mu.
func (b *InMemoryBackend) failoverDocker(d *dockState, shard int) error {
	ctx, cancel := context.WithTimeout(d.ctx, dockerResizeTimeout)
	defer cancel()

	if err := d.group.Failover(ctx, shard); err != nil {
		if errors.Is(err, redisdocker.ErrNoReplica) {
			return fmt.Errorf("shard %d has no replica to fail over to: %w", shard, ErrValidation)
		}

		return fmt.Errorf("failover shard %d: %w", shard, err)
	}

	return nil
}

// shardIndex returns the index of the shard name names in c, or -1.
func shardIndex(c *Cluster, name string) int {
	for i := range int(c.NumShards) {
		if name == shardName(c.Name, i) {
			return i
		}
	}

	return -1
}
