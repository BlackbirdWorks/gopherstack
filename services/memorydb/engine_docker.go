package memorydb

import (
	"github.com/alicebob/miniredis/v2"
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/redisdocker"
)

// dockState tracks a cluster whose nodes are real Redis containers.
type dockState struct {
	group  *redisdocker.Group
	cancel context.CancelFunc
}

// EnableDockerEngine switches the backend to docker mode: every cluster runs NumShards x (1+replicas) real Redis
// containers in cluster mode.
func (b *InMemoryBackend) EnableDockerEngine(rt container.Runtime, cfg redisdocker.Config) error {
	if cfg.CA == nil {
		ca, err := devtls.NewCA()
		if err != nil {
			return fmt.Errorf("memorydb docker engine CA: %w", err)
		}

		cfg.CA = ca
	}

	cfg.Runtime = rt
	cfg.Label = engineLabel

	b.mu.Lock("EnableDockerEngine")
	defer b.mu.Unlock()

	cfg.Ports = b.allocator
	b.engineMode = EngineDocker
	b.docker = redisdocker.New(cfg)

	return nil
}

// UseDockerEngine installs an already-built engine (tests inject a fake-runtime engine).
func (b *InMemoryBackend) UseDockerEngine(e *redisdocker.Engine) {
	b.mu.Lock("UseDockerEngine")
	defer b.mu.Unlock()

	b.engineMode = EngineDocker
	b.docker = e
}

// DockerCAPEM returns the PEM of the CA that signs docker-mode node certificates; clients trust it for TLS clusters.
func (b *InMemoryBackend) DockerCAPEM() []byte {
	b.mu.RLock("DockerCAPEM")
	defer b.mu.RUnlock()

	if b.docker == nil {
		return nil
	}

	return b.docker.CAPEM()
}

// Close removes every docker node container and waits for launches in flight.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")
	b.stopAllEnginesLocked()
	b.mu.Unlock()
	b.dockerWG.Wait()
}

// Shutdown releases docker resources when the server stops.
func (h *Handler) Shutdown(_ context.Context) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Close()
	}
}

func (b *InMemoryBackend) dockerSpec(c *Cluster, seeds [][]byte) redisdocker.Spec {
	ranges := shardSlotRanges(c.NumShards)
	shards := make([]redisdocker.ShardSpec, len(ranges))

	for i, r := range ranges {
		slots, _ := redisdocker.ParseSlots(r)
		shards[i] = redisdocker.ShardSpec{Slots: slots, Replicas: int(c.NumReplicasPerShard)}

		if i < len(seeds) {
			shards[i].Seed = seeds[i]
		}
	}

	return redisdocker.Spec{
		Engine: c.Engine, Version: c.EngineVersion, ClusterMode: true, TLS: c.TLSEnabled,
		TLSNames: []string{c.Endpoint}, Shards: shards,
	}
}

// launchDockerLocked starts c's containers in the background; c reports "creating" until they serve traffic.
// Must hold b.mu.
func (b *InMemoryBackend) launchDockerLocked(region string, c *Cluster, seeds [][]byte) {
	ctx, cancel := context.WithCancel(context.Background())
	d := &dockState{cancel: cancel}
	c.dock = d
	c.Status = statusCreating
	spec := b.dockerSpec(c, seeds)
	eng := b.docker

	b.dockerWG.Go(func() {
		g, err := eng.Launch(ctx, spec)
		b.finishLaunch(region, c, d, g, err)
	})
}

func (b *InMemoryBackend) finishLaunch(region string, c *Cluster, d *dockState, g *redisdocker.Group, err error) {
	b.mu.Lock("finishLaunch")
	defer b.mu.Unlock()

	cur, ok := tableGet(b.clusters[region], c.Name)
	if !ok || cur != c || c.dock != d {
		if g != nil {
			g.Close()
		}

		return
	}

	if err == nil {
		d.group = g
		c.endpoints = g.Endpoints()

		if err = b.applyDockerACLLocked(c); err == nil {
			c.Status = clusterStatusAvailable

			return
		}
	}

	logger.Load(context.Background()).Warn("memorydb: docker cluster failed to start", "cluster", c.Name, "error", err)
	b.appendEventLocked(region, &Event{
		Date: time.Now(), SourceName: c.Name, SourceType: resourceKindCluster,
		Message: "Cluster " + c.Name + " could not be created: the data plane failed to start",
	})

	if g != nil {
		g.Close()
	}

	c.dock, c.endpoints = nil, nil
	b.clustersStore(region).Delete(c.Name)
	delete(b.arnToResourceStore(region), c.ARN)
}

// relaunchDockerLocked starts fresh containers for a restored cluster; their keyspaces start empty.
func (b *InMemoryBackend) relaunchDockerLocked(c *Cluster) {
	if b.docker == nil {
		return
	}

	region := c.Region
	if region == "" {
		region = b.defaultRegion
	}

	b.launchDockerLocked(region, c, nil)
}

// releaseDockLocked removes c's containers in the background. Must hold b.mu.
func (b *InMemoryBackend) releaseDockLocked(c *Cluster) {
	d := c.dock
	if d == nil {
		return
	}

	c.dock, c.endpoints = nil, nil
	d.cancel()

	if g := d.group; g != nil {
		b.dockerWG.Go(g.Close)
	}
}

// aclPolicyLocked translates c's ACL and users into a Redis ACL policy. Must hold b.mu.
func (b *InMemoryBackend) aclPolicyLocked(c *Cluster) redisdocker.ACLPolicy {
	region := c.Region
	if region == "" {
		region = b.defaultRegion
	}

	acl, ok := b.aclsStore(region).Get(c.ACLName)
	if !ok || acl.Name == openAccessACL {
		return redisdocker.ACLPolicy{OpenDefault: true}
	}

	var policy redisdocker.ACLPolicy

	for _, name := range acl.UserNames {
		u, found := b.usersStore(region).Get(name)
		if !found {
			continue
		}

		policy.Users = append(policy.Users, redisdocker.ACLUser{
			Name: u.Name, AccessString: u.AccessString, Passwords: u.Passwords,
			NoPassword: u.AuthType == authTypeNoPassword,
		})
	}

	return policy
}

func (b *InMemoryBackend) applyDockerACLLocked(c *Cluster) error {
	if c.dock == nil || c.dock.group == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), aclSyncTimeout)
	defer cancel()

	if err := c.dock.group.ApplyACL(ctx, b.aclPolicyLocked(c)); err != nil {
		return fmt.Errorf("sync ACL to cluster %s: %w", c.Name, err)
	}

	return nil
}

const aclSyncTimeout = 30 * time.Second

// syncDockerACLLocked re-applies c's ACL to its containers, logging (never returning) failures. Must hold b.mu.
func (b *InMemoryBackend) syncDockerACLLocked(c *Cluster) {
	if err := b.applyDockerACLLocked(c); err != nil {
		logger.Load(context.Background()).Warn("memorydb: ACL sync failed", "cluster", c.Name, "error", err)
	}
}

// dockerSnapshotLocked captures the keyspace of every shard primary; nil when c has no containers. Must hold b.mu.
func (b *InMemoryBackend) dockerSnapshotLocked(c *Cluster) ([]byte, bool, error) {
	if c.dock == nil || c.dock.group == nil {
		return nil, false, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), aclSyncTimeout)
	defer cancel()

	shards, err := c.dock.group.Snapshot(ctx)
	if err != nil {
		return nil, true, fmt.Errorf("snapshot cluster %s: %w", c.Name, err)
	}

	return redisdocker.EncodeBundle(shards), true, nil
}

// captureDataLocked snapshots c's keyspace from whichever data plane serves it. Must hold b.mu.
func (b *InMemoryBackend) captureDataLocked(c *Cluster) ([]byte, error) {
	if data, ok, err := b.dockerSnapshotLocked(c); ok || err != nil {
		return data, err
	}

	return c.engine.Snapshot()
}

func (c *Cluster) mini() *miniredis.Miniredis {
	if c.engine == nil {
		return nil
	}

	return c.engine.Mini
}
