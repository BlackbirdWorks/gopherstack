package elasticache

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/redisdocker"
)

const (
	dockerLabel    = "elasticache"
	dockerOpTimout = 30 * time.Second
)

// dockRef locates a cluster's node inside a launched container group.
type dockRef struct {
	group      *redisdocker.Group
	shard      int
	node       int
	standalone bool
	seeded     bool
}

type restoreKey struct{}

// WithRestoreSnapshot tells a cluster create to start its data plane from snapshotName.
func WithRestoreSnapshot(ctx context.Context, snapshotName string) context.Context {
	return context.WithValue(ctx, restoreKey{}, snapshotName)
}

func restoreSnapshotFrom(ctx context.Context) string {
	name, _ := ctx.Value(restoreKey{}).(string)

	return name
}

// EnableDockerEngine switches the backend to docker mode: clusters and replication groups run real Redis containers.
func (b *InMemoryBackend) EnableDockerEngine(rt container.Runtime, cfg redisdocker.Config) error {
	if cfg.CA == nil {
		ca, err := devtls.NewCA()
		if err != nil {
			return fmt.Errorf("elasticache docker engine CA: %w", err)
		}

		cfg.CA = ca
	}

	cfg.Runtime = rt
	cfg.Label = dockerLabel

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

// DockerCAPEM returns the PEM of the CA that signs docker-mode node certificates, or nil.
func (b *InMemoryBackend) DockerCAPEM() []byte {
	b.mu.RLock("DockerCAPEM")
	defer b.mu.RUnlock()

	if b.docker == nil {
		return nil
	}

	return b.docker.CAPEM()
}

// Close removes every docker container the backend still runs.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")
	b.closeAllDockLocked()
	b.mu.Unlock()
	b.dockerWG.Wait()
}

// Shutdown releases docker resources when the server stops.
func (h *Handler) Shutdown(_ context.Context) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Close()
	}
}

func (b *InMemoryBackend) dockerOn() bool { return b.docker != nil }

// closeGroupAsync removes a group's containers in the background. Must hold b.mu (or be single-threaded).
func (b *InMemoryBackend) closeGroupAsync(g *redisdocker.Group) {
	if g != nil {
		b.dockerWG.Go(g.Close)
	}
}

// closeAllDockLocked releases every docker group, as Reset and Restore discard the tables. Must hold b.mu.
func (b *InMemoryBackend) closeAllDockLocked() {
	for _, t := range b.replicationGroups {
		for _, rg := range t.All() {
			b.closeGroupAsync(rg.dock)
			rg.dock = nil
		}
	}

	for _, t := range b.clusters {
		for _, c := range t.All() {
			b.releaseDockRefLocked(c)
		}
	}

	for _, g := range b.strayGroups {
		b.closeGroupAsync(g)
	}

	b.strayGroups = nil
}

// releaseDockRefLocked closes a standalone cluster's container; group members are closed with their group.
func (b *InMemoryBackend) releaseDockRefLocked(c *Cluster) {
	if c.dock != nil && c.dock.standalone {
		b.closeGroupAsync(c.dock.group)
	}

	c.dock = nil
}

func (b *InMemoryBackend) engineName(engine string) string {
	if engine == engineValkey {
		return redisdocker.EngineValkey
	}

	return redisdocker.EngineRedis
}

// startDockerStandalone launches one container for a standalone cluster, seeded from the snapshot in ctx.
func (b *InMemoryBackend) startDockerStandalone(ctx context.Context, engine string) (*clusterEngine, error) {
	seeds, err := b.seedsForSnapshot(ctx, restoreSnapshotFrom(ctx), 1, dataKeyStandalone)
	if err != nil {
		return nil, err
	}

	g, err := b.docker.Launch(ctx, redisdocker.Spec{
		Engine: b.engineName(engine), Version: defaultEngineVersion(engine),
		Shards: []redisdocker.ShardSpec{{Seed: seeds[0]}},
	})
	if err != nil {
		return nil, fmt.Errorf("start redis container: %w", err)
	}

	ep := g.Endpoints()[0][0]

	return &clusterEngine{
		port: ep.Port, svcPort: ep.Port, connectAddr: ep.Addr(),
		dock: &dockRef{group: g, standalone: true, seeded: seeds[0] != nil},
	}, nil
}

// seedsForSnapshot reads the per-shard RDB seeds of snapshotName; empty name or no data yields nil seeds.
func (b *InMemoryBackend) seedsForSnapshot(ctx context.Context, name string, shards int, key string) ([][]byte, error) {
	out := make([][]byte, shards)
	if name == "" {
		return out, nil
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("seedsForSnapshot")
	defer b.mu.RUnlock()

	snap, ok := b.snapshotsStore(region).Get(name)
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	for i := range out {
		blob := snap.Data[shardKey(key, i, shards)]
		if blob == nil && shards == 1 {
			blob = clusterBlob(snap)
		}

		seed, err := redisdocker.SeedFromBlob(blob)
		if err != nil {
			return nil, fmt.Errorf("snapshot %s: %w", name, err)
		}

		out[i] = seed
	}

	return out, nil
}

func shardKey(standalone string, i, shards int) string {
	if shards == 1 {
		return standalone
	}

	return strconv.Itoa(i)
}

// dockerGroupSpec turns a prepared replication group into a container group spec.
func (b *InMemoryBackend) dockerGroupSpec(opts ReplicationGroupCreateOpts, prep preparedGroup, seeds [][]byte) (
	redisdocker.Spec, error,
) {
	clusterMode := prep.clusterMode && opts.ClusterMode != statusDisabled
	spec := redisdocker.Spec{
		Engine: b.engineName(
			opts.Engine,
		), Version: firstNonEmpty(opts.EngineVersion, defaultEngineVersion(opts.Engine)),
		ClusterMode: clusterMode,
		TLS:         opts.TransitEncryptionEnabled && opts.TransitEncryptionMode != transitEncryptionModePreferred,
	}

	for i, plan := range prep.plans {
		sh := redisdocker.ShardSpec{Replicas: len(plan.replicas)}
		if i < len(seeds) {
			sh.Seed = seeds[i]
		}

		if clusterMode {
			slots, err := redisdocker.ParseSlots(plan.slots)
			if err != nil {
				return spec, fmt.Errorf("%w: node group %s: %w", ErrInvalidParameterValue, plan.id, err)
			}

			sh.Slots = slots
		}

		spec.Shards = append(spec.Shards, sh)
	}

	return spec, nil
}

// groupSeeds resolves the per-shard seeds a group restores from opts.SnapshotName.
func (b *InMemoryBackend) groupSeeds(ctx context.Context, opts ReplicationGroupCreateOpts, prep preparedGroup) (
	[][]byte, error,
) {
	out := make([][]byte, len(prep.plans))
	if opts.SnapshotName == "" {
		return out, nil
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("groupSeeds")
	defer b.mu.RUnlock()

	snap, ok := b.snapshotsStore(region).Get(opts.SnapshotName)
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	for i, plan := range prep.plans {
		blob := snap.Data[plan.id]
		if blob == nil && len(prep.plans) == 1 {
			blob = clusterBlob(snap)
		}

		seed, err := redisdocker.SeedFromBlob(blob)
		if err != nil {
			return nil, fmt.Errorf("snapshot %s: %w", opts.SnapshotName, err)
		}

		out[i] = seed
	}

	return out, nil
}

// launchDockerGroup starts the containers of a replication group and returns one engine per node, shard-major.
func (b *InMemoryBackend) launchDockerGroup(ctx context.Context, opts ReplicationGroupCreateOpts, prep preparedGroup) (
	*redisdocker.Group, [][]*clusterEngine, error,
) {
	seeds, err := b.groupSeeds(ctx, opts, prep)
	if err != nil {
		return nil, nil, err
	}

	spec, err := b.dockerGroupSpec(opts, prep, seeds)
	if err != nil {
		return nil, nil, err
	}

	g, err := b.docker.Launch(ctx, spec)
	if err != nil {
		return nil, nil, fmt.Errorf("start redis group: %w", err)
	}

	policy := b.groupACLPolicy(getRegion(ctx, b.region), opts.UserGroupIDs, opts.AuthToken)

	actx, cancel := context.WithTimeout(ctx, dockerOpTimout)
	defer cancel()

	if err = g.ApplyACL(actx, policy); err != nil {
		g.Close()

		return nil, nil, fmt.Errorf("sync ACL: %w", err)
	}

	eps := g.Endpoints()
	engines := make([][]*clusterEngine, len(eps))

	for si, shard := range eps {
		for ni, ep := range shard {
			engines[si] = append(engines[si], &clusterEngine{
				port: ep.Port, svcPort: ep.Port, connectAddr: ep.Addr(),
				dock: &dockRef{group: g, shard: si, node: ni, seeded: seeds[si] != nil},
			})
		}
	}

	return g, engines, nil
}

// groupACLPolicy builds the Redis ACL policy of a replication group from its auth token and user groups.
func (b *InMemoryBackend) groupACLPolicy(region string, userGroupIDs []string, authToken string) redisdocker.ACLPolicy {
	b.mu.RLock("groupACLPolicy")
	defer b.mu.RUnlock()

	return b.groupACLPolicyLocked(region, userGroupIDs, authToken)
}

func (b *InMemoryBackend) groupACLPolicyLocked(
	region string,
	userGroupIDs []string,
	authToken string,
) redisdocker.ACLPolicy {
	policy := redisdocker.ACLPolicy{DefaultPassword: authToken, OpenDefault: len(userGroupIDs) == 0 && authToken == ""}
	seen := map[string]bool{}

	for _, gid := range userGroupIDs {
		ug, ok := b.userGroupsStoreRO(region).Get(gid)
		if !ok {
			continue
		}

		for _, uid := range ug.UserIDs {
			u, found := b.usersStoreRO(region).Get(uid)
			if !found || seen[u.UserName] {
				continue
			}

			seen[u.UserName] = true
			policy.Users = append(policy.Users, redisdocker.ACLUser{
				Name: u.UserName, AccessString: u.AccessString, Passwords: u.Passwords,
				NoPassword: u.AuthType == authTypeNoPasswordOutput,
			})
		}
	}

	return policy
}

// syncDockerACLsLocked re-applies every docker replication group's ACL, logging (never returning) failures.
// Must hold b.mu.
func (b *InMemoryBackend) syncDockerACLsLocked() {
	if !b.dockerOn() {
		return
	}

	for region, t := range b.replicationGroups {
		for _, rg := range t.All() {
			if rg.dock == nil {
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), dockerOpTimout)
			err := rg.dock.ApplyACL(ctx, b.groupACLPolicyLocked(region, rg.UserGroupIDs, rg.AuthToken))

			cancel()

			if err != nil {
				logger.Load(context.Background()).Warn("elasticache: ACL sync failed",
					"replicationGroup", rg.ReplicationGroupID, "error", err)
			}
		}
	}
}

// SetUserPasswords stores the passwords a user authenticates with; they reach Redis through the ACL sync.
func (b *InMemoryBackend) SetUserPasswords(ctx context.Context, userID string, passwords []string) {
	b.mu.Lock("SetUserPasswords")
	defer b.mu.Unlock()
	defer b.syncDockerACLsLocked()

	if u, ok := b.usersStore(getRegion(ctx, b.region)).Get(userID); ok {
		u.Passwords = append([]string(nil), passwords...)
	}
}

// dockerSnapshotGroup captures each node group's dataset keyed by NodeGroupID.
func (b *InMemoryBackend) dockerSnapshotGroup(rg *ReplicationGroup) (map[string][]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dockerOpTimout)
	defer cancel()

	shards, err := rg.dock.Snapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot replication group %s: %w", rg.ReplicationGroupID, err)
	}

	data := make(map[string][]byte, len(shards))

	for i, rdb := range shards {
		if i < len(rg.NodeGroups) {
			data[rg.NodeGroups[i].NodeGroupID] = rdb
		}
	}

	return data, nil
}

func (b *InMemoryBackend) dockerSnapshotCluster(c *Cluster) (map[string][]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dockerOpTimout)
	defer cancel()

	shards, err := c.dock.group.Snapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot cluster %s: %w", c.ClusterID, err)
	}

	return map[string][]byte{dataKeyStandalone: shards[c.dock.shard]}, nil
}

// newReplicaLocked creates a replica's cluster row: its own container in docker mode, else the primary's engine.
func (b *InMemoryBackend) newReplicaLocked(
	t memberTemplate, id string, np nodePlan, shard, replica int, primary *Cluster,
) *Cluster {
	if t.docker != nil {
		return b.newMemberLocked(t, id, np, t.docker[shard][replica+1], nil)
	}

	return b.newMemberLocked(t, id, np, nil, primary)
}

// dockerReader returns the first replica of ng when it has its own container, so the reader endpoint targets it.
func (b *InMemoryBackend) dockerReader(t memberTemplate, ng NodeGroup) *Cluster {
	if t.docker == nil || len(ng.Replicas) == 0 {
		return nil
	}

	c, _ := b.clustersStore(t.region).Get(ng.Replicas[0].CacheClusterID)

	return c
}

// createDockerGroup launches the containers of a replication group and stores the group around them.
func (b *InMemoryBackend) createDockerGroup(
	ctx context.Context, region string, opts ReplicationGroupCreateOpts, prep preparedGroup,
) (*ReplicationGroup, error) {
	if prep.adoptID != "" {
		return nil, fmt.Errorf("%w: PrimaryClusterId is not supported by the docker engine",
			ErrInvalidParameterCombination)
	}

	if opts.AuthTokenEnabled && opts.AuthToken == "" {
		opts.AuthToken = generateAuthToken()
	}

	g, engines, err := b.launchDockerGroup(ctx, opts, prep)
	if err != nil {
		return nil, err
	}

	prep.docker = engines

	primaries := make([]*clusterEngine, len(engines))
	for i, shard := range engines {
		primaries[i] = shard[0]
	}

	b.mu.Lock("CreateReplicationGroupFull.insert")
	defer b.mu.Unlock()

	rg, err := b.insertGroupLocked(region, opts, prep, primaries)
	if err != nil {
		b.closeGroupAsync(g)

		return nil, err
	}

	rg.dock = g

	return b.replicationGroupView(rg), nil
}
