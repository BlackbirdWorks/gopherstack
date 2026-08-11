package kafka

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]map[string]*T resource field on InMemoryBackend (nested
// region -> key -> value, to isolate same-named resources across regions) is
// flattened into one *store.Table[T] keyed by the resource's own ARN, which
// already encodes its region (arn:partition:service:region:account:resource,
// see regionFromARN) -- so the ARN alone is globally unique and the outer
// region layer of the map was redundant. This follows the pattern
// established by services/ec2 (12e611a4, data-driven registration) and
// services/sqs (0f09d77c, composite-key flattening of a region-nested map).
//
// Every kafka resource type's identity field already carries a normal JSON
// tag (unlike services/ses's "dirty" tables, which needed a DTO to round-trip
// a json:"-" identity field), so every table below is "clean": registered
// directly on b.registry and driven through b.registry.SnapshotAll() /
// RestoreAll() in persistence.go, with no DTO layer needed.
//
// Two fields remain plain (non-Table) maps because their values are not *T:
//   - scramSecrets: map[string][]string, a slice of secret ARNs per cluster
//   - clusterPolicies: map[string]string, a policy document per cluster
//
// Both are also flattened from map[region]map[clusterArn]V to
// map[clusterArn]V for the same reason (clusterArn alone is already globally
// unique) -- see persistence.go for how they are persisted alongside the
// registry-backed tables.
//
// Secondary indexes replace the O(1) region-scoped (or cluster-scoped)
// lookups the old nested map structure gave for free:
//   - clustersByRegion / configurationsByRegion / replicatorsByRegion /
//     vpcConnectionsByRegion group by region (derived from the ARN) for
//     List*(ctx) and the per-region name-uniqueness checks Create* performs.
//   - clustersByName groups by "<region>|<clusterName>", replacing the
//     standalone clusterNames map, for the O(1) name-uniqueness check
//     CreateCluster and CreateServerlessCluster perform.
//   - topicsByCluster / vpcConnectionsByCluster / clusterOperationsByCluster
//     group by the owning cluster ARN, replacing the linear prefix-scans
//     (or the collectClusterChildrenLocked helper) that used to walk a
//     region-scoped map filtering by a foreign-key field.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func clusterKeyFn(c *Cluster) string                    { return c.ClusterArn }
func configurationKeyFn(c *Configuration) string        { return c.Arn }
func replicatorKeyFn(r *Replicator) string              { return r.ReplicatorArn }
func topicKeyFn(t *Topic) string                        { return topicKey(t.ClusterArn, t.TopicName) }
func vpcConnectionKeyFn(v *VpcConnection) string        { return v.VpcConnectionArn }
func clusterOperationKeyFn(op *ClusterOperation) string { return op.ClusterOperationArn }
func channelKeyFn(ch *Channel) string                   { return ch.ChannelArn }

func clusterRegionIndexKeyFn(c *Cluster) string { return regionFromARN(c.ClusterArn, "") }

func clusterNameIndexKeyFn(c *Cluster) string {
	return regionFromARN(c.ClusterArn, "") + "|" + c.ClusterName
}

func configurationRegionIndexKeyFn(c *Configuration) string { return regionFromARN(c.Arn, "") }

func replicatorRegionIndexKeyFn(r *Replicator) string { return regionFromARN(r.ReplicatorArn, "") }

func vpcConnectionRegionIndexKeyFn(v *VpcConnection) string {
	return regionFromARN(v.VpcConnectionArn, "")
}

func vpcConnectionClusterIndexKeyFn(v *VpcConnection) string { return v.TargetClusterArn }

func topicClusterIndexKeyFn(t *Topic) string { return t.ClusterArn }

func clusterOperationClusterIndexKeyFn(op *ClusterOperation) string { return op.ClusterArn }

func channelClusterIndexKeyFn(ch *Channel) string { return ch.ClusterArn }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must never run on
// every Reset(): store.Register panics on a duplicate name, so runtime
// resets go through b.registry.ResetAll() instead (see InMemoryBackend.Reset).
func registerAllTables(b *InMemoryBackend) {
	b.clusters = store.Register(b.registry, "clusters", store.New(clusterKeyFn))
	b.clustersByRegion = b.clusters.AddIndex("region", clusterRegionIndexKeyFn)
	b.clustersByName = b.clusters.AddIndex("name", clusterNameIndexKeyFn)

	b.configurations = store.Register(b.registry, "configurations", store.New(configurationKeyFn))
	b.configurationsByRegion = b.configurations.AddIndex("region", configurationRegionIndexKeyFn)

	b.replicators = store.Register(b.registry, "replicators", store.New(replicatorKeyFn))
	b.replicatorsByRegion = b.replicators.AddIndex("region", replicatorRegionIndexKeyFn)

	b.topics = store.Register(b.registry, "topics", store.New(topicKeyFn))
	b.topicsByCluster = b.topics.AddIndex("cluster", topicClusterIndexKeyFn)

	b.vpcConnections = store.Register(b.registry, "vpcConnections", store.New(vpcConnectionKeyFn))
	b.vpcConnectionsByRegion = b.vpcConnections.AddIndex("region", vpcConnectionRegionIndexKeyFn)
	b.vpcConnectionsByCluster = b.vpcConnections.AddIndex("cluster", vpcConnectionClusterIndexKeyFn)

	b.clusterOperations = store.Register(b.registry, "clusterOperations", store.New(clusterOperationKeyFn))
	b.clusterOperationsByCluster = b.clusterOperations.AddIndex("cluster", clusterOperationClusterIndexKeyFn)

	b.channels = store.Register(b.registry, "channels", store.New(channelKeyFn))
	b.channelsByCluster = b.channels.AddIndex("cluster", channelClusterIndexKeyFn)
}
