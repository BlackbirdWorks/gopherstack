package medialive

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- Cluster operations ---

// CreateCluster creates a new Cluster.
func (b *InMemoryBackend) CreateCluster(
	name, clusterType, instanceRoleArn string,
	networkSettings ClusterNetworkSettings,
	tags map[string]string,
) (*Cluster, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}

	if clusterType == "" {
		clusterType = "ON_PREMISES"
	}

	id := newID()
	c := &storedCluster{
		ARN:             b.clusterARN(id),
		ID:              id,
		Name:            name,
		ClusterType:     clusterType,
		InstanceRoleArn: instanceRoleArn,
		State:           clusterStateActive,
		Tags:            copyTags(tags),
		Nodes:           make(map[string]*storedNode),
		NetworkSettings: networkSettings,
	}

	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	b.clusters.Put(c)

	return c.toCluster(b.channelIDsForCluster(id)), nil
}

// DescribeCluster returns a Cluster by ID.
func (b *InMemoryBackend) DescribeCluster(clusterID string) (*Cluster, error) {
	b.mu.RLock("DescribeCluster")
	defer b.mu.RUnlock()

	c, ok := b.clusters.Get(clusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	return c.toCluster(b.channelIDsForCluster(clusterID)), nil
}

// UpdateCluster updates a Cluster's mutable fields. A zero-value
// networkSettings leaves the stored NetworkSettings untouched (mirrors
// UpdateClusterInput's "include this parameter only if you want to change
// it" semantics for NetworkSettings).
func (b *InMemoryBackend) UpdateCluster(
	clusterID, name string,
	networkSettings ClusterNetworkSettings,
	hasNetworkSettings bool,
) (*Cluster, error) {
	b.mu.Lock("UpdateCluster")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	if name != "" {
		c.Name = name
	}

	if hasNetworkSettings {
		c.NetworkSettings = networkSettings
	}

	return c.toCluster(b.channelIDsForCluster(clusterID)), nil
}

// DeleteCluster deletes a Cluster.
func (b *InMemoryBackend) DeleteCluster(clusterID string) (*Cluster, error) {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	c.State = clusterStateDeleted
	channelIDs := b.channelIDsForCluster(clusterID)
	b.clusters.Delete(clusterID)
	delete(b.tags, c.ARN)
	b.cascadeDeleteChannelPlacementGroups(clusterID)

	return c.toCluster(channelIDs), nil
}

// cascadeDeleteChannelPlacementGroups removes every ChannelPlacementGroup
// belonging to clusterID (and its tags), so deleting a Cluster doesn't leave
// orphaned ChannelPlacementGroup rows -- and their b.tags entries -- pointing
// at a Cluster ID that no longer exists. Unlike Nodes (embedded directly in
// storedCluster.Nodes, so they're removed automatically), ChannelPlacementGroups
// live in their own top-level table keyed by "clusterID/groupID", so nothing
// removes them when the parent Cluster row goes away without this. Caller
// must already hold b.mu (Lock).
func (b *InMemoryBackend) cascadeDeleteChannelPlacementGroups(clusterID string) {
	for _, g := range b.channelPlacementGroups.All() {
		if g.ClusterID != clusterID {
			continue
		}

		b.channelPlacementGroups.Delete(cpgKey(g.ClusterID, g.ID))
		delete(b.tags, g.ARN)
	}
}

// channelIDsForCluster returns the sorted set of Channel IDs whose
// AnywhereSettings.ClusterID matches clusterID. Caller must already hold
// b.mu (Lock or RLock) -- see the real DescribeClusterOutput's "channelIds"
// field (types.DescribeClusterSummary), a live association gopherstack
// derives from Channel.AnywhereSettings rather than persisting redundantly.
func (b *InMemoryBackend) channelIDsForCluster(clusterID string) []string {
	ids := []string{}

	for _, ch := range b.channels.All() {
		if ch.AnywhereSettings.ClusterID == clusterID {
			ids = append(ids, ch.ID)
		}
	}

	sort.Strings(ids)

	return ids
}

// ListClusters returns a paginated list of Clusters.
func (b *InMemoryBackend) ListClusters(
	maxResults int,
	nextToken string,
) ([]*ClusterSummary, string, error) {
	b.mu.RLock("ListClusters")
	defer b.mu.RUnlock()

	all := b.clusters.All()

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*ClusterSummary, 0, len(pg.Data))
	for _, c := range pg.Data {
		summaries = append(summaries, c.toSummary(b.channelIDsForCluster(c.ID)))
	}

	return summaries, pg.Next, nil
}

// ListClusterAlerts returns alerts for a Cluster.
func (b *InMemoryBackend) ListClusterAlerts(
	clusterID string,
	_ int,
	_ string,
) ([]map[string]any, string, error) {
	b.mu.RLock("ListClusterAlerts")
	defer b.mu.RUnlock()

	cl, ok := b.clusters.Get(clusterID)
	if !ok {
		return nil, "", fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	// Field names/casing here mirror the real ClusterAlert shape
	// (id/alertType/message/state/setTimestamp -- verified against
	// aws-sdk-go-v2/service/medialive's ClusterAlert deserializer); there
	// is no "AlertCode"/"AlertMessage"/"SetTime"/"ClearedTime" on the real
	// wire.
	var alerts []map[string]any
	if cl.State != clusterStateActive {
		alerts = []map[string]any{
			{
				keyID:           "cluster-not-ready",
				"alertType":     "CLUSTER_NOT_READY",
				keyLowerMessage: "Cluster is not in ACTIVE state",
				keyState:        "SET",
				"setTimestamp":  formatISO8601(time.Unix(0, 0).UTC()),
			},
		}
	} else {
		alerts = []map[string]any{}
	}

	return alerts, "", nil
}
