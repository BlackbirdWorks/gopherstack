package medialive

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- ChannelPlacementGroup operations ---

// cpgKey builds the composite map key for a placement group.
func cpgKey(clusterID, groupID string) string {
	return clusterID + "/" + groupID
}

// CreateChannelPlacementGroup creates a placement group within a cluster.
func (b *InMemoryBackend) CreateChannelPlacementGroup(
	clusterID, name string,
	nodes []string,
	tags map[string]string,
) (*ChannelPlacementGroup, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: clusterId required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateChannelPlacementGroup")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterID) {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	id := newID()
	ns := make([]string, len(nodes))
	copy(ns, nodes)

	g := &storedChannelPlacementGroup{
		Tags:      copyTags(tags),
		ARN:       b.channelPlacementGroupARN(id),
		ID:        id,
		Name:      name,
		ClusterID: clusterID,
		State:     channelPlacementGroupStateUnassigned,
		Nodes:     ns,
	}

	b.channelPlacementGroups.Put(g)

	return g.toGroup(b.channelIDsForPlacementGroup(id)), nil
}

// channelIDsForPlacementGroup returns the sorted set of Channel IDs whose
// AnywhereSettings.ChannelPlacementGroupID matches groupID. Caller must
// already hold b.mu (Lock or RLock).
func (b *InMemoryBackend) channelIDsForPlacementGroup(groupID string) []string {
	ids := []string{}

	for _, ch := range b.channels.All() {
		if ch.AnywhereSettings.ChannelPlacementGroupID == groupID {
			ids = append(ids, ch.ID)
		}
	}

	sort.Strings(ids)

	return ids
}

// DescribeChannelPlacementGroup returns a placement group by cluster and group ID.
func (b *InMemoryBackend) DescribeChannelPlacementGroup(
	clusterID, groupID string,
) (*ChannelPlacementGroup, error) {
	b.mu.RLock("DescribeChannelPlacementGroup")
	defer b.mu.RUnlock()

	g, ok := b.channelPlacementGroups.Get(cpgKey(clusterID, groupID))
	if !ok {
		return nil, fmt.Errorf("%w: channelPlacementGroup %s not found", ErrNotFound, groupID)
	}

	return g.toGroup(b.channelIDsForPlacementGroup(groupID)), nil
}

// UpdateChannelPlacementGroup updates a placement group's mutable fields.
func (b *InMemoryBackend) UpdateChannelPlacementGroup(
	clusterID, groupID, name string,
	nodes []string,
) (*ChannelPlacementGroup, error) {
	b.mu.Lock("UpdateChannelPlacementGroup")
	defer b.mu.Unlock()

	g, ok := b.channelPlacementGroups.Get(cpgKey(clusterID, groupID))
	if !ok {
		return nil, fmt.Errorf("%w: channelPlacementGroup %s not found", ErrNotFound, groupID)
	}

	if name != "" {
		g.Name = name
	}

	if nodes != nil {
		ns := make([]string, len(nodes))
		copy(ns, nodes)
		g.Nodes = ns
	}

	return g.toGroup(b.channelIDsForPlacementGroup(groupID)), nil
}

// DeleteChannelPlacementGroup deletes a placement group.
func (b *InMemoryBackend) DeleteChannelPlacementGroup(
	clusterID, groupID string,
) (*ChannelPlacementGroup, error) {
	b.mu.Lock("DeleteChannelPlacementGroup")
	defer b.mu.Unlock()

	key := cpgKey(clusterID, groupID)

	g, ok := b.channelPlacementGroups.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: channelPlacementGroup %s not found", ErrNotFound, groupID)
	}

	g.State = channelPlacementGroupStateDeleting
	out := g.toGroup(b.channelIDsForPlacementGroup(groupID))
	b.channelPlacementGroups.Delete(key)
	delete(b.tags, g.ARN)

	return out, nil
}

// ListChannelPlacementGroups returns a paginated list of placement groups in a cluster.
func (b *InMemoryBackend) ListChannelPlacementGroups(
	clusterID string,
	maxResults int,
	nextToken string,
) ([]*ChannelPlacementGroup, string, error) {
	b.mu.RLock("ListChannelPlacementGroups")
	defer b.mu.RUnlock()

	if !b.clusters.Has(clusterID) {
		return nil, "", fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	all := make([]*storedChannelPlacementGroup, 0, b.channelPlacementGroups.Len())
	for _, g := range b.channelPlacementGroups.All() {
		if g.ClusterID == clusterID {
			all = append(all, g)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	out := make([]*ChannelPlacementGroup, 0, len(pg.Data))
	for _, g := range pg.Data {
		out = append(out, g.toGroup(b.channelIDsForPlacementGroup(g.ID)))
	}

	return out, pg.Next, nil
}
