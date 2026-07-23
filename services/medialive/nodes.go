package medialive

import (
	"fmt"
	"slices"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- Node operations ---

// CreateNode creates a Node within a Cluster.
func (b *InMemoryBackend) CreateNode(
	clusterID, name, role string,
	tags map[string]string,
) (*Node, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: clusterId required", ErrInvalidParameter)
	}

	if role == "" {
		role = nodeRoleActive
	}

	b.mu.Lock("CreateNode")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	id := newID()
	if name == "" {
		name = id
	}

	n := &storedNode{
		ARN:             b.nodeARN(id),
		ID:              id,
		Name:            name,
		ClusterID:       clusterID,
		Role:            role,
		State:           nodeStateActive,
		ConnectionState: nodeConnectionConn,
		Tags:            copyTags(tags),
	}

	c.Nodes[id] = n

	return n.toNode(b.channelPlacementGroupIDsForNode(clusterID, id)), nil
}

// channelPlacementGroupIDsForNode returns the sorted set of
// ChannelPlacementGroup IDs (within clusterID) whose Nodes list contains
// nodeID. Caller must already hold b.mu (Lock or RLock) -- see the real
// DescribeNodeOutput's "channelPlacementGroups" field, a live association
// gopherstack derives from ChannelPlacementGroup.Nodes rather than
// persisting redundantly.
func (b *InMemoryBackend) channelPlacementGroupIDsForNode(clusterID, nodeID string) []string {
	ids := []string{}

	for _, g := range b.channelPlacementGroups.All() {
		if g.ClusterID != clusterID {
			continue
		}

		if slices.Contains(g.Nodes, nodeID) {
			ids = append(ids, g.ID)
		}
	}

	sort.Strings(ids)

	return ids
}

// DescribeNode returns a Node by cluster ID and node ID.
func (b *InMemoryBackend) DescribeNode(clusterID, nodeID string) (*Node, error) {
	b.mu.RLock("DescribeNode")
	defer b.mu.RUnlock()

	c, ok := b.clusters.Get(clusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	n, ok := c.Nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("%w: node %s not found", ErrNotFound, nodeID)
	}

	return n.toNode(b.channelPlacementGroupIDsForNode(clusterID, nodeID)), nil
}

// UpdateNode updates a Node's mutable fields.
func (b *InMemoryBackend) UpdateNode(clusterID, nodeID, name, role string) (*Node, error) {
	b.mu.Lock("UpdateNode")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	n, ok := c.Nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("%w: node %s not found", ErrNotFound, nodeID)
	}

	if name != "" {
		n.Name = name
	}

	if role != "" {
		n.Role = role
	}

	return n.toNode(b.channelPlacementGroupIDsForNode(clusterID, nodeID)), nil
}

// UpdateNodeState updates the state of a Node.
func (b *InMemoryBackend) UpdateNodeState(clusterID, nodeID, state string) (*Node, error) {
	b.mu.Lock("UpdateNodeState")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	n, ok := c.Nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("%w: node %s not found", ErrNotFound, nodeID)
	}

	if state != "" {
		n.State = state
	}

	return n.toNode(b.channelPlacementGroupIDsForNode(clusterID, nodeID)), nil
}

// DeleteNode removes a Node from a Cluster.
func (b *InMemoryBackend) DeleteNode(clusterID, nodeID string) (*Node, error) {
	b.mu.Lock("DeleteNode")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	n, ok := c.Nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("%w: node %s not found", ErrNotFound, nodeID)
	}

	cpgIDs := b.channelPlacementGroupIDsForNode(clusterID, nodeID)
	delete(c.Nodes, nodeID)
	delete(b.tags, n.ARN)

	return n.toNode(cpgIDs), nil
}

// paginateNodes returns a sorted, paginated node-summary slice from a cluster.
func (b *InMemoryBackend) paginateNodes(
	c *storedCluster,
	maxResults int,
	nextToken string,
) ([]*NodeSummary, string) {
	nodes := make([]*storedNode, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		nodes = append(nodes, n)
	}

	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })

	pg := page.New(nodes, nextToken, maxResults, defaultMaxResults)

	out := make([]*NodeSummary, 0, len(pg.Data))
	for _, n := range pg.Data {
		out = append(out, n.toSummary(b.channelPlacementGroupIDsForNode(c.ID, n.ID)))
	}

	return out, pg.Next
}

// ListNodes returns a paginated list of Nodes in a Cluster.
func (b *InMemoryBackend) ListNodes(
	clusterID string,
	maxResults int,
	nextToken string,
) ([]*NodeSummary, string, error) {
	b.mu.RLock("ListNodes")
	defer b.mu.RUnlock()

	c, ok := b.clusters.Get(clusterID)
	if !ok {
		return nil, "", fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	summaries, next := b.paginateNodes(c, maxResults, nextToken)

	return summaries, next, nil
}

// CreateNodeRegistrationScript returns a registration script for a Cluster Node.
func (b *InMemoryBackend) CreateNodeRegistrationScript(clusterID string) (string, error) {
	b.mu.RLock("CreateNodeRegistrationScript")
	defer b.mu.RUnlock()

	if !b.clusters.Has(clusterID) {
		return "", fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	return "#!/bin/bash\n# Node registration script for cluster " + clusterID + "\n", nil
}
