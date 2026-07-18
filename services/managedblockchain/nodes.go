package managedblockchain

import (
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// nodeStatusAvailable is the status for a ready node.
const nodeStatusAvailable = "AVAILABLE"

// nodeARN builds the ARN for a Managed Blockchain node.
func nodeARN(region, accountID, nodeID string) string {
	return arn.Build("managedblockchain", region, accountID, "nodes/"+nodeID)
}

// cloneNode returns a deep copy of n with the Tags map cloned.
func cloneNode(n *Node) *Node {
	cp := *n
	cp.Tags = maps.Clone(n.Tags)

	return &cp
}

// CreateNode creates a new peer node within a member.
func (b *InMemoryBackend) CreateNode(
	region, accountID, networkID, memberID, instanceType, availabilityZone string,
	tags map[string]string,
) (*Node, error) {
	b.mu.Lock("CreateNode")
	defer b.mu.Unlock()

	if _, exists := b.networks.Get(networkID); !exists {
		return nil, ErrNetworkNotFound
	}

	if _, exists := b.members.Get(memberKey(networkID, memberID)); !exists {
		return nil, ErrMemberNotFound
	}

	now := time.Now().UTC()
	nodeID := uuid.NewString()

	t := make(map[string]string)
	maps.Copy(t, tags)

	node := &Node{
		ID:               nodeID,
		Arn:              nodeARN(region, accountID, nodeID),
		NetworkID:        networkID,
		MemberID:         memberID,
		InstanceType:     instanceType,
		AvailabilityZone: availabilityZone,
		Status:           nodeStatusAvailable,
		CreationDate:     &now,
		Tags:             t,
	}

	b.nodes.Put(node)
	b.arnToResource[node.Arn] = node

	return cloneNode(node), nil
}

// GetNode returns a node by network ID, member ID, and node ID.
func (b *InMemoryBackend) GetNode(networkID, memberID, nodeID string) (*Node, error) {
	b.mu.RLock("GetNode")
	defer b.mu.RUnlock()

	if _, exists := b.networks.Get(networkID); !exists {
		return nil, ErrNetworkNotFound
	}

	node, exists := b.nodes.Get(nodeKey(networkID, memberID, nodeID))
	if !exists {
		return nil, ErrNodeNotFound
	}

	return cloneNode(node), nil
}

// ListNodes returns all nodes for a member sorted by ID, optionally filtered.
func (b *InMemoryBackend) ListNodes(networkID, memberID string, filter ListNodesFilter) ([]*Node, error) {
	b.mu.RLock("ListNodes")
	defer b.mu.RUnlock()

	if _, exists := b.networks.Get(networkID); !exists {
		return nil, ErrNetworkNotFound
	}

	nodes := b.nodesByMember.Get(nodeMemberKey(networkID, memberID))
	all := make([]*Node, 0, len(nodes))

	for _, n := range nodes {
		if filter.Status != "" && n.Status != filter.Status {
			continue
		}

		all = append(all, cloneNode(n))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].ID < all[j].ID
	})

	return all, nil
}

// DeleteNode removes a node from a member.
func (b *InMemoryBackend) DeleteNode(networkID, memberID, nodeID string) error {
	b.mu.Lock("DeleteNode")
	defer b.mu.Unlock()

	if _, exists := b.networks.Get(networkID); !exists {
		return ErrNetworkNotFound
	}

	node, exists := b.nodes.Get(nodeKey(networkID, memberID, nodeID))
	if !exists {
		return ErrNodeNotFound
	}

	delete(b.arnToResource, node.Arn)
	b.nodes.Delete(nodeKey(networkID, memberID, nodeID))

	return nil
}

// deleteNodesForMemberLocked cascade-deletes every node belonging to
// (networkID, memberID) from both the nodes table and the ARN index. The
// index's result slice is cloned before the delete loop since Table.Delete
// mutates the very index group being ranged over. Must be called with mu
// held. Called from members.go's DeleteMember and proposals.go's
// executeProposalActionsLocked removal-action cascade.
func (b *InMemoryBackend) deleteNodesForMemberLocked(networkID, memberID string) {
	nodes := slices.Clone(b.nodesByMember.Get(nodeMemberKey(networkID, memberID)))

	for _, node := range nodes {
		delete(b.arnToResource, node.Arn)
		b.nodes.Delete(nodeKey(node.NetworkID, node.MemberID, node.ID))
	}
}

// AddNodeInternal adds a node directly to the backend (for testing and seeding).
// The network and member must already exist.
func (b *InMemoryBackend) AddNodeInternal(region, accountID, networkID, memberID, instanceType string) *Node {
	b.mu.Lock("AddNodeInternal")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	nodeID := uuid.NewString()

	node := &Node{
		ID:           nodeID,
		Arn:          nodeARN(region, accountID, nodeID),
		NetworkID:    networkID,
		MemberID:     memberID,
		InstanceType: instanceType,
		Status:       nodeStatusAvailable,
		CreationDate: &now,
		Tags:         make(map[string]string),
	}

	b.nodes.Put(node)
	b.arnToResource[node.Arn] = node

	return cloneNode(node)
}

// UpdateNode updates a node's log publishing configuration.
func (b *InMemoryBackend) UpdateNode(
	networkID, memberID, nodeID string,
	logConfig *NodeLogPublishingConfigState,
) (*Node, error) {
	b.mu.Lock("UpdateNode")
	defer b.mu.Unlock()

	if _, exists := b.networks.Get(networkID); !exists {
		return nil, ErrNetworkNotFound
	}

	node, exists := b.nodes.Get(nodeKey(networkID, memberID, nodeID))
	if !exists {
		return nil, ErrNodeNotFound
	}

	if logConfig != nil {
		node.LogPublishingConfiguration = cloneNodeLogConfig(logConfig)
	}

	return cloneNode(node), nil
}

// cloneNodeLogConfig returns a deep copy of NodeLogPublishingConfigState.
func cloneNodeLogConfig(c *NodeLogPublishingConfigState) *NodeLogPublishingConfigState {
	if c == nil {
		return nil
	}

	cp := &NodeLogPublishingConfigState{}

	if c.Fabric != nil {
		fabric := &NodeFabricLogState{}

		if c.Fabric.ChaincodeLogs != nil {
			fabric.ChaincodeLogs = cloneLogConfig(c.Fabric.ChaincodeLogs)
		}

		if c.Fabric.PeerLogs != nil {
			fabric.PeerLogs = cloneLogConfig(c.Fabric.PeerLogs)
		}

		cp.Fabric = fabric
	}

	return cp
}
