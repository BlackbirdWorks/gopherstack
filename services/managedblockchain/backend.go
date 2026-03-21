package managedblockchain

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNetworkNotFound is returned when a network does not exist.
	ErrNetworkNotFound = awserr.New("ResourceNotFoundException: network not found", awserr.ErrNotFound)
	// ErrMemberNotFound is returned when a member does not exist.
	ErrMemberNotFound = awserr.New("ResourceNotFoundException: member not found", awserr.ErrNotFound)
	// ErrResourceNotFound is returned when a resource (network or member) cannot be found by ARN.
	ErrResourceNotFound = awserr.New("ResourceNotFoundException: resource not found", awserr.ErrNotFound)
	// ErrNetworkAlreadyExists is returned when a network already exists.
	ErrNetworkAlreadyExists = awserr.New(
		"ResourceAlreadyExistsException: network already exists",
		awserr.ErrAlreadyExists,
	)
	// ErrMissingNetworkName is returned when the network name is missing.
	ErrMissingNetworkName = errors.New("Name is required for CreateNetwork")
	// ErrMissingMemberName is returned when the member name is missing.
	ErrMissingMemberName = errors.New("Name is required for member configuration")
	// ErrMissingNetworkID is returned when the network ID is missing from a path.
	ErrMissingNetworkID = errors.New("networkId is required")
	// ErrNodeNotFound is returned when a node does not exist.
	ErrNodeNotFound = awserr.New("ResourceNotFoundException: node not found", awserr.ErrNotFound)
)

const (
	// networkStatusAvailable is the status for a ready network.
	networkStatusAvailable = "AVAILABLE"
	// memberStatusAvailable is the status for a ready member.
	memberStatusAvailable = "AVAILABLE"
	// nodeStatusAvailable is the status for a ready node.
	nodeStatusAvailable = "AVAILABLE"
	// defaultFramework is the default framework for new networks.
	defaultFramework = "HYPERLEDGER_FABRIC"
	// defaultFrameworkVersion is the default framework version.
	defaultFrameworkVersion = "1.4"
)

// StorageBackend is the interface for the Managed Blockchain in-memory backend.
type StorageBackend interface {
	CreateNetwork(
		region, accountID, name, description, framework, frameworkVersion, memberName, memberDescription string,
		tags map[string]string,
	) (*Network, *Member, error)
	GetNetwork(networkID string) (*Network, error)
	ListNetworks() ([]*Network, error)
	CreateMember(region, accountID, networkID, name, description string, tags map[string]string) (*Member, error)
	GetMember(networkID, memberID string) (*Member, error)
	ListMembers(networkID string) ([]*Member, error)
	DeleteMember(networkID, memberID string) error
	CreateNode(
		region, accountID, networkID, memberID, instanceType, availabilityZone string,
		tags map[string]string,
	) (*Node, error)
	GetNode(networkID, memberID, nodeID string) (*Node, error)
	ListNodes(networkID, memberID string) ([]*Node, error)
	DeleteNode(networkID, memberID, nodeID string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	networks      map[string]*Network
	members       map[string]map[string]*Member          // networkID → memberID → Member
	nodes         map[string]map[string]map[string]*Node // networkID → memberID → nodeID → Node
	arnToResource map[string]any                         // ARN → *Network, *Member, or *Node
	mu            sync.RWMutex
}

// NewInMemoryBackend creates a new in-memory Managed Blockchain backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		networks:      make(map[string]*Network),
		members:       make(map[string]map[string]*Member),
		nodes:         make(map[string]map[string]map[string]*Node),
		arnToResource: make(map[string]any),
	}
}

// networkARN builds the ARN for a Managed Blockchain network.
func networkARN(region, accountID, networkID string) string {
	return arn.Build("managedblockchain", region, accountID, fmt.Sprintf("networks/%s", networkID))
}

// memberARN builds the ARN for a Managed Blockchain member.
func memberARN(region, accountID, memberID string) string {
	return arn.Build("managedblockchain", region, accountID, fmt.Sprintf("members/%s", memberID))
}

// nodeARN builds the ARN for a Managed Blockchain node.
func nodeARN(region, accountID, nodeID string) string {
	return arn.Build("managedblockchain", region, accountID, fmt.Sprintf("nodes/%s", nodeID))
}

// CreateNetwork creates a new Managed Blockchain network and its first member.
func (b *InMemoryBackend) CreateNetwork(
	region, accountID, name, description, framework, frameworkVersion, memberName, memberDescription string,
	tags map[string]string,
) (*Network, *Member, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, n := range b.networks {
		if n.Name == name {
			return nil, nil, ErrNetworkAlreadyExists
		}
	}

	now := time.Now().UTC()
	networkID := uuid.NewString()
	memberID := uuid.NewString()

	fw := framework
	if fw == "" {
		fw = defaultFramework
	}

	fwv := frameworkVersion
	if fwv == "" {
		fwv = defaultFrameworkVersion
	}

	t := make(map[string]string)
	maps.Copy(t, tags)

	network := &Network{
		ID:               networkID,
		Arn:              networkARN(region, accountID, networkID),
		Name:             name,
		Description:      description,
		Framework:        fw,
		FrameworkVersion: fwv,
		Status:           networkStatusAvailable,
		CreationDate:     &now,
		Tags:             t,
	}

	b.networks[networkID] = network
	b.members[networkID] = make(map[string]*Member)
	b.arnToResource[network.Arn] = network

	member := &Member{
		ID:           memberID,
		Arn:          memberARN(region, accountID, memberID),
		Name:         memberName,
		Description:  memberDescription,
		NetworkID:    networkID,
		Status:       memberStatusAvailable,
		CreationDate: &now,
		Tags:         make(map[string]string),
	}

	b.members[networkID][memberID] = member
	b.arnToResource[member.Arn] = member

	return cloneNetwork(network), cloneMember(member), nil
}

// cloneNetwork returns a deep copy of n with the Tags map cloned.
func cloneNetwork(n *Network) *Network {
	cp := *n
	cp.Tags = maps.Clone(n.Tags)

	return &cp
}

// cloneMember returns a deep copy of m with the Tags map cloned.
func cloneMember(m *Member) *Member {
	cp := *m
	cp.Tags = maps.Clone(m.Tags)

	return &cp
}

// GetNetwork returns the details of a network by ID.
func (b *InMemoryBackend) GetNetwork(networkID string) (*Network, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	network, exists := b.networks[networkID]
	if !exists {
		return nil, ErrNetworkNotFound
	}

	return cloneNetwork(network), nil
}

// ListNetworks returns all networks.
func (b *InMemoryBackend) ListNetworks() ([]*Network, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	all := make([]*Network, 0, len(b.networks))

	for _, n := range b.networks {
		all = append(all, cloneNetwork(n))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name < all[j].Name
	})

	return all, nil
}

// CreateMember creates a new member in an existing network.
func (b *InMemoryBackend) CreateMember(
	region, accountID, networkID, name, description string,
	tags map[string]string,
) (*Member, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.networks[networkID]; !exists {
		return nil, ErrNetworkNotFound
	}

	now := time.Now().UTC()
	memberID := uuid.NewString()

	t := make(map[string]string)
	maps.Copy(t, tags)

	member := &Member{
		ID:           memberID,
		Arn:          memberARN(region, accountID, memberID),
		Name:         name,
		Description:  description,
		NetworkID:    networkID,
		Status:       memberStatusAvailable,
		CreationDate: &now,
		Tags:         t,
	}

	if b.members[networkID] == nil {
		b.members[networkID] = make(map[string]*Member)
	}

	b.members[networkID][memberID] = member
	b.arnToResource[member.Arn] = member

	return cloneMember(member), nil
}

// GetMember returns a member by network ID and member ID.
func (b *InMemoryBackend) GetMember(networkID, memberID string) (*Member, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, exists := b.networks[networkID]; !exists {
		return nil, ErrNetworkNotFound
	}

	members, ok := b.members[networkID]
	if !ok {
		return nil, ErrMemberNotFound
	}

	member, exists := members[memberID]
	if !exists {
		return nil, ErrMemberNotFound
	}

	return cloneMember(member), nil
}

// ListMembers returns all members in a network.
func (b *InMemoryBackend) ListMembers(networkID string) ([]*Member, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, exists := b.networks[networkID]; !exists {
		return nil, ErrNetworkNotFound
	}

	members := b.members[networkID]
	all := make([]*Member, 0, len(members))

	for _, m := range members {
		all = append(all, cloneMember(m))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name < all[j].Name
	})

	return all, nil
}

// DeleteMember removes a member from a network.
func (b *InMemoryBackend) DeleteMember(networkID, memberID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.networks[networkID]; !exists {
		return ErrNetworkNotFound
	}

	members, ok := b.members[networkID]
	if !ok || members[memberID] == nil {
		return ErrMemberNotFound
	}

	m := members[memberID]
	delete(b.arnToResource, m.Arn)
	delete(members, memberID)

	return nil
}

// ListTagsForResource returns tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	res, ok := b.arnToResource[resourceARN]
	if !ok {
		return nil, ErrResourceNotFound
	}

	switch r := res.(type) {
	case *Network:
		result := make(map[string]string, len(r.Tags))
		maps.Copy(result, r.Tags)

		return result, nil
	case *Member:
		result := make(map[string]string, len(r.Tags))
		maps.Copy(result, r.Tags)

		return result, nil
	}

	return nil, ErrResourceNotFound
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	res, ok := b.arnToResource[resourceARN]
	if !ok {
		return ErrResourceNotFound
	}

	switch r := res.(type) {
	case *Network:
		if r.Tags == nil {
			r.Tags = make(map[string]string)
		}

		maps.Copy(r.Tags, tags)

		return nil
	case *Member:
		if r.Tags == nil {
			r.Tags = make(map[string]string)
		}

		maps.Copy(r.Tags, tags)

		return nil
	}

	return ErrResourceNotFound
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	res, ok := b.arnToResource[resourceARN]
	if !ok {
		return ErrResourceNotFound
	}

	switch r := res.(type) {
	case *Network:
		for _, k := range tagKeys {
			delete(r.Tags, k)
		}

		return nil
	case *Member:
		for _, k := range tagKeys {
			delete(r.Tags, k)
		}

		return nil
	}

	return ErrResourceNotFound
}

// Reset clears all in-memory state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.networks = make(map[string]*Network)
	b.members = make(map[string]map[string]*Member)
	b.nodes = make(map[string]map[string]map[string]*Node)
	b.arnToResource = make(map[string]any)
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
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.networks[networkID]; !exists {
		return nil, ErrNetworkNotFound
	}

	if _, ok := b.members[networkID]; !ok {
		return nil, ErrMemberNotFound
	}

	if _, ok := b.members[networkID][memberID]; !ok {
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

	if b.nodes[networkID] == nil {
		b.nodes[networkID] = make(map[string]map[string]*Node)
	}

	if b.nodes[networkID][memberID] == nil {
		b.nodes[networkID][memberID] = make(map[string]*Node)
	}

	b.nodes[networkID][memberID][nodeID] = node
	b.arnToResource[node.Arn] = node

	return cloneNode(node), nil
}

// GetNode returns a node by network ID, member ID, and node ID.
func (b *InMemoryBackend) GetNode(networkID, memberID, nodeID string) (*Node, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, exists := b.networks[networkID]; !exists {
		return nil, ErrNetworkNotFound
	}

	if b.nodes[networkID] == nil || b.nodes[networkID][memberID] == nil {
		return nil, ErrNodeNotFound
	}

	node, ok := b.nodes[networkID][memberID][nodeID]
	if !ok {
		return nil, ErrNodeNotFound
	}

	return cloneNode(node), nil
}

// ListNodes returns all nodes for a member sorted by ID.
func (b *InMemoryBackend) ListNodes(networkID, memberID string) ([]*Node, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, exists := b.networks[networkID]; !exists {
		return nil, ErrNetworkNotFound
	}

	if b.nodes[networkID] == nil || b.nodes[networkID][memberID] == nil {
		return []*Node{}, nil
	}

	all := make([]*Node, 0, len(b.nodes[networkID][memberID]))
	for _, n := range b.nodes[networkID][memberID] {
		all = append(all, cloneNode(n))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].ID < all[j].ID
	})

	return all, nil
}

// DeleteNode removes a node from a member.
func (b *InMemoryBackend) DeleteNode(networkID, memberID, nodeID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.networks[networkID]; !exists {
		return ErrNetworkNotFound
	}

	if b.nodes[networkID] == nil || b.nodes[networkID][memberID] == nil {
		return ErrNodeNotFound
	}

	node, ok := b.nodes[networkID][memberID][nodeID]
	if !ok {
		return ErrNodeNotFound
	}

	delete(b.arnToResource, node.Arn)
	delete(b.nodes[networkID][memberID], nodeID)

	return nil
}
