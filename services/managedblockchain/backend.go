package managedblockchain

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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
	// ErrAccessorNotFound is returned when an accessor does not exist.
	ErrAccessorNotFound = awserr.New("ResourceNotFoundException: accessor not found", awserr.ErrNotFound)
	// ErrProposalNotFound is returned when a proposal does not exist.
	ErrProposalNotFound = awserr.New("ResourceNotFoundException: proposal not found", awserr.ErrNotFound)
	// ErrInvitationNotFound is returned when an invitation does not exist.
	ErrInvitationNotFound = awserr.New("ResourceNotFoundException: invitation not found", awserr.ErrNotFound)
	// ErrMissingMemberID is returned when the member ID is missing for a proposal.
	ErrMissingMemberID = errors.New("MemberId is required for CreateProposal")
	// ErrMissingVoterMemberID is returned when the voter member ID is missing for VoteOnProposal.
	ErrMissingVoterMemberID = errors.New("VoterMemberId is required for VoteOnProposal")
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("InvalidRequestException: validation error", awserr.ErrInvalidParameter)
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
	// accessorStatusAvailable is the status for a ready accessor.
	accessorStatusAvailable = "AVAILABLE"
	// accessorDefaultType is the default accessor type.
	accessorDefaultType = "BILLING_TOKEN"
	// proposalStatusInProgress is the status for an in-progress proposal.
	proposalStatusInProgress = "IN_PROGRESS"
	// proposalStatusApproved is the status for an approved proposal.
	proposalStatusApproved = "APPROVED"
	// proposalStatusRejected is the status for a rejected proposal.
	proposalStatusRejected = "REJECTED"
	// proposalExpirationHours is the number of hours before a proposal expires.
	proposalExpirationHours = 24
	// invitationStatusPending is the status for a pending invitation.
	invitationStatusPending = "PENDING"
	// invitationStatusRejected is the status for a rejected invitation.
	invitationStatusRejected = "REJECTED"
)

// ListNetworksFilter contains optional filters for ListNetworks.
type ListNetworksFilter struct {
	Name      string
	Framework string
	Status    string
}

// ListMembersFilter contains optional filters for ListMembers.
type ListMembersFilter struct {
	IsOwned *bool
	Name    string
	Status  string
}

// ListNodesFilter contains optional filters for ListNodes.
type ListNodesFilter struct {
	Status string
}

// ListAccessorsFilter contains optional filters for ListAccessors.
type ListAccessorsFilter struct {
	NetworkType string
}

// StorageBackend is the interface for the Managed Blockchain in-memory backend.
type StorageBackend interface {
	CreateNetwork(
		region, accountID, name, description, framework, frameworkVersion, memberName, memberDescription string,
		tags map[string]string,
		votingPolicy *VotingPolicy,
	) (*Network, *Member, error)
	GetNetwork(networkID string) (*Network, error)
	ListNetworks(filter ListNetworksFilter) ([]*Network, error)
	CreateMember(region, accountID, networkID, name, description string, tags map[string]string) (*Member, error)
	GetMember(networkID, memberID string) (*Member, error)
	ListMembers(networkID string, filter ListMembersFilter) ([]*Member, error)
	DeleteMember(networkID, memberID string) error
	CreateNode(
		region, accountID, networkID, memberID, instanceType, availabilityZone string,
		tags map[string]string,
	) (*Node, error)
	GetNode(networkID, memberID, nodeID string) (*Node, error)
	ListNodes(networkID, memberID string, filter ListNodesFilter) ([]*Node, error)
	DeleteNode(networkID, memberID, nodeID string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	CreateAccessor(region, accountID, accessorType, networkType string, tags map[string]string) (*Accessor, error)
	GetAccessor(accessorID string) (*Accessor, error)
	DeleteAccessor(accessorID string) error
	ListAccessors(filter ListAccessorsFilter) ([]*Accessor, error)
	CreateProposal(
		region, accountID, networkID, memberID, description string,
		actions *ProposalActions,
		tags map[string]string,
	) (*Proposal, error)
	GetProposal(networkID, proposalID string) (*Proposal, error)
	ListProposals(networkID string) ([]*Proposal, error)
	ListProposalVotes(networkID, proposalID string) ([]*ProposalVote, error)
	ListInvitations() ([]*Invitation, error)
	RejectInvitation(invitationID string) error
	UpdateMember(networkID, memberID string, logConfig *MemberLogPublishingConfigState) (*Member, error)
	UpdateNode(networkID, memberID, nodeID string, logConfig *NodeLogPublishingConfigState) (*Node, error)
	VoteOnProposal(networkID, proposalID, memberID, vote string) error
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	networks      map[string]*Network
	members       map[string]map[string]*Member          // networkID → memberID → Member
	nodes         map[string]map[string]map[string]*Node // networkID → memberID → nodeID → Node
	arnToResource map[string]any                         // ARN → *Network, *Member, *Node, or *Accessor
	accessors     map[string]*Accessor                   // accessorID → Accessor
	proposals     map[string]map[string]*Proposal        // networkID → proposalID → Proposal
	proposalVotes map[string][]*ProposalVote             // proposalID → votes
	invitations   map[string]*Invitation                 // invitationID → Invitation
	mu            *lockmetrics.RWMutex
}

// var _ assertion ensures InMemoryBackend implements StorageBackend at compile time.
var _ StorageBackend = (*InMemoryBackend)(nil)

// NewInMemoryBackend creates a new in-memory Managed Blockchain backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		networks:      make(map[string]*Network),
		members:       make(map[string]map[string]*Member),
		nodes:         make(map[string]map[string]map[string]*Node),
		arnToResource: make(map[string]any),
		accessors:     make(map[string]*Accessor),
		proposals:     make(map[string]map[string]*Proposal),
		proposalVotes: make(map[string][]*ProposalVote),
		invitations:   make(map[string]*Invitation),
		mu:            lockmetrics.New("managedblockchain"),
	}
}

// networkARN builds the ARN for a Managed Blockchain network.
func networkARN(region, accountID, networkID string) string {
	return arn.Build("managedblockchain", region, accountID, "networks/"+networkID)
}

// memberARN builds the ARN for a Managed Blockchain member.
func memberARN(region, accountID, memberID string) string {
	return arn.Build("managedblockchain", region, accountID, "members/"+memberID)
}

// nodeARN builds the ARN for a Managed Blockchain node.
func nodeARN(region, accountID, nodeID string) string {
	return arn.Build("managedblockchain", region, accountID, "nodes/"+nodeID)
}

// accessorARN builds the ARN for a Managed Blockchain accessor.
func accessorARN(region, accountID, accessorID string) string {
	return arn.Build("managedblockchain", region, accountID, "accessors/"+accessorID)
}

// proposalARN builds the ARN for a Managed Blockchain proposal.
func proposalARN(region, accountID, networkID, proposalID string) string {
	return arn.Build("managedblockchain", region, accountID,
		fmt.Sprintf("networks/%s/proposals/%s", networkID, proposalID))
}

// invitationARN builds the ARN for a Managed Blockchain invitation.
func invitationARN(region, accountID, invitationID string) string {
	return arn.Build("managedblockchain", region, accountID, "invitations/"+invitationID)
}

// CreateNetwork creates a new Managed Blockchain network and its first member.
func (b *InMemoryBackend) CreateNetwork(
	region, accountID, name, description, framework, frameworkVersion, memberName, memberDescription string,
	tags map[string]string,
	votingPolicy *VotingPolicy,
) (*Network, *Member, error) {
	b.mu.Lock("CreateNetwork")
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
		VotingPolicy:     cloneVotingPolicy(votingPolicy),
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
		IsOwned:      true,
	}

	b.members[networkID][memberID] = member
	b.arnToResource[member.Arn] = member

	return cloneNetwork(network), cloneMember(member), nil
}

// cloneVotingPolicy returns a deep copy of a VotingPolicy.
func cloneVotingPolicy(vp *VotingPolicy) *VotingPolicy {
	if vp == nil {
		return nil
	}

	cp := *vp

	if vp.ApprovalThresholdPolicy != nil {
		atp := *vp.ApprovalThresholdPolicy
		cp.ApprovalThresholdPolicy = &atp
	}

	return &cp
}

// cloneNetwork returns a deep copy of n with the Tags map cloned.
func cloneNetwork(n *Network) *Network {
	cp := *n
	cp.Tags = maps.Clone(n.Tags)
	cp.VotingPolicy = cloneVotingPolicy(n.VotingPolicy)

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
	b.mu.RLock("GetNetwork")
	defer b.mu.RUnlock()

	network, exists := b.networks[networkID]
	if !exists {
		return nil, ErrNetworkNotFound
	}

	return cloneNetwork(network), nil
}

// ListNetworks returns all networks, optionally filtered.
func (b *InMemoryBackend) ListNetworks(filter ListNetworksFilter) ([]*Network, error) {
	b.mu.RLock("ListNetworks")
	defer b.mu.RUnlock()

	all := make([]*Network, 0, len(b.networks))

	for _, n := range b.networks {
		if filter.Name != "" && n.Name != filter.Name {
			continue
		}

		if filter.Framework != "" && n.Framework != filter.Framework {
			continue
		}

		if filter.Status != "" && n.Status != filter.Status {
			continue
		}

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
	b.mu.Lock("CreateMember")
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
		IsOwned:      true,
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
	b.mu.RLock("GetMember")
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

// ListMembers returns all members in a network, optionally filtered.
func (b *InMemoryBackend) ListMembers(networkID string, filter ListMembersFilter) ([]*Member, error) {
	b.mu.RLock("ListMembers")
	defer b.mu.RUnlock()

	if _, exists := b.networks[networkID]; !exists {
		return nil, ErrNetworkNotFound
	}

	members := b.members[networkID]
	all := make([]*Member, 0, len(members))

	for _, m := range members {
		if filter.Name != "" && m.Name != filter.Name {
			continue
		}

		if filter.Status != "" && m.Status != filter.Status {
			continue
		}

		if filter.IsOwned != nil && m.IsOwned != *filter.IsOwned {
			continue
		}

		all = append(all, cloneMember(m))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name < all[j].Name
	})

	return all, nil
}

// DeleteMember removes a member from a network, cascading the delete to all of its nodes.
func (b *InMemoryBackend) DeleteMember(networkID, memberID string) error {
	b.mu.Lock("DeleteMember")
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

	// Cascade-delete all nodes that belong to this member.
	if b.nodes[networkID] != nil {
		for _, node := range b.nodes[networkID][memberID] {
			delete(b.arnToResource, node.Arn)
		}

		delete(b.nodes[networkID], memberID)
	}

	return nil
}

// ListTagsForResource returns tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
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
	case *Node:
		result := make(map[string]string, len(r.Tags))
		maps.Copy(result, r.Tags)

		return result, nil
	case *Accessor:
		result := make(map[string]string, len(r.Tags))
		maps.Copy(result, r.Tags)

		return result, nil
	}

	return nil, ErrResourceNotFound
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
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
	case *Node:
		if r.Tags == nil {
			r.Tags = make(map[string]string)
		}

		maps.Copy(r.Tags, tags)

		return nil
	case *Accessor:
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
	b.mu.Lock("UntagResource")
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
	case *Node:
		for _, k := range tagKeys {
			delete(r.Tags, k)
		}

		return nil
	case *Accessor:
		for _, k := range tagKeys {
			delete(r.Tags, k)
		}

		return nil
	}

	return ErrResourceNotFound
}

// Reset clears all in-memory state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.networks = make(map[string]*Network)
	b.members = make(map[string]map[string]*Member)
	b.nodes = make(map[string]map[string]map[string]*Node)
	b.arnToResource = make(map[string]any)
	b.accessors = make(map[string]*Accessor)
	b.proposals = make(map[string]map[string]*Proposal)
	b.proposalVotes = make(map[string][]*ProposalVote)
	b.invitations = make(map[string]*Invitation)
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
	b.mu.RLock("GetNode")
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

// ListNodes returns all nodes for a member sorted by ID, optionally filtered.
func (b *InMemoryBackend) ListNodes(networkID, memberID string, filter ListNodesFilter) ([]*Node, error) {
	b.mu.RLock("ListNodes")
	defer b.mu.RUnlock()

	if _, exists := b.networks[networkID]; !exists {
		return nil, ErrNetworkNotFound
	}

	if b.nodes[networkID] == nil || b.nodes[networkID][memberID] == nil {
		return []*Node{}, nil
	}

	all := make([]*Node, 0, len(b.nodes[networkID][memberID]))

	for _, n := range b.nodes[networkID][memberID] {
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

// cloneAccessor returns a deep copy of a with the Tags map cloned.
func cloneAccessor(a *Accessor) *Accessor {
	cp := *a
	cp.Tags = maps.Clone(a.Tags)

	return &cp
}

// CreateAccessor creates a new accessor for token-based access.
func (b *InMemoryBackend) CreateAccessor(
	region, accountID, accessorType, networkType string,
	tags map[string]string,
) (*Accessor, error) {
	b.mu.Lock("CreateAccessor")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	accessorID := uuid.NewString()
	billingToken := uuid.NewString()

	aType := accessorType
	if aType == "" {
		aType = accessorDefaultType
	}

	t := make(map[string]string)
	maps.Copy(t, tags)

	accessor := &Accessor{
		ID:           accessorID,
		Arn:          accessorARN(region, accountID, accessorID),
		BillingToken: billingToken,
		Type:         aType,
		NetworkType:  networkType,
		Status:       accessorStatusAvailable,
		CreationDate: &now,
		Tags:         t,
	}

	b.accessors[accessorID] = accessor
	b.arnToResource[accessor.Arn] = accessor

	return cloneAccessor(accessor), nil
}

// GetAccessor returns an accessor by ID.
func (b *InMemoryBackend) GetAccessor(accessorID string) (*Accessor, error) {
	b.mu.RLock("GetAccessor")
	defer b.mu.RUnlock()

	accessor, ok := b.accessors[accessorID]
	if !ok {
		return nil, ErrAccessorNotFound
	}

	return cloneAccessor(accessor), nil
}

// DeleteAccessor removes an accessor.
func (b *InMemoryBackend) DeleteAccessor(accessorID string) error {
	b.mu.Lock("DeleteAccessor")
	defer b.mu.Unlock()

	accessor, ok := b.accessors[accessorID]
	if !ok {
		return ErrAccessorNotFound
	}

	delete(b.arnToResource, accessor.Arn)
	delete(b.accessors, accessorID)

	return nil
}

// ListAccessors returns all accessors sorted by ID, optionally filtered.
func (b *InMemoryBackend) ListAccessors(filter ListAccessorsFilter) ([]*Accessor, error) {
	b.mu.RLock("ListAccessors")
	defer b.mu.RUnlock()

	all := make([]*Accessor, 0, len(b.accessors))

	for _, a := range b.accessors {
		if filter.NetworkType != "" && a.NetworkType != filter.NetworkType {
			continue
		}

		all = append(all, cloneAccessor(a))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].ID < all[j].ID
	})

	return all, nil
}

// cloneProposalActions returns a deep copy of ProposalActions.
func cloneProposalActions(a *ProposalActions) *ProposalActions {
	if a == nil {
		return nil
	}

	cp := &ProposalActions{}

	if len(a.Invitations) > 0 {
		cp.Invitations = make([]InviteAction, len(a.Invitations))
		copy(cp.Invitations, a.Invitations)
	}

	if len(a.Removals) > 0 {
		cp.Removals = make([]RemoveAction, len(a.Removals))
		copy(cp.Removals, a.Removals)
	}

	return cp
}

// cloneProposal returns a deep copy of p with the Tags map cloned.
func cloneProposal(p *Proposal) *Proposal {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)
	cp.Actions = cloneProposalActions(p.Actions)

	return &cp
}

// CreateProposal creates a new governance proposal on a network.
func (b *InMemoryBackend) CreateProposal(
	region, accountID, networkID, memberID, description string,
	actions *ProposalActions,
	tags map[string]string,
) (*Proposal, error) {
	b.mu.Lock("CreateProposal")
	defer b.mu.Unlock()

	if _, exists := b.networks[networkID]; !exists {
		return nil, ErrNetworkNotFound
	}

	members, ok := b.members[networkID]
	if !ok {
		return nil, ErrMemberNotFound
	}

	member, ok := members[memberID]
	if !ok {
		return nil, ErrMemberNotFound
	}

	now := time.Now().UTC()
	expiry := now.Add(proposalExpirationHours * time.Hour)
	proposalID := uuid.NewString()

	t := make(map[string]string)
	maps.Copy(t, tags)

	memberCount := int32(len(members)) //nolint:gosec
	outstandingVotes := memberCount

	proposal := &Proposal{
		ProposalID:           proposalID,
		Arn:                  proposalARN(region, accountID, networkID, proposalID),
		NetworkID:            networkID,
		ProposedByMemberID:   memberID,
		ProposedByMemberName: member.Name,
		Description:          description,
		Status:               proposalStatusInProgress,
		CreationDate:         &now,
		ExpirationDate:       &expiry,
		Tags:                 t,
		Actions:              cloneProposalActions(actions),
		OutstandingVoteCount: outstandingVotes,
	}

	if b.proposals[networkID] == nil {
		b.proposals[networkID] = make(map[string]*Proposal)
	}

	b.proposals[networkID][proposalID] = proposal
	b.proposalVotes[proposalID] = []*ProposalVote{}

	return cloneProposal(proposal), nil
}

// GetProposal returns a proposal by network ID and proposal ID.
func (b *InMemoryBackend) GetProposal(networkID, proposalID string) (*Proposal, error) {
	b.mu.RLock("GetProposal")
	defer b.mu.RUnlock()

	if _, exists := b.networks[networkID]; !exists {
		return nil, ErrNetworkNotFound
	}

	proposals, ok := b.proposals[networkID]
	if !ok {
		return nil, ErrProposalNotFound
	}

	proposal, ok := proposals[proposalID]
	if !ok {
		return nil, ErrProposalNotFound
	}

	return cloneProposal(proposal), nil
}

// ListProposals returns all proposals for a network sorted by proposal ID.
func (b *InMemoryBackend) ListProposals(networkID string) ([]*Proposal, error) {
	b.mu.RLock("ListProposals")
	defer b.mu.RUnlock()

	if _, exists := b.networks[networkID]; !exists {
		return nil, ErrNetworkNotFound
	}

	proposals := b.proposals[networkID]
	all := make([]*Proposal, 0, len(proposals))

	for _, p := range proposals {
		all = append(all, cloneProposal(p))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].ProposalID < all[j].ProposalID
	})

	return all, nil
}

// ListProposalVotes returns all votes for a proposal.
func (b *InMemoryBackend) ListProposalVotes(networkID, proposalID string) ([]*ProposalVote, error) {
	b.mu.RLock("ListProposalVotes")
	defer b.mu.RUnlock()

	if _, exists := b.networks[networkID]; !exists {
		return nil, ErrNetworkNotFound
	}

	proposals, found := b.proposals[networkID]
	if !found {
		return nil, ErrProposalNotFound
	}

	if _, found = proposals[proposalID]; !found {
		return nil, ErrProposalNotFound
	}

	votes := b.proposalVotes[proposalID]
	result := make([]*ProposalVote, len(votes))

	for i, v := range votes {
		cp := *v
		result[i] = &cp
	}

	return result, nil
}

// cloneInvitation returns a deep copy of an Invitation.
func cloneInvitation(inv *Invitation) *Invitation {
	cp := *inv

	if inv.NetworkSummary != nil {
		ns := *inv.NetworkSummary
		cp.NetworkSummary = &ns
	}

	return &cp
}

// ListInvitations returns all invitations sorted by invitation ID.
func (b *InMemoryBackend) ListInvitations() ([]*Invitation, error) {
	b.mu.RLock("ListInvitations")
	defer b.mu.RUnlock()

	all := make([]*Invitation, 0, len(b.invitations))

	for _, inv := range b.invitations {
		all = append(all, cloneInvitation(inv))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].InvitationID < all[j].InvitationID
	})

	return all, nil
}

// RejectInvitation rejects an invitation by setting its status to REJECTED.
func (b *InMemoryBackend) RejectInvitation(invitationID string) error {
	b.mu.Lock("RejectInvitation")
	defer b.mu.Unlock()

	inv, ok := b.invitations[invitationID]
	if !ok {
		return ErrInvitationNotFound
	}

	inv.Status = invitationStatusRejected

	return nil
}

// AddInvitationInternal adds an invitation directly to the backend (for testing and seeding).
func (b *InMemoryBackend) AddInvitationInternal(region, accountID, networkID, networkName string) *Invitation {
	b.mu.Lock("AddInvitationInternal")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	invitationID := uuid.NewString()

	var netSummary *InvitationNetworkSummary

	if n, ok := b.networks[networkID]; ok {
		netSummary = &InvitationNetworkSummary{
			ID:               n.ID,
			Arn:              n.Arn,
			Name:             n.Name,
			Description:      n.Description,
			Framework:        n.Framework,
			FrameworkVersion: n.FrameworkVersion,
			Status:           n.Status,
			CreationDate:     n.CreationDate,
		}
	} else {
		netSummary = &InvitationNetworkSummary{
			ID:   networkID,
			Name: networkName,
		}
	}

	inv := &Invitation{
		InvitationID:   invitationID,
		Arn:            invitationARN(region, accountID, invitationID),
		NetworkID:      networkID,
		NetworkName:    networkName,
		Status:         invitationStatusPending,
		CreationDate:   &now,
		NetworkSummary: netSummary,
	}

	b.invitations[invitationID] = inv

	return cloneInvitation(inv)
}

// AddNetworkInternal adds a network directly to the backend (for testing and seeding).
func (b *InMemoryBackend) AddNetworkInternal(region, accountID, name string) *Network {
	b.mu.Lock("AddNetworkInternal")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	networkID := uuid.NewString()

	network := &Network{
		ID:               networkID,
		Arn:              networkARN(region, accountID, networkID),
		Name:             name,
		Framework:        defaultFramework,
		FrameworkVersion: defaultFrameworkVersion,
		Status:           networkStatusAvailable,
		CreationDate:     &now,
		Tags:             make(map[string]string),
	}

	b.networks[networkID] = network
	b.members[networkID] = make(map[string]*Member)
	b.arnToResource[network.Arn] = network

	return cloneNetwork(network)
}

// AddMemberInternal adds a member directly to the backend (for testing and seeding).
// The network must already exist.
func (b *InMemoryBackend) AddMemberInternal(region, accountID, networkID, name string) *Member {
	b.mu.Lock("AddMemberInternal")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	memberID := uuid.NewString()

	member := &Member{
		ID:           memberID,
		Arn:          memberARN(region, accountID, memberID),
		Name:         name,
		NetworkID:    networkID,
		Status:       memberStatusAvailable,
		CreationDate: &now,
		Tags:         make(map[string]string),
		IsOwned:      true,
	}

	if b.members[networkID] == nil {
		b.members[networkID] = make(map[string]*Member)
	}

	b.members[networkID][memberID] = member
	b.arnToResource[member.Arn] = member

	return cloneMember(member)
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

	if b.nodes[networkID] == nil {
		b.nodes[networkID] = make(map[string]map[string]*Node)
	}

	if b.nodes[networkID][memberID] == nil {
		b.nodes[networkID][memberID] = make(map[string]*Node)
	}

	b.nodes[networkID][memberID][nodeID] = node
	b.arnToResource[node.Arn] = node

	return cloneNode(node)
}

// AddAccessorInternal adds an accessor directly to the backend (for testing and seeding).
func (b *InMemoryBackend) AddAccessorInternal(region, accountID, accessorType, networkType string) *Accessor {
	b.mu.Lock("AddAccessorInternal")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	accessorID := uuid.NewString()
	billingToken := uuid.NewString()

	accessor := &Accessor{
		ID:           accessorID,
		Arn:          accessorARN(region, accountID, accessorID),
		BillingToken: billingToken,
		Type:         accessorType,
		NetworkType:  networkType,
		Status:       accessorStatusAvailable,
		CreationDate: &now,
		Tags:         make(map[string]string),
	}

	b.accessors[accessorID] = accessor
	b.arnToResource[accessor.Arn] = accessor

	return cloneAccessor(accessor)
}

// AddProposalInternal adds a proposal directly to the backend (for testing and seeding).
// The network and member must already exist.
func (b *InMemoryBackend) AddProposalInternal(region, accountID, networkID, memberID, description string) *Proposal {
	b.mu.Lock("AddProposalInternal")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	expiry := now.Add(proposalExpirationHours * time.Hour)
	proposalID := uuid.NewString()

	var memberName string

	var memberCount int32

	if members, ok := b.members[networkID]; ok {
		memberCount = int32(len(members)) //nolint:gosec

		if m, exists := members[memberID]; exists {
			memberName = m.Name
		}
	}

	proposal := &Proposal{
		ProposalID:           proposalID,
		Arn:                  proposalARN(region, accountID, networkID, proposalID),
		NetworkID:            networkID,
		ProposedByMemberID:   memberID,
		ProposedByMemberName: memberName,
		Description:          description,
		Status:               proposalStatusInProgress,
		CreationDate:         &now,
		ExpirationDate:       &expiry,
		Tags:                 make(map[string]string),
		OutstandingVoteCount: memberCount,
	}

	if b.proposals[networkID] == nil {
		b.proposals[networkID] = make(map[string]*Proposal)
	}

	b.proposals[networkID][proposalID] = proposal
	b.proposalVotes[proposalID] = []*ProposalVote{}

	return cloneProposal(proposal)
}

// UpdateMember updates a member's log publishing configuration.
func (b *InMemoryBackend) UpdateMember(
	networkID, memberID string,
	logConfig *MemberLogPublishingConfigState,
) (*Member, error) {
	b.mu.Lock("UpdateMember")
	defer b.mu.Unlock()

	if _, exists := b.networks[networkID]; !exists {
		return nil, ErrNetworkNotFound
	}

	members, ok := b.members[networkID]
	if !ok || members[memberID] == nil {
		return nil, ErrMemberNotFound
	}

	m := members[memberID]

	if logConfig != nil {
		m.LogPublishingConfiguration = cloneMemberLogConfig(logConfig)
	}

	return cloneMember(m), nil
}

// cloneMemberLogConfig returns a deep copy of MemberLogPublishingConfigState.
func cloneMemberLogConfig(c *MemberLogPublishingConfigState) *MemberLogPublishingConfigState {
	if c == nil {
		return nil
	}

	cp := &MemberLogPublishingConfigState{}

	if c.Fabric != nil {
		fabric := &MemberFabricLogState{}

		if c.Fabric.CALogs != nil {
			caLogs := cloneLogConfig(c.Fabric.CALogs)
			fabric.CALogs = caLogs
		}

		cp.Fabric = fabric
	}

	return cp
}

// cloneLogConfig returns a deep copy of LogConfigState.
func cloneLogConfig(c *LogConfigState) *LogConfigState {
	if c == nil {
		return nil
	}

	cp := &LogConfigState{}

	if c.CloudWatch != nil {
		cw := *c.CloudWatch
		cp.CloudWatch = &cw
	}

	return cp
}

// UpdateNode updates a node's log publishing configuration.
func (b *InMemoryBackend) UpdateNode(
	networkID, memberID, nodeID string,
	logConfig *NodeLogPublishingConfigState,
) (*Node, error) {
	b.mu.Lock("UpdateNode")
	defer b.mu.Unlock()

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

// VoteOnProposal records a YES or NO vote on a proposal and transitions its status when threshold met.
func (b *InMemoryBackend) VoteOnProposal(networkID, proposalID, memberID, vote string) error {
	b.mu.Lock("VoteOnProposal")
	defer b.mu.Unlock()

	if _, exists := b.networks[networkID]; !exists {
		return ErrNetworkNotFound
	}

	proposals, found := b.proposals[networkID]
	if !found {
		return ErrProposalNotFound
	}

	proposal, found := proposals[proposalID]
	if !found {
		return ErrProposalNotFound
	}

	if proposal.Status != proposalStatusInProgress {
		return ErrValidation
	}

	if _, ok := b.members[networkID][memberID]; !ok {
		return ErrMemberNotFound
	}

	if vote != "YES" && vote != "NO" {
		return ErrValidation
	}

	// Check for duplicate vote.
	for _, v := range b.proposalVotes[proposalID] {
		if v.MemberID == memberID {
			return ErrValidation
		}
	}

	memberName := b.members[networkID][memberID].Name
	b.proposalVotes[proposalID] = append(b.proposalVotes[proposalID], &ProposalVote{
		MemberID:   memberID,
		MemberName: memberName,
		Vote:       vote,
	})

	if vote == "YES" {
		proposal.YesVoteCount++
	} else {
		proposal.NoVoteCount++
	}

	// Recalculate outstanding votes.
	totalMembers := int32(len(b.members[networkID])) //nolint:gosec
	proposal.OutstandingVoteCount = totalMembers - proposal.YesVoteCount - proposal.NoVoteCount

	// Apply threshold policy if network has one.
	network := b.networks[networkID]
	b.applyVoteThresholdLocked(network, proposal, totalMembers)

	return nil
}

// applyVoteThresholdLocked checks vote counts against the network's voting policy
// and transitions the proposal status when thresholds are met. Must be called with mu held.
func (b *InMemoryBackend) applyVoteThresholdLocked(network *Network, proposal *Proposal, totalMembers int32) {
	if network.VotingPolicy == nil || network.VotingPolicy.ApprovalThresholdPolicy == nil {
		return
	}

	atp := network.VotingPolicy.ApprovalThresholdPolicy
	threshold := atp.ThresholdPercentage
	comparator := atp.ThresholdComparator

	if totalMembers == 0 || threshold == 0 {
		return
	}

	yesPercent := (proposal.YesVoteCount * 100) / totalMembers
	noPercent := (proposal.NoVoteCount * 100) / totalMembers

	var yesApproved bool

	switch comparator {
	case "GREATER_THAN":
		yesApproved = yesPercent > threshold
	case "GREATER_THAN_OR_EQUAL_TO":
		yesApproved = yesPercent >= threshold
	default:
		yesApproved = yesPercent > threshold
	}

	var noRejected bool

	rejectionThreshold := int32(100) - threshold

	switch comparator {
	case "GREATER_THAN":
		noRejected = noPercent > rejectionThreshold
	case "GREATER_THAN_OR_EQUAL_TO":
		noRejected = noPercent >= rejectionThreshold
	default:
		noRejected = noPercent > rejectionThreshold
	}

	if yesApproved {
		proposal.Status = proposalStatusApproved
	} else if noRejected {
		proposal.Status = proposalStatusRejected
	}
}
