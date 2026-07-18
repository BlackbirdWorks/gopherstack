package managedblockchain

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
	ListProposals(networkID, statusFilter string) ([]*Proposal, error)
	ListProposalVotes(networkID, proposalID string) ([]*ProposalVote, error)
	ListInvitations() ([]*Invitation, error)
	RejectInvitation(invitationID string) error
	UpdateMember(networkID, memberID string, logConfig *MemberLogPublishingConfigState) (*Member, error)
	UpdateNode(networkID, memberID, nodeID string, logConfig *NodeLogPublishingConfigState) (*Node, error)
	VoteOnProposal(networkID, proposalID, memberID, vote string) error
}
