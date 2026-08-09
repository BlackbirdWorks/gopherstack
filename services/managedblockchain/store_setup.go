package managedblockchain

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// resource collection previously held as a hand-rolled map on
// InMemoryBackend (see backend.go) is registered exactly once, here, as a
// *store.Table[T]. See pkgs/store's package doc for the underlying
// primitive, and services/sesv2/services/codebuild/services/macie2 for the
// clean-direct-register template, and services/emr/services/workmail for the
// parent-nested composite-key + Index template this file mostly follows.
//
// networks and accessors were already flat map[string]*T collections keyed
// by their own real identity field (Network.ID, Accessor.ID), so both
// register directly with no composite key and no secondary index.
//
// members, nodes, and proposals were previously nested by parent
// (map[networkID]map[memberID]*Member, the doubly-nested
// map[networkID]map[memberID]map[nodeID]*Node, and
// map[networkID]map[proposalID]*Proposal). Each becomes a flat
// *store.Table[T] keyed by a composite "parent|id" string, with a companion
// *store.Index grouping entries by their immediate parent for the per-parent
// scans (ListMembers, ListNodes, ListProposals) the nested maps used to
// answer directly. Unlike services/workmail's org-nested types, none of
// Member/Node/Proposal needed a new hidden field for this: NetworkID (and
// Node's MemberID) were already real, exported, JSON-tagged fields on each
// struct, so the composite key is derived entirely from data already
// present and already round-tripping through a plain json.Marshal -- no DTO
// wrapper is needed and all three register directly on b.registry.
//
// invitations was already a flat map[string]*Invitation keyed by its own
// real identity field (Invitation.InvitationID), so it also registers
// directly with no composite key.
//
// proposalVotes was previously map[proposalID][]*ProposalVote: a slice, not
// a map, since a ProposalVote has no identity field of its own other than
// the (proposalID, memberID) pair a member's single vote is unique under.
// It becomes a *store.Table[ProposalVote] keyed by the composite
// "proposalID|memberID" string, with a "byProposal" *store.Index grouping
// entries by proposalID (in append/insertion order, matching the order the
// original slice preserved) for ListProposalVotes' per-proposal scan. Unlike
// the three tables above, ProposalVote's proposalID component has no
// existing exported field to derive from -- it is a new unexported field
// (see models.go) that does NOT round-trip through a plain json.Marshal, so
// this table is deliberately built with store.New only, NOT store.Register:
// persistence.go persists it through a separate ephemeral proposalVoteDTO
// registry instead (the same technique services/workmail's permissions
// table uses for its own two-component hidden key).
//
// arnToResource (map[string]any, an ARN → *Network/*Member/*Node/*Accessor/*Proposal
// reverse-lookup cache) is deliberately NOT converted: store.Table requires
// every value to share one concrete type V with its own identity, but this
// map's values are a type union with no common identity field. It remains a
// plain map, unchanged by this refactor, and is not persisted -- it is
// rebuilt from the tables above by rebuildARNIndexLocked on every Restore
// (see persistence.go), exactly as it was before.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func networkKeyFn(v *Network) string { return v.ID }

// memberKey derives the composite primary key for the members table from a
// member's parent network ID and its own ID.
func memberKey(networkID, memberID string) string { return networkID + "|" + memberID }

func memberKeyFn(v *Member) string             { return memberKey(v.NetworkID, v.ID) }
func memberNetworkIndexKeyFn(v *Member) string { return v.NetworkID }

// nodeKey derives the composite primary key for the nodes table from a
// node's parent network ID, parent member ID, and its own ID.
func nodeKey(networkID, memberID, nodeID string) string {
	return networkID + "|" + memberID + "|" + nodeID
}

func nodeKeyFn(v *Node) string { return nodeKey(v.NetworkID, v.MemberID, v.ID) }

// nodeMemberKey derives the "byMember" index key nodes are grouped under,
// matching the (networkID, memberID) pair every hot-path lookup
// (ListNodes/DeleteMember's cascade/proposal-removal cascade) already knows.
func nodeMemberKey(networkID, memberID string) string { return networkID + "|" + memberID }

func nodeMemberIndexKeyFn(v *Node) string { return nodeMemberKey(v.NetworkID, v.MemberID) }

func accessorKeyFn(v *Accessor) string { return v.ID }

// proposalKey derives the composite primary key for the proposals table
// from a proposal's parent network ID and its own ProposalID.
func proposalKey(networkID, proposalID string) string { return networkID + "|" + proposalID }

func proposalKeyFn(v *Proposal) string             { return proposalKey(v.NetworkID, v.ProposalID) }
func proposalNetworkIndexKeyFn(v *Proposal) string { return v.NetworkID }

// proposalVoteKey derives the composite primary key for the proposalVotes
// table from the parent proposal's ID and the voting member's ID -- the
// same pair VoteOnProposal's duplicate-vote check already enforces
// uniqueness under.
func proposalVoteKey(proposalID, memberID string) string { return proposalID + "|" + memberID }

func proposalVoteKeyFn(v *ProposalVote) string { return proposalVoteKey(v.proposalID, v.MemberID) }

func proposalVoteProposalIndexKeyFn(v *ProposalVote) string { return v.proposalID }

func invitationKeyFn(v *Invitation) string { return v.InvitationID }

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created -- see NewInMemoryBackend), never
// on every Reset() -- store.Register panics on a duplicate name, so runtime
// resets go through b.resetLocked (b.registry.ResetAll() plus
// b.proposalVotes.Reset()) instead.
func registerAllTables(b *InMemoryBackend) {
	b.networks = store.Register(b.registry, "networks", store.New(networkKeyFn))

	b.members = store.Register(b.registry, "members", store.New(memberKeyFn))
	b.membersByNetwork = b.members.AddIndex("byNetwork", memberNetworkIndexKeyFn)

	b.nodes = store.Register(b.registry, "nodes", store.New(nodeKeyFn))
	b.nodesByMember = b.nodes.AddIndex("byMember", nodeMemberIndexKeyFn)

	b.accessors = store.Register(b.registry, "accessors", store.New(accessorKeyFn))

	b.proposals = store.Register(b.registry, "proposals", store.New(proposalKeyFn))
	b.proposalsByNetwork = b.proposals.AddIndex("byNetwork", proposalNetworkIndexKeyFn)

	// proposalVotes is deliberately built with store.New only, NOT
	// store.Register -- see this file's doc comment.
	b.proposalVotes = store.New(proposalVoteKeyFn)
	b.proposalVotesByProposal = b.proposalVotes.AddIndex("byProposal", proposalVoteProposalIndexKeyFn)

	b.invitations = store.Register(b.registry, "invitations", store.New(invitationKeyFn))
}
