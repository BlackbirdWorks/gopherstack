package managedblockchain

import (
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend is the in-memory implementation of StorageBackend.
//
// networks, members, nodes, accessors, proposals, proposalVotes, and
// invitations are all *store.Table (Phase 3.3 of the datalayer refactor);
// see store_setup.go for their key functions, indexes, and registration.
// arnToResource remains a plain map -- see store_setup.go's doc comment for
// why it cannot become a store.Table.
type InMemoryBackend struct {
	registry                *store.Registry
	networks                *store.Table[Network]
	members                 *store.Table[Member]
	membersByNetwork        *store.Index[Member]
	nodes                   *store.Table[Node]
	nodesByMember           *store.Index[Node]
	arnToResource           map[string]any // ARN → *Network, *Member, *Node, *Accessor, or *Proposal
	accessors               *store.Table[Accessor]
	proposals               *store.Table[Proposal]
	proposalsByNetwork      *store.Index[Proposal]
	proposalVotes           *store.Table[ProposalVote]
	proposalVotesByProposal *store.Index[ProposalVote]
	invitations             *store.Table[Invitation]
	mu                      *lockmetrics.RWMutex
}

// var _ assertion ensures InMemoryBackend implements StorageBackend at compile time.
var _ StorageBackend = (*InMemoryBackend)(nil)

// NewInMemoryBackend creates a new in-memory Managed Blockchain backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		arnToResource: make(map[string]any),
		registry:      store.NewRegistry(),
		mu:            lockmetrics.New("managedblockchain"),
	}

	registerAllTables(b)

	return b
}

// Reset clears all in-memory state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetLocked()
}

// resetLocked clears every registered table, the un-registered
// proposalVotes table, and the ARN index, returning the backend to the
// state NewInMemoryBackend leaves it in. Must be called with mu held.
func (b *InMemoryBackend) resetLocked() {
	b.registry.ResetAll()
	b.proposalVotes.Reset()
	b.arnToResource = make(map[string]any)
}
