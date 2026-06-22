package managedblockchain

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Networks      map[string]*Network                    `json:"networks"`
	Members       map[string]map[string]*Member          `json:"members"`
	Nodes         map[string]map[string]map[string]*Node `json:"nodes"`
	Accessors     map[string]*Accessor                   `json:"accessors"`
	Proposals     map[string]map[string]*Proposal        `json:"proposals"`
	ProposalVotes map[string][]*ProposalVote             `json:"proposalVotes"`
	Invitations   map[string]*Invitation                 `json:"invitations"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Networks:      b.networks,
		Members:       b.members,
		Nodes:         b.nodes,
		Accessors:     b.accessors,
		Proposals:     b.proposals,
		ProposalVotes: b.proposalVotes,
		Invitations:   b.invitations,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "managedblockchain: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "managedblockchain", data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.networks = snap.Networks
	b.members = snap.Members
	b.nodes = snap.Nodes
	b.accessors = snap.Accessors
	b.proposals = snap.Proposals
	b.proposalVotes = snap.ProposalVotes
	b.invitations = snap.Invitations

	b.rebuildARNIndexLocked()

	return nil
}

// ensureNonNilMaps initialises any nil maps in the snapshot to empty maps.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Networks == nil {
		snap.Networks = make(map[string]*Network)
	}

	if snap.Members == nil {
		snap.Members = make(map[string]map[string]*Member)
	}

	if snap.Nodes == nil {
		snap.Nodes = make(map[string]map[string]map[string]*Node)
	}

	if snap.Accessors == nil {
		snap.Accessors = make(map[string]*Accessor)
	}

	if snap.Proposals == nil {
		snap.Proposals = make(map[string]map[string]*Proposal)
	}

	if snap.ProposalVotes == nil {
		snap.ProposalVotes = make(map[string][]*ProposalVote)
	}

	if snap.Invitations == nil {
		snap.Invitations = make(map[string]*Invitation)
	}
}

// rebuildARNIndexLocked rebuilds arnToResource from the current state. Must be called with mu held.
func (b *InMemoryBackend) rebuildARNIndexLocked() {
	b.arnToResource = make(map[string]any)

	for _, n := range b.networks {
		b.arnToResource[n.Arn] = n
	}

	for _, memberMap := range b.members {
		for _, m := range memberMap {
			b.arnToResource[m.Arn] = m
		}
	}

	for _, memberMap := range b.nodes {
		for _, nodeMap := range memberMap {
			for _, node := range nodeMap {
				b.arnToResource[node.Arn] = node
			}
		}
	}

	for _, a := range b.accessors {
		b.arnToResource[a.Arn] = a
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		return mem.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		return mem.Restore(ctx, data)
	}

	return nil
}

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		mem.Reset()
	}
}
