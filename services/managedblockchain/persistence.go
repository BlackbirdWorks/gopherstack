package managedblockchain

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// managedblockchainSnapshotVersion identifies the shape of [backendSnapshot].
// It must be bumped whenever a change to a DTO type, a registered table's
// value type, or backendSnapshot itself would make an older snapshot unsafe
// to decode as the current shape. Restore discards (resetLocked, not a
// partial decode) any snapshot whose Version does not match -- including
// one with no version field at all, which decodes as 0 -- rather than risk
// misinterpreting fields. Version 1 was the first version: the
// pre-Phase-3.3 backend had no version guard at all, so any legacy
// snapshot was treated the same as any other incompatible one. Bumped to 2
// when Network/Member/Node each gained new registered-table value fields
// (FrameworkAttributes, KmsKeyArn, StateDB, VpcEndpointServiceName) --
// technically additive/omitempty and backward-compatible to decode, but
// bumped anyway per this constant's own "any value-type change" policy,
// rather than special-casing this particular change as safe.
const managedblockchainSnapshotVersion = 2

// proposalVoteDTO wraps a ProposalVote for JSON round-tripping through the
// ephemeral persistence registry below. ProposalVote hides its proposalID
// behind an unexported field (see models.go), so a plain json.Marshal of
// the table's contents would silently drop it -- the same technique
// services/workmail's permissionDTO uses for its own hidden composite-key
// component.
type proposalVoteDTO struct {
	Value      *ProposalVote `json:"value"`
	ProposalID string        `json:"proposalID"`
}

func proposalVoteDTOKeyFn(d *proposalVoteDTO) string {
	return proposalVoteKey(d.ProposalID, d.Value.MemberID)
}

// buildProposalVoteDTORegistry constructs the ephemeral one-table DTO
// registry used by both Snapshot and Restore for proposalVotes, the only
// table NOT on b.registry (see store_setup.go). It is built fresh on every
// call, mirroring services/workmail's buildPersistenceDTORegistry.
func buildProposalVoteDTORegistry() (*store.Registry, *store.Table[proposalVoteDTO]) {
	dtoReg := store.NewRegistry()
	tbl := store.Register(dtoReg, "proposalVotes", store.New(proposalVoteDTOKeyFn))

	return dtoReg, tbl
}

// backendSnapshot is the top-level on-disk shape for the Managed Blockchain
// backend. Tables holds one JSON-encoded array per table name: "networks",
// "members", "nodes", "accessors", "proposals", and "invitations" produced
// directly by b.registry.SnapshotAll() (none of those hide any field from
// JSON -- Member/Node/Proposal's composite keys are derived entirely from
// already-exported, already-JSON-tagged fields), plus "proposalVotes" from
// the DTO registry above. arnToResource is deliberately NOT persisted: it
// is a derived reverse-lookup cache rebuilt from the tables above by
// rebuildARNIndexLocked on Restore, exactly as it was before this refactor.
type backendSnapshot struct {
	Tables  map[string]json.RawMessage `json:"tables"`
	Version int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "managedblockchain: snapshot table marshal failed", "error", err)

		return nil
	}

	dtoReg, dtoTable := buildProposalVoteDTORegistry()

	for _, v := range b.proposalVotes.Snapshot() {
		dtoTable.Put(&proposalVoteDTO{Value: v, ProposalID: v.proposalID})
	}

	dtoTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "managedblockchain: snapshot DTO table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dtoTables)

	snap := backendSnapshot{
		Version: managedblockchainSnapshotVersion,
		Tables:  tables,
	}

	return persistence.MarshalSnapshot(ctx, "managedblockchain", snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "managedblockchain", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != managedblockchainSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"managedblockchain: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", managedblockchainSnapshotVersion)

		b.resetLocked()

		return nil
	}

	if err := b.restoreTablesLocked(snap.Tables); err != nil {
		return err
	}

	b.rebuildARNIndexLocked()

	return nil
}

// restoreTablesLocked rebuilds every store.Table on b from tables' per-table
// JSON, factored out of Restore to keep Restore's own cognitive complexity
// low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreTablesLocked(tables map[string]json.RawMessage) error {
	if err := b.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("managedblockchain: restore tables: %w", err)
	}

	dtoReg, dtoTable := buildProposalVoteDTORegistry()
	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("managedblockchain: restore proposalVotes: %w", err)
	}

	items := make([]*ProposalVote, 0, dtoTable.Len())

	for _, d := range dtoTable.All() {
		if d.Value == nil {
			continue
		}

		d.Value.proposalID = d.ProposalID
		items = append(items, d.Value)
	}

	b.proposalVotes.Restore(items)

	return nil
}

// rebuildARNIndexLocked rebuilds arnToResource from the current state. Must be called with mu held.
func (b *InMemoryBackend) rebuildARNIndexLocked() {
	b.arnToResource = make(map[string]any)

	for _, n := range b.networks.All() {
		b.arnToResource[n.Arn] = n
	}

	for _, m := range b.members.All() {
		b.arnToResource[m.Arn] = m
	}

	for _, node := range b.nodes.All() {
		b.arnToResource[node.Arn] = node
	}

	for _, a := range b.accessors.All() {
		b.arnToResource[a.Arn] = a
	}

	for _, p := range b.proposals.All() {
		b.arnToResource[p.Arn] = p
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
