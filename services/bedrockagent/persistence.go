package bedrockagent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// bedrockagentSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a registered table's value type or
// backendSnapshot itself would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll plus resetting every raw map/counter, not a
// partial decode) any mismatch -- see Restore below. This is the first
// version: BedrockAgent had no persistence at all before Phase 3.3 (neither
// Handler nor InMemoryBackend implemented Snapshot/Restore -- see the
// Snapshot/Restore doc comment on StorageBackend in interfaces.go, and the
// Handler.Snapshot/Restore delegation at the bottom of this file), so there
// is no legacy snapshot shape to be compatible with -- any snapshot without
// a matching Version (including one with no version field, which decodes as
// 0) is discarded the same way any other incompatible snapshot is.
//
// Bumped 1 -> 2 for the parity-4 PutResourcePolicy/GetResourcePolicy/
// DeleteResourcePolicy addition: a new registered table ("resourcePolicies",
// value type ResourcePolicy) and a new raw counter field
// (ResourcePolicyCounter) were added to backendSnapshot, neither of which an
// old Version-1 snapshot has data for -- decoding one as Version 2 would
// leave the resource-policy revision counter silently at zero instead of
// reflecting reality, so it must be discarded like any other
// shape-incompatible snapshot rather than partially decoded.
//
// Bumped 2 -> 3 (gopherstack-hjdd) for 732c2bafa: AgentCollaborator.UpdatedAt
// (a registered table's value type) was retagged from the wrong key
// "updatedAt" to the real deserializer's "lastUpdatedAt". A Version-2
// snapshot's "updatedAt" data is unrecognized by the new tag and would
// silently decode as the zero time instead of the real value.
const bedrockagentSnapshotVersion = 3

// backendSnapshot is the top-level on-disk shape for the BedrockAgent
// backend.
//
// Tables holds one JSON-encoded array per registered table name, produced
// directly by b.registry.SnapshotAll(): every converted resource collection
// derives its store.Table key entirely from real, already-wire-visible
// fields on the value type (see store_setup.go), so none of them need a DTO
// wrapper.
//
// AgentsByName, KBsByName, FlowsByName, and PromptsByName are the
// map[string]string reverse-lookup caches left un-converted (see
// backend.go's InMemoryBackend field doc comment); AgentVersionCtrs,
// FlowVersionCtrs, and PromptVersionCtrs are the map[string]int per-parent
// version counters; Tags is the one remaining grouping map with a
// non-*T value. The scalar *Counter fields (AgentCounter,
// ActionGroupCounter, ...) feed InMemoryBackend.nextID and must round-trip
// too, or a restored backend would start minting IDs that collide with
// pre-restore ones.
type backendSnapshot struct {
	Tables                map[string]json.RawMessage   `json:"tables"`
	AgentsByName          map[string]string            `json:"agentsByName"`
	KBsByName             map[string]string            `json:"kbsByName"`
	FlowsByName           map[string]string            `json:"flowsByName"`
	PromptsByName         map[string]string            `json:"promptsByName"`
	AgentVersionCtrs      map[string]int               `json:"agentVersionCtrs"`
	FlowVersionCtrs       map[string]int               `json:"flowVersionCtrs"`
	PromptVersionCtrs     map[string]int               `json:"promptVersionCtrs"`
	Tags                  map[string]map[string]string `json:"tags"`
	AccountID             string                       `json:"accountID"`
	DefaultRegion         string                       `json:"defaultRegion"`
	DSCounter             int                          `json:"dsCounter"`
	CollabCounter         int                          `json:"collabCounter"`
	KBCounter             int                          `json:"kbCounter"`
	FlowCounter           int                          `json:"flowCounter"`
	AliasCounter          int                          `json:"aliasCounter"`
	AgentCounter          int                          `json:"agentCounter"`
	ActionGroupCounter    int                          `json:"actionGroupCounter"`
	FlowAliasCounter      int                          `json:"flowAliasCounter"`
	PromptCounter         int                          `json:"promptCounter"`
	JobCounter            int                          `json:"jobCounter"`
	ResourcePolicyCounter int                          `json:"resourcePolicyCounter"`
	Version               int                          `json:"version"`
}

// Snapshot serializes the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "bedrockagent: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:               bedrockagentSnapshotVersion,
		Tables:                tables,
		AgentsByName:          b.agentsByName,
		KBsByName:             b.kbsByName,
		FlowsByName:           b.flowsByName,
		PromptsByName:         b.promptsByName,
		AgentVersionCtrs:      b.agentVersionCtrs,
		FlowVersionCtrs:       b.flowVersionCtrs,
		PromptVersionCtrs:     b.promptVersionCtrs,
		Tags:                  b.tags,
		AccountID:             b.accountID,
		DefaultRegion:         b.defaultRegion,
		DSCounter:             b.dsCounter,
		CollabCounter:         b.collabCounter,
		KBCounter:             b.kbCounter,
		FlowCounter:           b.flowCounter,
		AliasCounter:          b.aliasCounter,
		AgentCounter:          b.agentCounter,
		ActionGroupCounter:    b.actionGroupCounter,
		FlowAliasCounter:      b.flowAliasCounter,
		PromptCounter:         b.promptCounter,
		JobCounter:            b.jobCounter,
		ResourcePolicyCounter: b.resourcePolicyCounter,
	}

	return persistence.MarshalSnapshot(ctx, "bedrockagent", snap)
}

// Restore deserializes backend state from a snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "bedrockagent", data, &snap); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if snap.Version != bedrockagentSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"bedrockagent: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", bedrockagentSnapshotVersion)

		b.registry.ResetAll()
		b.resetRawState()
		b.accountID = snap.AccountID
		b.defaultRegion = snap.DefaultRegion

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("bedrockagent: restore snapshot tables: %w", err)
	}

	b.restoreRawState(&snap)

	b.accountID = snap.AccountID
	b.defaultRegion = snap.DefaultRegion

	return nil
}

// resetRawState resets every raw (non-store.Table) map and ID counter to its
// zero state. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) resetRawState() {
	b.agentsByName = make(map[string]string)
	b.kbsByName = make(map[string]string)
	b.flowsByName = make(map[string]string)
	b.promptsByName = make(map[string]string)
	b.agentVersionCtrs = make(map[string]int)
	b.flowVersionCtrs = make(map[string]int)
	b.promptVersionCtrs = make(map[string]int)
	b.tags = make(map[string]map[string]string)
	b.dsCounter = 0
	b.collabCounter = 0
	b.kbCounter = 0
	b.flowCounter = 0
	b.aliasCounter = 0
	b.agentCounter = 0
	b.actionGroupCounter = 0
	b.flowAliasCounter = 0
	b.promptCounter = 0
	b.jobCounter = 0
	b.resourcePolicyCounter = 0
}

// restoreRawState restores every raw (non-store.Table) map and ID counter
// from snap, defaulting each map to empty when absent from the snapshot
// (e.g. an older snapshot predating a field, though
// bedrockagentSnapshotVersion's own guard makes that unreachable today).
// Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreRawState(snap *backendSnapshot) {
	b.agentsByName = snap.AgentsByName
	if b.agentsByName == nil {
		b.agentsByName = make(map[string]string)
	}

	b.kbsByName = snap.KBsByName
	if b.kbsByName == nil {
		b.kbsByName = make(map[string]string)
	}

	b.flowsByName = snap.FlowsByName
	if b.flowsByName == nil {
		b.flowsByName = make(map[string]string)
	}

	b.promptsByName = snap.PromptsByName
	if b.promptsByName == nil {
		b.promptsByName = make(map[string]string)
	}

	b.agentVersionCtrs = snap.AgentVersionCtrs
	if b.agentVersionCtrs == nil {
		b.agentVersionCtrs = make(map[string]int)
	}

	b.flowVersionCtrs = snap.FlowVersionCtrs
	if b.flowVersionCtrs == nil {
		b.flowVersionCtrs = make(map[string]int)
	}

	b.promptVersionCtrs = snap.PromptVersionCtrs
	if b.promptVersionCtrs == nil {
		b.promptVersionCtrs = make(map[string]int)
	}

	b.tags = snap.Tags
	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}

	b.dsCounter = snap.DSCounter
	b.collabCounter = snap.CollabCounter
	b.kbCounter = snap.KBCounter
	b.flowCounter = snap.FlowCounter
	b.aliasCounter = snap.AliasCounter
	b.agentCounter = snap.AgentCounter
	b.actionGroupCounter = snap.ActionGroupCounter
	b.flowAliasCounter = snap.FlowAliasCounter
	b.promptCounter = snap.PromptCounter
	b.jobCounter = snap.JobCounter
	b.resourcePolicyCounter = snap.ResourcePolicyCounter
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Handler previously had no Snapshot/Restore of its own -- and neither did
// InMemoryBackend -- so cli.go's generic setupPersistence (which
// type-asserts the registered service.Registerable, i.e. the Handler, for a
// Snapshot/Restore pair) never picked BedrockAgent up at all: dead wiring,
// with no persistence underneath it either. This delegation (matching the
// codecommit/codepipeline/cleanrooms pattern) is what wires BedrockAgent
// into persistence for the first time.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
