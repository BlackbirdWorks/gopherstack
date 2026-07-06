package eventbridge

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// eventbridgeSnapshotVersion identifies the shape of backendSnapshot's Tables
// blob (i.e. the set of resource tables registered on b.registry -- see
// store_setup.go). It must be bumped whenever a change there would make an
// older snapshot unsafe to decode as the current shape. Restore compares this
// against the persisted value and discards (rather than attempts to
// partially decode) any mismatch -- see Restore below. Pre-Phase-3.3
// snapshots have no "version" field at all, which unmarshals as 0 -- also a
// mismatch, so they are discarded the same way. This mirrors the services/ssm
// conversion and the services/sqs pilot (commit 0f09d77c) / services/ec2
// conversion (commit 12e611a4).
const eventbridgeSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the EventBridge backend.
//
// Tables holds one JSON-encoded, key-sorted array per registered
// *store.Table[V] on b.registry, produced by
// [github.com/blackbirdworks/gopherstack/pkgs/store.Registry.SnapshotAll].
// Each per-region table is registered under "<resourceName>/<region>" (bus,
// event source, replay, API destination, archive, connection, endpoint,
// partner source); rules and targets are registered one level deeper still,
// under "<resourceName>/<region>/<parent>" -- see store_setup.go's
// getOrCreateTable/getOrCreateNestedTable. Restore must pre-register every
// (resource, region[, parent]) tuple found in Tables before calling
// registry.RestoreAll, see preRegisterSnapshotTables below.
//
// pipes, registries, and schemas are *store.Table-backed too but live on
// b.auxRegistry, not b.registry, and are deliberately NOT included in
// Tables -- see store_setup.go's package doc: they were never part of
// backendSnapshot before this conversion, so leaving them out preserves that
// (surprising, pre-existing) behavior byte-for-byte rather than silently
// starting to persist state that never round-tripped through Restore before.
//
// archivedEvents, busePolicies, schemaVersions, and codeBindings are, for the
// same reason, left out of backendSnapshot exactly as they were before this
// conversion -- see store_setup.go's package doc for why each doesn't fit
// Table's func(*V) string keying requirement in the first place.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage `json:"tables"`
	AccountID string                     `json:"accountID"`
	Region    string                     `json:"region"`
	EventLog  []EventLogEntry            `json:"eventLog"`
	Version   int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx, "eventbridge: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   eventbridgeSnapshotVersion,
		Tables:    tables,
		EventLog:  b.eventLog,
		AccountID: b.accountID,
		Region:    b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).
			WarnContext(ctx, "persistence: snapshot marshal failed", "service", "eventbridge", "error", err)

		return nil
	}

	return data
}

// preRegisterSnapshotTables ensures every (resource, region[, parent]) tuple
// present in tables is registered on b.registry before registry.RestoreAll is
// called. RestoreAll only restores tables already registered (see
// [github.com/blackbirdworks/gopherstack/pkgs/store.Registry.RestoreAll]);
// a region/bus a fresh backend has never touched would otherwise not yet have
// a table registered under that name, and its persisted data would be
// silently dropped instead of restored. Each table name has the shape
// "<resourceName>/<region>" or "<resourceName>/<region>/<parent>" (see
// store_setup.go's getOrCreateTable/getOrCreateNestedTable); splitting on the
// first "/" recovers the prefix, and tableAccessorsByPrefix's rules/targets
// entries split the remainder again themselves (region never contains "/",
// but bus/rule names may).
func (b *InMemoryBackend) preRegisterSnapshotTables(tables map[string]json.RawMessage) {
	for key := range tables {
		name, rest, found := strings.Cut(key, "/")
		if !found {
			continue
		}

		if register, ok := tableAccessorsByPrefix[name]; ok {
			register(b, rest)
		}
	}
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// The logger and delivery targets are not restored — they are re-wired by the CLI.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "eventbridge", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != eventbridgeSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption. Mirrors the services/ssm conversion and the
		// services/sqs pilot (commit 0f09d77c) / services/ec2 conversion
		// (commit 12e611a4).
		logger.Load(ctx).WarnContext(ctx,
			"eventbridge: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", eventbridgeSnapshotVersion)

		b.registry.ResetAll()
		b.ruleIndex = make(map[string]map[string]map[ruleIndexKey]map[string]*Rule)
		b.targetsByARN = make(map[string]map[string]map[string]struct{})
		b.patternCache = sync.Map{}

		// Match NewInMemoryBackendWithContext's construction-time state: a
		// fresh backend is never truly empty of buses, so "starting empty"
		// here means starting exactly as fresh as a new backend would.
		b.busesTable(b.region).Put(&EventBus{
			Name:        defaultEventBusName,
			Arn:         b.busARN(b.region, defaultEventBusName),
			CreatedTime: time.Now(),
		})

		return nil
	}

	if snap.Tables == nil {
		snap.Tables = make(map[string]json.RawMessage)
	}

	b.preRegisterSnapshotTables(snap.Tables)

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("eventbridge: restore snapshot tables: %w", err)
	}

	b.eventLog = snap.EventLog
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.ruleIndex = make(map[string]map[string]map[ruleIndexKey]map[string]*Rule)
	b.targetsByARN = make(map[string]map[string]map[string]struct{})
	b.patternCache = sync.Map{}

	if err := b.rebuildRuleIndexesLocked(); err != nil {
		return err
	}

	b.rebuildTargetsByARNLocked()

	return nil
}

func (b *InMemoryBackend) rebuildRuleIndexesLocked() error {
	for region, regRules := range b.rules {
		for busKey, busRules := range regRules {
			for _, rule := range busRules.All() {
				if rule.EventPattern != "" {
					compiled, err := b.getOrCompilePattern(rule.EventPattern)
					if err != nil {
						return err
					}
					rule.compiledPattern = compiled
				}
				b.addRuleToIndex(region, busKey, rule)
			}
		}
	}

	return nil
}

// rebuildTargetsByARNLocked rebuilds the targetsByARN index from b.targets.
// Must be called with the write lock held.
func (b *InMemoryBackend) rebuildTargetsByARNLocked() {
	for region, regionTargets := range b.targets {
		for targetKey, tTable := range regionTargets {
			for _, t := range tTable.All() {
				b.arnIndexAdd(region, t.Arn, targetKey)
			}
		}
	}
}

// handlerSnapshot is the full persisted state for a Handler, combining both
// backend state and the handler-level tag data that lives outside the backend.
type handlerSnapshot struct {
	Tags    map[string]map[string]string `json:"tags,omitempty"`
	Backend []byte                       `json:"backend"`
}

// Snapshot implements persistence.Persistable by serialising both the backend
// state and the handler-owned tag data.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}

	var backendData []byte
	if s, ok := h.Backend.(snapshotter); ok {
		backendData = s.Snapshot(ctx)
	}

	// Collect tags outside the backend lock.
	h.tagsMu.RLock("Snapshot")
	tagMap := make(map[string]map[string]string, len(h.tags))
	for k, t := range h.tags {
		tagMap[k] = t.Clone()
	}
	h.tagsMu.RUnlock()

	snap := handlerSnapshot{
		Backend: backendData,
		Tags:    tagMap,
	}

	return persistence.MarshalSnapshot(ctx, "eventbridge", snap)
}

// Restore implements persistence.Persistable by restoring both the backend
// state and the handler-owned tag data.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	// Attempt to decode as the combined handlerSnapshot format first.
	var snap handlerSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "eventbridge", data, &snap); err != nil {
		return err
	}

	if err := h.restoreBackend(ctx, snap.Backend, data); err != nil {
		return err
	}

	h.restoreTags(snap.Tags)

	return nil
}

// restoreBackend restores backend state from the snapshot.
// If backendData is non-nil it came from the new combined format; otherwise
// the caller falls back to the raw data (legacy bare-backend format).
func (h *Handler) restoreBackend(ctx context.Context, backendData, rawData []byte) error {
	type restorer interface {
		Restore(context.Context, []byte) error
	}

	r, ok := h.Backend.(restorer)
	if !ok {
		return nil
	}

	src := backendData
	if src == nil {
		src = rawData
	}

	return r.Restore(ctx, src)
}

// restoreTags replaces the handler's tag store with the persisted tag map.
// All existing tag collections are closed to prevent Prometheus metric
// registry leaks, then replaced with values from the snapshot.
func (h *Handler) restoreTags(tagMap map[string]map[string]string) {
	h.tagsMu.Lock("Restore")
	defer h.tagsMu.Unlock()

	for _, t := range h.tags {
		t.Close()
	}

	h.tags = make(map[string]*svcTags.Tags, len(tagMap))

	for resourceID, kv := range tagMap {
		t := svcTags.New("eb." + resourceID + ".tags")
		t.Merge(kv)
		h.tags[resourceID] = t
	}
}
