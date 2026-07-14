package kms

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// kmsSnapshotVersion identifies the shape of backendSnapshot's Tables blob
// (i.e. the set of per-region keys/aliases/grants/customKeyStores tables
// registered on b.registry -- see keysStore/aliasesStore/grantsRegion/
// customKeyStoresStore in backend.go). It must be bumped whenever a change
// there would make an older snapshot unsafe to decode as the current shape.
// Restore compares this against the persisted value and discards (rather
// than attempts to partially decode) any mismatch -- see Restore below. This
// mirrors the services/ec2 and services/sqs pkgs/store conversions (commits
// 12e611a4, 0f09d77c).
const kmsSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the KMS backend.
//
// Tables holds one JSON-encoded array per registered per-region table,
// produced by [store.Registry.SnapshotAll] -- e.g. "keys:us-east-1",
// "grants:eu-west-1". Policies, KeyMaterials, and KeyMaterialHistory remain
// plain region-nested maps: a policy is a bare string and a keyMaterial/
// history entry carries no identity field of its own (see policiesStore/
// keyMaterialsStore/keyMaterialHistoryStore in backend.go), so neither can be
// expressed as a store.Table.
type backendSnapshot struct {
	Tables             map[string]json.RawMessage                    `json:"tables"`
	Policies           map[string]map[string]string                  `json:"policies"`
	KeyMaterials       map[string]map[string]serializedKeyMaterial   `json:"key_materials,omitempty"`
	KeyMaterialHistory map[string]map[string][]serializedKeyMaterial `json:"key_material_history,omitempty"`
	AccountID          string                                        `json:"accountID"`
	Region             string                                        `json:"region"`
	Version            int                                           `json:"version"`
}

// snapshotRegionKeyMaterials serializes b.keyMaterials into region-nested form.
func snapshotRegionKeyMaterials(
	ctx context.Context,
	km map[string]map[string]*keyMaterial,
) map[string]map[string]serializedKeyMaterial {
	out := make(map[string]map[string]serializedKeyMaterial, len(km))

	for region, regionKMs := range km {
		regionOut := make(map[string]serializedKeyMaterial, len(regionKMs))

		for keyID, m := range regionKMs {
			s, err := marshalKeyMaterial(m)
			if err != nil {
				logger.Load(ctx).WarnContext(ctx, "KMS snapshot: skipping key material",
					"region", region, "keyID", keyID, "error", err)

				continue
			}

			regionOut[keyID] = s
		}

		if len(regionOut) > 0 {
			out[region] = regionOut
		}
	}

	return out
}

// snapshotRegionKeyMaterialHistory serializes b.keyMaterialHistory into region-nested form.
func snapshotRegionKeyMaterialHistory(
	ctx context.Context,
	hist map[string]map[string][]*keyMaterial,
) map[string]map[string][]serializedKeyMaterial {
	out := make(map[string]map[string][]serializedKeyMaterial, len(hist))

	for region, regionHist := range hist {
		regionOut := make(map[string][]serializedKeyMaterial, len(regionHist))

		for keyID, history := range regionHist {
			entries := snapshotKeyMaterialHistory(ctx, region, keyID, history)
			if len(entries) > 0 {
				regionOut[keyID] = entries
			}
		}

		if len(regionOut) > 0 {
			out[region] = regionOut
		}
	}

	return out
}

// snapshotKeyMaterialHistory serializes a single key's material history slice.
func snapshotKeyMaterialHistory(
	ctx context.Context,
	region, keyID string,
	history []*keyMaterial,
) []serializedKeyMaterial {
	entries := make([]serializedKeyMaterial, 0, len(history))

	for _, m := range history {
		s, err := marshalKeyMaterial(m)
		if err != nil {
			logger.Load(ctx).WarnContext(ctx, "KMS snapshot: skipping historical key material",
				"region", region, "keyID", keyID, "error", err)

			continue
		}

		entries = append(entries, s)
	}

	return entries
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
// Key materials that cannot be serialized are omitted from the snapshot with a warning log.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx, "kms: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:            kmsSnapshotVersion,
		Tables:             tables,
		Policies:           b.policies,
		KeyMaterials:       snapshotRegionKeyMaterials(ctx, b.keyMaterials),
		KeyMaterialHistory: snapshotRegionKeyMaterialHistory(ctx, b.keyMaterialHistory),
		AccountID:          b.accountID,
		Region:             b.defaultRegion,
	}

	return persistence.MarshalSnapshot(ctx, "kms", snap)
}

// restoreRegionKeyMaterials deserializes the region-nested key materials map.
func restoreRegionKeyMaterials(
	serialized map[string]map[string]serializedKeyMaterial,
) (map[string]map[string]*keyMaterial, error) {
	out := make(map[string]map[string]*keyMaterial, len(serialized))

	for region, regionKMs := range serialized {
		regionOut := make(map[string]*keyMaterial, len(regionKMs))

		for keyID, s := range regionKMs {
			km, err := unmarshalKeyMaterial(s)
			if err != nil {
				return nil, fmt.Errorf("restoring key material for region %s key %s: %w", region, keyID, err)
			}

			regionOut[keyID] = km
		}

		out[region] = regionOut
	}

	return out, nil
}

// restoreRegionKeyMaterialHistory deserializes the region-nested key material history map.
func restoreRegionKeyMaterialHistory(
	serialized map[string]map[string][]serializedKeyMaterial,
) (map[string]map[string][]*keyMaterial, error) {
	out := make(map[string]map[string][]*keyMaterial, len(serialized))

	for region, regionHist := range serialized {
		regionOut := make(map[string][]*keyMaterial, len(regionHist))

		for keyID, entries := range regionHist {
			history, err := restoreKeyMaterialHistory(region, keyID, entries)
			if err != nil {
				return nil, err
			}

			regionOut[keyID] = history
		}

		out[region] = regionOut
	}

	return out, nil
}

// restoreKeyMaterialHistory deserializes a single key's material history.
func restoreKeyMaterialHistory(region, keyID string, entries []serializedKeyMaterial) ([]*keyMaterial, error) {
	history := make([]*keyMaterial, 0, len(entries))

	for i, s := range entries {
		km, err := unmarshalKeyMaterial(s)
		if err != nil {
			return nil, fmt.Errorf(
				"restoring historical key material[%d] for region %s key %s: %w",
				i,
				region,
				keyID,
				err,
			)
		}

		history = append(history, km)
	}

	return history, nil
}

// warnMissingKeyMaterials logs a warning for any key that lacks key material after restore.
func warnMissingKeyMaterials(
	ctx context.Context,
	keys map[string]*store.Table[Key],
	materials map[string]map[string]*keyMaterial,
) {
	for region, t := range keys {
		regionMaterials := materials[region]

		for _, key := range t.All() {
			if key.KeyState == KeyStatePendingImport {
				continue
			}

			if regionMaterials == nil {
				logger.Load(ctx).WarnContext(ctx,
					"KMS restore: key has no material in snapshot; crypto operations will fail",
					"region", region, "keyID", key.KeyID)

				continue
			}

			if _, hasMaterial := regionMaterials[key.KeyID]; !hasMaterial {
				logger.Load(ctx).WarnContext(ctx,
					"KMS restore: key has no material in snapshot; crypto operations will fail",
					"region", region, "keyID", key.KeyID)
			}
		}
	}
}

// preRegisterSnapshotTables force-registers every per-region table named in a
// snapshot's Tables blob before calling registry.RestoreAll. Per-region
// tables are otherwise registered lazily on first access (see keysStore/
// aliasesStore/grantsRegion/customKeyStoresStore), so on a fresh backend a
// region present only in the snapshot (never yet touched by a live op) would
// have no registered table for RestoreAll to find -- store.Registry.RestoreAll
// only restores tables already known to the registry, silently ignoring any
// snapshot entry whose name isn't registered.
func (b *InMemoryBackend) preRegisterSnapshotTables(tables map[string]json.RawMessage) {
	for name := range tables {
		kind, region, ok := strings.Cut(name, ":")
		if !ok {
			continue
		}

		switch kind {
		case "keys":
			b.keysStore(region)
		case "aliases":
			b.aliasesStore(region)
		case "grants":
			b.grantsRegion(region)
		case "customKeyStores":
			b.customKeyStoresStore(region)
		}
	}
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// If a key in the snapshot does not have corresponding key material (e.g. from an older snapshot
// format), a warning is logged. Callers of Encrypt/Sign/etc. will receive ErrKeyMaterialUnavailable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "kms", data, &snap); err != nil {
		return err
	}

	restoredMaterials, err := restoreRegionKeyMaterials(snap.KeyMaterials)
	if err != nil {
		return err
	}

	restoredHistory, err := restoreRegionKeyMaterialHistory(snap.KeyMaterialHistory)
	if err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != kmsSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption. Mirrors the services/ec2 and services/sqs pilots.
		logger.Load(ctx).WarnContext(ctx,
			"kms: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", kmsSnapshotVersion)

		b.registry.ResetAll()
		b.policies = make(map[string]map[string]string)
		b.keyMaterials = make(map[string]map[string]*keyMaterial)
		b.keyMaterialHistory = make(map[string]map[string][]*keyMaterial)
		b.clearResolutionCache()

		return nil
	}

	b.preRegisterSnapshotTables(snap.Tables)

	if err = b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("kms: restore snapshot tables: %w", err)
	}

	warnMissingKeyMaterials(ctx, b.keys, restoredMaterials)

	b.policies = snap.Policies
	if b.policies == nil {
		b.policies = make(map[string]map[string]string)
	}

	b.keyMaterials = restoredMaterials
	b.keyMaterialHistory = restoredHistory
	b.accountID = snap.AccountID
	b.defaultRegion = snap.Region
	b.clearResolutionCache()

	return nil
}

// handlerSnapshotFormat identifies the shape of the JSON produced by
// Handler.Snapshot. It is stamped into every new snapshot so Handler.Restore
// can distinguish it from a legacy snapshot (the raw bytes previously
// returned by delegating straight to Backend.Snapshot, with no wrapper at
// all) without misinterpreting one shape as the other.
const handlerSnapshotFormat = 1

// handlerSnapshot wraps the backend snapshot together with handler-level
// resource tags. Tags are NOT part of InMemoryBackend's own state: they live
// in Handler.tags, a side map keyed by KeyID (see setTags/getTags), because
// KMS resource tags are applied at the handler layer (CreateKey/TagResource)
// rather than embedded in the Key struct the way most other services embed
// *tags.Tags directly in their resource type. Without this wrapper, tags
// silently vanished across any Snapshot/Restore cycle (e.g. a gopherstack
// process restart with persistence enabled) even though ListResourceTags
// kept returning them correctly within a single running process -- exactly
// the kind of gap that produces perpetual `terraform plan` drift on the
// `tags` attribute of `aws_kms_key` after a restart.
type handlerSnapshot struct {
	Tags          map[string]map[string]string `json:"tags,omitempty"`
	Backend       json.RawMessage              `json:"backend,omitempty"`
	HandlerFormat int                          `json:"handlerFormat"`
}

// snapshotTags returns a deep copy of every non-empty tag set in h.tags,
// keyed by resource ID (KeyID).
func (h *Handler) snapshotTags() map[string]map[string]string {
	h.tagsMu.RLock("Snapshot")
	defer h.tagsMu.RUnlock()

	out := make(map[string]map[string]string, len(h.tags))

	for id, t := range h.tags {
		if t == nil {
			continue
		}

		if kv := t.Clone(); len(kv) > 0 {
			out[id] = kv
		}
	}

	return out
}

// restoreTags replaces h.tags with fresh *tags.Tags built from a restored
// snapshot, closing out any pre-existing entries first (mirrors Reset).
func (h *Handler) restoreTags(snapTags map[string]map[string]string) {
	h.tagsMu.Lock("Restore")
	defer h.tagsMu.Unlock()

	for _, t := range h.tags {
		if t != nil {
			t.Close()
		}
	}

	h.tags = make(map[string]*tags.Tags, len(snapTags))

	for id, kv := range snapTags {
		if len(kv) == 0 {
			continue
		}

		h.tags[id] = tags.New("kms." + id + ".tags")
		h.tags[id].Merge(kv)
	}
}

// restoreBackend delegates to Backend.Restore, tolerating a backend that
// doesn't implement it and a legacy/empty payload.
func (h *Handler) restoreBackend(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	type restorer interface {
		Restore(context.Context, []byte) error
	}

	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(ctx, data)
	}

	return nil
}

// Snapshot implements persistence.Persistable. It wraps the backend's own
// snapshot together with handler-level resource tags (see handlerSnapshot)
// so a Restore round-trip preserves tags, not just key/alias/grant state.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}

	var backendBytes []byte

	if s, ok := h.Backend.(snapshotter); ok {
		backendBytes = s.Snapshot(ctx)
	}

	if backendBytes == nil {
		// Backend snapshot failed or the backend doesn't support snapshotting
		// at all; nil is skipped by the persistence.Manager, matching the
		// pre-existing delegate-only behavior.
		return nil
	}

	snap := handlerSnapshot{
		HandlerFormat: handlerSnapshotFormat,
		Backend:       backendBytes,
		Tags:          h.snapshotTags(),
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "kms: handler snapshot marshal failed", "error", err)

		return backendBytes
	}

	return data
}

// Restore implements persistence.Persistable. It accepts both the current
// wrapped format (handlerSnapshot) and a legacy snapshot (raw bytes produced
// by delegating straight to Backend.Snapshot, with no handler-level tags) so
// that pre-existing on-disk snapshots taken before tags were persisted still
// restore backend state cleanly instead of erroring out.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	var peek struct {
		HandlerFormat int `json:"handlerFormat"`
	}

	if err := json.Unmarshal(data, &peek); err != nil {
		return fmt.Errorf("kms: restore handler snapshot: %w", err)
	}

	if peek.HandlerFormat != handlerSnapshotFormat {
		// Legacy snapshot: data IS the backend snapshot; there are no
		// handler-level tags to restore.
		return h.restoreBackend(ctx, data)
	}

	var snap handlerSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("kms: restore handler snapshot: %w", err)
	}

	if err := h.restoreBackend(ctx, snap.Backend); err != nil {
		return err
	}

	h.restoreTags(snap.Tags)

	return nil
}
