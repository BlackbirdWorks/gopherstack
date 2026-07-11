package wafv2

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// wafv2SnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to a DTO type or backendSnapshot itself would make
// an older snapshot unsafe to decode as the current shape. Restore compares
// this against the persisted value and discards (ResetAll, not a partial
// decode) any mismatch -- see Restore. The pre-Phase-3.3 snapshot format had
// no version field at all, so an old snapshot decodes with Version == 0,
// which is guaranteed to mismatch wafv2SnapshotVersion and is discarded the
// same way any other incompatible snapshot is.
const wafv2SnapshotVersion = 1

// managedRuleSetSnapshot and apiKeySnapshot are DTOs used only for
// Snapshot/Restore of the "dirty" tables -- see store_setup.go's file doc
// comment for why managedRuleSets/apiKeys can't be registered directly on
// b.registry. Each wraps the live value alongside the Region field that
// value intentionally excludes from JSON (`json:"-"`), so it survives the
// round trip.
type managedRuleSetSnapshot struct {
	Region  string         `json:"region"`
	RuleSet ManagedRuleSet `json:"ruleSet"`
}

func managedRuleSetSnapshotKey(v *managedRuleSetSnapshot) string {
	return regionKey(v.Region, v.RuleSet.ID)
}

type apiKeySnapshot struct {
	Region string `json:"region"`
	Key    APIKey `json:"key"`
}

func apiKeySnapshotKey(v *apiKeySnapshot) string {
	return regionKey(v.Region, apiKeyMapKey(v.Key.Scope, v.Key.APIKeyValue))
}

// newDirtyDTORegistry builds the ephemeral DTO [store.Registry] shared by
// Snapshot and Restore for the tables that can't be registered on
// b.registry directly (see store_setup.go).
func newDirtyDTORegistry() (
	*store.Registry,
	*store.Table[managedRuleSetSnapshot],
	*store.Table[apiKeySnapshot],
) {
	dtoReg := store.NewRegistry()
	msDTOs := store.Register(dtoReg, "managedRuleSets", store.New(managedRuleSetSnapshotKey))
	akDTOs := store.Register(dtoReg, "apiKeys", store.New(apiKeySnapshotKey))

	return dtoReg, msDTOs, akDTOs
}

// backendSnapshot is the top-level on-disk shape for the WAFv2 backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// merging b.registry.SnapshotAll() (the "clean" tables: webACLs, ipSets,
// regexPatternSets, ruleGroups) with the ephemeral DTO registry's
// SnapshotAll() (the "dirty" tables: managedRuleSets, apiKeys). Version
// guards against decoding a snapshot from an incompatible (older or newer)
// build of this backend as though it were the current shape; see Restore.
//
// LoggingConfigs, PermissionPolicies, and Associations are left as plain
// region-nested maps: their values are json.RawMessage/string, not *T, so
// they do not fit store.Table's keyed-by-identity-value shape and are
// unaffected by this conversion.
type backendSnapshot struct {
	Tables             map[string]json.RawMessage            `json:"tables"`
	LoggingConfigs     map[string]map[string]json.RawMessage `json:"loggingConfigs,omitempty"`
	PermissionPolicies map[string]map[string]string          `json:"permissionPolicies,omitempty"`
	Associations       map[string]map[string]string          `json:"associations,omitempty"`
	AccountID          string                                `json:"accountID"`
	Region             string                                `json:"region"`
	Version            int                                   `json:"version"`
}

// Snapshot serializes the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "wafv2: snapshot table marshal failed", "error", err)

		return nil
	}

	dtoReg, msDTOs, akDTOs := newDirtyDTORegistry()

	for _, ms := range b.managedRuleSets.Snapshot() {
		msDTOs.Put(&managedRuleSetSnapshot{Region: ms.Region, RuleSet: *ms})
	}

	for _, ak := range b.apiKeys.Snapshot() {
		akDTOs.Put(&apiKeySnapshot{Region: ak.Region, Key: *ak})
	}

	dirtyTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "wafv2: snapshot DTO table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dirtyTables)

	snap := backendSnapshot{
		Version:            wafv2SnapshotVersion,
		Tables:             tables,
		LoggingConfigs:     b.loggingConfigs,
		PermissionPolicies: b.permissionPolicies,
		Associations:       b.associations,
		AccountID:          b.accountID,
		Region:             b.region,
	}

	return persistence.MarshalSnapshot(ctx, "wafv2", snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "wafv2", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != wafv2SnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"wafv2: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", wafv2SnapshotVersion)

		b.resetTablesLocked()
		b.loggingConfigs = make(map[string]map[string]json.RawMessage)
		b.permissionPolicies = make(map[string]map[string]string)
		b.associations = make(map[string]map[string]string)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("wafv2: restore snapshot tables: %w", err)
	}

	dtoReg, msDTOs, akDTOs := newDirtyDTORegistry()
	if err := dtoReg.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("wafv2: restore snapshot DTO tables: %w", err)
	}

	liveManagedRuleSets := make([]*ManagedRuleSet, 0, msDTOs.Len())

	for _, dto := range msDTOs.All() {
		ms := dto.RuleSet
		ms.Region = dto.Region
		liveManagedRuleSets = append(liveManagedRuleSets, &ms)
	}

	b.managedRuleSets.Restore(liveManagedRuleSets)

	liveAPIKeys := make([]*APIKey, 0, akDTOs.Len())

	for _, dto := range akDTOs.All() {
		ak := dto.Key
		ak.Region = dto.Region
		liveAPIKeys = append(liveAPIKeys, &ak)
	}

	b.apiKeys.Restore(liveAPIKeys)

	if snap.LoggingConfigs != nil {
		b.loggingConfigs = snap.LoggingConfigs
	} else {
		b.loggingConfigs = make(map[string]map[string]json.RawMessage)
	}

	if snap.PermissionPolicies != nil {
		b.permissionPolicies = snap.PermissionPolicies
	} else {
		b.permissionPolicies = make(map[string]map[string]string)
	}

	if snap.Associations != nil {
		b.associations = snap.Associations
	} else {
		b.associations = make(map[string]map[string]string)
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
