package ses

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// sesSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to a DTO type or backendSnapshot itself would
// make an older snapshot unsafe to decode as the current shape. Restore
// compares this against the persisted value and discards (ResetAll, not a
// partial decode) any mismatch -- see Restore. The pre-Phase-3.3 snapshot
// format had no version field at all, so an old snapshot decodes with
// Version == 0, which is guaranteed to mismatch sesSnapshotVersion and is
// discarded the same way any other incompatible snapshot is.
const sesSnapshotVersion = 1

// identitySnapshot, configSetSnapshot, trackingOptionsSnapshot, and
// eventDestinationSnapshot are DTOs used only for Snapshot/Restore of the
// "dirty" tables -- see store_setup.go's file doc comment for why
// identities/configSets/trackingOptions/eventDestinations can't be
// registered directly on b.registry. Each wraps the live value alongside the
// identity field that value intentionally excludes from JSON (`json:"-"`),
// so the identity survives the round trip.
type identitySnapshot struct {
	Identity string         `json:"identity"`
	Record   IdentityRecord `json:"record"`
}

func identitySnapshotKey(v *identitySnapshot) string { return v.Identity }

type configSetSnapshot struct {
	Name string           `json:"name"`
	Set  ConfigurationSet `json:"set"`
}

func configSetSnapshotKey(v *configSetSnapshot) string { return v.Name }

type trackingOptionsSnapshot struct {
	Options       TrackingOptions `json:"options"`
	ConfigSetName string          `json:"configSetName"`
}

func trackingOptionsSnapshotKey(v *trackingOptionsSnapshot) string { return v.ConfigSetName }

type eventDestinationSnapshot struct {
	ConfigSetName string           `json:"configSetName"`
	Destination   EventDestination `json:"destination"`
}

func eventDestinationSnapshotKey(v *eventDestinationSnapshot) string {
	return eventDestinationKey(v.ConfigSetName, v.Destination.Name)
}

// newDirtyDTORegistry builds the ephemeral DTO [store.Registry] shared by
// Snapshot and Restore for the tables that can't be registered on
// b.registry directly (see store_setup.go).
func newDirtyDTORegistry() (
	*store.Registry,
	*store.Table[identitySnapshot],
	*store.Table[configSetSnapshot],
	*store.Table[trackingOptionsSnapshot],
	*store.Table[eventDestinationSnapshot],
) {
	dtoReg := store.NewRegistry()
	idDTOs := store.Register(dtoReg, "identities", store.New(identitySnapshotKey))
	csDTOs := store.Register(dtoReg, "configSets", store.New(configSetSnapshotKey))
	toDTOs := store.Register(dtoReg, "trackingOptions", store.New(trackingOptionsSnapshotKey))
	edDTOs := store.Register(dtoReg, "eventDestinations", store.New(eventDestinationSnapshotKey))

	return dtoReg, idDTOs, csDTOs, toDTOs, edDTOs
}

// backendSnapshot is the top-level on-disk shape for the SES backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// merging b.registry.SnapshotAll() (the "clean" tables: templates,
// receiptRuleSets, receiptFilters, customVerifTemplates) with the ephemeral
// DTO registry's SnapshotAll() (the "dirty" tables: identities, configSets,
// trackingOptions, eventDestinations). Version guards against decoding a
// snapshot from an incompatible (older or newer) build of this backend as
// though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables                map[string]json.RawMessage   `json:"tables"`
	Policies              map[string]map[string]string `json:"policies,omitempty"`
	ActiveRuleSet         string                       `json:"activeRuleSet,omitempty"`
	Region                string                       `json:"region,omitempty"`
	AccountID             string                       `json:"accountID,omitempty"`
	Emails                []Email                      `json:"emails"`
	Version               int                          `json:"version"`
	AccountSendingEnabled bool                         `json:"accountSendingEnabled"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "ses: snapshot table marshal failed", "error", err)

		return nil
	}

	dtoReg, idDTOs, csDTOs, toDTOs, edDTOs := newDirtyDTORegistry()

	for _, v := range b.identities.Snapshot() {
		idDTOs.Put(&identitySnapshot{Identity: v.Identity, Record: *v})
	}

	for _, v := range b.configSets.Snapshot() {
		csDTOs.Put(&configSetSnapshot{Name: v.Name, Set: *v})
	}

	for _, v := range b.trackingOptions.Snapshot() {
		toDTOs.Put(&trackingOptionsSnapshot{ConfigSetName: v.ConfigSetName, Options: *v})
	}

	for _, v := range b.eventDestinations.Snapshot() {
		edDTOs.Put(&eventDestinationSnapshot{ConfigSetName: v.ConfigSetName, Destination: *v})
	}

	dirtyTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "ses: snapshot DTO table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dirtyTables)

	pols := make(map[string]map[string]string, len(b.policies))
	for identity, polMap := range b.policies {
		pm := make(map[string]string, len(polMap))
		maps.Copy(pm, polMap)
		pols[identity] = pm
	}

	emails := make([]Email, len(b.emails))
	copy(emails, b.emails)

	snap := backendSnapshot{
		Version:               sesSnapshotVersion,
		Tables:                tables,
		Emails:                emails,
		Policies:              pols,
		ActiveRuleSet:         b.activeRuleSet,
		AccountSendingEnabled: b.accountSendingEnabled,
		Region:                b.region,
		AccountID:             b.accountID,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "ses: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ses", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != sesSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"ses: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", sesSnapshotVersion)

		b.resetTablesLocked()
		b.emails = nil
		b.policies = make(map[string]map[string]string)
		b.activeRuleSet = ""
		b.accountSendingEnabled = true

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("ses: restore snapshot tables: %w", err)
	}

	if err := b.restoreDirtyTablesLocked(snap.Tables); err != nil {
		return err
	}

	if snap.Policies == nil {
		snap.Policies = make(map[string]map[string]string)
	}

	b.policies = snap.Policies
	b.activeRuleSet = snap.ActiveRuleSet
	b.accountSendingEnabled = snap.AccountSendingEnabled

	if snap.Region != "" {
		b.region = snap.Region
	}

	if snap.AccountID != "" {
		b.accountID = snap.AccountID
	}

	b.restoreEmailsLocked(snap.Emails)

	return nil
}

// restoreDirtyTablesLocked restores the "dirty" tables (identities,
// configSets, trackingOptions, eventDestinations) from tables via the
// ephemeral DTO registry, converting each DTO back to its live type. The
// caller MUST hold b.mu for writing.
func (b *InMemoryBackend) restoreDirtyTablesLocked(tables map[string]json.RawMessage) error {
	dtoReg, idDTOs, csDTOs, toDTOs, edDTOs := newDirtyDTORegistry()

	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("ses: restore snapshot DTO tables: %w", err)
	}

	liveIdentities := make([]*IdentityRecord, 0, idDTOs.Len())

	for _, dto := range idDTOs.All() {
		rec := dto.Record
		rec.Identity = dto.Identity
		liveIdentities = append(liveIdentities, &rec)
	}

	b.identities.Restore(liveIdentities)

	liveConfigSets := make([]*ConfigurationSet, 0, csDTOs.Len())

	for _, dto := range csDTOs.All() {
		cs := dto.Set
		cs.Name = dto.Name
		liveConfigSets = append(liveConfigSets, &cs)
	}

	b.configSets.Restore(liveConfigSets)

	liveTrackingOptions := make([]*TrackingOptions, 0, toDTOs.Len())

	for _, dto := range toDTOs.All() {
		to := dto.Options
		to.ConfigSetName = dto.ConfigSetName
		liveTrackingOptions = append(liveTrackingOptions, &to)
	}

	b.trackingOptions.Restore(liveTrackingOptions)

	liveEventDestinations := make([]*EventDestination, 0, edDTOs.Len())

	for _, dto := range edDTOs.All() {
		d := dto.Destination
		d.ConfigSetName = dto.ConfigSetName
		liveEventDestinations = append(liveEventDestinations, &d)
	}

	b.eventDestinations.Restore(liveEventDestinations)

	return nil
}

// restoreEmailsLocked prunes snapEmails to the current TTL window and
// maxRetainedEmails cap, then rebuilds b.emails and the emailsByID derived
// cache from the result. The caller MUST hold b.mu for writing.
func (b *InMemoryBackend) restoreEmailsLocked(snapEmails []Email) {
	// Drop emails outside the current TTL window and cap to maxRetainedEmails
	// so that memory is bounded immediately after restore.
	cutoff := time.Now().Add(-b.emailTTL)

	start := 0

	for start < len(snapEmails) && snapEmails[start].Timestamp.Before(cutoff) {
		start++
	}

	valid := snapEmails[start:]

	if len(valid) > maxRetainedEmails {
		valid = valid[len(valid)-maxRetainedEmails:]
	}

	b.emails = make([]Email, len(valid))
	copy(b.emails, valid)

	// Rebuild the O(1) lookup table from the pruned slice. emailsByID is a
	// derived cache, never independently persisted -- see store_setup.go.
	b.emailsByID.Reset()

	for i := range b.emails {
		ec := b.emails[i]
		b.emailsByID.Put(&ec)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
