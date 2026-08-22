package mediaconvert

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// mediaconvertSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to backendSnapshot (or a value/DTO type
// held by one of the registered tables) would make an older snapshot unsafe
// to decode as the current shape. Restore compares this against the
// persisted value and discards (ResetAll, not a partial decode) any
// mismatch -- see Restore. The pre-Phase-3.3 snapshot format had no version
// field at all, so an old snapshot decodes with Version == 0, which is
// guaranteed to mismatch mediaconvertSnapshotVersion and is discarded the
// same way any other incompatible snapshot is.
//
// Bumped 1 -> 2 (gopherstack-hjdd) for d83f4b5d3: Job.LastShareDetails (the
// registered "jobs" table's value type) switched its wire representation from
// a {shareToken, sharedAt} object to the real deserializer's bare string, via
// a custom Job.MarshalJSON/UnmarshalJSON pair. A Version-1 snapshot's object
// no longer unmarshals into the new string field at all -- an outright decode
// error that takes down the whole restore, not silent loss -- so it must be
// discarded like any other shape-incompatible snapshot.
const mediaconvertSnapshotVersion = 2

// counterSnapshot and tokenSnapshot are DTOs used only for Snapshot/Restore
// of the "dirty" tables -- see store_setup.go's file doc comment for why
// queueCounters/tokenIndex can't be registered directly on b.registry. Each
// wraps the live value alongside the identity field that value carries only
// as an unexported field (queueJobCounter.queueArn / tokenEntry.token), so
// the identity survives the round trip despite being excluded from the live
// type's own (nonexistent, since every other field is unexported too) JSON
// encoding.
type counterSnapshot struct {
	QueueArn string          `json:"queueArn"`
	Counter  queueJobCounter `json:"counter"`
}

func counterSnapshotKey(v *counterSnapshot) string { return v.QueueArn }

type tokenSnapshot struct {
	Token string     `json:"token"`
	Entry tokenEntry `json:"entry"`
}

func tokenSnapshotKey(v *tokenSnapshot) string { return v.Token }

// newDirtyDTORegistry builds the ephemeral DTO [store.Registry] shared by
// Snapshot and Restore for the tables that can't be registered on
// b.registry directly (see store_setup.go).
func newDirtyDTORegistry() (*store.Registry, *store.Table[counterSnapshot], *store.Table[tokenSnapshot]) {
	dtoReg := store.NewRegistry()
	cDTOs := store.Register(dtoReg, "queueCounters", store.New(counterSnapshotKey))
	tDTOs := store.Register(dtoReg, "tokenIndex", store.New(tokenSnapshotKey))

	return dtoReg, cDTOs, tDTOs
}

// backendSnapshot is the top-level on-disk shape for the MediaConvert
// backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// merging b.registry.SnapshotAll() (the "clean" tables: queues, jobTemplates,
// jobs, presets) with the ephemeral DTO registry's SnapshotAll() (the "dirty"
// tables: queueCounters, tokenIndex). Tags and Certificates are left as plain
// maps (not store.Table-backed, see store_setup.go) but were persisted
// before this refactor and remain so. The queries table is intentionally
// absent: it was never persisted pre-Phase-3.3 either (see the old Restore,
// which always reset it to empty).
type backendSnapshot struct {
	Tables       map[string]json.RawMessage   `json:"tables"`
	Tags         map[string]map[string]string `json:"tags"`
	Certificates map[string]struct{}          `json:"certificates"`
	Policy       *Policy                      `json:"policy,omitempty"`
	AccountID    string                       `json:"accountID"`
	Region       string                       `json:"region"`
	Version      int                          `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "mediaconvert: snapshot table marshal failed", "error", err)

		return nil
	}

	dtoReg, cDTOs, tDTOs := newDirtyDTORegistry()

	for _, c := range b.queueCounters.Snapshot() {
		cDTOs.Put(&counterSnapshot{QueueArn: c.queueArn, Counter: *c})
	}

	for _, t := range b.tokenIndex.Snapshot() {
		tDTOs.Put(&tokenSnapshot{Token: t.token, Entry: *t})
	}

	dirtyTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "mediaconvert: snapshot DTO table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dirtyTables)

	tags := make(map[string]map[string]string, len(b.tags))
	for k, v := range b.tags {
		tags[k] = nonNilTagsCopy(v)
	}

	certs := make(map[string]struct{}, len(b.certificates))
	for k := range b.certificates {
		certs[k] = struct{}{}
	}

	var pol *Policy
	if b.policy != nil {
		cp := *b.policy
		pol = &cp
	}

	snap := backendSnapshot{
		Version:      mediaconvertSnapshotVersion,
		Tables:       tables,
		Tags:         tags,
		Certificates: certs,
		Policy:       pol,
		AccountID:    b.accountID,
		Region:       b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "mediaconvert: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "mediaconvert", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != mediaconvertSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"mediaconvert: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", mediaconvertSnapshotVersion)

		b.resetTablesLocked()
		b.tags = make(map[string]map[string]string)
		b.certificates = make(map[string]struct{})
		b.policy = nil

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("mediaconvert: restore snapshot tables: %w", err)
	}

	if err := b.restoreDirtyTablesLocked(snap.Tables); err != nil {
		return err
	}

	// queries was never persisted pre-Phase-3.3 either; always start empty.
	b.queries.Reset()

	if snap.Tags != nil {
		b.tags = snap.Tags
	} else {
		b.tags = make(map[string]map[string]string)
	}

	if snap.Certificates != nil {
		b.certificates = snap.Certificates
	} else {
		b.certificates = make(map[string]struct{})
	}

	b.policy = snap.Policy
	b.accountID = snap.AccountID
	b.region = snap.Region

	if b.queueCounters.Len() == 0 {
		b.rebuildCountersLocked()
	}

	return nil
}

// restoreDirtyTablesLocked restores the "dirty" tables (queueCounters,
// tokenIndex) from tables via the ephemeral DTO registry, converting each DTO
// back to its live type. The caller MUST hold b.mu for writing.
func (b *InMemoryBackend) restoreDirtyTablesLocked(tables map[string]json.RawMessage) error {
	dtoReg, cDTOs, tDTOs := newDirtyDTORegistry()

	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("mediaconvert: restore snapshot DTO tables: %w", err)
	}

	liveCounters := make([]*queueJobCounter, 0, cDTOs.Len())

	for _, dto := range cDTOs.All() {
		c := dto.Counter
		c.queueArn = dto.QueueArn
		liveCounters = append(liveCounters, &c)
	}

	b.queueCounters.Restore(liveCounters)

	liveTokens := make([]*tokenEntry, 0, tDTOs.Len())

	for _, dto := range tDTOs.All() {
		e := dto.Entry
		e.token = dto.Token
		liveTokens = append(liveTokens, &e)
	}

	b.tokenIndex.Restore(liveTokens)

	return nil
}
