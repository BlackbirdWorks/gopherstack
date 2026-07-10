package account

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// accountSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to a registered table's value type or
// backendSnapshot itself would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll plus resetting every raw field, not a partial
// decode) any mismatch -- see Restore below. This is the first version:
// Account had no persistence at all before Phase 3.3 -- Handler had no
// Snapshot/Restore, and neither did InMemoryBackend, so nothing ever picked
// Account up for snapshot/restore (see the Handler.Snapshot/Restore
// delegation at the bottom of this file, and the Snapshot/Restore doc
// comment on StorageBackend in backend.go). There is therefore no legacy
// snapshot shape to be compatible with -- any snapshot without a matching
// Version (including one with no version field, which decodes as 0) is
// discarded the same way any other incompatible snapshot is.
const accountSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Account backend.
//
// Tables holds one JSON-encoded array per registered table name, produced
// directly by b.registry.SnapshotAll(): the one converted resource
// collection (alternateContacts) derives its store.Table key entirely from
// a real, already-wire-visible field (AlternateContact.AlternateContactType
// -- see store_setup.go), so it needs no DTO wrapper.
//
// Every other stateful field is persisted directly here: ContactInfo is a
// single struct pointer (not a collection, so nothing for store.Table to
// key on) and Regions is a plain (non-map) slice, so both stay raw; the
// rest are scalars.
type backendSnapshot struct {
	Tables       map[string]json.RawMessage `json:"tables"`
	ContactInfo  *ContactInformation        `json:"contactInfo,omitempty"`
	AccountID    string                     `json:"accountID"`
	Region       string                     `json:"region"`
	AccountName  string                     `json:"accountName"`
	PrimaryEmail string                     `json:"primaryEmail"`
	PendingEmail string                     `json:"pendingEmail"`
	PendingOTP   string                     `json:"pendingOTP"`
	Regions      []*Region                  `json:"regions"`
	Version      int                        `json:"version"`
	Closed       bool                       `json:"closed"`
}

// Snapshot serializes the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "account: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:      accountSnapshotVersion,
		Tables:       tables,
		ContactInfo:  b.contactInfo,
		Regions:      b.regions,
		AccountID:    b.accountID,
		Region:       b.region,
		AccountName:  b.accountName,
		PrimaryEmail: b.primaryEmail,
		PendingEmail: b.pendingEmail,
		PendingOTP:   b.pendingOTP,
		Closed:       b.closed,
	}

	return persistence.MarshalSnapshot(ctx, "account", snap)
}

// Restore deserializes backend state from a snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "account", data, &snap); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if snap.Version != accountSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"account: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", accountSnapshotVersion)

		b.registry.ResetAll()
		b.contactInfo = nil
		b.accountName = defaultAccountName
		b.primaryEmail = defaultPrimaryEmail
		b.pendingEmail = ""
		b.pendingOTP = ""
		b.closed = false
		b.accountID = snap.AccountID
		b.region = snap.Region
		b.initDefaultRegions()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("account: restore snapshot tables: %w", err)
	}

	b.contactInfo = snap.ContactInfo
	b.regions = snap.Regions
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.accountName = snap.AccountName
	b.primaryEmail = snap.PrimaryEmail
	b.pendingEmail = snap.PendingEmail
	b.pendingOTP = snap.PendingOTP
	b.closed = snap.Closed

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Handler previously had no Snapshot/Restore of its own -- and neither did
// InMemoryBackend -- so a persistence manager that type-asserts the
// registered service.Registerable (i.e. the Handler) for a Snapshot/Restore
// pair never picked Account up at all: dead wiring, with no persistence
// underneath it either. This delegation (matching the cleanrooms/codecommit
// pattern) is what wires Account into persistence for the first time.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
