package sns

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// snsSnapshotVersion identifies the shape of backendSnapshot's Tables blob
// (i.e. the set of resources registered on b.registry -- see registerAllTables
// in store_setup.go). It must be bumped whenever a change there would make an
// older snapshot unsafe to decode as the current shape. Restore compares this
// against the persisted value and discards (rather than attempts to partially
// decode) any mismatch -- see Restore below. This mirrors the services/sqs
// pilot (commit 0f09d77c) and the services/ec2 sweep (commit 12e611a4).
//
// Snapshots written before this field existed decode with Version == 0, which
// never equals snsSnapshotVersion, so they are discarded the same way as any
// other incompatible version rather than partially decoded.
const snsSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the SNS backend.
//
// Tables holds one JSON-encoded array per registered store.Table, produced by
// [store.Registry.SnapshotAll] -- "topics", "subscriptions",
// "platformApplications", "platformEndpoints", and "smsSandbox". The remaining
// fields below are resource maps left un-registered (raw) because their value
// type is not a pure keyed identity or is slice/scalar-valued -- see the
// comment on registerAllTables in store_setup.go for why each one.
type backendSnapshot struct {
	Tables               map[string]json.RawMessage       `json:"tables"`
	TopicTags            map[string]*svcTags.Tags         `json:"topicTags"`
	OptedOutPhoneNumbers map[string]bool                  `json:"optedOutPhoneNumbers,omitempty"`
	SMSAttributes        map[string]string                `json:"smsAttributes,omitempty"`
	OriginationNumbers   map[string][]XMLOriginationPhone `json:"originationNumbers,omitempty"`
	// TopicMessageArchive holds per-topic ArchivePolicy message archives, keyed by
	// topic ARN. Without persisting this, a restart (or snapshot restore) silently
	// discards all archived messages, so ReplayPolicy subscriptions set up before
	// the restart can no longer replay anything.
	TopicMessageArchive map[string][]*ArchivedMessage `json:"topicMessageArchive,omitempty"`
	SMSSandboxEnabled   *bool                         `json:"smsSandboxEnabled,omitempty"`
	AccountID           string                        `json:"accountID"`
	Region              string                        `json:"region"`
	Version             int                           `json:"version"`
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
		logger.Load(ctx).WarnContext(ctx, "sns: snapshot table marshal failed", "error", err)

		return nil
	}

	sandboxEnabled := b.smsSandboxEnabled
	snap := backendSnapshot{
		Version:              snsSnapshotVersion,
		Tables:               tables,
		TopicTags:            b.topicTags,
		OptedOutPhoneNumbers: b.optedOutPhoneNumbers,
		SMSAttributes:        b.smsAttributes,
		OriginationNumbers:   b.originationNumbers,
		TopicMessageArchive:  b.topicMessageArchive,
		AccountID:            b.accountID,
		Region:               b.region,
		SMSSandboxEnabled:    &sandboxEnabled,
	}

	return persistence.MarshalSnapshot(ctx, "sns", snap)
}

// fillDefaults replaces any nil map field with an empty, non-nil map so older
// snapshots (written before a field existed) restore cleanly instead of leaving
// the backend with a nil map that panics on first write.
func (snap *backendSnapshot) fillDefaults() {
	if snap.TopicTags == nil {
		snap.TopicTags = make(map[string]*svcTags.Tags)
	}

	if snap.OptedOutPhoneNumbers == nil {
		snap.OptedOutPhoneNumbers = make(map[string]bool)
	}

	if snap.SMSAttributes == nil {
		snap.SMSAttributes = make(map[string]string)
	}

	if snap.OriginationNumbers == nil {
		snap.OriginationNumbers = make(map[string][]XMLOriginationPhone)
	}

	if snap.TopicMessageArchive == nil {
		snap.TopicMessageArchive = make(map[string][]*ArchivedMessage)
	}
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// The event emitter is not restored — it is re-wired by the CLI after restore.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "sns", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snsSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption. Mirrors the services/sqs pilot (commit 0f09d77c).
		logger.Load(ctx).WarnContext(ctx,
			"sns: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snsSnapshotVersion)

		b.registry.ResetAll()

		return nil
	}

	snap.fillDefaults()

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("sns: restore snapshot tables: %w", err)
	}

	b.topicTags = snap.TopicTags
	b.optedOutPhoneNumbers = snap.OptedOutPhoneNumbers
	b.smsAttributes = snap.SMSAttributes
	b.originationNumbers = snap.OriginationNumbers
	b.topicMessageArchive = snap.TopicMessageArchive
	b.accountID = snap.AccountID
	b.region = snap.Region
	if snap.SMSSandboxEnabled != nil {
		b.smsSandboxEnabled = *snap.SMSSandboxEnabled
	} else {
		b.smsSandboxEnabled = true // default for old snapshots that lack this field
	}

	// Restore the parsed filter-policy cache for each subscription -- it is
	// transient (unexported field, never marshaled) and not persisted.
	// subscriptionsByTopic is rebuilt automatically by b.registry.RestoreAll
	// above (store.Table.Restore repopulates every registered secondary index
	// from scratch), so no manual topicSubscriptions reconstruction is needed
	// here anymore.
	for _, sub := range b.subscriptions.All() {
		// Ignore errors so a future stricter validation upgrade does not break
		// loading older snapshots.
		sub.parsedFilterPolicy, _ = parseFilterPolicy(sub.FilterPolicy)
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	type restorer interface {
		Restore(context.Context, []byte) error
	}
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(ctx, data)
	}

	return nil
}
