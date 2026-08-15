package pinpoint

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// Snapshot implements persistence.Persistable by delegating to the backend.
//
// h.Backend is the StorageBackend interface, which already declares
// Snapshot(ctx context.Context) []byte (see interfaces.go) with a shape
// matching persistence.Persistable exactly, so this can call it directly --
// no local type assertion needed. But interface membership alone does not
// help: h.Backend is a named field, not an embedded one, so InMemoryBackend's
// methods are never promoted onto *Handler. Without this delegation, cli.go's
// setupPersistence type-asserts the registered service.Registerable (this
// *Handler) against persistence.Persistable, fails silently, and never
// registers pinpoint for snapshot/restore despite the backend being fully
// capable. Mirrors services/securityhub's Handler-level delegation.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// Compile-time proof Handler satisfies the persistence layer's contract.
var _ persistence.Persistable = (*Handler)(nil)

// pinpointSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to the persisted table set (or a persisted
// value type) would make an older snapshot unsafe to decode as the current
// shape. Restore compares this against the persisted value and discards
// (rather than attempts to partially decode) any mismatch — see Restore
// below.
const pinpointSnapshotVersion = 2

// backendSnapshot is the top-level on-disk shape for the Pinpoint backend.
//
// Tables holds one JSON-encoded array per persisted [store.Table]-backed
// resource, produced by [store.Registry.SnapshotAll]: apps, campaigns,
// channels, emailTemplates, endpoints, eventStreams, exportJobs, importJobs,
// inAppTemplates, journeys, pushTemplates, recommenders, segments,
// smsTemplates, voiceTemplates.
//
// The remaining backend state is map-shaped (map[string][]T / map[string]T,
// not map[string]*T), so it cannot go through [store.Table] (which requires
// a pure key function on a single concrete pointer type) and is persisted
// as plain JSON fields instead: AppSettings, CampaignVersions,
// SegmentVersions, TemplateVersionHistory, CampaignActivities, JourneyRuns,
// AppEvents, SentMessages, OtpCodes. Every value type here is already a
// plain JSON-friendly struct with no live/non-serialisable state, so a
// direct field-for-field mapping (no separate DTO type) is sufficient.
type backendSnapshot struct {
	Tables                 map[string]json.RawMessage       `json:"tables"`
	AppSettings            map[string]*StoredAppSettings    `json:"appSettings"`
	CampaignVersions       map[string][]*Campaign           `json:"campaignVersions"`
	SegmentVersions        map[string][]*Segment            `json:"segmentVersions"`
	TemplateVersionHistory map[string][]templateVersionItem `json:"templateVersionHistory"`
	CampaignActivities     map[string][]campaignActivity    `json:"campaignActivities"`
	JourneyRuns            map[string][]*journeyRun         `json:"journeyRuns"`
	AppEvents              map[string][]storedPinpointEvent `json:"appEvents"`
	SentMessages           map[string]int                   `json:"sentMessages"`
	OtpCodes               map[string]string                `json:"otpCodes"`
	AccountID              string                           `json:"accountID"`
	Region                 string                           `json:"region"`
	Version                int                              `json:"version"`
}

// persistRegistry builds an ephemeral [store.Registry] over every
// [store.Table]-backed resource this backend holds, registering the SAME
// *store.Table pointers the backend itself reads and writes through (rather
// than copying into a separate DTO type), since every persisted value type
// here is already a plain JSON-friendly struct with no live/non-serialisable
// state. It is rebuilt on every Snapshot/Restore call rather than cached as
// a long-lived field: [store.Register] panics on a duplicate name, and
// registering the same tables into two different [store.Registry] values is
// safe (Registry holds no back-reference on the Table), so a fresh registry
// scoped to the call is simpler than guarding a shared one.
func (b *InMemoryBackend) persistRegistry() *store.Registry {
	reg := store.NewRegistry()
	store.Register(reg, "apps", b.apps)
	store.Register(reg, "campaigns", b.campaigns)
	store.Register(reg, "channels", b.channels)
	store.Register(reg, "emailTemplates", b.emailTemplates)
	store.Register(reg, "endpoints", b.endpoints)
	store.Register(reg, "eventStreams", b.eventStreams)
	store.Register(reg, "exportJobs", b.exportJobs)
	store.Register(reg, "importJobs", b.importJobs)
	store.Register(reg, "inAppTemplates", b.inAppTemplates)
	store.Register(reg, "journeys", b.journeys)
	store.Register(reg, "pushTemplates", b.pushTemplates)
	store.Register(reg, "recommenders", b.recommenders)
	store.Register(reg, "segments", b.segments)
	store.Register(reg, "smsTemplates", b.smsTemplates)
	store.Register(reg, "voiceTemplates", b.voiceTemplates)

	return reg
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.persistRegistry().SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "pinpoint: failed to marshal snapshot", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:                pinpointSnapshotVersion,
		Tables:                 tables,
		AccountID:              b.accountID,
		Region:                 b.region,
		AppSettings:            b.appSettings,
		CampaignVersions:       b.campaignVersions,
		SegmentVersions:        b.segmentVersions,
		TemplateVersionHistory: b.templateVersionHistory,
		CampaignActivities:     b.campaignActivities,
		JourneyRuns:            b.journeyRuns,
		AppEvents:              b.appEvents,
		SentMessages:           b.sentMessages,
		OtpCodes:               b.otpCodes,
	}

	return persistence.MarshalSnapshot(ctx, "pinpoint", snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "pinpoint", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != pinpointSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape — that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"pinpoint: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", pinpointSnapshotVersion)

		b.registry.ResetAll()
		b.arnIndex = make(map[string]tagHolder)
		b.resetMapStateLocked()

		return nil
	}

	if err := b.persistRegistry().RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("pinpoint: restore snapshot tables: %w", err)
	}

	b.accountID = snap.AccountID
	b.region = snap.Region
	b.restoreMapStateLocked(snap)

	// Rebuild the ARN index from all restored resources.
	b.arnIndex = make(map[string]tagHolder)
	rebuildARNIndexLocked(b)

	return nil
}

// resetMapStateLocked clears every map-shaped (non-store.Table) piece of
// backend state to a non-nil empty map. Split out of Restore's discard path
// so an incompatible-version snapshot leaves the backend in the same
// pristine state as [InMemoryBackend.Reset], not with a nil map that would
// panic on first write. The caller must hold b.mu.
func (b *InMemoryBackend) resetMapStateLocked() {
	b.appSettings = make(map[string]*StoredAppSettings)
	b.campaignVersions = make(map[string][]*Campaign)
	b.segmentVersions = make(map[string][]*Segment)
	b.templateVersionHistory = make(map[string][]templateVersionItem)
	b.campaignActivities = make(map[string][]campaignActivity)
	b.journeyRuns = make(map[string][]*journeyRun)
	b.appEvents = make(map[string][]storedPinpointEvent)
	b.sentMessages = make(map[string]int)
	b.otpCodes = make(map[string]string)
}

// restoreMapStateLocked installs every map-shaped field from snap onto b,
// defaulting any nil map (e.g. an older snapshot written before a field
// existed) to a non-nil empty map. The caller must hold b.mu.
func (b *InMemoryBackend) restoreMapStateLocked(snap backendSnapshot) {
	b.appSettings = nonNilAppSettingsMap(snap.AppSettings)
	b.campaignVersions = nonNilCampaignVersionsMap(snap.CampaignVersions)
	b.segmentVersions = nonNilSegmentVersionsMap(snap.SegmentVersions)
	b.templateVersionHistory = nonNilTemplateVersionHistoryMap(snap.TemplateVersionHistory)
	b.campaignActivities = nonNilCampaignActivitiesMap(snap.CampaignActivities)
	b.journeyRuns = nonNilJourneyRunsMap(snap.JourneyRuns)
	b.appEvents = nonNilAppEventsMap(snap.AppEvents)
	b.sentMessages = nonNilSentMessagesMap(snap.SentMessages)
	b.otpCodes = nonNilOtpCodesMap(snap.OtpCodes)
}

// The nonNil*Map helpers below each default a possibly-nil restored map to a
// non-nil empty map of the same type, one per map-shaped state field. Kept
// as separate tiny generic-free functions (rather than one generic helper)
// because Go generics cannot abstract over "map[string]T for varying T" here
// without the caller repeating the type anyway, and separate named helpers
// keep restoreMapStateLocked's call sites self-documenting.
func nonNilAppSettingsMap(m map[string]*StoredAppSettings) map[string]*StoredAppSettings {
	if m == nil {
		return make(map[string]*StoredAppSettings)
	}

	return m
}

func nonNilCampaignVersionsMap(m map[string][]*Campaign) map[string][]*Campaign {
	if m == nil {
		return make(map[string][]*Campaign)
	}

	return m
}

func nonNilSegmentVersionsMap(m map[string][]*Segment) map[string][]*Segment {
	if m == nil {
		return make(map[string][]*Segment)
	}

	return m
}

func nonNilTemplateVersionHistoryMap(m map[string][]templateVersionItem) map[string][]templateVersionItem {
	if m == nil {
		return make(map[string][]templateVersionItem)
	}

	return m
}

func nonNilCampaignActivitiesMap(m map[string][]campaignActivity) map[string][]campaignActivity {
	if m == nil {
		return make(map[string][]campaignActivity)
	}

	return m
}

func nonNilJourneyRunsMap(m map[string][]*journeyRun) map[string][]*journeyRun {
	if m == nil {
		return make(map[string][]*journeyRun)
	}

	return m
}

func nonNilAppEventsMap(m map[string][]storedPinpointEvent) map[string][]storedPinpointEvent {
	if m == nil {
		return make(map[string][]storedPinpointEvent)
	}

	return m
}

func nonNilSentMessagesMap(m map[string]int) map[string]int {
	if m == nil {
		return make(map[string]int)
	}

	return m
}

func nonNilOtpCodesMap(m map[string]string) map[string]string {
	if m == nil {
		return make(map[string]string)
	}

	return m
}

// rebuildARNIndexLocked rebuilds arnIndex from all in-memory resources.
// Must be called with b.mu write lock held.
func rebuildARNIndexLocked(b *InMemoryBackend) {
	for _, v := range b.apps.All() {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.campaigns.All() {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.emailTemplates.All() {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.inAppTemplates.All() {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.journeys.All() {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.pushTemplates.All() {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.segments.All() {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.smsTemplates.All() {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.voiceTemplates.All() {
		b.arnIndex[v.ARN] = v
	}
}
