package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// cwlSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to any DTO or backendSnapshot itself would make an
// older snapshot unsafe to decode as the current shape. Restore compares this
// against the persisted value and discards (rather than attempts to partially
// decode) any mismatch -- see Restore below.
const cwlSnapshotVersion = 1

// logGroupSnapshot, logStreamSnapshot, subscriptionFilterSnapshot, and
// metricFilterSnapshot are the on-disk DTOs for the four region-qualified
// "dirty" tables (see store_setup.go / models.go): each embeds the live value
// type by value to reuse its exported-field JSON shape, then adds back the
// unexported identity metadata (region, and for streams also logGroupName and
// inline events) as ordinary exported fields so a round trip through
// json.Marshal/Unmarshal does not lose them. This is the same DTO-registry
// pattern services/sqs's persistence.go (commit 0f09d77c) uses for Queue.

type logGroupSnapshot struct {
	Region string `json:"region"`
	LogGroup
}

func logGroupSnapshotKey(s *logGroupSnapshot) string {
	return groupTableKey(s.Region, s.LogGroupName)
}

type logStreamSnapshot struct {
	Region       string            `json:"region"`
	LogGroupName string            `json:"logGroupName"`
	Events       []*OutputLogEvent `json:"events"`
	LogStream
}

func logStreamSnapshotKey(s *logStreamSnapshot) string {
	return streamTableKey(s.Region, s.LogGroupName, s.LogStreamName)
}

type subscriptionFilterSnapshot struct {
	Region string `json:"region"`
	SubscriptionFilter
}

func subscriptionFilterSnapshotKey(s *subscriptionFilterSnapshot) string {
	return subFilterTableKey(s.Region, s.LogGroupName, s.FilterName)
}

type metricFilterSnapshot struct {
	Region string `json:"region"`
	MetricFilter
}

func metricFilterSnapshotKey(s *metricFilterSnapshot) string {
	return metricFilterTableKey(s.Region, s.LogGroupName, s.FilterName)
}

// newRegionDTORegistry builds the ephemeral registry used to encode/decode the
// four region-qualified tables. It is rebuilt fresh on every Snapshot/Restore
// call (never stored on the backend) purely to reuse store's deterministic,
// type-erased JSON encoding instead of hand-rolling the marshal step.
func newRegionDTORegistry() (
	*store.Registry,
	*store.Table[logGroupSnapshot],
	*store.Table[logStreamSnapshot],
	*store.Table[subscriptionFilterSnapshot],
	*store.Table[metricFilterSnapshot],
) {
	reg := store.NewRegistry()
	groups := store.Register(reg, "groups", store.New(logGroupSnapshotKey))
	streams := store.Register(reg, "streams", store.New(logStreamSnapshotKey))
	subFilters := store.Register(reg, "subscriptionFilters", store.New(subscriptionFilterSnapshotKey))
	metricFilters := store.Register(reg, "metricFilters", store.New(metricFilterSnapshotKey))

	return reg, groups, streams, subFilters, metricFilters
}

// backendSnapshot is the top-level on-disk shape for the cloudwatchlogs backend.
//
// Tables holds one JSON-encoded array per registered table, produced by
// [store.Registry.SnapshotAll] for every "clean" table on b.registry plus the
// four region-qualified DTO tables above. Version guards against decoding a
// snapshot from an incompatible (older or newer) build of this backend as
// though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage `json:"tables"`
	AccountID string                     `json:"accountID"`
	Region    string                     `json:"region"`
	Version   int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
//
// Ephemeral, never-persisted state is deliberately excluded, matching this
// backend's behavior before Phase 3.3: Insights query results/cache
// (b.queries, b.parsedQueries, b.ephemeralRegistry) and the compiled filter
// pattern cache (b.compiledPatterns) are not part of backendSnapshot.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx,
			"cloudwatchlogs: snapshot table marshal failed", "error", err)

		return nil
	}

	dtoReg, groupDTOs, streamDTOs, subFilterDTOs, metricFilterDTOs := newRegionDTORegistry()

	for _, g := range b.groups.Snapshot() {
		groupDTOs.Put(&logGroupSnapshot{LogGroup: *g, Region: g.region})
	}

	for _, s := range b.streams.Snapshot() {
		streamDTOs.Put(&logStreamSnapshot{
			LogStream:    *s,
			Region:       s.region,
			LogGroupName: s.logGroupName,
			Events:       s.events,
		})
	}

	for _, f := range b.subscriptionFilters.Snapshot() {
		subFilterDTOs.Put(&subscriptionFilterSnapshot{SubscriptionFilter: *f, Region: f.region})
	}

	for _, f := range b.metricFilters.Snapshot() {
		metricFilterDTOs.Put(&metricFilterSnapshot{MetricFilter: *f, Region: f.region})
	}

	dtoTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx,
			"cloudwatchlogs: snapshot region-table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dtoTables)

	snap := backendSnapshot{
		Version:   cwlSnapshotVersion,
		Tables:    tables,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "cloudwatchlogs", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "cloudwatchlogs", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != cwlSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"cloudwatchlogs: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", cwlSnapshotVersion)

		b.registry.ResetAll()
		b.groups.Reset()
		b.streams.Reset()
		b.subscriptionFilters.Reset()
		b.metricFilters.Reset()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("cloudwatchlogs: restore snapshot tables: %w", err)
	}

	dtoReg, groupDTOs, streamDTOs, subFilterDTOs, metricFilterDTOs := newRegionDTORegistry()

	if err := dtoReg.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("cloudwatchlogs: restore region-qualified snapshot tables: %w", err)
	}

	liveGroups := make([]*LogGroup, 0, groupDTOs.Len())

	for _, dto := range groupDTOs.All() {
		g := dto.LogGroup
		g.region = dto.Region
		liveGroups = append(liveGroups, &g)
	}

	b.groups.Restore(liveGroups)

	liveStreams := make([]*LogStream, 0, streamDTOs.Len())

	for _, dto := range streamDTOs.All() {
		s := dto.LogStream
		s.region = dto.Region
		s.logGroupName = dto.LogGroupName
		s.events = dto.Events
		liveStreams = append(liveStreams, &s)
	}

	b.streams.Restore(liveStreams)

	liveSubFilters := make([]*SubscriptionFilter, 0, subFilterDTOs.Len())

	for _, dto := range subFilterDTOs.All() {
		f := dto.SubscriptionFilter
		f.region = dto.Region
		liveSubFilters = append(liveSubFilters, &f)
	}

	b.subscriptionFilters.Restore(liveSubFilters)

	liveMetricFilters := make([]*MetricFilter, 0, metricFilterDTOs.Len())

	for _, dto := range metricFilterDTOs.All() {
		f := dto.MetricFilter
		f.region = dto.Region
		liveMetricFilters = append(liveMetricFilters, &f)
	}

	b.metricFilters.Restore(liveMetricFilters)

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
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

	return persistence.MarshalSnapshot(ctx, "cloudwatchlogs", snap)
}

// Restore implements persistence.Persistable by restoring both the backend
// state and the handler-owned tag data.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	// Attempt to decode as the combined handlerSnapshot format first.
	var snap handlerSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "cloudwatchlogs", data, &snap); err != nil {
		return err
	}

	if err := h.restoreBackend(ctx, snap.Backend, data); err != nil {
		return err
	}

	h.restoreTags(snap.Tags)

	return nil
}

// restoreBackend restores backend state from the snapshot.
// If backendData is non-nil it came from the new combined format; otherwise the
// caller should fall back to the raw data (legacy bare-backend format).
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
// All existing tags are discarded and replaced with the snapshot values.
func (h *Handler) restoreTags(tagMap map[string]map[string]string) {
	h.tagsMu.Lock("Restore")
	defer h.tagsMu.Unlock()

	// Close existing tag collections to prevent Prometheus metric registry leaks.
	for _, t := range h.tags {
		t.Close()
	}

	// Replace with a fresh map seeded from the snapshot.
	h.tags = make(map[string]*tags.Tags, len(tagMap))

	for resourceID, kv := range tagMap {
		t := tags.New("cwl." + resourceID + ".tags")
		t.Merge(kv)
		h.tags[resourceID] = t
	}
}
