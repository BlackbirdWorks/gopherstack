package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

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
//
// Bumped 1 -> 2 (gopherstack-hjdd). Multiple registered tables' value types
// changed shape across several commits with no bump applied at the time:
//
//   - ca3afb3ca: IndexPolicy.LastUpdated (time.Time) -> LastUpdateTime
//     (int64), same wire key "lastUpdateTime". PutIndexPolicy genuinely sets
//     this to time.Now(), so a pre-fix snapshot's RFC3339 string no longer
//     unmarshals into the new int64 field at all -- an outright decode error
//     that takes down the whole restore (indexPolicies is a plain
//     store.Register table, not DTO-wrapped).
//   - 9f62f7f5d: ScheduledQuery.Arn retagged "arn" -> ScheduledQueryArn
//     "scheduledQueryArn". scheduledQueryKeyFn keys the table on exactly this
//     field, so a pre-fix snapshot's ARN silently decodes empty and every
//     restored scheduled query collides onto the same "" key -- the same
//     collapse-to-one-record class as bedrock's Flow/Prompt bug.
//   - 9f62f7f5d: ImportTask.Status retagged "status" -> "importStatus", and
//     LogAnomalyDetector.DetectorStatus retagged "detectorStatus" ->
//     AnomalyDetectorStatus "anomalyDetectorStatus". Neither field is part of
//     either table's key, so these are narrower silent losses (the status
//     field alone decodes empty) rather than a key collision.
//
// NOT bump candidates, examined and disqualified: 357edbc07's
// Delivery.CreationTime "creationTime" -> "-" only affects an internal
// bookkeeping field (DescribeDeliveries sort order) with no real-AWS wire
// counterpart -- disclosed as fabricated in its own doc comment -- so losing
// it on restore reorders a list, it does not destroy user data. 567e2c4f8's
// Anomaly field renames are moot for persistence: Anomaly lives on
// b.ephemeralRegistry, never included in backendSnapshot at all.
//
// Also NOT a bump (gopherstack-gqxy0, fixed this pass): CWLDestination.CreatedAt
// and Transformer.CreatedAt gained real tags in place (both real wire members
// per the SDK, previously json:"-" purely by omission -- see models.go), and
// DeliveryDestination.CreatedAt/DeliverySource.CreatedAt/CWLIntegration.CreatedAt/
// ImportTask.ImportRoleArn (the latter the "filed separately" bug this comment
// used to note) each moved to a persisted twin under the same table name
// (persistence.go's deliveryDestinationSnapshot et al.). Every case is purely
// additive: an old snapshot decodes the previously-dropped field as zero/empty,
// exactly as before, matching TestSnapshotVersionGuard's tolerance for this
// shape (see opensearch's dbc50bc49/c5475e9a6 precedent for the same judgment).
const cwlSnapshotVersion = 2

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

// deliveryDestinationSnapshot, deliverySourceSnapshot, cwlIntegrationSnapshot,
// and importTaskSnapshot are persisted twins for the four tables
// registerDirtyDTOTables (store_setup.go) deliberately keeps off b.registry:
// each duplicates its live type's field set, but gives the one field the
// wire correctly excludes (json:"-") a real tag so it survives
// Table.Snapshot's json.Marshal round trip (gopherstack-gqxy0).

type deliveryDestinationSnapshot struct {
	CreatedAt               time.Time         `json:"createdAt,omitzero"`
	Tags                    map[string]string `json:"tags,omitempty"`
	Name                    string            `json:"name"`
	Arn                     string            `json:"arn"`
	OutputFormat            string            `json:"outputFormat,omitempty"`
	TargetArn               string            `json:"deliveryDestinationConfiguration,omitempty"`
	DeliveryDestinationType string            `json:"deliveryDestinationType,omitempty"`
	Policy                  string            `json:"policy,omitempty"`
}

func deliveryDestinationSnapshotKey(v *deliveryDestinationSnapshot) string { return v.Name }

func toDeliveryDestinationSnapshot(d *DeliveryDestination) *deliveryDestinationSnapshot {
	return &deliveryDestinationSnapshot{
		CreatedAt:               d.CreatedAt,
		Tags:                    d.Tags,
		Name:                    d.Name,
		Arn:                     d.Arn,
		OutputFormat:            d.OutputFormat,
		TargetArn:               d.TargetArn,
		DeliveryDestinationType: d.DeliveryDestinationType,
		Policy:                  d.Policy,
	}
}

func fromDeliveryDestinationSnapshot(v *deliveryDestinationSnapshot) *DeliveryDestination {
	return &DeliveryDestination{
		CreatedAt:               v.CreatedAt,
		Tags:                    v.Tags,
		Name:                    v.Name,
		Arn:                     v.Arn,
		OutputFormat:            v.OutputFormat,
		TargetArn:               v.TargetArn,
		DeliveryDestinationType: v.DeliveryDestinationType,
		Policy:                  v.Policy,
	}
}

type deliverySourceSnapshot struct {
	CreatedAt    time.Time         `json:"createdAt,omitzero"`
	Tags         map[string]string `json:"tags,omitempty"`
	Name         string            `json:"name"`
	Arn          string            `json:"arn"`
	LogType      string            `json:"logType,omitempty"`
	Service      string            `json:"service,omitempty"`
	ResourceArns []string          `json:"resourceArns,omitempty"`
}

func deliverySourceSnapshotKey(v *deliverySourceSnapshot) string { return v.Name }

func toDeliverySourceSnapshot(s *DeliverySource) *deliverySourceSnapshot {
	return &deliverySourceSnapshot{
		CreatedAt:    s.CreatedAt,
		Tags:         s.Tags,
		Name:         s.Name,
		Arn:          s.Arn,
		LogType:      s.LogType,
		Service:      s.Service,
		ResourceArns: s.ResourceArns,
	}
}

func fromDeliverySourceSnapshot(v *deliverySourceSnapshot) *DeliverySource {
	return &DeliverySource{
		CreatedAt:    v.CreatedAt,
		Tags:         v.Tags,
		Name:         v.Name,
		Arn:          v.Arn,
		LogType:      v.LogType,
		Service:      v.Service,
		ResourceArns: v.ResourceArns,
	}
}

type cwlIntegrationSnapshot struct {
	CreatedAt                time.Time                 `json:"createdAt,omitzero"`
	OpenSearchResourceConfig *OpenSearchResourceConfig `json:"openSearchResourceConfig,omitempty"`
	Name                     string                    `json:"integrationName"`
	Type                     string                    `json:"integrationType"`
	Status                   string                    `json:"integrationStatus"`
}

func cwlIntegrationSnapshotKey(v *cwlIntegrationSnapshot) string { return v.Name }

func toCWLIntegrationSnapshot(i *CWLIntegration) *cwlIntegrationSnapshot {
	return &cwlIntegrationSnapshot{
		CreatedAt:                i.CreatedAt,
		OpenSearchResourceConfig: i.OpenSearchResourceConfig,
		Name:                     i.Name,
		Type:                     i.Type,
		Status:                   i.Status,
	}
}

func fromCWLIntegrationSnapshot(v *cwlIntegrationSnapshot) *CWLIntegration {
	return &CWLIntegration{
		CreatedAt:                v.CreatedAt,
		OpenSearchResourceConfig: v.OpenSearchResourceConfig,
		Name:                     v.Name,
		Type:                     v.Type,
		Status:                   v.Status,
	}
}

type importTaskSnapshot struct {
	ImportID             string `json:"importId"`
	ImportSourceArn      string `json:"importSourceArn"`
	ImportRoleArn        string `json:"importRoleArn"`
	ImportDestinationArn string `json:"importDestinationArn"`
	Status               string `json:"importStatus"`
	CreationTime         int64  `json:"creationTime"`
	LastUpdatedTime      int64  `json:"lastUpdatedTime"`
}

func importTaskSnapshotKey(v *importTaskSnapshot) string { return v.ImportID }

func toImportTaskSnapshot(t *ImportTask) *importTaskSnapshot {
	return &importTaskSnapshot{
		ImportID:             t.ImportID,
		ImportSourceArn:      t.ImportSourceArn,
		ImportRoleArn:        t.ImportRoleArn,
		ImportDestinationArn: t.ImportDestinationArn,
		Status:               t.Status,
		CreationTime:         t.CreationTime,
		LastUpdatedTime:      t.LastUpdatedTime,
	}
}

func fromImportTaskSnapshot(v *importTaskSnapshot) *ImportTask {
	return &ImportTask{
		ImportID:             v.ImportID,
		ImportSourceArn:      v.ImportSourceArn,
		ImportRoleArn:        v.ImportRoleArn,
		ImportDestinationArn: v.ImportDestinationArn,
		Status:               v.Status,
		CreationTime:         v.CreationTime,
		LastUpdatedTime:      v.LastUpdatedTime,
	}
}

// newDirtyDTORegistry builds the ephemeral registry used to encode/decode the
// four tables registerDirtyDTOTables (store_setup.go) keeps off b.registry.
// Like newRegionDTORegistry above, it is rebuilt fresh on every Snapshot/
// Restore call purely to reuse store's deterministic, type-erased JSON
// encoding. Each DTO is registered under the same table name these tables
// used when they were still on b.registry, so an existing snapshot's entries
// still line up (gopherstack-gqxy0).
func newDirtyDTORegistry() (
	*store.Registry,
	*store.Table[deliveryDestinationSnapshot],
	*store.Table[deliverySourceSnapshot],
	*store.Table[cwlIntegrationSnapshot],
	*store.Table[importTaskSnapshot],
) {
	reg := store.NewRegistry()
	deliveryDestDTOs := store.Register(reg, "deliveryDestinations", store.New(deliveryDestinationSnapshotKey))
	deliverySrcDTOs := store.Register(reg, "deliverySources", store.New(deliverySourceSnapshotKey))
	integrationDTOs := store.Register(reg, "integrations", store.New(cwlIntegrationSnapshotKey))
	importTaskDTOs := store.Register(reg, "importTasks", store.New(importTaskSnapshotKey))

	return reg, deliveryDestDTOs, deliverySrcDTOs, integrationDTOs, importTaskDTOs
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

	dirtyReg, deliveryDestDTOs, deliverySrcDTOs, integrationDTOs, importTaskDTOs := newDirtyDTORegistry()

	for _, d := range b.deliveryDestinations.Snapshot() {
		deliveryDestDTOs.Put(toDeliveryDestinationSnapshot(d))
	}

	for _, s := range b.deliverySources.Snapshot() {
		deliverySrcDTOs.Put(toDeliverySourceSnapshot(s))
	}

	for _, i := range b.integrations.Snapshot() {
		integrationDTOs.Put(toCWLIntegrationSnapshot(i))
	}

	for _, t := range b.importTasks.Snapshot() {
		importTaskDTOs.Put(toImportTaskSnapshot(t))
	}

	dirtyTables, err := dirtyReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx,
			"cloudwatchlogs: snapshot dirty-table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dirtyTables)

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
		b.deliveryDestinations.Reset()
		b.deliverySources.Reset()
		b.integrations.Reset()
		b.importTasks.Reset()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("cloudwatchlogs: restore snapshot tables: %w", err)
	}

	dtoReg, groupDTOs, streamDTOs, subFilterDTOs, metricFilterDTOs := newRegionDTORegistry()

	if err := dtoReg.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("cloudwatchlogs: restore region-qualified snapshot tables: %w", err)
	}

	restoreRegionDTOTables(b, groupDTOs, streamDTOs, subFilterDTOs, metricFilterDTOs)

	dirtyReg, deliveryDestDTOs, deliverySrcDTOs, integrationDTOs, importTaskDTOs := newDirtyDTORegistry()

	if err := dirtyReg.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("cloudwatchlogs: restore dirty snapshot tables: %w", err)
	}

	restoreDirtyDTOTables(b, deliveryDestDTOs, deliverySrcDTOs, integrationDTOs, importTaskDTOs)

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// restoreRegionDTOTables converts each of the four region-qualified DTO tables
// newRegionDTORegistry builds back into its live type and restores it onto b,
// split out of Restore to keep it under funlen's statement budget (this repo
// bans a funlen nolint).
func restoreRegionDTOTables(
	b *InMemoryBackend,
	groupDTOs *store.Table[logGroupSnapshot],
	streamDTOs *store.Table[logStreamSnapshot],
	subFilterDTOs *store.Table[subscriptionFilterSnapshot],
	metricFilterDTOs *store.Table[metricFilterSnapshot],
) {
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
}

// restoreDirtyDTOTables converts each of the four DTO tables newDirtyDTORegistry
// builds back into its live type and restores it onto b, split out of Restore
// to keep it under funlen's statement budget (this repo bans a funlen nolint).
func restoreDirtyDTOTables(
	b *InMemoryBackend,
	deliveryDestDTOs *store.Table[deliveryDestinationSnapshot],
	deliverySrcDTOs *store.Table[deliverySourceSnapshot],
	integrationDTOs *store.Table[cwlIntegrationSnapshot],
	importTaskDTOs *store.Table[importTaskSnapshot],
) {
	liveDeliveryDests := make([]*DeliveryDestination, 0, deliveryDestDTOs.Len())
	for _, dto := range deliveryDestDTOs.All() {
		liveDeliveryDests = append(liveDeliveryDests, fromDeliveryDestinationSnapshot(dto))
	}

	b.deliveryDestinations.Restore(liveDeliveryDests)

	liveDeliverySrcs := make([]*DeliverySource, 0, deliverySrcDTOs.Len())
	for _, dto := range deliverySrcDTOs.All() {
		liveDeliverySrcs = append(liveDeliverySrcs, fromDeliverySourceSnapshot(dto))
	}

	b.deliverySources.Restore(liveDeliverySrcs)

	liveIntegrations := make([]*CWLIntegration, 0, integrationDTOs.Len())
	for _, dto := range integrationDTOs.All() {
		liveIntegrations = append(liveIntegrations, fromCWLIntegrationSnapshot(dto))
	}

	b.integrations.Restore(liveIntegrations)

	liveImportTasks := make([]*ImportTask, 0, importTaskDTOs.Len())
	for _, dto := range importTaskDTOs.All() {
		liveImportTasks = append(liveImportTasks, fromImportTaskSnapshot(dto))
	}

	b.importTasks.Restore(liveImportTasks)
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
