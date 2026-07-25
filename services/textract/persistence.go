package textract

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// textractSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to a DTO type or backendSnapshot itself would
// make an older snapshot unsafe to decode as the current shape. Restore
// compares this against the persisted value and discards (resetTablesLocked,
// not a partial decode) any mismatch -- see Restore. The pre-Phase-3.3
// snapshot format had no version field at all, so an old snapshot decodes
// with Version == 0, which is guaranteed to mismatch textractSnapshotVersion
// and is discarded the same way any other incompatible snapshot is.
//
// Bumped 1 -> 2: AdapterVersion.EvaluationMetrics changed from a single
// struct to a []AdapterVersionEvaluationMetric list, LendingField's
// Type/ValueDetection fields changed shape (Type string, ValueDetections
// list), and PageClassification's PageType/PageNumber switched from
// []LendingDetection to []Prediction -- all field-diff fixes against the
// real SDK's wire shapes. A v1 snapshot decoded against these v2 DTOs would
// silently corrupt or drop data instead of erroring.
const textractSnapshotVersion = 2

// jobSnapshot, expenseJobSnapshot, lendingJobSnapshot, adapterSnapshot, and
// adapterVersionSnapshot are DTOs used only for Snapshot/Restore of the five
// store.Table-backed resource fields -- see store_setup.go's file doc
// comment for why they can't be registered directly on a persistent
// registry. Each wraps the live value alongside the Region field that value
// intentionally excludes from JSON (`json:"-"`), so it survives the round
// trip.
type jobSnapshot struct {
	Region string      `json:"region"`
	Job    DocumentJob `json:"job"`
}

func jobSnapshotKey(v *jobSnapshot) string { return regionKey(v.Region, v.Job.JobID) }

type expenseJobSnapshot struct {
	Region string     `json:"region"`
	Job    ExpenseJob `json:"job"`
}

func expenseJobSnapshotKey(v *expenseJobSnapshot) string { return regionKey(v.Region, v.Job.JobID) }

type lendingJobSnapshot struct {
	Region string     `json:"region"`
	Job    LendingJob `json:"job"`
}

func lendingJobSnapshotKey(v *lendingJobSnapshot) string { return regionKey(v.Region, v.Job.JobID) }

type adapterSnapshot struct {
	Region  string  `json:"region"`
	Adapter Adapter `json:"adapter"`
}

func adapterSnapshotKey(v *adapterSnapshot) string {
	return regionKey(v.Region, v.Adapter.AdapterID)
}

type adapterVersionSnapshot struct {
	Region  string         `json:"region"`
	Version AdapterVersion `json:"version"`
}

func adapterVersionSnapshotKey(v *adapterVersionSnapshot) string {
	return regionKey(v.Region, adapterVersionKey(v.Version.AdapterID, v.Version.AdapterVersion))
}

// dtoRegistry bundles the ephemeral DTO [store.Registry] shared by Snapshot
// and Restore for the five tables that can't be registered on a persistent
// registry (see store_setup.go).
type dtoRegistry struct {
	reg             *store.Registry
	jobs            *store.Table[jobSnapshot]
	expenseJobs     *store.Table[expenseJobSnapshot]
	lendingJobs     *store.Table[lendingJobSnapshot]
	adapters        *store.Table[adapterSnapshot]
	adapterVersions *store.Table[adapterVersionSnapshot]
}

// newDTORegistry builds a fresh, empty [dtoRegistry]. It is called anew by
// both Snapshot and Restore -- never held on InMemoryBackend -- since it
// exists only to give the five dirty tables a real-field home for Region
// during (de)serialization.
func newDTORegistry() *dtoRegistry {
	reg := store.NewRegistry()

	return &dtoRegistry{
		reg:             reg,
		jobs:            store.Register(reg, "jobs", store.New(jobSnapshotKey)),
		expenseJobs:     store.Register(reg, "expenseJobs", store.New(expenseJobSnapshotKey)),
		lendingJobs:     store.Register(reg, "lendingJobs", store.New(lendingJobSnapshotKey)),
		adapters:        store.Register(reg, "adapters", store.New(adapterSnapshotKey)),
		adapterVersions: store.Register(reg, "adapterVersions", store.New(adapterVersionSnapshotKey)),
	}
}

// backendSnapshot is the top-level on-disk shape for the Textract backend.
//
// Tables holds one JSON-encoded array per DTO table name, produced by the
// ephemeral [dtoRegistry]'s SnapshotAll(). Version guards against decoding a
// snapshot from an incompatible (older or newer) build of this backend as
// though it were the current shape; see Restore.
//
// ClientTokenToJobID, AdapterClientTokenToID, ExpenseClientTokenToJobID, and
// LendingClientTokenToJobID are left as plain region-nested maps: their
// values are strings, not *T, so they do not fit store.Table's
// keyed-by-identity-value shape and are unaffected by this conversion.
type backendSnapshot struct {
	Tables                    map[string]json.RawMessage   `json:"tables"`
	ClientTokenToJobID        map[string]map[string]string `json:"clientTokenToJobId,omitempty"`
	AdapterClientTokenToID    map[string]map[string]string `json:"adapterClientTokenToId,omitempty"`
	ExpenseClientTokenToJobID map[string]map[string]string `json:"expenseClientTokenToJobId,omitempty"`
	LendingClientTokenToJobID map[string]map[string]string `json:"lendingClientTokenToJobId,omitempty"`
	Version                   int                          `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	dto := newDTORegistry()

	for _, j := range b.jobs.Snapshot() {
		dto.jobs.Put(&jobSnapshot{Region: j.Region, Job: *cloneJob(j)})
	}

	for _, j := range b.expenseJobs.Snapshot() {
		dto.expenseJobs.Put(&expenseJobSnapshot{Region: j.Region, Job: *cloneExpenseJob(j)})
	}

	for _, j := range b.lendingJobs.Snapshot() {
		dto.lendingJobs.Put(&lendingJobSnapshot{Region: j.Region, Job: *cloneLendingJob(j)})
	}

	for _, a := range b.adapters.Snapshot() {
		dto.adapters.Put(&adapterSnapshot{Region: a.Region, Adapter: *cloneAdapter(a)})
	}

	for _, av := range b.adapterVersions.Snapshot() {
		dto.adapterVersions.Put(&adapterVersionSnapshot{Region: av.Region, Version: *cloneAdapterVersion(av)})
	}

	tables, err := dto.reg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "textract: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:                   textractSnapshotVersion,
		Tables:                    tables,
		ClientTokenToJobID:        cloneRegionTokenMap(b.clientTokenToJobID),
		AdapterClientTokenToID:    cloneRegionTokenMap(b.adapterClientTokenToID),
		ExpenseClientTokenToJobID: cloneRegionTokenMap(b.expenseClientTokenToJobID),
		LendingClientTokenToJobID: cloneRegionTokenMap(b.lendingClientTokenToJobID),
	}

	return persistence.MarshalSnapshot(ctx, "textract", snap)
}

// cloneRegionTokenMap returns a deep copy of a region-nested
// clientToken→resourceID map, so Snapshot doesn't alias live backend state.
func cloneRegionTokenMap(m map[string]map[string]string) map[string]map[string]string {
	cp := make(map[string]map[string]string, len(m))

	for region, regionTokens := range m {
		regionCopy := make(map[string]string, len(regionTokens))
		maps.Copy(regionCopy, regionTokens)
		cp[region] = regionCopy
	}

	return cp
}

// restoredTokenMap returns m, or a fresh empty map if m is nil -- Restore
// uses this so a snapshot predating a given token map (or one that never
// populated it) still leaves the backend with a non-nil map to write into.
func restoredTokenMap(m map[string]map[string]string) map[string]map[string]string {
	if m != nil {
		return m
	}

	return make(map[string]map[string]string)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "textract", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != textractSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"textract: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", textractSnapshotVersion)

		b.resetTablesLocked()
		b.resetClientTokenMapsLocked()

		return nil
	}

	dto := newDTORegistry()
	if err := dto.reg.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("textract: restore snapshot tables: %w", err)
	}

	liveJobs := make([]*DocumentJob, 0, dto.jobs.Len())

	for _, s := range dto.jobs.All() {
		j := s.Job
		j.Region = s.Region
		liveJobs = append(liveJobs, &j)
	}

	b.jobs.Restore(liveJobs)

	liveExpenseJobs := make([]*ExpenseJob, 0, dto.expenseJobs.Len())

	for _, s := range dto.expenseJobs.All() {
		j := s.Job
		j.Region = s.Region
		liveExpenseJobs = append(liveExpenseJobs, &j)
	}

	b.expenseJobs.Restore(liveExpenseJobs)

	liveLendingJobs := make([]*LendingJob, 0, dto.lendingJobs.Len())

	for _, s := range dto.lendingJobs.All() {
		j := s.Job
		j.Region = s.Region
		liveLendingJobs = append(liveLendingJobs, &j)
	}

	b.lendingJobs.Restore(liveLendingJobs)

	liveAdapters := make([]*Adapter, 0, dto.adapters.Len())

	for _, s := range dto.adapters.All() {
		a := s.Adapter
		a.Region = s.Region
		liveAdapters = append(liveAdapters, &a)
	}

	b.adapters.Restore(liveAdapters)

	liveAdapterVersions := make([]*AdapterVersion, 0, dto.adapterVersions.Len())

	for _, s := range dto.adapterVersions.All() {
		av := s.Version
		av.Region = s.Region
		liveAdapterVersions = append(liveAdapterVersions, &av)
	}

	b.adapterVersions.Restore(liveAdapterVersions)

	b.clientTokenToJobID = restoredTokenMap(snap.ClientTokenToJobID)
	b.adapterClientTokenToID = restoredTokenMap(snap.AdapterClientTokenToID)
	b.expenseClientTokenToJobID = restoredTokenMap(snap.ExpenseClientTokenToJobID)
	b.lendingClientTokenToJobID = restoredTokenMap(snap.LendingClientTokenToJobID)

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
