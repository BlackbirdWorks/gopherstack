package s3control

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// s3controlSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to a DTO type or backendSnapshot itself would
// make an older snapshot unsafe to decode as the current shape. Restore
// compares this against the persisted value and discards (ResetAll, not a
// partial decode) any mismatch -- see Restore. The pre-Phase-3.3 snapshot
// format had no version field at all, so an old snapshot decodes with
// Version == 0, which is guaranteed to mismatch s3controlSnapshotVersion and
// is discarded the same way any other incompatible snapshot is.
const s3controlSnapshotVersion = 1

// mrapRequestSnapshot and accessPointPABSnapshot are DTOs used only for
// Snapshot/Restore of the "dirty" tables -- see store_setup.go's file doc
// comment for why mrapRequests/accessPointPABs can't be registered directly
// on b.registry. Each wraps the live value alongside the identity field that
// value intentionally excludes from JSON (`json:"-"`), so the identity
// survives the round trip.
type mrapRequestSnapshot struct {
	Token   string                        `json:"token"`
	Request MultiRegionAccessPointRequest `json:"request"`
}

func mrapRequestSnapshotKey(v *mrapRequestSnapshot) string { return v.Token }

type accessPointPABSnapshot struct {
	AccountID string            `json:"accountID"`
	APName    string            `json:"apName"`
	PAB       PublicAccessBlock `json:"pab"`
}

func accessPointPABSnapshotKey(v *accessPointPABSnapshot) string {
	return v.AccountID + ":" + v.APName
}

// newDirtyDTORegistry builds the ephemeral DTO [store.Registry] shared by
// Snapshot and Restore for the tables that can't be registered on
// b.registry directly (see store_setup.go).
func newDirtyDTORegistry() (
	*store.Registry,
	*store.Table[mrapRequestSnapshot],
	*store.Table[accessPointPABSnapshot],
) {
	dtoReg := store.NewRegistry()
	mrapDTOs := store.Register(dtoReg, "mrapRequests", store.New(mrapRequestSnapshotKey))
	pabDTOs := store.Register(dtoReg, "accessPointPABs", store.New(accessPointPABSnapshotKey))

	return dtoReg, mrapDTOs, pabDTOs
}

// backendSnapshot is the top-level on-disk shape for the S3 Control backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// merging b.registry.SnapshotAll() (the "clean" tables) with the ephemeral
// DTO registry's SnapshotAll() (the "dirty" tables: mrapRequests,
// accessPointPABs). Version guards against decoding a snapshot from an
// incompatible (older or newer) build of this backend as though it were the
// current shape; see Restore.
type backendSnapshot struct {
	Tables map[string]json.RawMessage `json:"tables"`
	// batch2 additions (raw, non-*T maps -- never converted to store.Table)
	BucketReplication     map[string]string            `json:"bucketReplication"`
	StorageLensConfigs    map[string]string            `json:"storageLensConfigs"`
	StorageLensConfigTags map[string]TagSet            `json:"storageLensConfigTags"`
	ResourceTags          map[string]map[string]string `json:"resourceTags"`
	AccessPointPolicies   map[string]string            `json:"accessPointPolicies"`
	Version               int                          `json:"version"`
	NextID                int64                        `json:"nextID"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "s3control: snapshot table marshal failed", "error", err)

		return nil
	}

	dtoReg, mrapDTOs, pabDTOs := newDirtyDTORegistry()

	for _, v := range b.mrapRequests.Snapshot() {
		mrapDTOs.Put(&mrapRequestSnapshot{Token: v.Token, Request: *v})
	}

	for _, v := range b.accessPointPABs.Snapshot() {
		pabDTOs.Put(&accessPointPABSnapshot{AccountID: v.AccountID, APName: v.APName, PAB: *v})
	}

	dirtyTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "s3control: snapshot DTO table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dirtyTables)

	snap := backendSnapshot{
		Version:               s3controlSnapshotVersion,
		Tables:                tables,
		BucketReplication:     cloneMapStr(b.bucketReplication),
		StorageLensConfigs:    cloneMapStr(b.storageLensConfigs),
		StorageLensConfigTags: cloneMapTagSet(b.storageLensConfigTags),
		ResourceTags:          cloneMapResourceTags(b.resourceTags),
		AccessPointPolicies:   cloneMapStr(b.accessPointPolicies),
		NextID:                b.nextID,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "s3control: failed to snapshot backend", "error", err)

		return nil
	}

	return data
}

func cloneMapStr(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	maps.Copy(out, m)

	return out
}

func cloneMapTagSet(m map[string]TagSet) map[string]TagSet {
	out := make(map[string]TagSet, len(m))
	for k, v := range m {
		cp := make(TagSet, len(v))
		maps.Copy(cp, v)
		out[k] = cp
	}

	return out
}

func cloneMapResourceTags(m map[string]map[string]string) map[string]map[string]string {
	out := make(map[string]map[string]string, len(m))
	for k, v := range m {
		cp := make(map[string]string, len(v))
		maps.Copy(cp, v)
		out[k] = cp
	}

	return out
}

func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.BucketReplication == nil {
		snap.BucketReplication = make(map[string]string)
	}

	if snap.StorageLensConfigs == nil {
		snap.StorageLensConfigs = make(map[string]string)
	}

	if snap.StorageLensConfigTags == nil {
		snap.StorageLensConfigTags = make(map[string]TagSet)
	}

	if snap.ResourceTags == nil {
		snap.ResourceTags = make(map[string]map[string]string)
	}

	if snap.AccessPointPolicies == nil {
		snap.AccessPointPolicies = make(map[string]string)
	}
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "s3control", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != s3controlSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"s3control: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", s3controlSnapshotVersion)

		b.resetTablesLocked()
		b.bucketReplication = make(map[string]string)
		b.storageLensConfigs = make(map[string]string)
		b.storageLensConfigTags = make(map[string]TagSet)
		b.resourceTags = make(map[string]map[string]string)
		b.accessPointPolicies = make(map[string]string)
		b.nextID = 0

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("s3control: restore snapshot tables: %w", err)
	}

	if err := b.restoreDirtyTablesLocked(snap.Tables); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.bucketReplication = snap.BucketReplication
	b.storageLensConfigs = snap.StorageLensConfigs
	b.storageLensConfigTags = snap.StorageLensConfigTags
	b.resourceTags = snap.ResourceTags
	b.accessPointPolicies = snap.AccessPointPolicies
	b.nextID = snap.NextID

	return nil
}

// restoreDirtyTablesLocked restores the "dirty" tables (mrapRequests,
// accessPointPABs) from tables via the ephemeral DTO registry, converting
// each DTO back to its live type. The caller MUST hold b.mu for writing.
func (b *InMemoryBackend) restoreDirtyTablesLocked(tables map[string]json.RawMessage) error {
	dtoReg, mrapDTOs, pabDTOs := newDirtyDTORegistry()

	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("s3control: restore snapshot DTO tables: %w", err)
	}

	liveMRAPRequests := make([]*MultiRegionAccessPointRequest, 0, mrapDTOs.Len())

	for _, dto := range mrapDTOs.All() {
		req := dto.Request
		req.Token = dto.Token
		liveMRAPRequests = append(liveMRAPRequests, &req)
	}

	b.mrapRequests.Restore(liveMRAPRequests)

	livePABs := make([]*PublicAccessBlock, 0, pabDTOs.Len())

	for _, dto := range pabDTOs.All() {
		pab := dto.PAB
		pab.AccountID = dto.AccountID
		pab.APName = dto.APName
		livePABs = append(livePABs, &pab)
	}

	b.accessPointPABs.Restore(livePABs)

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
