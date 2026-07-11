package redshiftdata

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// redshiftdataSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to backendSnapshot (or a value type it
// holds) would make an older snapshot unsafe to decode as the current shape.
// Restore compares this against the persisted value and discards (reset, not
// a partial decode) any mismatch -- see Restore. Snapshots taken before this
// field existed decode with Version == 0, which is guaranteed to mismatch
// redshiftdataSnapshotVersion and is discarded the same way any other
// incompatible snapshot is.
const redshiftdataSnapshotVersion = 1

// regionSnapshot holds the serialized state for a single region.
//
// The statements map and RingBuf slice are the two internal maps/slices this
// backend owns. Both stay a raw map/slice (not a [pkgs/store.Table]) rather
// than participating in a [pkgs/store.Registry]: RingBuf's whole purpose is
// tracking FIFO insertion order for the per-region 1000-statement eviction
// cap, and regions are created lazily by storeFor() rather than known up
// front, so there is no fixed set of tables to register once at construction
// time the way [pkgs/store.Registry] expects. See persistence-audit note in
// backend.go's regionStore doc.
type regionSnapshot struct {
	Statements map[string]*Statement `json:"statements"`
	RingBuf    []string              `json:"ringBuf"`
}

type backendSnapshot struct {
	Stores    map[string]*regionSnapshot `json:"stores"`
	AccountID string                     `json:"accountID"`
	Region    string                     `json:"region"`
	Version   int                        `json:"version"`
}

// Snapshot serializes the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	storesSnap := make(map[string]*regionSnapshot, len(b.stores))

	for region, store := range b.stores {
		ringCopy := make([]string, store.ringLen)
		for i := range store.ringLen {
			ringCopy[i] = store.ringBuf[(store.ringHead+i)%maxStatementHistory]
		}

		stmtsCopy := make(map[string]*Statement, len(store.statements))
		for k, v := range store.statements {
			stmtsCopy[k] = cloneStatement(v)
		}

		storesSnap[region] = &regionSnapshot{
			Statements: stmtsCopy,
			RingBuf:    ringCopy,
		}
	}

	snap := backendSnapshot{
		Version:   redshiftdataSnapshotVersion,
		Stores:    storesSnap,
		AccountID: b.accountID,
		Region:    b.defaultRegion,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "redshiftdata: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "redshiftdata", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != redshiftdataSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"redshiftdata: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", redshiftdataSnapshotVersion)

		b.stores = make(map[string]*regionStore)

		return nil
	}

	b.accountID = snap.AccountID
	b.defaultRegion = snap.Region
	b.stores = make(map[string]*regionStore, len(snap.Stores))

	for region, rs := range snap.Stores {
		if rs.Statements == nil {
			rs.Statements = make(map[string]*Statement)
		}

		store := &regionStore{
			statements: rs.Statements,
		}

		n := len(rs.RingBuf)
		if n > maxStatementHistory {
			rs.RingBuf = rs.RingBuf[n-maxStatementHistory:]
		}

		for _, id := range rs.RingBuf {
			if _, ok := store.statements[id]; ok {
				store.ringBuf[store.ringLen] = id
				store.ringLen++
			}
		}

		b.stores[region] = store
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Before this, Handler had no Snapshot/Restore of its own, so redshiftdata
// was silently excluded from Gopherstack's persistence.Manager (it type-
// asserts each service's Registerable to persistence.Persistable) even
// though InMemoryBackend fully implemented both methods -- a dead-wiring
// bug fixed as part of the Phase 3.3 rollout.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
