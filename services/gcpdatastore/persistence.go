package gcpdatastore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/datastore/admin/apiv1/adminpb"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type entitySnap struct {
	Created time.Time       `json:"created"`
	Updated time.Time       `json:"updated"`
	Entity  json.RawMessage `json:"entity"`
	Version int64           `json:"version"`
}

type backendSnapshot struct {
	LastCommit time.Time                  `json:"lastCommit"`
	Entities   map[string]entitySnap      `json:"entities"`
	UsedIDs    map[string][]int64         `json:"usedIds"`
	Indexes    map[string]json.RawMessage `json:"indexes"`
	Operations map[string]json.RawMessage `json:"operations"`
	NextIndex  int64                      `json:"nextIndex"`
	Version    int                        `json:"version"`
}

// Snapshot serialises the current state of every entity, reserved ids, indexes and operations.
// Version history and open transactions are not persisted.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version:    snapshotVersion,
		LastCommit: b.lastCommit,
		NextIndex:  b.nextIndex,
		Entities:   make(map[string]entitySnap, len(b.ents)),
		UsedIDs:    make(map[string][]int64, len(b.used)),
		Indexes:    make(map[string]json.RawMessage, len(b.indexes)),
		Operations: b.ops.Snapshot(),
	}

	for ks, rec := range b.ents {
		ver := rec.cur()
		if ver == nil {
			continue
		}

		e := ver.ent

		raw, err := protojson.Marshal(e)
		if err != nil {
			logger.Load(ctx).WarnContext(ctx, "gcpdatastore: skipping unmarshalable entity", "error", err)

			continue
		}

		snap.Entities[ks] = entitySnap{Entity: raw, Created: ver.created, Updated: ver.at, Version: ver.version}
	}

	for p, ids := range b.used {
		for id := range ids {
			snap.UsedIDs[p] = append(snap.UsedIDs[p], id)
		}
	}

	for k, idx := range b.indexes {
		if raw, err := protojson.Marshal(idx); err == nil {
			snap.Indexes[k] = raw
		}
	}

	return persistence.MarshalSnapshot(ctx, "gcpdatastore", snap)
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpdatastore", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpdatastore: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	ents := make(map[string]*entRec, len(snap.Entities))

	for ks, es := range snap.Entities {
		e := &datastorepb.Entity{}
		if err := opts.Unmarshal(es.Entity, e); err != nil {
			return fmt.Errorf("gcpdatastore: restore entity: %w", err)
		}

		ents[ks] = &entRec{
			key:  e.GetKey(),
			vers: []entVer{{at: es.Updated, created: es.Created, ent: e, version: es.Version}},
		}
	}

	indexes := make(map[string]*adminpb.Index, len(snap.Indexes))

	for k, raw := range snap.Indexes {
		idx := &adminpb.Index{}
		if err := opts.Unmarshal(raw, idx); err != nil {
			return fmt.Errorf("gcpdatastore: restore index: %w", err)
		}

		indexes[k] = idx
	}

	if err := b.ops.Restore(snap.Operations); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.ents, b.indexes = ents, indexes
	b.txns = make(map[string]*txnRec)
	b.changes = nil
	b.lastCommit, b.nextIndex = snap.LastCommit, snap.NextIndex
	b.used = make(map[string]map[int64]struct{}, len(snap.UsedIDs))

	for p, ids := range snap.UsedIDs {
		m := make(map[int64]struct{}, len(ids))
		for _, id := range ids {
			m[id] = struct{}{}
		}

		b.used[p] = m
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpdatastore: restore snapshot: %w", err)
	}

	return nil
}
