package gcpfirestore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	LastCommit time.Time                  `json:"lastCommit"`
	Docs       map[string]json.RawMessage `json:"docs"`
	Version    int                        `json:"version"`
}

// Snapshot serialises the current state of every document; version history and transactions are not persisted.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version:    snapshotVersion,
		LastCommit: b.lastCommit,
		Docs:       make(map[string]json.RawMessage, len(b.docs)),
	}

	for name, rec := range b.docs {
		d := rec.cur()
		if d == nil {
			continue
		}

		raw, err := protojson.Marshal(d)
		if err != nil {
			logger.Load(ctx).
				WarnContext(ctx, "gcpfirestore: skipping unmarshalable document", "name", name, "error", err)

			continue
		}

		snap.Docs[name] = raw
	}

	return persistence.MarshalSnapshot(ctx, "gcpfirestore", snap)
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpfirestore", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpfirestore: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		b.docs = make(map[string]*docRec)
		b.notifyLocked()

		return nil
	}

	docs := make(map[string]*docRec, len(snap.Docs))

	for name, raw := range snap.Docs {
		d := &firestorepb.Document{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, d); err != nil {
			return fmt.Errorf("gcpfirestore: restore document %q: %w", name, err)
		}

		docs[name] = &docRec{vers: []docVer{{at: d.GetUpdateTime().AsTime(), doc: d}}}
	}

	b.docs = docs
	b.txns = make(map[string]*txnRec)
	b.lastCommit = snap.LastCommit
	b.notifyLocked()

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpfirestore: restore snapshot: %w", err)
	}

	return nil
}
