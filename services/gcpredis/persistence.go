package gcpredis

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/redis/apiv1/redispb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type instSnap struct {
	Instance   json.RawMessage `json:"instance"`
	AuthString string          `json:"authString,omitempty"`
	Keyspace   []byte          `json:"keyspace,omitempty"`
}

type backendSnapshot struct {
	Instances  map[string]instSnap        `json:"instances"`
	Operations map[string]json.RawMessage `json:"operations"`
	Version    int                        `json:"version"`
}

// Snapshot serialises every instance with its keyspace; Redis servers and TLS CAs are rebuilt on restore.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version:    snapshotVersion,
		Instances:  make(map[string]instSnap, len(b.instances)),
		Operations: b.ops.Snapshot(),
	}

	for name, r := range b.instances {
		raw, err := protojson.Marshal(r.pb)
		if err != nil {
			continue
		}

		blob, _ := r.engine.Snapshot()
		snap.Instances[name] = instSnap{Instance: raw, AuthString: r.authString, Keyspace: blob}
	}

	return persistence.MarshalSnapshot(ctx, "gcpredis", snap)
}

// Restore loads a snapshot. Instances caught mid-creation or mid-deletion are dropped, other transient states
// settle to READY, and every Redis server starts again on a fresh port.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpredis", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpredis: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	recs, err := b.rebuild(&snap)
	if err != nil {
		return err
	}

	if err = b.ops.Restore(snap.Operations); err != nil {
		closeRecs(recs)

		return err
	}

	b.ops.AbortPending(status.New(codes.Aborted, "Operation was interrupted by an emulator restart."))

	b.mu.Lock("Restore")
	old := b.takeEnginesLocked()
	b.instances = recs
	b.mu.Unlock()

	closeAll(old)

	return nil
}

func (b *InMemoryBackend) rebuild(snap *backendSnapshot) (map[string]*instRec, error) {
	recs := make(map[string]*instRec, len(snap.Instances))

	for name, s := range snap.Instances {
		pb := &redispb.Instance{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(s.Instance, pb); err != nil {
			closeRecs(recs)

			return nil, fmt.Errorf("gcpredis: restore instance %q: %w", name, err)
		}

		if pb.GetState() == redispb.Instance_DELETING || pb.GetHost() == "" {
			continue
		}

		rec := &instRec{pb: pb, authString: s.AuthString}
		recs[name] = rec

		if err := b.revive(rec, s.Keyspace); err != nil {
			closeRecs(recs)

			return nil, fmt.Errorf("gcpredis: restore instance %q: %w", name, err)
		}
	}

	return recs, nil
}

func (b *InMemoryBackend) revive(rec *instRec, keyspace []byte) error {
	eng, certs, err := b.startEngine(rec.pb.GetTransitEncryptionMode(), rec.authString)
	if err != nil {
		return err
	}

	if err = eng.Restore(keyspace); err != nil {
		eng.Close()

		return fmt.Errorf("load keyspace: %w", err)
	}

	rec.pb.State = redispb.Instance_READY
	bindEngine(rec, eng, certs)

	return nil
}

func closeRecs(recs map[string]*instRec) {
	for _, r := range recs {
		r.engine.Close()
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpredis: restore snapshot: %w", err)
	}

	return nil
}
