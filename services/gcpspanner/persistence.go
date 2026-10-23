package gcpspanner

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type dbSnap struct {
	Database json.RawMessage `json:"database"`
	DDL      []string        `json:"ddl,omitempty"`
}

type instSnap struct {
	Databases map[string]dbSnap `json:"databases,omitempty"`
	Instance  json.RawMessage   `json:"instance"`
	Policy    json.RawMessage   `json:"policy,omitempty"`
}

type backendSnapshot struct {
	Instances  map[string]instSnap        `json:"instances"`
	Operations map[string]json.RawMessage `json:"operations"`
	Version    int                        `json:"version"`
	EtagSeq    uint64                     `json:"etagSeq"`
}

// Snapshot serialises instances, policies, databases and their DDL. Table data lives in the emulator and is not
// persisted: it is lost on restart, while the schema is replayed into a fresh emulator.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version: snapshotVersion, EtagSeq: b.etagSeq,
		Instances:  make(map[string]instSnap, len(b.instances)),
		Operations: b.ops.Snapshot(),
	}

	for name, r := range b.instances {
		if s, ok := snapInstance(r); ok {
			snap.Instances[name] = s
		}
	}

	return persistence.MarshalSnapshot(ctx, "gcpspanner", snap)
}

func snapInstance(r *instRec) (instSnap, bool) {
	raw, err := protojson.Marshal(r.pb)
	if err != nil {
		return instSnap{}, false
	}

	s := instSnap{Instance: raw, Databases: make(map[string]dbSnap, len(r.dbs))}

	if r.policy != nil {
		s.Policy, _ = protojson.Marshal(r.policy)
	}

	for id, d := range r.dbs {
		if db, dbErr := protojson.Marshal(d.pb); dbErr == nil {
			s.Databases[id] = dbSnap{Database: db, DDL: cloneDDL(d.ddl)}
		}
	}

	return s, true
}

// Restore loads a snapshot. Instances caught mid-creation are dropped; the emulator starts empty on first use
// and is rebuilt from the restored schema.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpspanner", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpspanner: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	recs, err := rebuild(&snap)
	if err != nil {
		return err
	}

	b.wg.Wait()
	b.emu.close(ctx)

	if err = b.ops.Restore(snap.Operations); err != nil {
		return err
	}

	b.ops.AbortPending(status.New(codes.Aborted, "Operation was interrupted by an emulator restart."))

	b.mu.Lock("Restore")
	b.instances, b.etagSeq = recs, snap.EtagSeq
	b.mu.Unlock()

	return nil
}

func rebuild(snap *backendSnapshot) (map[string]*instRec, error) {
	recs := make(map[string]*instRec, len(snap.Instances))
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}

	for name, s := range snap.Instances {
		pb := &instancepb.Instance{}
		if err := opts.Unmarshal(s.Instance, pb); err != nil {
			return nil, fmt.Errorf("gcpspanner: restore instance %q: %w", name, err)
		}

		if pb.GetState() != instancepb.Instance_READY {
			continue
		}

		rec := &instRec{pb: pb, dbs: make(map[string]*dbRec, len(s.Databases))}

		if len(s.Policy) > 0 {
			rec.policy = &iampb.Policy{}
			if err := opts.Unmarshal(s.Policy, rec.policy); err != nil {
				return nil, fmt.Errorf("gcpspanner: restore policy of %q: %w", name, err)
			}
		}

		if err := rebuildDBs(rec, s.Databases, opts); err != nil {
			return nil, fmt.Errorf("gcpspanner: restore databases of %q: %w", name, err)
		}

		recs[name] = rec
	}

	return recs, nil
}

func rebuildDBs(rec *instRec, dbs map[string]dbSnap, opts protojson.UnmarshalOptions) error {
	for id, d := range dbs {
		pb := &databasepb.Database{}
		if err := opts.Unmarshal(d.Database, pb); err != nil {
			return err //nolint:wrapcheck // wrapped by the caller
		}

		rec.dbs[id] = &dbRec{pb: pb, ddl: d.DDL}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpspanner: restore snapshot: %w", err)
	}

	return nil
}
