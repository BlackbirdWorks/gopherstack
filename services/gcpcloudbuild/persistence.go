package gcpcloudbuild

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type buildSnap struct {
	OpName string          `json:"opName"`
	Build  json.RawMessage `json:"build"`
	Orig   json.RawMessage `json:"orig"`
}

type backendSnapshot struct {
	Builds     map[string]buildSnap       `json:"builds"`
	Triggers   map[string]json.RawMessage `json:"triggers"`
	Pools      map[string]json.RawMessage `json:"workerPools"`
	Operations map[string]json.RawMessage `json:"operations"`
	Version    int                        `json:"version"`
}

func marshalPB(m proto.Message) json.RawMessage {
	out, err := protojson.Marshal(m)
	if err != nil {
		return nil
	}

	return out
}

func unmarshalPB(raw json.RawMessage, m proto.Message) error {
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(raw, m)
}

// Snapshot serialises builds, triggers, worker pools and operations. Workspaces and containers of running
// builds are not part of it.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version: snapshotVersion, Operations: b.ops.Snapshot(),
		Builds:   make(map[string]buildSnap, len(b.builds)),
		Triggers: make(map[string]json.RawMessage, len(b.triggers)),
		Pools:    make(map[string]json.RawMessage, len(b.pools)),
	}

	for k, rec := range b.builds {
		snap.Builds[k] = buildSnap{
			Build:  marshalPB(rec.pb),
			Orig:   marshalPB(rec.orig),
			OpName: rec.opName,
		}
	}

	for k, t := range b.triggers {
		snap.Triggers[k] = marshalPB(t)
	}

	for k, p := range b.pools {
		snap.Pools[k] = marshalPB(p)
	}

	return persistence.MarshalSnapshot(ctx, "gcpcloudbuild", snap)
}

// Restore loads a snapshot. Builds that were queued or running when it was taken ended with the old process and
// are marked INTERNAL_ERROR.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpcloudbuild", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).
			WarnContext(ctx, "gcpcloudbuild: discarding incompatible snapshot version, starting empty",
				"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	builds, err := restoreBuilds(snap.Builds)
	if err != nil {
		return err
	}

	triggers, err := restoreMap(
		snap.Triggers,
		func() *cloudbuildpb.BuildTrigger { return &cloudbuildpb.BuildTrigger{} },
	)
	if err != nil {
		return err
	}

	pools, err := restoreMap(
		snap.Pools,
		func() *cloudbuildpb.WorkerPool { return &cloudbuildpb.WorkerPool{} },
	)
	if err != nil {
		return err
	}

	if err = b.ops.Restore(snap.Operations); err != nil {
		return fmt.Errorf("gcpcloudbuild: restore operations: %w", err)
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.builds, b.triggers, b.pools = builds, triggers, pools
	b.abortInterrupted()

	return nil
}

// abortInterrupted ends builds whose worker did not survive the restart. Callers hold b.mu.
func (b *InMemoryBackend) abortInterrupted() {
	for _, rec := range b.builds {
		s := rec.pb.GetStatus()
		if s == cloudbuildpb.Build_QUEUED || s == cloudbuildpb.Build_WORKING {
			b.finishLocked(
				rec,
				cloudbuildpb.Build_INTERNAL_ERROR,
				"build was interrupted by a restart",
				nil,
			)
		}
	}

	b.ops.AbortPending(status.New(codes.Aborted, "operation was interrupted by a restart"))
}

func restoreBuilds(in map[string]buildSnap) (map[string]*buildRec, error) {
	out := make(map[string]*buildRec, len(in))

	for k, bs := range in {
		pb, orig := &cloudbuildpb.Build{}, &cloudbuildpb.Build{}
		if err := unmarshalPB(bs.Build, pb); err != nil {
			return nil, fmt.Errorf("gcpcloudbuild: restore build %q: %w", k, err)
		}

		if err := unmarshalPB(bs.Orig, orig); err != nil {
			return nil, fmt.Errorf("gcpcloudbuild: restore build %q request: %w", k, err)
		}

		r, err := parseName(pb.GetName(), "builds")
		if err != nil {
			return nil, fmt.Errorf("gcpcloudbuild: restore build %q: %w", k, err)
		}

		out[k] = &buildRec{pb: pb, orig: orig, ref: r, opName: bs.OpName}
	}

	return out, nil
}

func restoreMap[T proto.Message](in map[string]json.RawMessage, mk func() T) (map[string]T, error) {
	out := make(map[string]T, len(in))

	for k, raw := range in {
		m := mk()
		if err := unmarshalPB(raw, m); err != nil {
			return nil, fmt.Errorf("gcpcloudbuild: restore %q: %w", k, err)
		}

		out[k] = m
	}

	return out, nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpcloudbuild: restore snapshot: %w", err)
	}

	return nil
}
