package gcpbatch

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type jobSnap struct {
	Rec *jobRec         `json:"rec"`
	Job json.RawMessage `json:"job"`
}

type backendSnapshot struct {
	Jobs     map[string]jobSnap `json:"jobs"`
	Ops      map[string]*opRec  `json:"operations"`
	Requests map[string]string  `json:"requests"`
	Version  int                `json:"version"`
}

// Snapshot serialises jobs, tasks, operations and request ids. Running containers are not kept.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version: snapshotVersion, Ops: b.ops, Requests: b.requests,
		Jobs: make(map[string]jobSnap, len(b.jobs)),
	}

	for n, r := range b.jobs {
		raw, err := protojson.Marshal(r.pb)
		if err != nil {
			continue
		}

		snap.Jobs[n] = jobSnap{Job: raw, Rec: r}
	}

	return persistence.MarshalSnapshot(ctx, "gcpbatch", snap)
}

// abandon fails the jobs a restart interrupted: their containers are gone.
func (r *jobRec) abandon(at time.Time) {
	if !r.Engine || r.finished() || r.interrupted() {
		return
	}

	for i := range r.Tasks {
		r.setTask(i, taskFailed, at, nil, "the emulator restarted while the task was running")
	}

	r.setPhase(jobFailed, at)
}

// Restore loads a snapshot, replacing all state.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpbatch", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpbatch: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	jobs := make(map[string]*jobRec, len(snap.Jobs))

	for n, s := range snap.Jobs {
		pb := &batchpb.Job{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(s.Job, pb); err != nil {
			return fmt.Errorf("gcpbatch: restore job %q: %w", n, err)
		}

		if s.Rec == nil {
			continue
		}

		s.Rec.pb = pb
		jobs[n] = s.Rec
	}

	if snap.Ops == nil {
		snap.Ops = map[string]*opRec{}
	}

	if snap.Requests == nil {
		snap.Requests = map[string]string{}
	}

	b.Close()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	now := b.now()
	for _, r := range jobs {
		r.abandon(now)
	}

	b.jobs, b.ops, b.requests, b.cancels = jobs, snap.Ops, snap.Requests, map[string]context.CancelFunc{}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpbatch: restore snapshot: %w", err)
	}

	return nil
}
