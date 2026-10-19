package gcpscheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type jobSnap struct {
	RetryAt   time.Time       `json:"retryAt"`
	ExecStart time.Time       `json:"execStart"`
	Job       json.RawMessage `json:"job"`
	Failures  int32           `json:"failures"`
}

type backendSnapshot struct {
	Jobs    map[string]jobSnap `json:"jobs"`
	JobSeq  uint64             `json:"jobSeq"`
	Version int                `json:"version"`
}

// Snapshot serialises jobs with their retry state.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version: snapshotVersion,
		JobSeq:  b.jobSeq,
		Jobs:    make(map[string]jobSnap, len(b.jobs)),
	}

	for name, r := range b.jobs {
		raw, err := protojson.Marshal(r.pb)
		if err != nil {
			continue
		}

		snap.Jobs[name] = jobSnap{Job: raw, RetryAt: r.retryAt, ExecStart: r.execStart, Failures: r.failures}
	}

	return persistence.MarshalSnapshot(ctx, "gcpscheduler", snap)
}

// Restore loads backend state from a snapshot; in-flight attempts at snapshot time are simply not resumed.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpscheduler", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpscheduler: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		b.resetMaps()

		return nil
	}

	jobs := make(map[string]*jobRec, len(snap.Jobs))

	for name, js := range snap.Jobs {
		pb := &schedulerpb.Job{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(js.Job, pb); err != nil {
			return fmt.Errorf("gcpscheduler: restore job %q: %w", name, err)
		}

		sched, loc, err := prepare(pb)
		if err != nil {
			return fmt.Errorf("gcpscheduler: restore job %q: %w", name, err)
		}

		jobs[name] = &jobRec{
			pb: pb, sched: sched, loc: loc, retryAt: js.RetryAt, execStart: js.ExecStart, failures: js.Failures,
		}
	}

	b.jobs = jobs
	b.jobSeq = max(b.jobSeq, snap.JobSeq)

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpscheduler: restore snapshot: %w", err)
	}

	return nil
}
