package gcprun

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type svcSnap struct {
	Failure string          `json:"failure"`
	Service json.RawMessage `json:"service"`
	RevSeq  int             `json:"revSeq"`
}

type revSnap struct {
	StartErr string          `json:"startErr"`
	Revision json.RawMessage `json:"revision"`
	Seq      int             `json:"seq"`
}

type taskSnap struct {
	State string          `json:"state"`
	Task  json.RawMessage `json:"task"`
}

type backendSnapshot struct {
	Services   map[string]svcSnap         `json:"services"`
	Revisions  map[string]revSnap         `json:"revisions"`
	Jobs       map[string]json.RawMessage `json:"jobs"`
	Executions map[string]json.RawMessage `json:"executions"`
	Tasks      map[string]taskSnap        `json:"tasks"`
	Operations map[string]json.RawMessage `json:"operations"`
	Version    int                        `json:"version"`
}

func marshalPB(m proto.Message) json.RawMessage {
	raw, err := protojson.Marshal(m)
	if err != nil {
		return nil
	}

	return raw
}

func unmarshalPB[T proto.Message](raw json.RawMessage, m T) (T, error) {
	err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, m)

	return m, err
}

// Snapshot serialises all resources; containers and in-flight work are not part of it.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version:    snapshotVersion,
		Services:   make(map[string]svcSnap, len(b.services)),
		Revisions:  make(map[string]revSnap, len(b.revisions)),
		Jobs:       make(map[string]json.RawMessage, len(b.jobs)),
		Executions: make(map[string]json.RawMessage, len(b.executions)),
		Tasks:      make(map[string]taskSnap, len(b.tasks)),
		Operations: b.ops.Snapshot(),
	}

	for name, r := range b.services {
		snap.Services[name] = svcSnap{Service: marshalPB(r.pb), Failure: r.failure, RevSeq: r.revSeq}
	}

	for name, r := range b.revisions {
		snap.Revisions[name] = revSnap{Revision: marshalPB(r.pb), StartErr: r.startErr, Seq: r.seq}
	}

	for name, j := range b.jobs {
		snap.Jobs[name] = marshalPB(j)
	}

	for name, e := range b.executions {
		snap.Executions[name] = marshalPB(e.pb)
	}

	for name, t := range b.tasks {
		snap.Tasks[name] = taskSnap{Task: marshalPB(t.pb), State: string(t.state)}
	}

	return persistence.MarshalSnapshot(ctx, "gcprun", snap)
}

// Restore loads a snapshot. Work in flight at snapshot time is not resumed: unfinished executions fail,
// unfinished deployments fail, and revisions start their containers again on first request.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcprun", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcprun: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	if err := b.ops.Restore(snap.Operations); err != nil {
		return err
	}

	return b.swapState(&snap)
}

// swapState replaces live state with snap and tears down containers of the replaced revisions.
func (b *InMemoryBackend) swapState(snap *backendSnapshot) error {
	b.mu.Lock("Restore")
	b.cancelAllLocked()

	old := b.revisions
	b.resetMaps()

	err := b.restoreLocked(snap)
	if err != nil {
		b.resetMaps()
	} else {
		b.ops.AbortPending(status.New(codes.Aborted, "Operation was interrupted by an emulator restart."))
	}
	b.mu.Unlock()

	for _, r := range old {
		b.teardown(r)
	}

	return err
}

func (b *InMemoryBackend) restoreLocked(snap *backendSnapshot) error {
	for name, s := range snap.Services {
		pb, err := unmarshalPB(s.Service, &runpb.Service{})
		if err != nil {
			return fmt.Errorf("gcprun: restore service %q: %w", name, err)
		}

		b.services[name] = &svcRec{pb: pb, failure: s.Failure, revSeq: s.RevSeq}
	}

	for name, r := range snap.Revisions {
		pb, err := unmarshalPB(r.Revision, &runpb.Revision{})
		if err != nil {
			return fmt.Errorf("gcprun: restore revision %q: %w", name, err)
		}

		b.revisions[name] = &revRec{pb: pb, startErr: r.StartErr, seq: r.Seq}
	}

	for name, raw := range snap.Jobs {
		pb, err := unmarshalPB(raw, &runpb.Job{})
		if err != nil {
			return fmt.Errorf("gcprun: restore job %q: %w", name, err)
		}

		b.jobs[name] = pb
	}

	if err := b.restoreRuns(snap); err != nil {
		return err
	}

	b.interruptLocked()

	return nil
}

func (b *InMemoryBackend) restoreRuns(snap *backendSnapshot) error {
	for name, raw := range snap.Executions {
		pb, err := unmarshalPB(raw, &runpb.Execution{})
		if err != nil {
			return fmt.Errorf("gcprun: restore execution %q: %w", name, err)
		}

		b.executions[name] = &execRec{pb: pb, cancel: func() {}, done: make(chan struct{})}
	}

	for name, t := range snap.Tasks {
		pb, err := unmarshalPB(t.Task, &runpb.Task{})
		if err != nil {
			return fmt.Errorf("gcprun: restore task %q: %w", name, err)
		}

		b.tasks[name] = &taskRec{pb: pb, state: taskState(t.State)}
	}

	return nil
}

// interruptLocked fails work whose goroutines did not survive the restore.
func (b *InMemoryBackend) interruptLocked() {
	for _, r := range b.revisions {
		if r.pb.GetReconciling() {
			msg := "Revision deployment was interrupted by an emulator restart."
			r.pb.Reconciling = false
			r.pb.Conditions = setCondition(
				r.pb.GetConditions(),
				newCondition(condReady, runpb.Condition_CONDITION_FAILED, msg, b.ts()),
			)
		}
	}

	for _, rec := range b.services {
		rec.pending = 0
		b.refreshLocked(rec)
	}

	for name, e := range b.executions {
		if e.pb.GetCompletionTime() == nil {
			for _, tn := range sortedKeys(b.tasks, name+"/tasks/") {
				if t := b.tasks[tn]; !t.state.terminal() {
					b.finishTaskLocked(t, stateFailed, "Task was interrupted by an emulator restart.", 0)
				}
			}

			b.recountLocked(e)
			b.stampCompleteLocked(e)
		}

		close(e.done)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcprun: restore snapshot: %w", err)
	}

	return nil
}
