package gcpworkflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"cloud.google.com/go/workflows/apiv1/workflowspb"
	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

var errNoRevisions = errors.New("no revisions")

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type workflowSnap struct {
	Revisions []json.RawMessage `json:"revisions"`
}

type backendSnapshot struct {
	Workflows  map[string]workflowSnap    `json:"workflows"`
	Executions map[string]json.RawMessage `json:"executions"`
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

func unmarshalPB(raw []byte, m proto.Message) error {
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(raw, m)
}

// Snapshot serialises workflows with their revisions, executions and operations.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version:    snapshotVersion,
		Workflows:  make(map[string]workflowSnap, len(b.workflows)),
		Executions: make(map[string]json.RawMessage, len(b.execs)),
		Operations: b.ops.Snapshot(),
	}

	for n, rec := range b.workflows {
		ws := workflowSnap{Revisions: make([]json.RawMessage, len(rec.revs))}
		for i, r := range rec.revs {
			ws.Revisions[i] = marshalPB(r)
		}

		snap.Workflows[n] = ws
	}

	for n, rec := range b.execs {
		snap.Executions[n] = marshalPB(rec.pb)
	}

	return persistence.MarshalSnapshot(ctx, "gcpworkflows", snap)
}

type restored struct {
	workflows map[string]*workflowRec
	execs     map[string]*execRec
}

func restoreAll(snap *backendSnapshot) (*restored, error) {
	out := &restored{
		workflows: make(map[string]*workflowRec, len(snap.Workflows)),
		execs:     make(map[string]*execRec, len(snap.Executions)),
	}

	for n, ws := range snap.Workflows {
		rec := &workflowRec{progs: map[string]*wfinterp.Program{}}

		for _, raw := range ws.Revisions {
			w := &workflowspb.Workflow{}
			if err := unmarshalPB(raw, w); err != nil {
				return nil, fmt.Errorf("workflow %q: %w", n, err)
			}

			rec.revs = append(rec.revs, w)
		}

		if len(rec.revs) == 0 {
			return nil, fmt.Errorf("workflow %q: %w", n, errNoRevisions)
		}

		out.workflows[n] = rec
	}

	for n, raw := range snap.Executions {
		e := &executionspb.Execution{}
		if err := unmarshalPB(raw, e); err != nil {
			return nil, fmt.Errorf("execution %q: %w", n, err)
		}

		out.execs[n] = &execRec{pb: e}
	}

	return out, nil
}

// Restore loads a snapshot, replacing all state; executions that were running become FAILED.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpworkflows", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpworkflows: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	r, err := restoreAll(&snap)
	if err != nil {
		return fmt.Errorf("gcpworkflows: restore: %w", err)
	}

	if err = b.ops.Restore(snap.Operations); err != nil {
		return err
	}

	b.stopExecutions()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.workflows, b.execs, b.callbacks = r.workflows, r.execs, make(map[string]*callback)
	b.runCtx, b.stop = context.WithCancel(context.Background())

	for _, rec := range b.execs {
		if rec.pb.GetState() == executionspb.Execution_ACTIVE || rec.pb.GetState() == executionspb.Execution_QUEUED {
			end := b.ts()
			rec.pb.State, rec.pb.EndTime, rec.pb.Status = executionspb.Execution_FAILED, end, nil
			rec.pb.Duration = durationpb.New(end.AsTime().Sub(rec.pb.GetStartTime().AsTime()))
			rec.pb.Error = &executionspb.Execution_Error{
				Payload: `{"message":"The execution was interrupted by an emulator restart.","tags":["SystemError"]}`,
				Context: "The execution was interrupted by an emulator restart.",
			}
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpworkflows: restore snapshot: %w", err)
	}

	return nil
}
