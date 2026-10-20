package gcpcloudtasks

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type queueSnap struct {
	Tasks map[string]json.RawMessage `json:"tasks,omitempty"`
	Queue json.RawMessage            `json:"queue"`
}

type backendSnapshot struct {
	Queues     map[string]queueSnap       `json:"queues"`
	Tombstones map[string]time.Time       `json:"tombstones"`
	Policies   map[string]json.RawMessage `json:"policies"`
	Cmek       map[string]json.RawMessage `json:"cmek"`
	Ops        map[string]json.RawMessage `json:"operations,omitempty"`
	EtagSeq    uint64                     `json:"etagSeq"`
	TaskSeq    uint64                     `json:"taskSeq"`
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

// Snapshot serialises queues with their tasks, name tombstones, IAM policies and CMEK configs.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version:    snapshotVersion,
		EtagSeq:    b.etagSeq,
		TaskSeq:    b.taskSeq,
		Queues:     make(map[string]queueSnap, len(b.queues)),
		Tombstones: make(map[string]time.Time, len(b.tombstones)),
		Policies:   make(map[string]json.RawMessage, len(b.policies)),
		Cmek:       make(map[string]json.RawMessage, len(b.cmek)),
	}

	for name, q := range b.queues {
		qs := queueSnap{Queue: marshalPB(q.pb), Tasks: make(map[string]json.RawMessage, len(q.tasks))}
		for tn, t := range q.tasks {
			qs.Tasks[tn] = marshalPB(t.pb)
		}

		snap.Queues[name] = qs
	}

	maps.Copy(snap.Tombstones, b.tombstones)

	for n, p := range b.policies {
		snap.Policies[n] = marshalPB(p)
	}

	for n, c := range b.cmek {
		snap.Cmek[n] = marshalPB(c)
	}

	snap.Ops = b.ops.Snapshot()

	return persistence.MarshalSnapshot(ctx, "gcpcloudtasks", snap)
}

// Restore loads backend state from a snapshot; in-flight attempts at snapshot time are simply due again.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpcloudtasks", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpcloudtasks: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		b.resetMaps()
		b.ops.Reset()

		return nil
	}

	fresh := &InMemoryBackend{}
	fresh.resetMaps()

	if err := fresh.decode(snap); err != nil {
		return err
	}

	if err := b.ops.Restore(snap.Ops); err != nil {
		return err
	}

	b.queues, b.tombstones, b.policies, b.cmek = fresh.queues, fresh.tombstones, fresh.policies, fresh.cmek
	b.etagSeq = max(b.etagSeq, snap.EtagSeq)
	b.taskSeq = max(b.taskSeq, snap.TaskSeq)

	return nil
}

func (b *InMemoryBackend) decode(snap backendSnapshot) error {
	for name, qs := range snap.Queues {
		pb := &cloudtaskspb.Queue{}
		if err := unmarshalPB(qs.Queue, pb); err != nil {
			return fmt.Errorf("gcpcloudtasks: restore queue %q: %w", name, err)
		}

		q := &queueRec{pb: pb, tasks: make(map[string]*taskRec, len(qs.Tasks))}

		for tn, raw := range qs.Tasks {
			t := &cloudtaskspb.Task{}
			if err := unmarshalPB(raw, t); err != nil {
				return fmt.Errorf("gcpcloudtasks: restore task %q: %w", tn, err)
			}

			q.tasks[tn] = &taskRec{pb: t}
		}

		b.queues[name] = q
	}

	b.tombstones = snap.Tombstones
	if b.tombstones == nil {
		b.tombstones = make(map[string]time.Time)
	}

	for n, raw := range snap.Policies {
		p := &iampb.Policy{}
		if err := unmarshalPB(raw, p); err != nil {
			return fmt.Errorf("gcpcloudtasks: restore policy of %q: %w", n, err)
		}

		b.policies[n] = p
	}

	for n, raw := range snap.Cmek {
		c := &cloudtaskspb.CmekConfig{}
		if err := unmarshalPB(raw, c); err != nil {
			return fmt.Errorf("gcpcloudtasks: restore cmek config %q: %w", n, err)
		}

		b.cmek[n] = c
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpcloudtasks: restore snapshot: %w", err)
	}

	return nil
}
