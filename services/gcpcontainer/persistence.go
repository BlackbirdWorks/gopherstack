package gcpcontainer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type poolSnap struct {
	PrevVersion string          `json:"prevVersion"`
	DeleteOp    string          `json:"deleteOp"`
	Pool        json.RawMessage `json:"pool"`
	Size        int32           `json:"size"`
}

type clusterSnap struct {
	Rotation *rotationRec    `json:"rotation,omitempty"`
	Cluster  json.RawMessage `json:"cluster"`
	Project  string          `json:"project"`
	Failure  string          `json:"failure"`
	DeleteOp string          `json:"deleteOp"`
	Pools    []poolSnap      `json:"pools"`
}

type opSnap struct {
	EndAt    time.Time       `json:"endAt"`
	Project  string          `json:"project"`
	Location string          `json:"location"`
	Cluster  string          `json:"cluster"`
	Pool     string          `json:"pool"`
	Failure  string          `json:"failure"`
	Op       json.RawMessage `json:"op"`
	Kind     opKind          `json:"kind"`
	Manual   bool            `json:"manual"`
	Finished bool            `json:"finished"`
}

type backendSnapshot struct {
	Clusters map[string]clusterSnap `json:"clusters"`
	Ops      map[string]opSnap      `json:"operations"`
	Version  int                    `json:"version"`
	Seq      uint64                 `json:"seq"`
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

// Snapshot serialises clusters, node pools and operations.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version: snapshotVersion, Seq: b.seq,
		Clusters: make(map[string]clusterSnap, len(b.clusters)), Ops: make(map[string]opSnap, len(b.ops)),
	}

	for k, c := range b.clusters {
		cs := clusterSnap{
			Project:  c.Project,
			Cluster:  marshalPB(c.Cluster),
			Rotation: c.Rotation,
			Failure:  c.Failure,
			DeleteOp: c.DeleteOp,
		}

		for _, p := range c.Pools {
			cs.Pools = append(cs.Pools, poolSnap{
				Pool: marshalPB(p.Pool), PrevVersion: p.PrevVersion, DeleteOp: p.DeleteOp, Size: p.Size,
			})
		}

		snap.Clusters[k] = cs
	}

	for k, o := range b.ops {
		snap.Ops[k] = opSnap{
			Op: marshalPB(o.Op), EndAt: o.EndAt, Project: o.Project, Location: o.Location, Cluster: o.Cluster,
			Pool: o.Pool, Failure: o.Failure, Kind: o.Kind, Manual: o.Manual, Finished: o.Finished,
		}
	}

	return persistence.MarshalSnapshot(ctx, "gcpcontainer", snap)
}

func restoreClusters(in map[string]clusterSnap) (map[string]*clusterRec, error) {
	out := make(map[string]*clusterRec, len(in))

	for k, cs := range in {
		cl := &containerpb.Cluster{}
		if err := unmarshalPB(cs.Cluster, cl); err != nil {
			return nil, fmt.Errorf("cluster %q: %w", k, err)
		}

		rec := &clusterRec{
			Project: cs.Project, Cluster: cl, Rotation: cs.Rotation, Failure: cs.Failure, DeleteOp: cs.DeleteOp,
		}

		for _, ps := range cs.Pools {
			np := &containerpb.NodePool{}
			if err := unmarshalPB(ps.Pool, np); err != nil {
				return nil, fmt.Errorf("cluster %q node pool: %w", k, err)
			}

			rec.Pools = append(
				rec.Pools,
				&poolRec{Pool: np, PrevVersion: ps.PrevVersion, DeleteOp: ps.DeleteOp, Size: ps.Size},
			)
		}

		out[k] = rec
	}

	return out, nil
}

func restoreOps(in map[string]opSnap) (map[string]*opRec, error) {
	out := make(map[string]*opRec, len(in))

	for k, os := range in {
		op := &containerpb.Operation{}
		if err := unmarshalPB(os.Op, op); err != nil {
			return nil, fmt.Errorf("operation %q: %w", k, err)
		}

		out[k] = &opRec{
			Op: op, EndAt: os.EndAt, Project: os.Project, Location: os.Location, Cluster: os.Cluster, Pool: os.Pool,
			Failure: os.Failure, Kind: os.Kind, Manual: os.Manual, Finished: os.Finished,
		}
	}

	return out, nil
}

// Restore loads a snapshot, replacing all state. Engine clusters are started again from scratch.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpcontainer", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpcontainer: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	clusters, err := restoreClusters(snap.Clusters)
	if err != nil {
		return fmt.Errorf("gcpcontainer: restore: %w", err)
	}

	ops, err := restoreOps(snap.Ops)
	if err != nil {
		return fmt.Errorf("gcpcontainer: restore: %w", err)
	}

	b.stop()
	b.wg.Wait()

	b.mu.Lock("Restore")
	lcs := b.detachAllLocked()
	b.clusters, b.ops, b.seq = clusters, ops, snap.Seq
	b.runCtx, b.stop = newRunCtx()
	b.relaunchLocked()
	e := b.engine
	b.mu.Unlock()

	if e != nil {
		e.reap(lcs)
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpcontainer: restore snapshot: %w", err)
	}

	return nil
}
