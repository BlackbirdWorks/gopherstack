package gcpfunctions

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type fnSnap struct {
	Function json.RawMessage `json:"function"`
	Failure  string          `json:"failure"`
	SubName  string          `json:"subName"`
	Zip      []byte          `json:"zip"`
	RevSeq   int             `json:"revSeq"`
}

type uploadSnap struct {
	Data       []byte `json:"data"`
	Generation int64  `json:"generation"`
}

type backendSnapshot struct {
	Functions  map[string]fnSnap          `json:"functions"`
	Uploads    map[string]uploadSnap      `json:"uploads"`
	Operations map[string]json.RawMessage `json:"operations"`
	Version    int                        `json:"version"`
}

// Snapshot serialises all functions and sources; containers are not part of it.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version:    snapshotVersion,
		Functions:  make(map[string]fnSnap, len(b.functions)),
		Uploads:    make(map[string]uploadSnap, len(b.uploads)),
		Operations: b.ops.Snapshot(),
	}

	for name, r := range b.functions {
		raw, err := protojson.Marshal(r.pb)
		if err != nil {
			continue
		}

		snap.Functions[name] = fnSnap{
			Function: raw,
			Failure:  r.failure,
			SubName:  r.subName,
			Zip:      r.zip,
			RevSeq:   r.revSeq,
		}
	}

	for key, u := range b.uploads {
		snap.Uploads[key] = uploadSnap{Data: u.data, Generation: u.generation}
	}

	return persistence.MarshalSnapshot(ctx, "gcpfunctions", snap)
}

// Restore loads a snapshot. Deployments in flight at snapshot time fail, and containers start again on first use.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpfunctions", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpfunctions: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	if err := b.ops.Restore(snap.Operations); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	pools := b.takeAllLocked()
	b.resetMaps()

	err := b.restoreLocked(&snap)
	if err != nil {
		b.resetMaps()
	} else {
		b.ops.AbortPending(status.New(codes.Aborted, "Operation was interrupted by an emulator restart."))
	}
	b.mu.Unlock()

	closeAll(pools)

	return err
}

func (b *InMemoryBackend) restoreLocked(snap *backendSnapshot) error {
	for name, s := range snap.Functions {
		pb := &functionspb.Function{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(s.Function, pb); err != nil {
			return fmt.Errorf("gcpfunctions: restore function %q: %w", name, err)
		}

		rec := &fnRec{pb: pb, failure: s.Failure, subName: s.SubName, zip: s.Zip, revSeq: s.RevSeq}
		b.functions[name] = rec
		b.interruptLocked(rec)
	}

	for key, u := range snap.Uploads {
		b.uploads[key] = &uploadRec{data: u.Data, generation: u.Generation}
		b.lastGen = max(b.lastGen, u.Generation)
	}

	return nil
}

// interruptLocked fails work whose goroutines did not survive the restore.
func (b *InMemoryBackend) interruptLocked(rec *fnRec) {
	switch rec.pb.GetState() { //nolint:exhaustive // only in-flight states need repair
	case functionspb.Function_DEPLOYING, functionspb.Function_DELETING:
		rec.failure = "Deployment was interrupted by an emulator restart."
		rec.pb.State = functionspb.Function_FAILED
		rec.pb.StateMessages = []*functionspb.StateMessage{{
			Severity: functionspb.StateMessage_ERROR, Type: "DeploymentFailed", Message: rec.failure,
		}}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpfunctions: restore snapshot: %w", err)
	}

	return nil
}
