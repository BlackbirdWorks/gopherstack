package gcpsecretmanager

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type versionSnap struct {
	Version json.RawMessage `json:"version"`
	Data    []byte          `json:"data,omitempty"`
	Num     int64           `json:"num"`
}

type secretSnap struct {
	Secret   json.RawMessage `json:"secret"`
	Policy   json.RawMessage `json:"policy,omitempty"`
	Versions []versionSnap   `json:"versions,omitempty"`
	NextVer  int64           `json:"nextVer"`
}

type backendSnapshot struct {
	Secrets map[string]secretSnap `json:"secrets"`
	EtagSeq uint64                `json:"etagSeq"`
	Version int                   `json:"version"`
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

// Snapshot serialises all secrets, versions (with payloads) and policies.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version: snapshotVersion,
		EtagSeq: b.etagSeq,
		Secrets: make(map[string]secretSnap, len(b.secrets)),
	}

	for name, rec := range b.secrets {
		ss := secretSnap{Secret: marshalPB(rec.pb), NextVer: rec.nextVer}
		if rec.policy != nil {
			ss.Policy = marshalPB(rec.policy)
		}

		for _, v := range rec.versions {
			ss.Versions = append(ss.Versions, versionSnap{Version: marshalPB(v.pb), Data: v.data, Num: v.num})
		}

		snap.Secrets[name] = ss
	}

	return persistence.MarshalSnapshot(ctx, "gcpsecretmanager", snap)
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpsecretmanager", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpsecretmanager: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		b.secrets = make(map[string]*secretRec)

		return nil
	}

	secrets := make(map[string]*secretRec, len(snap.Secrets))

	for name, ss := range snap.Secrets {
		rec, err := decodeSecret(name, ss)
		if err != nil {
			return err
		}

		secrets[name] = rec
	}

	b.secrets = secrets
	b.etagSeq = max(b.etagSeq, snap.EtagSeq)

	return nil
}

func decodeSecret(name string, ss secretSnap) (*secretRec, error) {
	pb := &secretmanagerpb.Secret{}
	if err := unmarshalPB(ss.Secret, pb); err != nil {
		return nil, fmt.Errorf("gcpsecretmanager: restore secret %q: %w", name, err)
	}

	rec := &secretRec{pb: pb, nextVer: ss.NextVer}

	if len(ss.Policy) > 0 {
		rec.policy = &iampb.Policy{}
		if err := unmarshalPB(ss.Policy, rec.policy); err != nil {
			return nil, fmt.Errorf("gcpsecretmanager: restore policy of %q: %w", name, err)
		}
	}

	for _, vs := range ss.Versions {
		vpb := &secretmanagerpb.SecretVersion{}
		if err := unmarshalPB(vs.Version, vpb); err != nil {
			return nil, fmt.Errorf("gcpsecretmanager: restore version %d of %q: %w", vs.Num, name, err)
		}

		rec.versions = append(rec.versions, &versionRec{pb: vpb, data: vs.Data, num: vs.Num})
	}

	return rec, nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpsecretmanager: restore snapshot: %w", err)
	}

	return nil
}
