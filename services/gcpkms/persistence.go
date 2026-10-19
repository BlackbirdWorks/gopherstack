package gcpkms

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

// Key material is stored unencrypted in the snapshot: this is a local emulator, not a secure key store.
type versionSnap struct {
	Version  json.RawMessage `json:"version"`
	Material []byte          `json:"material,omitempty"`
	Deleted  bool            `json:"deleted,omitempty"`
}

type keySnap struct {
	Key      json.RawMessage `json:"key"`
	Versions []versionSnap   `json:"versions,omitempty"`
	Primary  int64           `json:"primary"`
}

type jobSnap struct {
	Job     json.RawMessage `json:"job"`
	Private []byte          `json:"private,omitempty"`
}

type backendSnapshot struct {
	Rings    map[string]json.RawMessage `json:"rings"`
	Keys     map[string]keySnap         `json:"keys"`
	Jobs     map[string]jobSnap         `json:"jobs"`
	Retired  map[string]json.RawMessage `json:"retired,omitempty"`
	Ops      map[string]json.RawMessage `json:"operations,omitempty"`
	Policies map[string]json.RawMessage `json:"policies"`
	EtagSeq  uint64                     `json:"etagSeq"`
	Version  int                        `json:"version"`
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

// Snapshot serialises all key rings, keys, versions (with key material), import jobs and policies.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version:  snapshotVersion,
		EtagSeq:  b.etagSeq,
		Rings:    make(map[string]json.RawMessage, len(b.rings)),
		Keys:     make(map[string]keySnap, len(b.keys)),
		Jobs:     make(map[string]jobSnap, len(b.jobs)),
		Retired:  make(map[string]json.RawMessage, len(b.retired)),
		Policies: make(map[string]json.RawMessage, len(b.policies)),
	}

	for name, r := range b.rings {
		snap.Rings[name] = marshalPB(r)
	}

	for name, k := range b.keys {
		ks := keySnap{Key: marshalPB(k.pb), Primary: k.primary}
		for _, v := range k.versions {
			ks.Versions = append(
				ks.Versions,
				versionSnap{Version: marshalPB(v.pb), Material: v.material, Deleted: v.deleted},
			)
		}

		snap.Keys[name] = ks
	}

	for name, j := range b.jobs {
		snap.Jobs[name] = jobSnap{Job: marshalPB(j.pb), Private: j.private}
	}

	for name, r := range b.retired {
		snap.Retired[name] = marshalPB(r)
	}

	for name, p := range b.policies {
		snap.Policies[name] = marshalPB(p)
	}

	snap.Ops = b.ops.Snapshot()

	return persistence.MarshalSnapshot(ctx, "gcpkms", snap)
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpkms", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpkms: discarding incompatible snapshot version, starting empty",
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

	b.rings, b.keys, b.jobs, b.policies = fresh.rings, fresh.keys, fresh.jobs, fresh.policies
	b.retired = fresh.retired

	if err := b.ops.Restore(snap.Ops); err != nil {
		return err
	}

	b.etagSeq = max(b.etagSeq, snap.EtagSeq)

	return nil
}

func (b *InMemoryBackend) decode(snap backendSnapshot) error {
	for name, raw := range snap.Rings {
		r := &kmspb.KeyRing{}
		if err := unmarshalPB(raw, r); err != nil {
			return fmt.Errorf("gcpkms: restore key ring %q: %w", name, err)
		}

		b.rings[name] = r
	}

	for name, ks := range snap.Keys {
		k, err := decodeKey(name, ks)
		if err != nil {
			return err
		}

		b.keys[name] = k
	}

	for name, js := range snap.Jobs {
		j := &kmspb.ImportJob{}
		if err := unmarshalPB(js.Job, j); err != nil {
			return fmt.Errorf("gcpkms: restore import job %q: %w", name, err)
		}

		b.jobs[name] = &jobRec{pb: j, private: js.Private}
	}

	for name, raw := range snap.Retired {
		r := &kmspb.RetiredResource{}
		if err := unmarshalPB(raw, r); err != nil {
			return fmt.Errorf("gcpkms: restore retired resource %q: %w", name, err)
		}

		b.retired[name] = r
	}

	for name, raw := range snap.Policies {
		p := &iampb.Policy{}
		if err := unmarshalPB(raw, p); err != nil {
			return fmt.Errorf("gcpkms: restore policy of %q: %w", name, err)
		}

		b.policies[name] = p
	}

	return nil
}

func decodeKey(name string, ks keySnap) (*keyRec, error) {
	pb := &kmspb.CryptoKey{}
	if err := unmarshalPB(ks.Key, pb); err != nil {
		return nil, fmt.Errorf("gcpkms: restore key %q: %w", name, err)
	}

	k := &keyRec{pb: pb, primary: ks.Primary}

	for i, vs := range ks.Versions {
		vpb := &kmspb.CryptoKeyVersion{}
		if err := unmarshalPB(vs.Version, vpb); err != nil {
			return nil, fmt.Errorf("gcpkms: restore version %d of %q: %w", i+1, name, err)
		}

		k.versions = append(
			k.versions,
			&versionRec{pb: vpb, material: vs.Material, id: int64(i + 1), deleted: vs.Deleted},
		)
	}

	return k, nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpkms: restore snapshot: %w", err)
	}

	return nil
}
