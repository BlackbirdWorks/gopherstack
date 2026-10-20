package gcpiam

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type keySnap struct {
	Key     json.RawMessage `json:"key"`
	Cert    []byte          `json:"cert,omitempty"`
	Public  []byte          `json:"public,omitempty"`
	Private []byte          `json:"private,omitempty"`
}

type accountSnap struct {
	Keys      map[string]keySnap `json:"keys,omitempty"`
	System    *keySnap           `json:"system,omitempty"`
	DeletedAt time.Time          `json:"deletedAt"`
	Account   json.RawMessage    `json:"account"`
	Policy    json.RawMessage    `json:"policy,omitempty"`
}

type roleSnap struct {
	DeletedAt time.Time       `json:"deletedAt"`
	Role      json.RawMessage `json:"role"`
}

type backendSnapshot struct {
	Accounts map[string]accountSnap `json:"accounts"`
	Roles    map[string]roleSnap    `json:"roles"`
	EtagSeq  uint64                 `json:"etagSeq"`
	Version  int                    `json:"version"`
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

func (k *keyRec) snap() keySnap {
	return keySnap{Key: marshalPB(k.pb), Cert: k.cert, Public: k.public, Private: k.private}
}

func (s keySnap) rec() (*keyRec, error) {
	pb := &adminpb.ServiceAccountKey{}
	if err := unmarshalPB(s.Key, pb); err != nil {
		return nil, err
	}

	return &keyRec{pb: pb, cert: s.Cert, public: s.Public, private: s.Private}, nil
}

// Snapshot serialises accounts (with keys and policies) and custom roles. Private keys of
// system-managed keys are included so signatures stay verifiable across restarts.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version:  snapshotVersion,
		EtagSeq:  b.etagSeq,
		Accounts: make(map[string]accountSnap, len(b.accounts)),
		Roles:    make(map[string]roleSnap, len(b.roles)),
	}

	for id, a := range b.accounts {
		as := accountSnap{Account: marshalPB(a.pb), DeletedAt: a.deletedAt, Keys: make(map[string]keySnap, len(a.keys))}

		for kid, k := range a.keys {
			as.Keys[kid] = k.snap()
		}

		if a.system != nil {
			s := a.system.snap()
			as.System = &s
		}

		if a.policy != nil {
			as.Policy = marshalPB(a.policy)
		}

		snap.Accounts[id] = as
	}

	for n, r := range b.roles {
		snap.Roles[n] = roleSnap{Role: marshalPB(r.pb), DeletedAt: r.deletedAt}
	}

	return persistence.MarshalSnapshot(ctx, "gcpiam", snap)
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpiam", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpiam: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		b.resetMaps()

		return nil
	}

	fresh := &InMemoryBackend{}
	fresh.resetMaps()

	if err := fresh.decode(snap); err != nil {
		return err
	}

	b.accounts, b.roles = fresh.accounts, fresh.roles
	b.etagSeq = max(b.etagSeq, snap.EtagSeq)

	return nil
}

func (b *InMemoryBackend) decode(snap backendSnapshot) error {
	for id, as := range snap.Accounts {
		rec, err := as.rec(id)
		if err != nil {
			return err
		}

		b.accounts[id] = rec
	}

	for n, rs := range snap.Roles {
		pb := &adminpb.Role{}
		if err := unmarshalPB(rs.Role, pb); err != nil {
			return fmt.Errorf("gcpiam: restore role %q: %w", n, err)
		}

		b.roles[n] = &roleRec{pb: pb, deletedAt: rs.DeletedAt}
	}

	return nil
}

func (as accountSnap) rec(id string) (*accountRec, error) {
	pb := &adminpb.ServiceAccount{}
	if err := unmarshalPB(as.Account, pb); err != nil {
		return nil, fmt.Errorf("gcpiam: restore service account %q: %w", id, err)
	}

	rec := &accountRec{pb: pb, deletedAt: as.DeletedAt, keys: make(map[string]*keyRec, len(as.Keys))}

	for kid, ks := range as.Keys {
		k, err := ks.rec()
		if err != nil {
			return nil, fmt.Errorf("gcpiam: restore key %q: %w", kid, err)
		}

		rec.keys[kid] = k
	}

	if as.System != nil {
		k, err := as.System.rec()
		if err != nil {
			return nil, fmt.Errorf("gcpiam: restore system key of %q: %w", id, err)
		}

		rec.system = k
	}

	if len(as.Policy) > 0 {
		rec.policy = &iampb.Policy{}
		if err := unmarshalPB(as.Policy, rec.policy); err != nil {
			return nil, fmt.Errorf("gcpiam: restore policy of %q: %w", id, err)
		}
	}

	return rec, nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpiam: restore snapshot: %w", err)
	}

	return nil
}
