package gcpartifactregistry

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type repoSnap struct {
	Repository json.RawMessage `json:"repository"`
	IAM        json.RawMessage `json:"iam,omitempty"`
}

type backendSnapshot struct {
	Repositories map[string]repoSnap        `json:"repositories"`
	Ledger       ociregistry.Ledger         `json:"ledger"`
	Operations   map[string]json.RawMessage `json:"operations"`
	Version      int                        `json:"version"`
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

// Snapshot serialises repositories, IAM policies, the image ledger and operations. Image blobs and manifests are
// not part of it: they live under the data directory when one is configured.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version: snapshotVersion, Ledger: b.ledger, Operations: b.ops.Snapshot(),
		Repositories: make(map[string]repoSnap, len(b.repos)),
	}

	for name, rec := range b.repos {
		rs := repoSnap{Repository: marshalPB(rec.pb)}
		if rec.iam != nil {
			rs.IAM = marshalPB(rec.iam)
		}

		snap.Repositories[name] = rs
	}

	return persistence.MarshalSnapshot(ctx, "gcpartifactregistry", snap)
}

// Restore loads a snapshot, replacing every repository.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpartifactregistry", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).
			WarnContext(ctx, "gcpartifactregistry: discarding incompatible snapshot version, starting empty",
				"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.repos, b.ledger = map[string]*repoRec{}, ociregistry.Ledger{}
		b.ops.Reset()

		return nil
	}

	repos, err := restoreRepos(snap.Repositories)
	if err != nil {
		return err
	}

	if snap.Ledger == nil {
		snap.Ledger = ociregistry.Ledger{}
	}

	if err = snap.Ledger.Normalize(); err != nil {
		return fmt.Errorf("gcpartifactregistry: restore ledger: %w", err)
	}

	if err = b.ops.Restore(snap.Operations); err != nil {
		return fmt.Errorf("gcpartifactregistry: restore operations: %w", err)
	}

	b.repos, b.ledger = repos, snap.Ledger
	b.release(false)

	return nil
}

func restoreRepos(in map[string]repoSnap) (map[string]*repoRec, error) {
	out := make(map[string]*repoRec, len(in))

	for name, rs := range in {
		pb := &artifactregistrypb.Repository{}
		if err := unmarshalPB(rs.Repository, pb); err != nil {
			return nil, fmt.Errorf("gcpartifactregistry: restore repository %q: %w", name, err)
		}

		rec := &repoRec{pb: pb}

		if len(rs.IAM) > 0 {
			rec.iam = &iampb.Policy{}
			if err := unmarshalPB(rs.IAM, rec.iam); err != nil {
				return nil, fmt.Errorf("gcpartifactregistry: restore policy of %q: %w", name, err)
			}
		}

		out[name] = rec
	}

	return out, nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpartifactregistry: restore snapshot: %w", err)
	}

	return nil
}
