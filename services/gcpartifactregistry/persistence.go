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
	Meta         map[string]*resMeta        `json:"meta,omitempty"`
	GenericPkgs  map[string]*resMeta        `json:"genericPackages,omitempty"`
	GenericVers  map[string]*resMeta        `json:"genericVersions,omitempty"`
	GenericFiles map[string]*genFile        `json:"genericFiles,omitempty"`
	Rules        map[string]json.RawMessage `json:"rules,omitempty"`
	Settings     map[string]json.RawMessage `json:"projectSettings,omitempty"`
	VPCSC        map[string]json.RawMessage `json:"vpcscConfigs,omitempty"`
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

// Snapshot serialises repositories, IAM policies, the ledger, generic artifacts, rules, settings and
// operations. Image blobs and manifests are not part of it: they live under the data directory when one is configured.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version: snapshotVersion, Ledger: b.ledger, Operations: b.ops.Snapshot(),
		Repositories: make(map[string]repoSnap, len(b.repos)),
		Meta:         b.meta, GenericPkgs: b.genPkgs, GenericVers: b.genVers, GenericFiles: b.genFiles,
		Rules: marshalAll(b.rules), Settings: marshalAll(b.settings), VPCSC: marshalAll(b.vpcsc),
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
		b.resetExtras()
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

	if err = b.restoreExtras(&snap); err != nil {
		return err
	}

	b.repos, b.ledger = repos, snap.Ledger
	b.release(false)

	return nil
}

func marshalAll[T proto.Message](in map[string]T) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(in))
	for k, v := range in {
		out[k] = marshalPB(v)
	}

	return out
}

func unmarshalAll[T proto.Message](in map[string]json.RawMessage, newMsg func() T) (map[string]T, error) {
	out := make(map[string]T, len(in))

	for k, raw := range in {
		m := newMsg()
		if err := unmarshalPB(raw, m); err != nil {
			return nil, fmt.Errorf("gcpartifactregistry: restore %q: %w", k, err)
		}

		out[k] = m
	}

	return out, nil
}

func orEmpty[V any](m map[string]*V) map[string]*V {
	if m == nil {
		return map[string]*V{}
	}

	return m
}

func (b *InMemoryBackend) restoreExtras(snap *backendSnapshot) error {
	rules, err := unmarshalAll(snap.Rules, func() *artifactregistrypb.Rule { return &artifactregistrypb.Rule{} })
	if err != nil {
		return err
	}

	settings, err := unmarshalAll(snap.Settings, func() *artifactregistrypb.ProjectSettings {
		return &artifactregistrypb.ProjectSettings{}
	})
	if err != nil {
		return err
	}

	vpcsc, err := unmarshalAll(snap.VPCSC, func() *artifactregistrypb.VPCSCConfig {
		return &artifactregistrypb.VPCSCConfig{}
	})
	if err != nil {
		return err
	}

	b.meta, b.genPkgs, b.genVers = orEmpty(snap.Meta), orEmpty(snap.GenericPkgs), orEmpty(snap.GenericVers)
	b.genFiles, b.rules, b.settings, b.vpcsc = orEmpty(snap.GenericFiles), rules, settings, vpcsc

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
